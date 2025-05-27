#!/usr/bin/env python3
import sys
# Add the current directory to the path to allow module imports
sys.path.insert(0, '.')
"""
Video Resizer Pre-Warmer and Optimizer

This tool helps with:
1. Pre-warming Cloudflare KV cache for video transformations
2. Analyzing file sizes in remote storage
3. Optimizing large video files using FFmpeg for better performance
"""

import os
import sys
import json
import time
import logging
import threading
import concurrent.futures
import signal
import math
import shutil
from threading import Lock
import numpy as np
from datetime import datetime

# Import modules
from modules import config
from modules import storage
from modules import video_utils
from modules import encoding
from modules import processing
from modules import reporting
from modules import comparison
from modules import load_testing
from modules import validation
from modules.stats import StreamingStats, SizeReductionStats

# Global state
running = True
processed_objects = {}
failed_objects = {}
errored_objects = {}
shutdown_event = threading.Event()  # Event for signaling orderly shutdown
executor = None  # Main thread pool executor

# Global locks
derivatives_lock = Lock()
errors_lock = Lock()
results_lock = Lock()
processed_locks = {}
failed_locks = {}
stats_lock = Lock()

def signal_handler(sig, frame):
    """Handle interrupt signals gracefully."""
    global running, shutdown_event
    logger.info("Interrupt received, stopping after current tasks complete...")
    running = False
    # Set shutdown event if it exists
    if 'shutdown_event' in globals() and shutdown_event:
        shutdown_event.set()

def cleanup_resources():
    """Clean up resources before exiting."""
    global executor, logger
    
    # Clean up the main executor if it exists
    if executor and not executor._shutdown:
        if logger:
            logger.info("Shutting down thread pool executor...")
        executor.shutdown(wait=False)
        if logger:
            logger.debug("Thread pool executor shutdown complete")

def initialize_stats(derivatives):
    """Initialize statistics dictionary."""
    stats = {
        'total_processed': 0,
        'success_count': 0,
        'error_count': 0,
        'total_size_bytes': 0,
        'success_bytes': 0,
        'error_bytes': 0,
        'ttfb_stats': StreamingStats('ttfb'),
        'total_time_stats': StreamingStats('total_time'),
        'size_reduction_stats': SizeReductionStats(),
        'errors_by_type': {},
        'by_size_category': {
            'small': {
                'count': 0,
                'size_bytes': 0,
                'ttfb_stats': StreamingStats('small_ttfb'),
                'total_time_stats': StreamingStats('small_total_time'),
            },
            'medium': {
                'count': 0,
                'size_bytes': 0,
                'ttfb_stats': StreamingStats('medium_ttfb'),
                'total_time_stats': StreamingStats('medium_total_time'),
            },
            'large': {
                'count': 0,
                'size_bytes': 0,
                'ttfb_stats': StreamingStats('large_ttfb'),
                'total_time_stats': StreamingStats('large_total_time'),
            }
        }
    }
    
    # Add derivative-specific stats if derivatives are used
    if derivatives:
        for derivative in derivatives:
            stats[f'{derivative}_success'] = 0
            stats[f'{derivative}_errors'] = 0
    else:
        # Add a default stat counter for non-derivative mode
        stats['default_success'] = 0
        stats['default_errors'] = 0
    
    return stats

def allocate_workers(args, file_sizes):
    """
    Allocate workers to different file size categories based on configuration.
    
    Args:
        args: Command line arguments
        file_sizes: Dictionary of file counts by size category
        
    Returns:
        Dictionary of worker allocations by size category
    """
    # Count files by size category
    small_count = file_sizes.get('small', 0)
    medium_count = file_sizes.get('medium', 0)
    large_count = file_sizes.get('large', 0)
    total_count = small_count + medium_count + large_count
    
    # If no file counts, use default allocation
    if total_count == 0:
        return {
            'small': max(1, args.workers // 2),
            'medium': max(1, args.workers // 4),
            'large': max(1, args.workers // 4)
        }
    
    # If custom worker counts specified, use those
    if args.small_file_workers > 0 and args.medium_file_workers > 0 and args.large_file_workers > 0:
        return {
            'small': args.small_file_workers,
            'medium': args.medium_file_workers,
            'large': args.large_file_workers
        }
    
    # Auto-calculate based on file distribution
    total_workers = args.workers
    
    # Calculate proportion of files in each category
    small_prop = small_count / total_count if total_count > 0 else 0.5
    medium_prop = medium_count / total_count if total_count > 0 else 0.3
    large_prop = large_count / total_count if total_count > 0 else 0.2
    
    # Adjust proportions for optimal processing
    # Small files can be processed faster, so allocate more workers
    # Large files benefit from dedicated workers
    if args.optimize_by_size:
        # Weight small files higher for parallelism
        small_weight = 1.2
        medium_weight = 1.0
        large_weight = 0.8
        
        # Calculate weighted proportions
        total_weight = (small_prop * small_weight + 
                        medium_prop * medium_weight + 
                        large_prop * large_weight)
        
        small_prop = (small_prop * small_weight) / total_weight if total_weight > 0 else 0.5
        medium_prop = (medium_prop * medium_weight) / total_weight if total_weight > 0 else 0.3
        large_prop = (large_prop * large_weight) / total_weight if total_weight > 0 else 0.2
    
    # Allocate workers based on weighted proportions
    small_workers = max(1, int(total_workers * small_prop))
    medium_workers = max(1, int(total_workers * medium_prop))
    large_workers = max(1, total_workers - small_workers - medium_workers)
    
    logger.info(f"Worker allocation: Small={small_workers}, Medium={medium_workers}, Large={large_workers}")
    
    return {
        'small': small_workers,
        'medium': medium_workers,
        'large': large_workers
    }

def process_objects(objects, args, derivatives, stats):
    """
    Process a list of objects with derivatives using multiple threads.
    
    Args:
        objects: List of file metadata objects
        args: Command line arguments
        derivatives: List of derivatives to process (can be empty)
        stats: Statistics dictionary
        
    Returns:
        Updated results dictionary
    """
    global running, shutdown_event, processed_objects, failed_objects, executor
    
    # Check if we're only doing optimization without processing
    if args.optimize_in_place and not args.base_url:
        # Skip processing step for in-place optimization only mode
        return {}
    
    # Group objects by size category for optimized processing
    objects_by_category = {
        'small': [],
        'medium': [],
        'large': []
    }
    
    for obj in objects:
        objects_by_category[obj.size_category].append(obj)
    
    # Count files by category
    file_counts = {
        category: len(objects)
        for category, objects in objects_by_category.items()
    }
    
    # Determine worker allocation
    if args.optimize_by_size:
        worker_allocation = allocate_workers(args, file_counts)
        logger.info(f"Using optimized worker allocation: {worker_allocation}")
    else:
        # Use equal allocation
        workers_per_category = max(1, args.workers // 3)
        worker_allocation = {
            'small': workers_per_category,
            'medium': workers_per_category,
            'large': max(1, args.workers - (2 * workers_per_category))
        }
        logger.info(f"Using equal worker allocation: {worker_allocation}")
    
    # Process each size category with appropriate worker count
    results = {}
    
    # Verify base_url is provided if we're doing media processing
    if not args.base_url and not (args.optimize_videos or args.optimize_in_place or args.list_files):
        logger.error("No base_url provided. Skipping media processing.")
        return results
    
    # Process different size categories (in order of processing speed)
    categories = ['small', 'medium', 'large']
    for category in categories:
        category_objects = objects_by_category[category]
        if not category_objects:
            logger.info(f"No {category} files to process.")
            continue
        
        logger.info(f"Processing {len(category_objects)} {category} files with {worker_allocation[category]} workers.")
        
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_allocation[category])
        try:
            # Submit tasks
            future_to_obj = {}
            
            for obj in category_objects:
                if not running or shutdown_event.is_set():
                    logger.info("Stopping submission of new tasks...")
                    break
                
                # Skip processing if we're only doing optimization
                if args.optimize_in_place and not args.base_url:
                    continue
                
                # If derivatives is empty, process the file without specifying derivatives
                if not derivatives:
                    future = executor.submit(
                        processing.process_object_without_derivatives,
                        obj,
                        args.base_url,
                        args.bucket,
                        args.directory,
                        args.timeout,
                        args.retry,
                        args.connection_close_delay,
                        logger,
                        args.small_file_threshold,
                        args.medium_file_threshold,
                        args.use_head_for_size
                    )
                else:
                    future = executor.submit(
                        processing.process_object,
                        obj,
                        args.base_url,
                        args.bucket,
                        args.directory,
                        derivatives,
                        args.timeout,
                        args.retry,
                        args.connection_close_delay,
                        logger,
                        args.small_file_threshold,
                        args.medium_file_threshold,
                        args.use_head_for_size
                    )
                
                future_to_obj[future] = obj
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_obj):
                if shutdown_event.is_set():
                    # Handle orderly shutdown
                    logger.info("Shutdown event detected. Processing results, then stopping...")
                    break
                    
                obj = future_to_obj[future]
                
                try:
                    obj_results = future.result()
                    
                    # Log completion
                    logger.info(f"Completed {obj.path} ({video_utils.format_file_size(obj.size_bytes)})")
                    
                    # Store results
                    with results_lock:
                        results[obj.path] = {
                            'path': obj.path,
                            'size_bytes': obj.size_bytes,
                            'size_category': obj.size_category,
                            'derivatives': obj_results,
                            'processing_time': obj.processing_duration
                        }
                    
                    # Track processed object
                    with derivatives_lock:
                        processed_objects[obj.path] = obj.to_dict()
                    
                    # Update stats
                    processing.update_processing_stats(stats, obj, obj_results, derivatives)
                    
                except Exception as e:
                    logger.error(f"Error processing {obj.path}: {str(e)}")
                    
                    with errors_lock:
                        errored_objects[obj.path] = {
                            'path': obj.path,
                            'size_bytes': obj.size_bytes,
                            'error': str(e)
                        }
        finally:
            # Always shut down the executor when done
            if executor and not executor._shutdown:
                executor.shutdown(wait=True)
    
    return results

def optimize_video_files(objects, args):
    """
    Optimize large video files using FFmpeg.
    
    Args:
        objects: List of file metadata objects
        args: Command line arguments
        
    Returns:
        List of optimization results
    """
    global executor, running, shutdown_event
    
    logger.info(f"Starting video optimization for {len(objects)} files...")
    
    # Create output directory
    os.makedirs(args.optimized_videos_dir, exist_ok=True)
    
    # Create work directory for downloads
    work_dir = os.path.join(args.optimized_videos_dir, "_downloads")
    os.makedirs(work_dir, exist_ok=True)
    
    # Configure optimization options
    codec = args.codec
    
    # If browser compatibility is enabled and output format is mp4, force h264 codec
    if args.browser_compatible and args.output_format == 'mp4' and codec != 'h264':
        logger.warning(f"Forcing H.264 codec for MP4 format for browser compatibility. Overriding codec: {codec} → h264")
        logger.warning(f"To use {codec} codec with MP4, add --browser-compatible=False to your command")
        codec = 'h264'
    
    optimization_options = {
        "codec": codec,
        "quality_profile": args.quality,
        "resolution": args.target_resolution,
        "fit_mode": args.fit,
        "audio_profile": args.audio_profile,
        "output_format": args.output_format,
        "create_webm": args.create_webm,
        "hardware_acceleration": args.hardware_acceleration,
        "disable_hardware_acceleration": args.disable_hardware_acceleration
    }
    
    # Start optimization with parallel workers
    results = []
    total_original_size = 0
    total_new_size = 0
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    try:
        # Submit tasks
        future_to_obj = {}
        
        for obj in objects:
            if not running or shutdown_event.is_set():
                logger.info("Stopping submission of new tasks...")
                break
                
            # Create paths
            remote_path = f"{args.remote}:{args.bucket}/{args.directory}/{obj.path}"
            download_path = os.path.join(work_dir, os.path.basename(obj.path))
            output_name = f"optimized_{os.path.basename(obj.path)}"
            if args.output_format != 'mp4' and not output_name.endswith(f".{args.output_format}"):
                output_name = f"{os.path.splitext(output_name)[0]}.{args.output_format}"
            output_path = os.path.join(args.optimized_videos_dir, output_name)
            
            # Submit optimization task
            logger.info(f"Submitting {obj.path} ({video_utils.format_file_size(obj.size_bytes)})")
            
            future = executor.submit(
                lambda r, d, o, opt: storage.download_from_rclone(r, d) and encoding.optimize_video(d, o, opt),
                remote_path, 
                download_path,
                output_path,
                optimization_options
            )
            
            future_to_obj[future] = obj
        
        # Process results
        for future in concurrent.futures.as_completed(future_to_obj):
            if shutdown_event.is_set():
                logger.info("Shutdown event detected. Processing results, then stopping...")
                break
                
            obj = future_to_obj[future]
            
            try:
                result = future.result()
                if result:
                    results.append(result)
                    total_original_size += result["original_size"]
                    total_new_size += result["new_size"]
                    
                    logger.info(f"Completed {obj.path}: "
                            f"{video_utils.format_file_size(obj.size_bytes)} → {video_utils.format_file_size(result['new_size'])} "
                            f"({result['reduction_percent']:.1f}% reduction)")
            except Exception as e:
                logger.error(f"Error optimizing {obj.path}: {str(e)}")
    finally:
        # Always shut down the executor when done
        if executor and not executor._shutdown:
            executor.shutdown(wait=True)
    
    # Generate final report
    if results:
        total_reduction = total_original_size - total_new_size
        percent_reduction = (total_reduction / total_original_size) * 100 if total_original_size > 0 else 0
        
        # Overall stats
        report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "stats": {
                "files_processed": len(results),
                "total_original_size": total_original_size,
                "total_original_size_formatted": video_utils.format_file_size(total_original_size),
                "total_new_size": total_new_size,
                "total_new_size_formatted": video_utils.format_file_size(total_new_size),
                "total_reduction_bytes": total_reduction,
                "total_reduction_bytes_formatted": video_utils.format_file_size(total_reduction),
                "percent_reduction": percent_reduction
            },
            "files": results
        }
        
        # Save report
        report_path = os.path.join(args.optimized_videos_dir, "optimization_report.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Generate markdown report
        md_report = reporting.generate_optimization_report(results)
        md_report_path = os.path.join(args.optimized_videos_dir, "optimization_report.md")
        with open(md_report_path, 'w') as f:
            f.write(md_report)
        
        logger.info(f"Successfully processed {len(results)} files")
        logger.info(f"Total size reduction: {video_utils.format_file_size(total_reduction)} "
                   f"({percent_reduction:.1f}%)")
        logger.info(f"Reports saved to {report_path} and {md_report_path}")
    
    return results

def optimize_and_replace_in_place(objects, args):
    """
    Optimize large video files using FFmpeg and replace them in-place in the remote storage.
    
    Args:
        objects: List of file metadata objects
        args: Command line arguments
        
    Returns:
        List of optimization results
    """
    global executor, running, shutdown_event
    
    # Apply size threshold filter if specified
    size_threshold_bytes = args.size_threshold * 1024 * 1024
    filtered_objects = [obj for obj in objects if obj.size_bytes >= size_threshold_bytes]
    
    if not filtered_objects:
        logger.info(f"No files found above the size threshold of {args.size_threshold} MiB")
        return []
    
    logger.info(f"Starting in-place video optimization for {len(filtered_objects)} files above {args.size_threshold} MiB...")
    
    # Create temporary work directory
    temp_dir = os.path.join(os.path.expanduser("~"), ".video_optimizer_temp")
    os.makedirs(temp_dir, exist_ok=True)
    
    # Create work directory for downloads
    download_dir = os.path.join(temp_dir, "downloads")
    os.makedirs(download_dir, exist_ok=True)
    
    # Create work directory for optimized files
    optimized_dir = os.path.join(temp_dir, "optimized")
    os.makedirs(optimized_dir, exist_ok=True)
    
    # Configure optimization options
    codec = args.codec
    
    # If browser compatibility is enabled and output format is mp4, force h264 codec
    if args.browser_compatible and args.output_format == 'mp4' and codec != 'h264':
        logger.warning(f"Forcing H.264 codec for MP4 format for browser compatibility. Overriding codec: {codec} → h264")
        logger.warning(f"To use {codec} codec with MP4, add --browser-compatible=False to your command")
        logger.warning(f"Note: H.265/HEVC videos played directly from R2 storage may not work in all browsers")
        codec = 'h264'
    
    optimization_options = {
        "codec": codec,
        "quality_profile": args.quality,
        "resolution": args.target_resolution,
        "fit_mode": args.fit,
        "audio_profile": args.audio_profile,
        "output_format": args.output_format,
        "create_webm": False,  # No WebM for in-place replacement
        "hardware_acceleration": args.hardware_acceleration,
        "disable_hardware_acceleration": args.disable_hardware_acceleration
    }
    
    # Start optimization with parallel workers
    results = []
    total_original_size = 0
    total_new_size = 0
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    try:
        # Submit tasks
        future_to_obj = {}
        
        for obj in filtered_objects:
            if not running or shutdown_event.is_set():
                logger.info("Stopping submission of new tasks...")
                break
                
            # Create paths
            filename = os.path.basename(obj.path)
            remote_path = f"{args.remote}:{args.bucket}/{args.directory}/{obj.path}"
            download_path = os.path.join(download_dir, filename)
            optimized_path = os.path.join(optimized_dir, filename)
            
            # Submit optimization and replacement task
            logger.info(f"Submitting {obj.path} ({video_utils.format_file_size(obj.size_bytes)}) for in-place optimization")
            
            def process_and_replace(remote_path, download_path, optimized_path, opt_options, obj_path):
                # Download the file
                if not storage.download_from_rclone(remote_path, download_path):
                    logger.error(f"Failed to download {remote_path}")
                    return None
                
                # Optimize the video
                result = encoding.optimize_video(download_path, optimized_path, opt_options)
                if not result:
                    logger.error(f"Failed to optimize {download_path}")
                    return None
                
                # Replace the file in-place
                if storage.replace_file_in_place(args.remote, args.bucket, args.directory, obj_path, optimized_path):
                    result["replaced_in_place"] = True
                    logger.info(f"Successfully replaced {obj_path} in-place")
                else:
                    result["replaced_in_place"] = False
                    logger.error(f"Failed to replace {obj_path} in-place")
                
                return result
            
            future = executor.submit(
                process_and_replace,
                remote_path,
                download_path,
                optimized_path,
                optimization_options,
                obj.path
            )
            
            future_to_obj[future] = obj
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_obj):
            if shutdown_event.is_set():
                logger.info("Shutdown event detected. Processing results, then stopping...")
                break
                
            obj = future_to_obj[future]
            
            try:
                result = future.result()
                if result:
                    results.append(result)
                    total_original_size += result["original_size"]
                    total_new_size += result["new_size"]
                    
                    replace_status = "✅ Replaced in-place" if result.get("replaced_in_place", False) else "❌ Not replaced"
                    logger.info(f"Completed {obj.path}: "
                            f"{video_utils.format_file_size(obj.size_bytes)} → {video_utils.format_file_size(result['new_size'])} "
                            f"({result['reduction_percent']:.1f}% reduction) - {replace_status}")
                    
                    # Clean up temporary files
                    download_path = os.path.join(download_dir, os.path.basename(obj.path))
                    optimized_path = os.path.join(optimized_dir, os.path.basename(obj.path))
                    
                    if os.path.exists(download_path):
                        os.remove(download_path)
                    if os.path.exists(optimized_path):
                        os.remove(optimized_path)
            except Exception as e:
                logger.error(f"Error processing {obj.path}: {str(e)}")
    finally:
        # Always shut down the executor when done
        if executor and not executor._shutdown:
            executor.shutdown(wait=True)
    
    # Generate final report
    if results:
        total_reduction = total_original_size - total_new_size
        percent_reduction = (total_reduction / total_original_size) * 100 if total_original_size > 0 else 0
        
        # Count how many files were successfully replaced
        replaced_count = sum(1 for r in results if r.get("replaced_in_place", False))
        
        # Overall stats
        report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "stats": {
                "files_processed": len(results),
                "files_replaced": replaced_count,
                "total_original_size": total_original_size,
                "total_original_size_formatted": video_utils.format_file_size(total_original_size),
                "total_new_size": total_new_size,
                "total_new_size_formatted": video_utils.format_file_size(total_new_size),
                "total_reduction_bytes": total_reduction,
                "total_reduction_bytes_formatted": video_utils.format_file_size(total_reduction),
                "percent_reduction": percent_reduction
            },
            "files": results
        }
        
        # Save report
        os.makedirs("optimization_reports", exist_ok=True)
        report_path = os.path.join("optimization_reports", f"in_place_optimization_report_{time.strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Generate markdown report
        md_report = reporting.generate_optimization_report(results, include_replacement_status=True)
        md_report_path = os.path.join("optimization_reports", f"in_place_optimization_report_{time.strftime('%Y%m%d_%H%M%S')}.md")
        with open(md_report_path, 'w') as f:
            f.write(md_report)
        
        logger.info(f"Successfully processed {len(results)} files, replaced {replaced_count} files in-place")
        logger.info(f"Total size reduction: {video_utils.format_file_size(total_reduction)} "
                  f"({percent_reduction:.1f}%)")
        logger.info(f"Reports saved to {report_path} and {md_report_path}")
    
    # Clean up temporary directories
    try:
        shutil.rmtree(temp_dir)
        logger.info(f"Cleaned up temporary directory: {temp_dir}")
    except Exception as e:
        logger.warning(f"Failed to clean up temporary directory {temp_dir}: {str(e)}")
    
    return results

def main():
    """Main entry point for the application."""
    # Parse arguments
    args = config.parse_arguments()
    
    # Set up logging
    global logger
    logger = config.setup_logging(args.verbose)
    
    # Install signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check if we need to run the pre-warming process
    run_prewarming = True
    
    # Skip pre-warming if only error report is requested and results file exists
    if args.generate_error_report and not args.run_load_test and os.path.exists(args.output):
        run_prewarming = False
        logger.info(f"Using existing results file: {args.output}")
    
    # Skip pre-warming if only load test is requested and results file exists
    if args.run_load_test and not args.generate_error_report and os.path.exists(args.output):
        run_prewarming = False
        logger.info(f"Using existing results file: {args.output}")
    
    # Results data placeholder
    results_data = None
    
    # If generating error report or running load test, but file doesn't exist, we need to run pre-warming
    if (args.generate_error_report or args.run_load_test) and not os.path.exists(args.output):
        logger.info(f"Results file {args.output} not found. Will run pre-warming process.")
        run_prewarming = True
        
        # Check if required parameters for pre-warming are provided
        if not args.remote or not args.bucket or not args.base_url:
            logger.error("Remote, bucket, and base-url are required for pre-warming.")
            logger.error("Please provide --remote, --bucket, and --base-url parameters.")
            return 1
            
    # Force run pre-warming if explicitly requested, even if file exists
    if args.force_prewarm:
        logger.info(f"Force pre-warming requested. Will run pre-warming process regardless of existing files.")
        run_prewarming = True
    
    # Only process derivatives and initialize stats if we're running pre-warming
    if run_prewarming:
        # Process derivatives if they are specified in the command line
        # If --derivatives is provided at all (even with default value), use them
        derivatives = args.derivatives.split(',')
        logger.info(f"Processing derivatives: {derivatives}")
        
        # Initialize stats
        stats = initialize_stats(derivatives)
    else:
        derivatives = args.derivatives.split(',')
        stats = None
    
    # Handle full workflow option, which enables all steps
    if args.full_workflow:
        logger.info("Running full workflow: pre-warming → error report → load test")
        args.generate_error_report = True
        args.run_load_test = True
        args.use_error_report_for_load_test = True
        args.format = 'json'  # Force JSON format for error reports in full workflow
        run_prewarming = True

    try:
        # If we need to run the pre-warming process
        if run_prewarming:
            # Start processing timer
            start_time = time.time()
            
            # List files from remote storage with sizes
            logger.info(f"Listing files from {args.remote}:{args.bucket}/{args.directory}")
            
            # Use list_objects instead of list_large_files to get all files without size filtering
            file_list = storage.list_objects(
                args.remote,
                args.bucket,
                args.directory,
                args.extension,
                args.limit,
                logger,
                args.use_aws_cli,
                get_sizes=True
            )
        
            # Convert to the expected format (list of tuples) if needed
            if file_list and isinstance(file_list[0], dict):
                file_list = [(item['path'], item['size']) for item in file_list]
            
            if not file_list:
                logger.error("No files found matching criteria")
                return 1
            
            logger.info(f"Found {len(file_list)} files matching criteria")
            
            # Create file metadata objects
            objects = []
            file_sizes_by_category = {
                'small': 0,
                'medium': 0,
                'large': 0
            }
            
            for file_path, size_bytes in file_list:
                obj = config.FileMetadata(
                    file_path, 
                    size_bytes, 
                    args.small_file_threshold, 
                    args.medium_file_threshold
                )
                objects.append(obj)
                file_sizes_by_category[obj.size_category] += 1
            
            # Print file size distribution
            logger.info(f"File size distribution:")
            for category, count in file_sizes_by_category.items():
                logger.info(f"  {category.capitalize()}: {count} files")
                
            # Report on large files separately for reference (using size threshold)
            size_threshold_bytes = args.size_threshold * 1024 * 1024
            large_files_count = sum(1 for obj in objects if obj.size_bytes >= size_threshold_bytes)
            logger.info(f"Found {large_files_count} files above {args.size_threshold} MiB (out of {len(objects)} total files)")
            
            # If only listing files, generate report and exit
            if args.list_files:
                logger.info(f"Generating file size report to {args.size_report_output}")
                report = reporting.generate_size_report(file_list, args.size_threshold)
                with open(args.size_report_output, 'w') as f:
                    f.write(report)
                logger.info(f"File size report saved to {args.size_report_output}")
                return 0
            
            # Track progress for statistics
            total_objects = len(objects)
            processed = 0
            results = {}  # Initialize results dictionary
            
            # Process files for pre-warming
            if not args.only_compare and not args.optimize_videos:
                logger.info(f"Processing {len(objects)} files with {args.workers} workers")
                
                # Process the objects and collect results
                results = process_objects(objects, args, derivatives, stats)
                processed = len(results)
                
                # Calculate processing time
                processing_time = time.time() - start_time
                
                # Convert streaming stats to output format
                stats_for_output = stats.copy()
                
                # Replace StreamingStats objects with their dictionary representations
                stats_for_output['ttfb_summary'] = stats['ttfb_stats'].to_dict()
                stats_for_output['total_time_summary'] = stats['total_time_stats'].to_dict()
                stats_for_output['size_reduction_summary'] = stats['size_reduction_stats'].to_dict()
                
                # Remove the StreamingStats objects from output
                stats_for_output.pop('ttfb_stats', None)
                stats_for_output.pop('total_time_stats', None)
                stats_for_output.pop('size_reduction_stats', None)
                
                # Convert size category stats
                for category_name, category_data in stats_for_output.get('by_size_category', {}).items():
                    if 'ttfb_stats' in category_data:
                        category_data['ttfb_summary'] = category_data['ttfb_stats'].to_dict()
                        category_data.pop('ttfb_stats', None)
                    if 'total_time_stats' in category_data:
                        category_data['total_time_summary'] = category_data['total_time_stats'].to_dict()
                        category_data.pop('total_time_stats', None)
                    
                # Size reduction statistics are already calculated in real-time
                # during processing via update_processing_stats
            
            # Save processing results
            with open(args.output, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'processing_time': processing_time,
                    'parameters': {
                        'remote': args.remote,
                        'bucket': args.bucket,
                        'directory': args.directory,
                        'base_url': args.base_url,
                        'derivatives': derivatives,
                        'workers': args.workers,
                        'timeout': args.timeout,
                        'connection_close_delay': args.connection_close_delay,
                        'retry_attempts': args.retry,
                        'small_threshold_mib': args.small_file_threshold,
                        'medium_threshold_mib': args.medium_file_threshold
                    },
                    'stats': stats_for_output,
                    'results': results
                }, f, indent=2, default=str)
            
            logger.info(f"Results saved to {args.output}")
            
            # If processing was stopped early by user interrupt, save results
            if shutdown_event.is_set():
                # Save the current results before exit
                metadata = {
                    "remote": args.remote,
                    "bucket": args.bucket,
                    "directory": args.directory,
                    "base_url": args.base_url,
                    "processed": processed,
                    "total": total_objects,
                    "derivatives_requested": derivatives,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "elapsed_seconds": time.time() - start_time,
                    "early_shutdown": True,
                }
                
                logger.info(f"Saving results before shutdown ({processed}/{total_objects} objects processed)")
                reporting.write_results_to_file(results, metadata, args.output, processed, total_objects, logger)
                logger.info(f"Results saved to {args.output}")
                
                # Clean up resources
                cleanup_resources()
                
                # Calculate statistics for early shutdown case
                elapsed_time = time.time() - start_time
                logger.info(f"Processing stopped early: {processed} of {total_objects} objects in {elapsed_time:.2f} seconds")
                reporting.print_summary_statistics(results, elapsed_time, total_objects, logger)
                return 0
            
            # Generate performance report
            report = reporting.generate_stats_report(stats, args.summary_format)
            with open(args.performance_report, 'w') as f:
                f.write(report)
            
            logger.info(f"Performance report saved to {args.performance_report}")
            
            # Print final summary statistics
            elapsed_time = time.time() - start_time
            reporting.print_summary_statistics(results, elapsed_time, total_objects, logger)
        
        # Optimize video files if requested
        if args.optimize_videos:
            logger.info(f"Starting video optimization...")
            optimize_video_files(objects, args)
        
        # Optimize and replace video files in-place if requested
        if args.optimize_in_place:
            logger.info(f"Starting in-place video optimization...")
            optimize_and_replace_in_place(objects, args)
            
        # Compare results with KV data if requested
        if args.compare and os.path.exists(args.compare):
            logger.info(f"Comparing results with KV data from {args.compare}")
            
            # Load KV data
            kv_data = comparison.load_cloudflare_kv_data(args.compare, logger)
            
            # Load our results if we didn't just generate them
            if args.only_compare and os.path.exists(args.output):
                our_data = comparison.load_transform_results(args.output, logger)
                if our_data:
                    results = our_data.get('results', {})
            
            # Perform comparison
            if kv_data and (not args.only_compare or our_data):
                comp_results = comparison.compare_results_with_kv(
                    {'results': results},
                    kv_data,
                    logger
                )
                
                if comp_results:
                    # Save comparison results
                    with open(args.comparison_output, 'w') as f:
                        json.dump(comp_results, f, indent=2)
                    
                    # Generate summary report
                    report = comparison.generate_comparison_report(comp_results, args.summary_format)
                    with open(args.summary_output, 'w') as f:
                        f.write(report)
                    
                    logger.info(f"Comparison complete, results saved to {args.comparison_output}")
                    logger.info(f"Summary report saved to {args.summary_output}")
                else:
                    logger.error("Comparison failed - invalid data format")
            else:
                logger.error("Comparison failed - missing data")
        
        # Generate error report if requested or if we're running a load test that needs an error report
        if args.generate_error_report or (args.run_load_test and args.use_error_report_for_load_test):
            error_report_generated = False
            
            # Only generate if not already done during this run
            if results_data is not None or os.path.exists(args.output):
                logger.info(f"Generating error report from {'results data' if results_data is not None else args.output} to {args.error_report_output}")
                try:
                    # Load existing results file if not already loaded
                    if results_data is None:
                        try:
                            with open(args.output, 'r') as f:
                                results_data = json.load(f)
                        except Exception as e:
                            logger.error(f"Error loading results file {args.output}: {str(e)}")
                            return 1
                    
                    # Determine output format based on explicit option or file extension
                    if args.format:
                        format_type = args.format
                    else:
                        format_type = 'json' if args.error_report_output.endswith('.json') else 'markdown'
                    
                    # Adjust output file extension based on selected format
                    output_path = args.error_report_output
                    
                    # If format doesn't match file extension, adjust the file path
                    if format_type == 'json' and not output_path.endswith('.json'):
                        # Remove existing extension if present
                        if '.' in os.path.basename(output_path):
                            output_path = os.path.splitext(output_path)[0] + '.json'
                        else:
                            output_path = output_path + '.json'
                    elif format_type == 'markdown' and not (output_path.endswith('.md') or output_path.endswith('.markdown')):
                        # Remove existing extension if present
                        if '.' in os.path.basename(output_path):
                            output_path = os.path.splitext(output_path)[0] + '.md'
                        else:
                            output_path = output_path + '.md'
                    
                    # Generate error report
                    report = reporting.generate_error_report(results_data, format_type=format_type)
                    
                    # Save to output file
                    with open(output_path, 'w') as f:
                        f.write(report)
                        
                    logger.info(f"Error report saved to {output_path} (format: {format_type})")
                    error_report_generated = True
                except Exception as e:
                    logger.error(f"Error generating error report: {str(e)}", exc_info=True)
                    # Continue with the workflow even if error report generation fails
            else:
                logger.warning(f"Cannot generate error report: No results data available")
            
        # Run load test if requested
        if args.run_load_test:
            logger.info("========================================================")
            logger.info("STARTING K6 LOAD TEST")
            logger.info("========================================================")
            
            # Check if required parameters are present
            if not args.base_url:
                logger.error("Base URL (--base-url) is required for load testing")
                return 1
                
            if not os.path.exists(args.output):
                logger.error(f"Results file '{args.output}' not found. Run pre-warming first or specify a valid file with --output")
                return 1
                
            logger.info(f"Base URL: {args.base_url}")
            logger.info(f"URL Format: {args.url_format}")
            logger.info(f"Results file: {args.output}")
            
            # Check if error report was generated or exists
            error_report_file = None
            if args.use_error_report_for_load_test and os.path.exists(args.error_report_output):
                error_report_file = args.error_report_output
                logger.info(f"Using error report file: {error_report_file}")
            elif args.use_error_report_for_load_test and not os.path.exists(args.error_report_output):
                logger.warning(f"Error report file {args.error_report_output} not found, load test will run without error exclusions")
            
            logger.info(f"Skip large files: {args.skip_large_files} (threshold: {args.large_file_threshold_mib} MiB)")
            logger.info(f"Using HEAD requests: {args.use_head_requests}")
            logger.info(f"Debug mode: {args.debug_mode}")
            logger.info("========================================================")
            
            # Check if k6 script exists
            if not os.path.exists(args.k6_script):
                logger.error(f"k6 script '{args.k6_script}' not found")
                return 1
                
            # Check if k6 is installed
            k6_available, k6_version = load_testing.check_k6_installed()
            if not k6_available:
                logger.error("k6 is not installed or not in PATH. Please install k6 to run load tests.")
                return 1
            
            logger.info(f"Using k6 version: {k6_version}")
            
            # Configure stages
            stage_config = {
                "stage1": {"users": args.stage1_users, "duration": args.stage1_duration},
                "stage2": {"users": args.stage2_users, "duration": args.stage2_duration},
                "stage3": {"users": args.stage3_users, "duration": args.stage3_duration},
                "stage4": {"users": args.stage4_users, "duration": args.stage4_duration},
                "stage5": {"users": args.stage5_users, "duration": args.stage5_duration},
            }
            
            # Run the load test with clean output and aggregated metrics
            success, test_results = load_testing.run_k6_test(
                args.k6_script,
                args.base_url,
                args.output,
                error_report_file,
                args.url_format,
                stage_config,
                args.debug_mode,
                args.use_head_requests,
                args.skip_large_files,
                args.large_file_threshold_mib,
                args.request_timeout,
                args.global_timeout,
                args.head_timeout,
                args.failure_rate_threshold,
                args.max_retries,
                clean_output=True,      # Use clean output to avoid duplicates
                show_progress=True,     # Show progress indicators
                output_format="json",   # Export detailed metrics to JSON
                connection_close_delay=args.connection_close_delay
            )
            
            if success:
                logger.info("Load test completed successfully")
                
                # Parse results from k6 output if available
                if 'output' in test_results:
                    metrics = load_testing.parse_k6_results(test_results['output'])
                    logger.info("========================================================")
                    logger.info("LOAD TEST SUMMARY")
                    logger.info("========================================================")
                    
                    if 'summary' in metrics:
                        logger.info(f"Summary: {metrics['summary']}")
                    else:
                        for key, value in metrics.items():
                            if key == 'avg_duration_ms':
                                logger.info(f"Average Request Duration: {value:.2f}ms")
                            elif key == 'failure_rate':
                                logger.info(f"Failure Rate: {value*100:.2f}%")
                            elif key == 'checks_rate':
                                logger.info(f"Checks Passed: {value*100:.2f}%")
                            elif key == 'iterations':
                                logger.info(f"Total Requests: {value}")
                            elif key == 'peak_vus':
                                logger.info(f"Peak VUs: {value}")
                            else:
                                logger.info(f"{key.replace('_', ' ').title()}: {value}")
                    
                    # Show failure report info if available
                    if 'failure_report_json' in test_results and test_results['failure_report_json']:
                        try:
                            with open(test_results['failure_report_json'], 'r') as f:
                                failure_data = json.load(f)
                                
                                # Show overall test failure info
                                logger.info("\nFailure Information:")
                                logger.info(f"Total Failures: {failure_data.get('total_failures', 0)}")
                                
                                # Show top failure types
                                tracking = failure_data.get('failure_tracking', {})
                                if tracking and tracking.get('by_type'):
                                    logger.info("\nTop Failure Types:")
                                    for failure_type, count in sorted(tracking['by_type'].items(), 
                                                                  key=lambda x: x[1], reverse=True)[:3]:
                                        pct = (count / tracking['count']) * 100 if tracking['count'] > 0 else 0
                                        logger.info(f"  - {failure_type}: {count} ({pct:.1f}%)")
                                    
                                # Show path to detailed report
                                logger.info(f"\nDetailed failure report: {test_results['failure_report_json']}")
                                if 'failure_report_markdown' in test_results and test_results['failure_report_markdown']:
                                    logger.info(f"Failure summary (Markdown): {test_results['failure_report_markdown']}")
                        except Exception as e:
                            logger.debug(f"Error processing failure data: {str(e)}")
                    
                    logger.info("========================================================")
            else:
                logger.warning(f"Load test completed with issues: {test_results.get('error', 'Unknown error')}")
                # Don't return non-zero code here, as the pre-warming was successful
        
        # Video validation section
        if args.validate_videos:
            logger.info("========================================================")
            logger.info("VIDEO VALIDATION")
            logger.info("========================================================")
            
            validator = validation.VideoValidator(workers=args.validation_workers)
            validation_results = None
            
            # Option 1: Validate from results file
            if args.validate_results:
                logger.info(f"Validating videos from results file: {args.validate_results}")
                validation_results = validator.validate_from_results(
                    args.validate_results,
                    base_path=args.validate_directory or ""
                )
            
            # Option 2: Validate directory
            elif args.validate_directory:
                logger.info(f"Validating videos in directory: {args.validate_directory}")
                validation_results = validation.validate_directory(
                    args.validate_directory,
                    pattern=args.video_pattern,
                    workers=args.validation_workers
                )
            
            # Option 3: Validate from current pre-warming results
            elif run_prewarming and 'results' in locals() and results:
                logger.info("Validating videos from current pre-warming results")
                # Save current results to temp file for validation
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                    json.dump({'results': results}, tmp)
                    tmp_path = tmp.name
                
                validation_results = validator.validate_from_results(
                    tmp_path,
                    base_path=args.validate_directory or ""
                )
                
                # Clean up temp file
                try:
                    os.unlink(tmp_path)
                except:
                    pass
            
            else:
                logger.error("No source specified for validation. Use --validate-results, --validate-directory, or run pre-warming first.")
                return 1
            
            # Generate and save validation report
            if validation_results:
                report = validator.generate_report(output_format=args.validation_format)
                
                # Save report to file
                with open(args.validation_report, 'w') as f:
                    f.write(report)
                
                logger.info(f"Validation report saved to: {args.validation_report}")
                
                # Log summary
                total = validation_results['total_files']
                valid = validation_results['valid_files']
                corrupted = validation_results['corrupted_files']
                missing = validation_results['missing_files']
                
                logger.info(f"\nValidation Summary:")
                logger.info(f"  Total files checked: {total}")
                if total > 0:
                    logger.info(f"  Valid files: {valid} ({valid/total*100:.1f}%)")
                    logger.info(f"  Corrupted files: {corrupted} ({corrupted/total*100:.1f}%)")
                    logger.info(f"  Missing files: {missing} ({missing/total*100:.1f}%)")
                
                # Return non-zero if corruption detected
                if corrupted > 0:
                    logger.warning(f"Found {corrupted} corrupted video files")
                    # Note: Not returning error code as validation is informational
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}", exc_info=True)
        # Try to clean up resources in case of error
        cleanup_resources()
        return 1
    finally:
        # Always ensure resources are cleaned up properly
        cleanup_resources()

if __name__ == "__main__":
    sys.exit(main())