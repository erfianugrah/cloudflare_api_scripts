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
        'ttfb_values': [],
        'total_time_values': [],
        'errors_by_type': {},
        'by_size_category': {
            'small': {
                'count': 0,
                'size_bytes': 0,
                'ttfb_values': [],
                'total_time_values': [],
            },
            'medium': {
                'count': 0,
                'size_bytes': 0,
                'ttfb_values': [],
                'total_time_values': [],
            },
            'large': {
                'count': 0,
                'size_bytes': 0,
                'ttfb_values': [],
                'total_time_values': [],
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
                        args.medium_file_threshold
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
                        args.medium_file_threshold
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
    
    # Handle error report generation if requested
    if args.generate_error_report:
        logger.info(f"Generating error report from {args.output} to {args.error_report_output}")
        try:
            # Load existing results file
            with open(args.output, 'r') as f:
                results_data = json.load(f)
            
            # Determine output format based on explicit option or file extension
            if args.format:
                format_type = args.format
            else:
                format_type = 'json' if args.error_report_output.endswith('.json') else 'markdown'
            
            # Generate error report
            report = reporting.generate_error_report(results_data, format_type=format_type)
            
            # Save to output file
            with open(args.error_report_output, 'w') as f:
                f.write(report)
                
            logger.info(f"Error report saved to {args.error_report_output} (format: {format_type})")
            return 0
        except Exception as e:
            logger.error(f"Error generating error report: {str(e)}", exc_info=True)
            return 1
    
    # Process derivatives if they are specified in the command line
    # If --derivatives is provided at all (even with default value), use them
    derivatives = args.derivatives.split(',')
    logger.info(f"Processing derivatives: {derivatives}")
    
    # Initialize stats
    stats = initialize_stats(derivatives)
    
    try:
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
        
        # Process files for pre-warming
        if not args.only_compare and not args.optimize_videos:
            logger.info(f"Processing {len(objects)} files with {args.workers} workers")
            
            # Process the objects and collect results
            results = process_objects(objects, args, derivatives, stats)
            processed = len(results)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Remove lists of ttfb/time values from the stats to reduce file size
            stats_for_output = stats.copy()
            stats_for_output.pop('ttfb_values', None)
            stats_for_output.pop('total_time_values', None)
            
            # Remove ttfb arrays from size categories too
            for category in stats_for_output.get('by_size_category', {}).values():
                category.pop('ttfb_values', None)
                category.pop('total_time_values', None)
            
            # Calculate summary statistics instead
            if stats.get('ttfb_values'):
                ttfb_array = stats['ttfb_values']
                stats_for_output['ttfb_summary'] = {
                    'count': len(ttfb_array),
                    'min': min(ttfb_array) if ttfb_array else None,
                    'max': max(ttfb_array) if ttfb_array else None,
                    'avg': sum(ttfb_array) / len(ttfb_array) if ttfb_array else None,
                }
            
            if stats.get('total_time_values'):
                time_array = stats['total_time_values']
                stats_for_output['total_time_summary'] = {
                    'count': len(time_array),
                    'min': min(time_array) if time_array else None,
                    'max': max(time_array) if time_array else None,
                    'avg': sum(time_array) / len(time_array) if time_array else None,
                }
                
            # Calculate and add size reduction statistics
            size_reduction_percentages = []
            original_sizes = []
            response_sizes = []
            
            # Log the structure of the first result to help debug
            if results and logger:
                first_result_key = next(iter(results))
                logger.debug(f"First result structure: {json.dumps(results[first_result_key], indent=2)[:500]}...")
                
            # Extract size data from results
            for obj_path, obj_result in results.items():
                derivatives_dict = obj_result.get('derivatives', {})
                
                # If structure has derivatives
                if derivatives_dict:
                    for deriv, deriv_result in derivatives_dict.items():
                        if (deriv_result.get('status') == 'success' and 
                            'size_reduction_percent' in deriv_result and 
                            'original_size_bytes' in deriv_result and
                            'response_size_bytes' in deriv_result):
                            size_reduction_percentages.append(deriv_result['size_reduction_percent'])
                            original_sizes.append(deriv_result['original_size_bytes'])
                            response_sizes.append(deriv_result['response_size_bytes'])
                            
                # Alternative structure without derivatives wrapper
                elif (obj_result.get('status') == 'success' and
                      'size_reduction_percent' in obj_result and
                      'original_size_bytes' in obj_result and
                      'response_size_bytes' in obj_result):
                    size_reduction_percentages.append(obj_result['size_reduction_percent'])
                    original_sizes.append(obj_result['original_size_bytes'])
                    response_sizes.append(obj_result['response_size_bytes'])
            
            # Add size reduction summary if we have data
            if size_reduction_percentages:
                # Total size statistics
                total_original = sum(original_sizes)
                total_response = sum(response_sizes)
                total_reduction = total_original - total_response
                overall_reduction_percent = (total_reduction / total_original) * 100 if total_original > 0 else 0
                
                stats_for_output['size_reduction_summary'] = {
                    'count': len(size_reduction_percentages),
                    'min_percent': min(size_reduction_percentages) if size_reduction_percentages else None,
                    'max_percent': max(size_reduction_percentages) if size_reduction_percentages else None,
                    'avg_percent': sum(size_reduction_percentages) / len(size_reduction_percentages) if size_reduction_percentages else None,
                    'total_original_bytes': total_original,
                    'total_transformed_bytes': total_response,
                    'total_reduction_bytes': total_reduction,
                    'overall_reduction_percent': overall_reduction_percent
                }
            
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