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
    
    # Add derivative-specific stats
    for derivative in derivatives:
        stats[f'{derivative}_success'] = 0
        stats[f'{derivative}_errors'] = 0
    
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
        derivatives: List of derivatives to process
        stats: Statistics dictionary
        
    Returns:
        Updated results dictionary
    """
    global running, shutdown_event, processed_objects, failed_objects, executor
    
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
    optimization_options = {
        "codec": args.codec,
        "quality_profile": args.quality,
        "resolution": args.target_resolution,
        "audio_profile": args.audio_profile,
        "output_format": args.output_format,
        "create_webm": args.create_webm
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
    
    # Parse derivatives
    derivatives = args.derivatives.split(',')
    logger.info(f"Processing derivatives: {derivatives}")
    
    # Initialize stats
    stats = initialize_stats(derivatives)
    
    try:
        # Start processing timer
        start_time = time.time()
        
        # List files from remote storage with sizes
        logger.info(f"Listing files from {args.remote}:{args.bucket}/{args.directory}")
        
        file_list = storage.list_large_files(
            args.remote,
            args.bucket,
            args.directory,
            args.extension,
            args.size_threshold,
            args.limit
        )
        
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
                    'stats': stats,
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