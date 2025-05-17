import argparse
import subprocess
import concurrent.futures
import requests
import logging
import json
import time
import os
import re
import signal
import sys
import threading
from threading import Lock
import math
import queue
from urllib.parse import urljoin
from datetime import datetime
from tabulate import tabulate

# Set up logging
def setup_logging(verbose=False):
    """Configure logging with appropriate level and format."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create a formatter that includes thread name for concurrent operations
    log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
    
    # Configure the root logger
    logging.basicConfig(
        level=log_level,
        format=log_format
    )
    
    # Get the logger instance
    logger = logging.getLogger(__name__)
    
    # Add a file handler for debug logs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"video_transform_{timestamp}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    
    logger.info(f"Logging initialized at {log_level} level. File logs will be saved to {log_file}")
    return logger

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process video assets with different derivatives')
    parser.add_argument('--remote', help='rclone remote name')
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--directory', default='', help='Directory path within bucket')
    parser.add_argument('--base-url', help='Base URL to prepend to object paths')
    parser.add_argument('--derivatives', default='desktop,tablet,mobile', help='Comma-separated list of derivatives')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers')
    parser.add_argument('--timeout', type=int, default=120, help='Request timeout in seconds')
    parser.add_argument('--connection-close-delay', type=int, default=10, help='Additional delay in seconds before closing connections')
    parser.add_argument('--output', default='video_transform_results.json', help='Output JSON file path')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of objects to process (0 = no limit)')
    parser.add_argument('--extension', default='.mp4', help='File extension to filter by (e.g., .mp4)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--retry', type=int, default=2, help='Number of retry attempts for failed requests')
    parser.add_argument('--compare', help='Path to Cloudflare KV JSON file for comparison')
    parser.add_argument('--comparison-output', default='comparison_results.json', help='Output file for comparison results')
    parser.add_argument('--summary-output', default='comparison_summary.md', help='Output file for comparison summary')
    parser.add_argument('--summary-format', default='markdown', choices=['markdown', 'json'], help='Format for the summary output (markdown or json)')
    parser.add_argument('--only-compare', action='store_true', help='Skip processing and only compare existing results with KV data')
    parser.add_argument('--use-aws-cli', action='store_true', help='Use AWS CLI instead of rclone for listing S3 objects')
    parser.add_argument('--generate-error-report', action='store_true', help='Generate or regenerate the 500 error reports from an existing results file')
    parser.add_argument('--list-files', action='store_true', help='List all files with their sizes sorted in descending order')
    parser.add_argument('--size-threshold', type=int, default=256, help='Size threshold in MiB for file size reporting (default: 256 MiB)')
    parser.add_argument('--size-report-output', default='file_size_report.md', help='Output file for size report')
    
    # Size category thresholds for optimized processing
    parser.add_argument('--small-file-threshold', type=int, default=50, 
                      help='Threshold in MiB for small files (default: 50 MiB)')
    parser.add_argument('--medium-file-threshold', type=int, default=200, 
                      help='Threshold in MiB for medium files (default: 200 MiB)')
    
    # Worker allocation control
    parser.add_argument('--small-file-workers', type=int, default=0,
                      help='Number of workers for small files (0 = auto-calculate based on total workers)')
    parser.add_argument('--medium-file-workers', type=int, default=0,
                      help='Number of workers for medium files (0 = auto-calculate based on total workers)')
    parser.add_argument('--large-file-workers', type=int, default=0,
                      help='Number of workers for large files (0 = auto-calculate based on total workers)')
    
    # Enable performance metrics and optimized processing
    parser.add_argument('--optimize-by-size', action='store_true',
                      help='Enable size-based optimization for parallel processing')
    parser.add_argument('--performance-report', default='performance_report.md',
                      help='Output file for performance analysis report')
    
    return parser.parse_args()

# File size category definitions
def get_size_category(size_bytes, small_threshold_mib=50, medium_threshold_mib=200):
    """
    Determine the size category of a file based on its size.
    
    Arguments:
        size_bytes: Size of the file in bytes
        small_threshold_mib: Threshold for small files in MiB (default: 50 MiB)
        medium_threshold_mib: Threshold for medium files in MiB (default: 200 MiB)
        
    Returns:
        String: 'small', 'medium', or 'large'
    """
    small_threshold = small_threshold_mib * 1024 * 1024  # Convert MiB to bytes
    medium_threshold = medium_threshold_mib * 1024 * 1024  # Convert MiB to bytes
    
    if size_bytes < small_threshold:
        return 'small'
    elif size_bytes < medium_threshold:
        return 'medium'
    else:
        return 'large'

class FileMetadata:
    """
    Class to store and manage file metadata for optimized processing.
    """
    def __init__(self, path, size_bytes, small_threshold_mib=50, medium_threshold_mib=200):
        """
        Initialize file metadata.
        
        Arguments:
            path: File path relative to bucket
            size_bytes: Size of the file in bytes
            small_threshold_mib: Threshold for small files in MiB
            medium_threshold_mib: Threshold for medium files in MiB
        """
        self.path = path
        self.size_bytes = size_bytes
        self.size_category = get_size_category(size_bytes, small_threshold_mib, medium_threshold_mib)
        self.processing_started = None
        self.processing_completed = None
        self.processing_duration = None
        self.derivatives = {}  # Will store derivative-specific timing data
        
    def start_processing(self):
        """Mark the start time of processing."""
        self.processing_started = time.time()
        
    def complete_processing(self):
        """Mark the completion time and calculate duration."""
        self.processing_completed = time.time()
        if self.processing_started:
            self.processing_duration = self.processing_completed - self.processing_started
        
    def start_derivative_processing(self, derivative):
        """Mark the start time of processing a specific derivative."""
        if derivative not in self.derivatives:
            self.derivatives[derivative] = {}
        self.derivatives[derivative]['started'] = time.time()
        
    def complete_derivative_processing(self, derivative):
        """Mark the completion time for a derivative and calculate duration."""
        if derivative in self.derivatives and 'started' in self.derivatives[derivative]:
            self.derivatives[derivative]['completed'] = time.time()
            self.derivatives[derivative]['duration'] = (
                self.derivatives[derivative]['completed'] - self.derivatives[derivative]['started']
            )
    
    def to_dict(self):
        """Convert metadata to dictionary for serialization."""
        return {
            'path': self.path,
            'size_bytes': self.size_bytes,
            'size_mib': self.size_bytes / (1024 * 1024),
            'size_category': self.size_category,
            'processing_started': self.processing_started,
            'processing_completed': self.processing_completed,
            'processing_duration': self.processing_duration,
            'derivatives': self.derivatives
        }

def list_objects(remote, bucket, directory, extension, limit=0, logger=None, use_aws_cli=False, get_sizes=True):
    """
    List objects in the specified bucket and directory, optionally with their sizes.
    Supports both rclone and AWS CLI for retrieving object lists.
    
    Arguments:
        remote: The rclone remote name
        bucket: The S3 bucket name
        directory: The directory within the bucket
        extension: File extension to filter by
        limit: Maximum number of objects to return (0 for no limit)
        logger: Logger instance
        use_aws_cli: Whether to use AWS CLI instead of rclone
        get_sizes: Whether to retrieve file sizes (slightly slower but more informative)
        
    Returns:
        If get_sizes=False: List of file paths
        If get_sizes=True: List of dicts with 'path' and 'size' keys
    """
    path = f"{remote}:{bucket}/{directory}"
    path = path.rstrip('/') # Remove trailing slash if present
    
    if use_aws_cli:
        logger.info(f"Listing objects from AWS S3 bucket: {bucket}/{directory}")
    else:
        logger.info(f"Listing objects from rclone path: {path}")
    
    logger.debug(f"Using filter extension: {extension}")
    
    try:
        start_time = time.time()
        
        if use_aws_cli:
            # Use AWS CLI to list objects, potentially with size information
            s3_path = f"s3://{bucket}/{directory}"
            if directory and not s3_path.endswith('/'):
                s3_path += '/'
                
            if get_sizes:
                # Use --human-readable to get sizes
                cmd = ['aws', 's3', 'ls', '--recursive', '--human-readable', s3_path]
            else:
                cmd = ['aws', 's3', 'ls', '--recursive', s3_path]
                
            logger.debug(f"Executing AWS CLI command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Parse AWS CLI output format (different from rclone)
            # AWS format: 2023-04-26 18:45:30       1234 path/to/file.mp4
            all_items = []
            for line in result.stdout.splitlines():
                parts = line.strip().split()
                if len(parts) >= 4:
                    # Extract file path and possibly size
                    file_path = ' '.join(parts[3:])
                    # Remove bucket prefix if present
                    if file_path.startswith(f"{bucket}/"):
                        file_path = file_path[len(bucket)+1:]
                    
                    if get_sizes:
                        size_bytes = parse_human_readable_size(parts[2])
                        all_items.append({'path': file_path, 'size': size_bytes})
                    else:
                        all_items.append(file_path)
        else:
            # Use rclone method, potentially with size information
            if get_sizes:
                # 'ls' recurses by default, no need for --recursive flag
                cmd = ['rclone', 'ls', path]
            else:
                # For lsf we need -R to recurse
                cmd = ['rclone', 'lsf', path, '-R']
                
            logger.debug(f"Executing rclone command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            all_items = []
            if get_sizes:
                # Parse rclone ls output which includes sizes
                # Format: "   12345 file.mp4"
                for line in result.stdout.splitlines():
                    parts = line.strip().split(maxsplit=1)
                    if len(parts) == 2:
                        size_bytes = int(parts[0])
                        file_path = parts[1]
                        all_items.append({'path': file_path, 'size': size_bytes})
            else:
                # Just get the file paths from lsf
                all_items = result.stdout.splitlines()
        
        duration = time.time() - start_time
        logger.debug(f"Command returned {len(all_items)} total items in {duration:.2f} seconds")
        
        # Filter out directories and by extension
        if get_sizes:
            objects = [item for item in all_items 
                      if not item['path'].endswith('/') and item['path'].lower().endswith(extension.lower())]
        else:
            objects = [line for line in all_items 
                      if not line.endswith('/') and line.lower().endswith(extension.lower())]
        
        logger.info(f"Found {len(objects)} {extension} files out of {len(all_items)} total items")
        
        # Apply limit if specified
        if limit > 0 and len(objects) > limit:
            logger.info(f"Limiting to {limit} objects (from {len(objects)} found)")
            objects = objects[:limit]
            if get_sizes:
                logger.debug(f"First {min(5, len(objects))} limited objects: {[obj['path'] for obj in objects[:5]]}")
            else:
                logger.debug(f"First {min(5, len(objects))} limited objects: {objects[:5]}")
        else:
            if get_sizes:
                logger.debug(f"First {min(5, len(objects))} objects: {[obj['path'] for obj in objects[:5]]}")
            else:
                logger.debug(f"First {min(5, len(objects))} objects: {objects[:5]}")
        
        # If we have size data, sort objects by size (largest first) for better processing
        if get_sizes:
            objects = sorted(objects, key=lambda x: x['size'], reverse=True)
            total_size = sum(obj['size'] for obj in objects)
            logger.info(f"Total size of all {len(objects)} objects: {format_file_size(total_size)}")
            
            # Log size statistics
            if objects:
                min_size = min(obj['size'] for obj in objects)
                max_size = max(obj['size'] for obj in objects)
                avg_size = total_size / len(objects)
                logger.info(f"Size range: {format_file_size(min_size)} - {format_file_size(max_size)}, Average: {format_file_size(avg_size)}")
        
        return objects
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing command: {' '.join(cmd)}")
        logger.error(f"Command stderr: {e.stderr}")
        logger.error(f"Command exit code: {e.returncode}")
        raise
        
def get_file_sizes(remote, bucket, directory, file_paths, logger=None, use_aws_cli=False):
    """
    Get sizes of files using either rclone or AWS CLI.
    Returns a list of tuples (file_path, size_in_bytes)
    """
    logger.info(f"Getting file sizes for {len(file_paths)} files")
    file_sizes = []
    
    try:
        if use_aws_cli:
            # AWS CLI approach for getting file sizes
            s3_path = f"s3://{bucket}/{directory}"
            if directory and not s3_path.endswith('/') and not s3_path.endswith('/'):
                s3_path += '/'
            
            # For AWS CLI, we get all file sizes at once, then filter
            cmd = ['aws', 's3', 'ls', '--recursive', '--human-readable', s3_path]
            logger.debug(f"Executing AWS CLI command: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Parse output which includes sizes
            for line in result.stdout.splitlines():
                parts = line.strip().split()
                if len(parts) >= 4:
                    # Format: date time size filename
                    size_str = parts[2]
                    file_path = ' '.join(parts[3:])
                    
                    # Remove bucket prefix if present
                    if file_path.startswith(f"{bucket}/"):
                        file_path = file_path[len(bucket)+1:]
                        
                    # Convert size string to bytes (handles KB, MB, GB)
                    size_bytes = parse_human_readable_size(size_str)
                    
                    # Only add if file is in our list
                    if file_path in file_paths:
                        file_sizes.append((file_path, size_bytes))
        else:
            # rclone approach - more efficient with --size flag
            path = f"{remote}:{bucket}/{directory}"
            path = path.rstrip('/')
            
            # Process files individually, since rclone size only works on directories
            for i, file_path in enumerate(file_paths):
                # Use rclone ls to get file size
                file_cmd = ['rclone', 'ls', f"{path}/{file_path}"]
                logger.debug(f"Executing rclone command for file {i+1}/{len(file_paths)}: rclone ls {path}/{file_path}")
                
                file_result = subprocess.run(file_cmd, capture_output=True, text=True, check=True)
                
                if file_result.stdout.strip():
                    # Format: "   12345 file.mp4"
                    size_bytes = int(file_result.stdout.strip().split()[0])
                    file_sizes.append((file_path, size_bytes))
                    logger.debug(f"File {file_path}: {format_file_size(size_bytes)}")
                else:
                    logger.warning(f"Could not get size for {file_path}")
                
                # Log progress every 10 files
                if (i + 1) % 10 == 0 or i == len(file_paths) - 1:
                    logger.info(f"Retrieved sizes for {i+1}/{len(file_paths)} files")
                
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing command: {' '.join(cmd)}")
        logger.error(f"Command stderr: {e.stderr}")
        logger.error(f"Command exit code: {e.returncode}")
        raise
    
    logger.info(f"Successfully retrieved sizes for {len(file_sizes)} files")
    return file_sizes

def parse_human_readable_size(size_str):
    """
    Parse a human-readable size string (e.g., '5.1 MiB', '3.4 GB') to bytes.
    """
    size_str = size_str.upper().replace('B', '').strip()
    
    if 'K' in size_str:
        return int(float(size_str.replace('K', '').strip()) * 1024)
    elif 'M' in size_str:
        return int(float(size_str.replace('M', '').strip()) * 1024 * 1024)
    elif 'G' in size_str:
        return int(float(size_str.replace('G', '').strip()) * 1024 * 1024 * 1024)
    elif 'T' in size_str:
        return int(float(size_str.replace('T', '').strip()) * 1024 * 1024 * 1024 * 1024)
    else:
        # Assume it's just bytes
        return int(float(size_str))

def format_file_size(size_bytes):
    """
    Format a size in bytes to a human-readable string.
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.2f} KiB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.2f} MiB"
    else:
        return f"{size_bytes/(1024*1024*1024):.2f} GiB"

def generate_size_report(file_sizes, size_threshold_mib, output_file, logger=None):
    """
    Generate a report of file sizes, including a histogram of files over the threshold.
    """
    logger.info(f"Generating file size report with threshold of {size_threshold_mib} MiB")
    
    # Convert threshold to bytes
    size_threshold = size_threshold_mib * 1024 * 1024
    
    # Sort file sizes in descending order
    sorted_files = sorted(file_sizes, key=lambda x: x[1], reverse=True)
    
    # Count files above threshold
    files_above_threshold = [f for f in sorted_files if f[1] >= size_threshold]
    files_above_threshold_count = len(files_above_threshold)
    
    # Calculate total size
    total_size = sum(size for _, size in sorted_files)
    size_above_threshold = sum(size for _, size in files_above_threshold)
    
    # Create size buckets for histogram
    # Using logarithmic buckets based on MiB
    # First bucket is 0-256 MiB, then 256-512, 512-1024, 1024-2048, etc.
    size_buckets = {}
    max_bucket_start = 2**math.ceil(math.log2(max([size for _, size in sorted_files])/1024/1024/256)) * 256
    
    bucket_starts = [256]
    current = 256
    while current < max_bucket_start:
        current *= 2
        bucket_starts.append(current)
    
    # Add one more bucket at the end
    bucket_starts.append(current * 2)
    
    # Initialize buckets
    for i in range(len(bucket_starts)):
        start = bucket_starts[i-1] if i > 0 else 0
        end = bucket_starts[i]
        label = f"{start}-{end} MiB" if i < len(bucket_starts)-1 else f"{start}+ MiB"
        size_buckets[label] = {
            'start_mib': start,
            'end_mib': end if i < len(bucket_starts)-1 else float('inf'),
            'count': 0,
            'total_size': 0
        }
    
    # Count files in each bucket
    for file_path, size in sorted_files:
        size_mib = size / 1024 / 1024
        for label, bucket in size_buckets.items():
            if bucket['start_mib'] <= size_mib < bucket['end_mib']:
                bucket['count'] += 1
                bucket['total_size'] += size
                break
    
    # Generate markdown report
    md_lines = [
        "# File Size Distribution Report",
        f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Summary",
        f"- **Total files analyzed**: {len(sorted_files)}",
        f"- **Total size**: {format_file_size(total_size)}",
        f"- **Files over {size_threshold_mib} MiB**: {files_above_threshold_count} ({files_above_threshold_count/len(sorted_files)*100:.1f}%)",
        f"- **Size of files over {size_threshold_mib} MiB**: {format_file_size(size_above_threshold)} ({size_above_threshold/total_size*100:.1f}% of total)",
        "",
        "## Size Distribution",
        "",
        "| Size Range | Count | % of Files | Total Size | % of Total Size |",
        "| --- | --- | --- | --- | --- |"
    ]
    
    # Add rows for each bucket
    for label, bucket in size_buckets.items():
        if bucket['count'] > 0:  # Only show non-empty buckets
            md_lines.append(f"| {label} | {bucket['count']} | {bucket['count']/len(sorted_files)*100:.1f}% | {format_file_size(bucket['total_size'])} | {bucket['total_size']/total_size*100:.1f}% |")
    
    # Add ASCII histogram
    md_lines.extend([
        "",
        "## Size Distribution Histogram",
        "```"
    ])
    
    # Calculate the maximum bar width
    max_bar_width = 60
    max_count = max([bucket['count'] for bucket in size_buckets.values()])
    
    # Generate ASCII histogram
    for label, bucket in size_buckets.items():
        if bucket['count'] > 0:  # Only show non-empty buckets
            bar_width = int((bucket['count'] / max_count) * max_bar_width) if max_count > 0 else 0
            bar = '#' * bar_width
            md_lines.append(f"{label.ljust(15)} | {bar} {bucket['count']}")
    
    md_lines.append("```")
    
    # Add the top 20 largest files
    md_lines.extend([
        "",
        "## Top 20 Largest Files",
        "",
        "| # | File | Size |",
        "| --- | --- | --- |"
    ])
    
    for i, (file_path, size) in enumerate(sorted_files[:20], 1):
        md_lines.append(f"| {i} | {file_path} | {format_file_size(size)} |")
    
    # Write report to file
    with open(output_file, 'w') as f:
        f.write('\n'.join(md_lines))
    
    logger.info(f"File size report written to {output_file}")

def get_derivative_dimensions(derivative, logger=None):
    """Get the requested dimensions for a specific derivative."""
    dimensions = {
        'desktop': {'width': 1920, 'height': 1080},
        'tablet': {'width': 1280, 'height': 720},
        'mobile': {'width': 854, 'height': 640}
    }
    
    if derivative in dimensions:
        logger.debug(f"Using standard dimensions for '{derivative}': {dimensions[derivative]}")
        return dimensions[derivative]
    else:
        logger.warning(f"Unknown derivative '{derivative}', using default dimensions")
        return {'width': 1280, 'height': 720}  # Default dimensions

def log_response_details(response, url, logger=None):
    """Log detailed information about the HTTP response."""
    logger.debug(f"Response status: {response.status_code}")
    logger.debug(f"Response headers: {dict(response.headers)}")
    
    content_length = response.headers.get('Content-Length', 'unknown')
    content_type = response.headers.get('Content-Type', 'unknown')
    etag = response.headers.get('ETag', 'unknown')
    
    logger.debug(f"Content-Length: {content_length}")
    logger.debug(f"Content-Type: {content_type}")
    logger.debug(f"ETag: {etag}")
    
    # Log a short preview of the response content
    try:
        if 'application/json' in content_type:
            logger.debug(f"JSON response preview: {response.text[:200]}...")
        elif 'text/' in content_type:
            logger.debug(f"Text response preview: {response.text[:200]}...")
        else:
            content_size = len(response.content)
            logger.debug(f"Binary response, size: {content_size} bytes")
    except Exception as e:
        logger.debug(f"Could not preview response content: {str(e)}")

def process_single_derivative(obj_data, derivative, base_url, bucket, directory, timeout, retry_attempts=2, connection_close_delay=10, logger=None, small_threshold_mib=50, medium_threshold_mib=200):
    """
    Process a single derivative for a video object.
    
    Args:
        obj_data: Either a string (path) or dict with 'path' and 'size' keys
        derivative: Single derivative to process (e.g. 'desktop', 'mobile')
        base_url: Base URL for the CDN
        bucket: S3 bucket name
        directory: Directory path within bucket
        timeout: Request timeout in seconds
        retry_attempts: Number of retry attempts for failed requests
        connection_close_delay: Delay after response before processing
        logger: Logger instance
        small_threshold_mib: Threshold for small files in MiB
        medium_threshold_mib: Threshold for medium files in MiB
        
    Returns:
        tuple: (rel_path, derivative_key, result_data, original_size, size_category)
    """
    # Determine if we have size information
    if isinstance(obj_data, dict) and 'path' in obj_data and 'size' in obj_data:
        obj_path = obj_data['path']
        original_size = obj_data['size']
        has_size_info = True
    else:
        obj_path = obj_data
        original_size = None
        has_size_info = False
    
    # Get size category if size info is available
    if has_size_info:
        size_category = get_size_category(original_size, small_threshold_mib, medium_threshold_mib)
        size_mib = original_size / (1024 * 1024)
    else:
        size_category = "unknown"
        size_mib = 0
    
    # Construct the relative path
    if directory:
        # Remove bucket and directory prefix to get relative path
        rel_path = obj_path.replace(f"{bucket}/{directory}/", "", 1)
    else:
        # Just remove bucket prefix
        rel_path = obj_path.replace(f"{bucket}/", "", 1)
    
    base_obj_url = urljoin(base_url, rel_path)
    logger.info(f"Processing derivative '{derivative}' for {rel_path}")
    logger.debug(f"Base URL: {base_obj_url}")
    
    # Add size information to log if available
    if has_size_info:
        logger.info(f"File size: {format_file_size(original_size)} ({size_category} file)")
    
    # Store processing start time
    processing_start_time = time.time()
    
    # Get dimensions for the derivative
    dimensions = get_derivative_dimensions(derivative, logger)
    
    # Construct URL with imwidth parameter
    url = f"{base_obj_url}?imwidth={dimensions['width']}"
    logger.debug(f"Request URL: {url}")
    
    # Keep track of retry attempts
    attempt = 0
    while attempt <= retry_attempts:
        attempt += 1
        try:
            logger.debug(f"Starting request (attempt {attempt}/{retry_attempts+1}): {url}")
            start_time = time.time()
            
            response = requests.get(url, timeout=timeout)
            response_time = time.time() - start_time
            
            logger.debug(f"Request completed in {response_time:.2f} seconds with status {response.status_code}")
            log_response_details(response, url, logger)
            
            # Add intentional delay after response to ensure complete data transfer
            logger.debug(f"Waiting {connection_close_delay} seconds after response before processing")
            time.sleep(connection_close_delay)
            
            duration = time.time() - start_time
            
            status = response.status_code
            
            # Store results in a format similar to the KV structure
            derivative_key = f"video:{rel_path}:derivative={derivative}"
            
            # Parse content-type to determine if JSON or video file
            content_type = response.headers.get('Content-Type', '')
            is_chunked = 'application/json' in content_type
            
            # Check for actual size based on content-type
            actual_size = len(response.content) if status == 200 else 0
            
            # Attempt to parse JSON content if present
            json_content = None
            total_size = actual_size
            if is_chunked and status == 200:
                try:
                    logger.debug(f"Attempting to parse JSON response for chunked content")
                    json_content = response.json()
                    logger.debug(f"JSON content: {json.dumps(json_content, indent=2)[:1000]}")
                    
                    if 'totalSize' in json_content:
                        total_size = json_content['totalSize']
                        logger.debug(f"Found totalSize in JSON: {total_size}")
                    else:
                        logger.debug(f"No totalSize in JSON, keys present: {json_content.keys()}")
                except Exception as e:
                    logger.warning(f"Failed to parse JSON for chunked content: {str(e)}")
            
            # Detailed logging of results
            if status == 200:
                logger.info(f"Success: {derivative} for {rel_path} - Size: {actual_size} bytes, "
                           f"Total Size: {total_size} bytes, Chunked: {is_chunked}")
                if has_size_info:
                    # Calculate and log size reduction if successful
                    size_reduction = original_size - total_size
                    size_reduction_percent = (size_reduction / original_size) * 100 if original_size > 0 else 0
                    logger.info(f"Size reduction: {format_file_size(size_reduction)} "
                               f"({size_reduction_percent:.1f}% of original)")
            else:
                # More verbose logging for non-200 responses
                logger.warning(f"Non-200 response: {derivative} for {rel_path} - Status: {status}")
                logger.debug(f"Full response details for failed request ({rel_path}, {derivative}):")
                logger.debug(f"  URL: {url}")
                logger.debug(f"  Status code: {status}")
                logger.debug(f"  Headers: {dict(response.headers)}")
                logger.debug(f"  Content preview: {response.text[:500] if response.text else 'No content'}")
            
            # Build the result information
            result_data = {
                'status': status,
                'contentLength': actual_size,
                'contentType': content_type,
                'actualTotalVideoSize': total_size,
                'isChunked': is_chunked,
                'duration': duration,
                'derivative': derivative,
                'width': dimensions['width'],
                'height': dimensions['height'],
                'sourcePath': f"/{rel_path}",
                'requestDimensions': f"{dimensions['width']}",
                'etag': response.headers.get('ETag', '').strip('"'),
                'attempt': attempt,
                'processingTime': time.time() - processing_start_time  # Total processing time for this derivative
            }
            
            # Add original size information for comparison
            if has_size_info:
                result_data['originalSize'] = original_size
                # Calculate size reduction statistics
                if status == 200:
                    result_data['sizeReduction'] = original_size - total_size
                    result_data['sizeReductionPercent'] = (result_data['sizeReduction'] / original_size) * 100 if original_size > 0 else 0
            
            # Add extra error details for non-200 responses
            if status != 200:
                # Extract potentially useful debugging information from headers
                result_data['errorDetails'] = {
                    'responseHeaders': dict(response.headers),
                    'responseText': response.text[:500] if response.text else 'No content'
                }
                
                # Keep track of the ray ID for troubleshooting but don't collect other CF headers
                cf_ray = response.headers.get('cf-ray', 'Not found')
                result_data['errorDetails']['cf_ray'] = cf_ray
                
                # Add original size context to error details
                if has_size_info:
                    result_data['errorDetails']['originalSize'] = {
                        'bytes': original_size,
                        'formatted': format_file_size(original_size),
                        'category': size_category
                    }
            
            # If successful, return the result and break the retry loop
            return (rel_path, derivative_key, result_data, original_size, size_category)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error (attempt {attempt}/{retry_attempts+1}): {url}")
            logger.error(f"Error details: {str(e)}")
            
            # If we've exhausted retries, record the error
            if attempt > retry_attempts:
                logger.error(f"All retry attempts failed for {url}")
                derivative_key = f"video:{rel_path}:derivative={derivative}"
                error_data = {
                    'status': 'Error',
                    'error': str(e),
                    'contentLength': 0,
                    'actualTotalVideoSize': 0,
                    'derivative': derivative,
                    'attempts': attempt,
                    'processingTime': time.time() - processing_start_time
                }
                
                # Add original size information to error data
                if has_size_info:
                    error_data['originalSize'] = original_size
                    error_data['originalSizeFormatted'] = format_file_size(original_size)
                    error_data['sizeCategory'] = size_category
                
                return (rel_path, derivative_key, error_data, original_size, size_category)
            else:
                # Calculate backoff time (exponential backoff)
                backoff = 2 ** (attempt - 1)
                logger.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)
    
    # Should never reach here, but just in case
    return (rel_path, None, None, original_size, size_category)


def process_object(obj_data, base_url, derivatives, bucket, directory, timeout, retry_attempts=2, connection_close_delay=10, logger=None, small_threshold_mib=50, medium_threshold_mib=200):
    """
    Process a single video object with different derivatives.
    (Legacy implementation that processes all derivatives sequentially)
    
    This is kept for backward compatibility. The preferred approach is to use
    process_single_derivative for each derivative in parallel.
    
    Args:
        obj_data: Either a string (path) or dict with 'path' and 'size' keys
        base_url: Base URL for the CDN
        derivatives: List of derivatives to process
        bucket: S3 bucket name
        directory: Directory path within bucket
        timeout: Request timeout in seconds
        retry_attempts: Number of retry attempts for failed requests
        connection_close_delay: Delay after response before processing
        logger: Logger instance
        small_threshold_mib: Threshold for small files in MiB
        medium_threshold_mib: Threshold for medium files in MiB
        
    Returns:
        dict: Results of processing the object
    """
    # Determine if we have size information
    if isinstance(obj_data, dict) and 'path' in obj_data and 'size' in obj_data:
        obj_path = obj_data['path']
        original_size = obj_data['size']
        has_size_info = True
    else:
        obj_path = obj_data
        original_size = None
        has_size_info = False
    
    # Create file metadata object if we have size info
    if has_size_info:
        file_metadata = FileMetadata(
            obj_path, 
            original_size, 
            small_threshold_mib, 
            medium_threshold_mib
        )
        file_metadata.start_processing()
    
    # Construct the relative path
    if directory:
        # Remove bucket and directory prefix to get relative path
        rel_path = obj_path.replace(f"{bucket}/{directory}/", "", 1)
    else:
        # Just remove bucket prefix
        rel_path = obj_path.replace(f"{bucket}/", "", 1)
    
    base_obj_url = urljoin(base_url, rel_path)
    logger.info(f"Processing object: {rel_path}")
    logger.debug(f"Base URL: {base_obj_url}")
    
    # Add size information to log if available
    if has_size_info:
        size_category = file_metadata.size_category
        size_mib = original_size / (1024 * 1024)
        logger.info(f"File size: {format_file_size(original_size)} ({size_category} file)")
    
    # Initialize results structure
    results = {
        "key": rel_path,
        "derivatives": {}
    }
    
    # Add file size information if available
    if has_size_info:
        results["original_size"] = {
            "bytes": original_size,
            "formatted": format_file_size(original_size),
            "size_category": size_category,
            "size_mib": size_mib
        }
    
    # Store processing start time
    processing_start_time = time.time()
    
    # Process each derivative
    for derivative in derivatives:
        # Mark start of derivative processing if we have file metadata
        if has_size_info:
            file_metadata.start_derivative_processing(derivative)
            
        # Process this derivative
        rel_path, derivative_key, result_data, _, _ = process_single_derivative(
            obj_data, 
            derivative, 
            base_url, 
            bucket, 
            directory, 
            timeout, 
            retry_attempts, 
            connection_close_delay, 
            logger, 
            small_threshold_mib, 
            medium_threshold_mib
        )
        
        # Add performance metrics for this derivative
        if has_size_info:
            file_metadata.complete_derivative_processing(derivative)
            if derivative in file_metadata.derivatives and 'duration' in file_metadata.derivatives[derivative]:
                result_data['processingMetrics'] = {
                    'totalDuration': file_metadata.derivatives[derivative]['duration'],
                    'requestDuration': result_data.get('duration', 0),
                    'processingOverhead': file_metadata.derivatives[derivative]['duration'] - result_data.get('duration', 0)
                }
        
        # Add this derivative's results
        results["derivatives"][derivative_key] = result_data
    
    # Record total processing time
    processing_end_time = time.time()
    processing_duration = processing_end_time - processing_start_time
    results["processingTime"] = processing_duration
    
    # Complete processing in file metadata if available
    if has_size_info:
        file_metadata.complete_processing()
        results["performanceMetrics"] = {
            "processingStarted": file_metadata.processing_started,
            "processingCompleted": file_metadata.processing_completed,
            "processingDuration": file_metadata.processing_duration,
            "derivativeTimings": {k: v.get('duration') for k, v in file_metadata.derivatives.items() if 'duration' in v}
        }
    
    return results

def write_results_to_file(all_results, metadata, output_file, processed, total_objects, logger=None):
    """
    Write current results to the output file, with special sections for errors and performance metrics.
    """
    try:
        logger.debug(f"Writing results to {output_file} ({processed}/{total_objects} processed)")
        
        # Create a list of all entries that have 500 errors
        http_500_errors = []
        # Collect performance metrics data
        performance_data = {
            'by_size_category': {'small': [], 'medium': [], 'large': []},
            'by_derivative': {},
            'overall': []
        }
        
        # Track file sizes for correlation with errors and performance
        size_data = {
            'error_sizes': [],
            'success_sizes': [],
            'all_sizes': []
        }
        
        # Process all results
        for key, data in all_results.items():
            # Extract the file name from the key for better readability
            file_path = key.split(':')[1] if ':' in key else key
            derivative = data.get('derivative', 'unknown')
            
            # Initialize derivative in performance data if needed
            if derivative not in performance_data['by_derivative']:
                performance_data['by_derivative'][derivative] = []
            
            # Track file size information if available
            if 'originalSize' in data:
                original_size = data['originalSize']
                size_data['all_sizes'].append(original_size)
                
                # Track success vs error sizes
                if data.get('status') == 200:
                    size_data['success_sizes'].append(original_size)
                else:
                    size_data['error_sizes'].append(original_size)
                    
            # Process 500 errors
            if data.get('status') == 500:
                # Ray ID is already captured in error_entry, no need for other CF headers
                
                # Create a full URL that can be used for testing
                base_url = metadata.get('base_url', '')
                path = data.get('sourcePath', '')
                
                # Remove leading slash if base_url ends with slash
                if path.startswith('/') and base_url.endswith('/'):
                    path = path[1:]
                
                full_url = f"{base_url}{path}"
                request_url = f"{full_url}?imwidth={data.get('width')}"
                
                error_entry = {
                    'file': file_path,
                    'derivative': derivative,
                    'cf_ray': data.get('errorDetails', {}).get('cf_ray', 'Not found'),
                    'relative_path': path,
                    'full_url': full_url,
                    'request_url': request_url
                }
                
                # Add size information to error entry if available
                if 'originalSize' in data:
                    error_entry['original_size'] = {
                        'bytes': data['originalSize'],
                        'formatted': format_file_size(data['originalSize'])
                    }
                    if 'original_size' in data and 'size_category' in data['original_size']:
                        error_entry['size_category'] = data['original_size']['size_category']
                
                http_500_errors.append(error_entry)
            
            # Collect performance metrics if available
            if 'processingMetrics' in data and 'requestDuration' in data['processingMetrics']:
                perf_entry = {
                    'file': file_path,
                    'derivative': derivative,
                    'duration': data['processingMetrics'].get('totalDuration', 0),
                    'request_duration': data['processingMetrics'].get('requestDuration', 0),
                    'overhead': data['processingMetrics'].get('processingOverhead', 0)
                }
                
                # Add size information
                if 'originalSize' in data:
                    perf_entry['original_size'] = data['originalSize']
                    perf_entry['original_size_formatted'] = format_file_size(data['originalSize'])
                    
                    # Add to appropriate size category if known
                    if 'original_size' in data and 'size_category' in data['original_size']:
                        category = data['original_size']['size_category']
                        if category in performance_data['by_size_category']:
                            performance_data['by_size_category'][category].append(perf_entry)
                
                # Add size reduction metrics if successful
                if data.get('status') == 200 and 'sizeReduction' in data:
                    perf_entry['size_reduction'] = data['sizeReduction']
                    perf_entry['size_reduction_percent'] = data.get('sizeReductionPercent', 0)
                
                # Add to overall and derivative-specific lists
                performance_data['overall'].append(perf_entry)
                performance_data['by_derivative'][derivative].append(perf_entry)
        
        # Calculate performance statistics
        performance_stats = calculate_performance_stats(performance_data, size_data, logger)
        
        # Add performance stats to metadata
        metadata['performance_stats'] = performance_stats
        
        # Add 500 errors to metadata for easy access
        metadata['http_500_errors'] = {
            'total_count': len(http_500_errors),
            'errors': http_500_errors
        }
        
        # Add size correlation data to metadata
        if size_data['all_sizes']:
            size_metrics = {
                'min_size': min(size_data['all_sizes']) if size_data['all_sizes'] else 0,
                'max_size': max(size_data['all_sizes']) if size_data['all_sizes'] else 0,
                'avg_size': sum(size_data['all_sizes']) / len(size_data['all_sizes']) if size_data['all_sizes'] else 0,
                'total_size': sum(size_data['all_sizes']),
                'success_count': len(size_data['success_sizes']),
                'error_count': len(size_data['error_sizes']),
                'avg_success_size': sum(size_data['success_sizes']) / len(size_data['success_sizes']) if size_data['success_sizes'] else 0,
                'avg_error_size': sum(size_data['error_sizes']) / len(size_data['error_sizes']) if size_data['error_sizes'] else 0
            }
            metadata['size_metrics'] = size_metrics
        
        # Generate separate 500 error report file (JSON)
        error_report_file = output_file.replace('.json', '_500_errors.json')
        with open(error_report_file, 'w') as ef:
            json.dump({
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'total_errors': len(http_500_errors),
                'errors': http_500_errors
            }, ef, indent=2)
        
        # Generate performance report file (JSON)
        perf_report_file = output_file.replace('.json', '_performance.json')
        with open(perf_report_file, 'w') as pf:
            json.dump({
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'performance_data': performance_data,
                'performance_stats': performance_stats
            }, pf, indent=2)
        
        # Generate a text error report for easy reading
        text_report_file = output_file.replace('.json', '_500_errors.txt')
        with open(text_report_file, 'w') as tf:
            tf.write(f"HTTP 500 Error Report - Generated at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            tf.write(f"Total Errors: {len(http_500_errors)}\n")
            tf.write(f"Base URL: {metadata.get('base_url', 'Not specified')}\n\n")
            
            # Add size correlation to error report if available
            if 'size_metrics' in metadata:
                sm = metadata['size_metrics']
                tf.write("Error Size Correlation:\n")
                tf.write(f"  Average file size: {format_file_size(sm['avg_size'])}\n")
                tf.write(f"  Average size of files with errors: {format_file_size(sm['avg_error_size'])}\n")
                tf.write(f"  Average size of successful files: {format_file_size(sm['avg_success_size'])}\n")
                tf.write(f"  Error rate correlation: {'Higher for larger files' if sm['avg_error_size'] > sm['avg_success_size'] else 'Not size-dependent'}\n\n")
            
            # Group by derivative
            by_derivative = {}
            for error in http_500_errors:
                derivative = error['derivative']
                if derivative not in by_derivative:
                    by_derivative[derivative] = []
                by_derivative[derivative].append(error)
            
            # Write summary counts by derivative
            tf.write("Error Count by Derivative:\n")
            for derivative, errors in sorted(by_derivative.items()):
                tf.write(f"  {derivative}: {len(errors)}\n")
            
            # Group by size category if available
            if any('size_category' in error for error in http_500_errors):
                by_size = {'small': [], 'medium': [], 'large': []}
                for error in http_500_errors:
                    if 'size_category' in error:
                        category = error['size_category']
                        if category in by_size:
                            by_size[category].append(error)
                
                tf.write("\nError Count by Size Category:\n")
                for category, errors in sorted(by_size.items()):
                    if errors:
                        tf.write(f"  {category}: {len(errors)}\n")
            
            # Write the full list of errors
            tf.write("\nFull Error List:\n")
            for i, error in enumerate(http_500_errors, 1):
                tf.write(f"\n{i}. File: {error['file']}\n")
                tf.write(f"   Derivative: {error['derivative']}\n")
                
                # Add size info if available
                if 'original_size' in error:
                    tf.write(f"   Original Size: {error['original_size']['formatted']}\n")
                if 'size_category' in error:
                    tf.write(f"   Size Category: {error['size_category']}\n")
                
                tf.write(f"   FULL REQUEST URL for testing (copy this to browser or curl):\n")
                tf.write(f"   {error['request_url']}\n")
                tf.write(f"   Base URL used: {metadata.get('base_url', 'Not specified')}\n\n")
                tf.write(f"   Relative path: {error['relative_path']}\n")
                tf.write(f"   CF-Ray: {error['cf_ray']}\n")
                
                # No need to output Cloudflare headers in the report
        
        # Generate performance report text file
        perf_text_file = output_file.replace('.json', '_performance.txt')
        with open(perf_text_file, 'w') as pf:
            pf.write(f"Performance Report - Generated at {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Write overall performance summary
            if 'overall' in performance_stats:
                overall = performance_stats['overall']
                pf.write("Overall Performance:\n")
                pf.write(f"  Average processing time: {overall['avg_duration']:.2f} seconds\n")
                pf.write(f"  Median processing time: {overall['median_duration']:.2f} seconds\n")
                pf.write(f"  Min processing time: {overall['min_duration']:.2f} seconds\n")
                pf.write(f"  Max processing time: {overall['max_duration']:.2f} seconds\n")
                pf.write(f"  Standard deviation: {overall['std_deviation']:.2f} seconds\n\n")
            
            # Write performance by size category
            if 'by_size_category' in performance_stats:
                pf.write("Performance by Size Category:\n")
                for category, stats in performance_stats['by_size_category'].items():
                    if stats['count'] > 0:
                        pf.write(f"  {category.upper()} files ({stats['count']} processed):\n")
                        pf.write(f"    Average processing time: {stats['avg_duration']:.2f} seconds\n")
                        pf.write(f"    Median processing time: {stats['median_duration']:.2f} seconds\n")
                        pf.write(f"    Min processing time: {stats['min_duration']:.2f} seconds\n")
                        pf.write(f"    Max processing time: {stats['max_duration']:.2f} seconds\n")
                        pf.write(f"    Average size: {format_file_size(stats['avg_size'])}\n")
                        if stats.get('avg_reduction_percent'):
                            pf.write(f"    Average size reduction: {stats['avg_reduction_percent']:.1f}%\n")
                        pf.write("\n")
            
            # Write performance by derivative
            if 'by_derivative' in performance_stats:
                pf.write("Performance by Derivative:\n")
                for derivative, stats in performance_stats['by_derivative'].items():
                    if stats['count'] > 0:
                        pf.write(f"  {derivative} ({stats['count']} processed):\n")
                        pf.write(f"    Average processing time: {stats['avg_duration']:.2f} seconds\n")
                        pf.write(f"    Median processing time: {stats['median_duration']:.2f} seconds\n")
                        pf.write(f"    Average network time: {stats['avg_request_duration']:.2f} seconds\n")
                        pf.write(f"    Average processing overhead: {stats['avg_overhead']:.2f} seconds\n")
                        if stats.get('avg_reduction_percent'):
                            pf.write(f"    Average size reduction: {stats['avg_reduction_percent']:.1f}%\n")
                        pf.write("\n")
            
            # Write performance correlation findings
            if 'correlations' in performance_stats:
                pf.write("Performance Correlations:\n")
                correlations = performance_stats['correlations']
                if 'size_vs_duration' in correlations:
                    pf.write(f"  Size vs. Duration correlation: {correlations['size_vs_duration']:.2f}")
                    pf.write(f" {'(Strong positive correlation)' if correlations['size_vs_duration'] > 0.7 else '(Moderate correlation)' if correlations['size_vs_duration'] > 0.4 else '(Weak correlation)'}\n")
                if 'size_vs_reduction' in correlations:
                    pf.write(f"  Size vs. Reduction % correlation: {correlations['size_vs_reduction']:.2f}")
                    pf.write(f" {'(Strong correlation)' if abs(correlations['size_vs_reduction']) > 0.7 else '(Moderate correlation)' if abs(correlations['size_vs_reduction']) > 0.4 else '(Weak correlation)'}\n")
                pf.write("\n")
            
            # Performance recommendations
            pf.write("Performance Recommendations:\n")
            if 'recommendations' in performance_stats:
                for recommendation in performance_stats['recommendations']:
                    pf.write(f"  - {recommendation}\n")
        
        # Write the main results file
        with open(output_file, 'w') as f:
            output_data = {
                "metadata": metadata,
                "results": all_results
            }
            json.dump(output_data, f, indent=2)
        
        # Log a clear summary to console for visibility when processing is complete
        if processed == total_objects:
            # Log error summary if errors exist
            if http_500_errors:
                logger.info(f"\n{'=' * 40}")
                logger.info(f"HTTP 500 ERROR SUMMARY")
                logger.info(f"{'=' * 40}")
                logger.info(f"Found {len(http_500_errors)} HTTP 500 errors")
                logger.info(f"See detailed reports at:")
                logger.info(f"  - {text_report_file} (Text format - view this first)")
                logger.info(f"  - {error_report_file} (JSON format)")
                logger.info(f"{'=' * 40}\n")
            
            # Log performance summary
            if performance_stats.get('overall', {}).get('count', 0) > 0:
                logger.info(f"\n{'=' * 40}")
                logger.info(f"PERFORMANCE SUMMARY")
                logger.info(f"{'=' * 40}")
                overall = performance_stats['overall']
                logger.info(f"Average processing time: {overall['avg_duration']:.2f} seconds")
                
                # Log performance by size category
                if 'by_size_category' in performance_stats:
                    logger.info(f"\nBy Size Category:")
                    for category, stats in performance_stats['by_size_category'].items():
                        if stats['count'] > 0:
                            logger.info(f"  {category.upper()} files ({stats['count']}): {stats['avg_duration']:.2f} seconds avg")
                
                logger.info(f"\nSee detailed performance report at:")
                logger.info(f"  - {perf_text_file} (Text format)")
                logger.info(f"  - {perf_report_file} (JSON format)")
                logger.info(f"{'=' * 40}\n")
        
        logger.debug(f"Successfully wrote {len(all_results)} results to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write results to {output_file}: {str(e)}")

def calculate_performance_stats(performance_data, size_data, logger=None):
    """
    Calculate performance statistics from the collected performance data.
    """
    stats = {
        'overall': {'count': 0, 'avg_duration': 0, 'median_duration': 0, 'min_duration': 0, 'max_duration': 0, 'std_deviation': 0},
        'by_size_category': {},
        'by_derivative': {},
        'correlations': {},
        'recommendations': []
    }
    
    try:
        # Calculate overall statistics
        overall_data = performance_data['overall']
        if overall_data:
            durations = [item['duration'] for item in overall_data if 'duration' in item]
            if durations:
                stats['overall'] = {
                    'count': len(durations),
                    'avg_duration': sum(durations) / len(durations),
                    'median_duration': sorted(durations)[len(durations) // 2],
                    'min_duration': min(durations),
                    'max_duration': max(durations),
                    'std_deviation': (sum((d - (sum(durations) / len(durations))) ** 2 for d in durations) / len(durations)) ** 0.5
                }
                
                # Calculate size vs. duration correlation if we have size data
                size_duration_pairs = [(item.get('original_size', 0), item.get('duration', 0)) 
                                    for item in overall_data 
                                    if 'original_size' in item and 'duration' in item]
                
                if size_duration_pairs:
                    # Calculate Pearson correlation coefficient
                    sizes = [p[0] for p in size_duration_pairs]
                    durations = [p[1] for p in size_duration_pairs]
                    
                    if len(sizes) > 1 and sum(sizes) > 0 and sum(durations) > 0:
                        size_mean = sum(sizes) / len(sizes)
                        duration_mean = sum(durations) / len(durations)
                        
                        # Calculate covariance and standard deviations
                        covariance = sum((s - size_mean) * (d - duration_mean) for s, d in zip(sizes, durations))
                        size_variance = sum((s - size_mean) ** 2 for s in sizes)
                        duration_variance = sum((d - duration_mean) ** 2 for d in durations)
                        
                        # Calculate correlation
                        if size_variance > 0 and duration_variance > 0:
                            correlation = covariance / ((size_variance * duration_variance) ** 0.5)
                            stats['correlations']['size_vs_duration'] = correlation
                        
                        # Add recommendations based on correlation
                        if 'size_vs_duration' in stats['correlations']:
                            correlation = stats['correlations']['size_vs_duration']
                            if correlation > 0.7:
                                stats['recommendations'].append(
                                    "Strong correlation between file size and processing time. Consider optimizing large file processing.")
                            elif correlation > 0.4:
                                stats['recommendations'].append(
                                    "Moderate correlation between file size and processing time. Size-based optimization may be beneficial.")
        
        # Calculate statistics by size category
        for category, items in performance_data['by_size_category'].items():
            if items:
                durations = [item['duration'] for item in items if 'duration' in item]
                sizes = [item.get('original_size', 0) for item in items if 'original_size' in item]
                reductions = [item.get('size_reduction_percent', 0) for item in items 
                            if 'size_reduction_percent' in item and item['size_reduction_percent'] is not None]
                
                if durations:
                    category_stats = {
                        'count': len(durations),
                        'avg_duration': sum(durations) / len(durations),
                        'median_duration': sorted(durations)[len(durations) // 2],
                        'min_duration': min(durations),
                        'max_duration': max(durations)
                    }
                    
                    if sizes:
                        category_stats['avg_size'] = sum(sizes) / len(sizes)
                    
                    if reductions:
                        category_stats['avg_reduction_percent'] = sum(reductions) / len(reductions)
                    
                    stats['by_size_category'][category] = category_stats
        
        # Calculate statistics by derivative
        for derivative, items in performance_data['by_derivative'].items():
            if items:
                durations = [item['duration'] for item in items if 'duration' in item]
                request_durations = [item['request_duration'] for item in items if 'request_duration' in item]
                overheads = [item['overhead'] for item in items if 'overhead' in item]
                reductions = [item.get('size_reduction_percent', 0) for item in items 
                            if 'size_reduction_percent' in item and item['size_reduction_percent'] is not None]
                
                if durations:
                    derivative_stats = {
                        'count': len(durations),
                        'avg_duration': sum(durations) / len(durations),
                        'median_duration': sorted(durations)[len(durations) // 2]
                    }
                    
                    if request_durations:
                        derivative_stats['avg_request_duration'] = sum(request_durations) / len(request_durations)
                    
                    if overheads:
                        derivative_stats['avg_overhead'] = sum(overheads) / len(overheads)
                    
                    if reductions:
                        derivative_stats['avg_reduction_percent'] = sum(reductions) / len(reductions)
                    
                    stats['by_derivative'][derivative] = derivative_stats
        
        # Generate performance recommendations
        if stats['overall']['count'] > 0:
            # Check for high standard deviation
            if stats['overall']['std_deviation'] > stats['overall']['avg_duration'] * 0.5:
                stats['recommendations'].append(
                    "High variability in processing times. Consider implementing more predictable processing strategies.")
            
            # Check for performance differences between size categories
            if all(category in stats['by_size_category'] for category in ['small', 'medium', 'large']):
                small_avg = stats['by_size_category']['small'].get('avg_duration', 0)
                large_avg = stats['by_size_category']['large'].get('avg_duration', 0)
                
                if large_avg > small_avg * 3 and stats['by_size_category']['large']['count'] > 2:
                    stats['recommendations'].append(
                        f"Large files take {large_avg/small_avg:.1f}x longer to process than small files. Consider dedicated workers for large files.")
            
            # Check derivative processing differences
            derivative_times = [(name, data.get('avg_duration', 0)) for name, data in stats['by_derivative'].items() if data.get('count', 0) > 2]
            if derivative_times:
                slowest = max(derivative_times, key=lambda x: x[1])
                fastest = min(derivative_times, key=lambda x: x[1])
                
                if slowest[1] > fastest[1] * 2:
                    stats['recommendations'].append(
                        f"The '{slowest[0]}' derivative is significantly slower to process ({slowest[1]:.1f}s vs {fastest[1]:.1f}s for '{fastest[0]}'). Consider optimizing this derivative specifically.")
        
        return stats
    except Exception as e:
        if logger:
            logger.error(f"Error calculating performance statistics: {str(e)}")
        return {
            'overall': {'count': 0, 'error': str(e)},
            'recommendations': ["Unable to calculate performance statistics due to an error."]
        }

def print_summary_statistics(all_results, elapsed_time, total_objects, logger=None):
    """Print detailed summary statistics."""
    logger.info("=" * 80)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 80)
    
    # Basic statistics
    successful_count = sum(1 for d in all_results.values() 
                        if isinstance(d.get('status'), int) and d.get('status') == 200)
    chunked_count = sum(1 for d in all_results.values() if d.get('isChunked'))
    total_size = sum(d.get('actualTotalVideoSize', 0) for d in all_results.values())
    
    # Calculate statistics by derivative
    derivative_stats = {}
    status_codes = {}
    content_types = {}
    error_details = {}  # For tracking specific error patterns
    
    for key, data in all_results.items():
        derivative = data.get('derivative')
        
        # Track derivative stats
        if derivative not in derivative_stats:
            derivative_stats[derivative] = {
                'count': 0, 
                'success': 0, 
                'chunked': 0, 
                'total_size': 0,
                'avg_size': 0,
                'min_size': float('inf'),
                'max_size': 0,
                'total_duration': 0,
                'avg_duration': 0
            }
        
        derivative_stats[derivative]['count'] += 1
        
        # Track status codes and detailed error patterns
        status = data.get('status')
        if status not in status_codes:
            status_codes[status] = 0
        status_codes[status] += 1
        
        # Track error details for non-200 responses
        if isinstance(status, int) and status != 200:
            # Create a key based on derivative and status code
            error_key = f"{derivative}_{status}"
            if error_key not in error_details:
                error_details[error_key] = {
                    'count': 0,
                    'examples': []
                }
            error_details[error_key]['count'] += 1
            
            # Store up to 5 examples of each error type
            if len(error_details[error_key]['examples']) < 5:
                # Extract the file name from the key for cleaner reporting
                file_path = key.split(':')[1] if ':' in key else key
                error_details[error_key]['examples'].append(file_path)
        
        # Track content types
        content_type = data.get('contentType', '')
        if content_type not in content_types:
            content_types[content_type] = 0
        content_types[content_type] += 1
        
        # Track success metrics
        if isinstance(status, int) and status == 200:
            derivative_stats[derivative]['success'] += 1
            size = data.get('actualTotalVideoSize', 0)
            derivative_stats[derivative]['total_size'] += size
            derivative_stats[derivative]['min_size'] = min(derivative_stats[derivative]['min_size'], size)
            derivative_stats[derivative]['max_size'] = max(derivative_stats[derivative]['max_size'], size)
            
            if 'duration' in data:
                derivative_stats[derivative]['total_duration'] += data['duration']
            
        if data.get('isChunked'):
            derivative_stats[derivative]['chunked'] += 1
    
    # Calculate averages
    for derivative, stats in derivative_stats.items():
        if stats['success'] > 0:
            stats['avg_size'] = stats['total_size'] / stats['success']
            stats['avg_duration'] = stats['total_duration'] / stats['success']
        # Reset min_size if no successful requests
        if stats['min_size'] == float('inf'):
            stats['min_size'] = 0
    
    # Print performance metrics
    avg_time = elapsed_time / total_objects if total_objects > 0 else 0
    logger.info(f"Performance:")
    logger.info(f"  Total processing time: {elapsed_time:.2f} seconds")
    logger.info(f"  Average time per object: {avg_time:.2f} seconds")
    logger.info(f"  Throughput: {total_objects / elapsed_time:.2f} objects/second")
    
    # Print overall metrics
    logger.info(f"Overall Statistics:")
    logger.info(f"  Objects processed: {total_objects}")
    logger.info(f"  Derivatives processed: {len(all_results)}")
    logger.info(f"  Success rate: {successful_count}/{len(all_results)} ({successful_count/len(all_results)*100:.1f}%)")
    logger.info(f"  Chunked files: {chunked_count}/{len(all_results)} ({chunked_count/len(all_results)*100:.1f}%)")
    logger.info(f"  Total size of all video assets: {total_size/1024/1024:.2f} MB")
    
    # Print status code distribution
    logger.info(f"Status Code Distribution:")
    for status, count in sorted(status_codes.items(), key=lambda x: str(x[0])):
        logger.info(f"  {status}: {count} ({count/len(all_results)*100:.1f}%)")
    
    # Print detailed error information if errors exist
    if error_details:
        logger.info(f"\nDetailed Error Analysis:")
        
        # Analyze 500 errors specifically
        http_500_errors = {k: v for k, v in error_details.items() if k.endswith('_500')}
        if http_500_errors:
            logger.info(f"HTTP 500 Error Analysis:")
            
            # Check if all 500 errors are associated with specific derivatives
            derivative_counts = {}
            for error_key in http_500_errors.keys():
                derivative = error_key.split('_')[0]
                if derivative not in derivative_counts:
                    derivative_counts[derivative] = 0
                derivative_counts[derivative] += http_500_errors[error_key]['count']
            
            total_500s = sum(v['count'] for v in http_500_errors.values())
            logger.info(f"  Total HTTP 500 errors: {total_500s}")
            logger.info(f"  Distribution by derivative:")
            for derivative, count in derivative_counts.items():
                logger.info(f"    {derivative}: {count} ({count/total_500s*100:.1f}%)")
            
            # Log examples of the files that encountered 500 errors
            logger.info(f"  Example files with 500 errors (max 10):")
            examples_shown = 0
            for error_key, details in http_500_errors.items():
                derivative = error_key.split('_')[0]
                for example in details['examples']:
                    logger.info(f"    - {derivative}: {example}")
                    examples_shown += 1
                    if examples_shown >= 10:
                        break
                if examples_shown >= 10:
                    break
        
        # Print all error types
        logger.info(f"\nAll Error Types:")
        for error_key, details in sorted(error_details.items()):
            derivative, status = error_key.split('_')
            logger.info(f"  Error pattern: {derivative} derivative with status {status}")
            logger.info(f"  Count: {details['count']}")
            if details['examples']:
                logger.info(f"  Example files:")
                for example in details['examples']:
                    logger.info(f"    - {example}")
            logger.info("")  # Add blank line for readability
    
    # Print content type distribution
    logger.info(f"Content Type Distribution:")
    for content_type, count in sorted(content_types.items()):
        logger.info(f"  {content_type or 'Unknown'}: {count}")
    
    # Print derivative-specific stats
    logger.info(f"Derivative Statistics:")
    for derivative, stats in sorted(derivative_stats.items()):
        logger.info(f"  {derivative}:")
        logger.info(f"    Requests: {stats['count']}")
        logger.info(f"    Success: {stats['success']}/{stats['count']} ({stats['success']/stats['count']*100:.1f}%)")
        logger.info(f"    Chunked: {stats['chunked']}/{stats['count']} ({stats['chunked']/stats['count']*100:.1f}%)")
        logger.info(f"    Size: {stats['total_size']/1024/1024:.2f} MB total, "
                   f"{stats['avg_size']/1024/1024:.2f} MB average")
        logger.info(f"    Size range: {stats['min_size']/1024/1024:.2f} - {stats['max_size']/1024/1024:.2f} MB")
        if stats['avg_duration'] > 0:
            logger.info(f"    Average request time: {stats['avg_duration']:.2f} seconds")
    
    logger.info("=" * 80)

def load_kv_data(kv_json_path, logger):
    """Load and parse the Cloudflare KV JSON data."""
    logger.info(f"Loading Cloudflare KV data from: {kv_json_path}")
    try:
        with open(kv_json_path, 'r') as f:
            kv_data = json.load(f)
        
        # Basic validation
        if not isinstance(kv_data, dict) or 'keys' not in kv_data:
            logger.error(f"Invalid KV JSON format. Expected object with 'keys' array.")
            return None
        
        logger.info(f"Successfully loaded KV data with {len(kv_data['keys'])} keys")
        return kv_data
    except Exception as e:
        logger.error(f"Failed to load KV data: {str(e)}")
        return None

def load_transform_results(transform_json_path, logger):
    """Load and parse the transformation results JSON data."""
    logger.info(f"Loading transformation results from: {transform_json_path}")
    try:
        with open(transform_json_path, 'r') as f:
            transform_data = json.load(f)
        
        # Basic validation
        if not isinstance(transform_data, dict) or 'results' not in transform_data:
            logger.error(f"Invalid transform results format. Expected object with 'results' property.")
            return None
        
        logger.info(f"Successfully loaded transform results with {len(transform_data['results'])} entries")
        return transform_data
    except Exception as e:
        logger.error(f"Failed to load transform results: {str(e)}")
        return None

def extract_video_path_and_derivative(key_name):
    """Extract the video path and derivative from a KV key name."""
    # Pattern: video:path/to/file.mp4:derivative=desktop
    match = re.match(r'video:(.+):derivative=(\w+)(?:_chunk_\d+)?$', key_name)
    if match:
        path = match.group(1)
        derivative = match.group(2)
        is_chunk = '_chunk_' in key_name
        chunk_index = None
        if is_chunk:
            chunk_match = re.search(r'_chunk_(\d+)$', key_name)
            if chunk_match:
                chunk_index = int(chunk_match.group(1))
        return path, derivative, is_chunk, chunk_index
    return None, None, False, None

def calculate_total_chunk_size(kv_data, parent_key):
    """Calculate the total size of all chunks for a parent key."""
    total_size = 0
    chunk_pattern = re.escape(parent_key) + r'_chunk_\d+$'
    
    for key_entry in kv_data['keys']:
        key_name = key_entry['name']
        if re.match(chunk_pattern, key_name):
            chunk_size = key_entry['metadata'].get('size', 0)
            total_size += chunk_size
    
    return total_size

def compare_transformation_with_kv(transform_results, kv_data, logger):
    """Compare transformation results with Cloudflare KV data."""
    logger.info("Starting comparison between transformation results and KV data")
    
    # Organize KV data for easier lookup
    kv_keys = {}
    chunk_parents = set()
    
    for key_entry in kv_data['keys']:
        key_name = key_entry['name']
        video_path, derivative, is_chunk, chunk_index = extract_video_path_and_derivative(key_name)
        
        if video_path and derivative:
            if is_chunk:
                # This is a chunk, track its parent
                parent_key = f"video:{video_path}:derivative={derivative}"
                chunk_parents.add(parent_key)
            else:
                # Store primary keys for comparison
                kv_keys[key_name] = {
                    'metadata': key_entry['metadata'],
                    'chunks': [],
                    'has_chunks': key_entry['metadata'].get('isChunked', False)
                }
    
    # Collect chunk information for chunked files
    for parent_key in chunk_parents:
        if parent_key in kv_keys:
            kv_keys[parent_key]['total_chunk_size'] = calculate_total_chunk_size(kv_data, parent_key)
    
    # Prepare results structure
    comparison_results = {
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
        'summary': {
            'keys_in_kv': len(kv_keys),
            'keys_in_transform': len(transform_results),
            'matches': 0,
            'mismatches': 0,
            'only_in_kv': 0,
            'only_in_transform': 0
        },
        'matches': [],
        'mismatches': [],
        'only_in_kv': [],
        'only_in_transform': []
    }
    
    # Compare transformation results with KV data
    for key, transform_data in transform_results.items():
        if key in kv_keys:
            kv_metadata = kv_keys[key]['metadata']
            
            # Determine what size to compare
            transform_size = transform_data.get('actualTotalVideoSize', 0)
            
            # Determine KV size (use chunk total if available)
            if kv_keys[key].get('has_chunks', False) and 'total_chunk_size' in kv_keys[key]:
                kv_size = kv_keys[key]['total_chunk_size']
            else:
                kv_size = kv_metadata.get('actualTotalVideoSize', 0)
            
            # Calculate size difference as percentage
            size_diff = 0
            size_diff_percent = 0
            if kv_size > 0:
                size_diff = transform_size - kv_size
                size_diff_percent = (size_diff / kv_size) * 100
            
            # Check if this is a match (allowing 1% difference)
            is_match = abs(size_diff_percent) <= 1
            
            match_data = {
                'key': key,
                'transform_size': transform_size,
                'kv_size': kv_size,
                'size_diff': size_diff,
                'size_diff_percent': size_diff_percent,
                'transform_content_type': transform_data.get('contentType', ''),
                'kv_content_type': kv_metadata.get('contentType', ''),
                'transform_is_chunked': transform_data.get('isChunked', False),
                'kv_is_chunked': kv_metadata.get('isChunked', False)
            }
            
            if is_match:
                comparison_results['matches'].append(match_data)
                comparison_results['summary']['matches'] += 1
            else:
                comparison_results['mismatches'].append(match_data)
                comparison_results['summary']['mismatches'] += 1
            
            # Mark this KV key as processed
            kv_keys[key]['processed'] = True
        else:
            # This key only exists in transformation results
            comparison_results['only_in_transform'].append({
                'key': key,
                'transform_size': transform_data.get('actualTotalVideoSize', 0),
                'transform_content_type': transform_data.get('contentType', ''),
                'transform_is_chunked': transform_data.get('isChunked', False)
            })
            comparison_results['summary']['only_in_transform'] += 1
    
    # Find keys that exist only in KV
    for key, kv_info in kv_keys.items():
        if not kv_info.get('processed', False):
            kv_metadata = kv_info['metadata']
            
            # Determine KV size
            if kv_info.get('has_chunks', False) and 'total_chunk_size' in kv_info:
                kv_size = kv_info['total_chunk_size']
            else:
                kv_size = kv_metadata.get('actualTotalVideoSize', 0)
            
            comparison_results['only_in_kv'].append({
                'key': key,
                'kv_size': kv_size,
                'kv_content_type': kv_metadata.get('contentType', ''),
                'kv_is_chunked': kv_metadata.get('isChunked', False)
            })
            comparison_results['summary']['only_in_kv'] += 1
    
    return comparison_results

def print_comparison_summary(comparison_results, logger):
    """Print a detailed summary of the comparison results."""
    logger.info("=" * 80)
    logger.info("COMPARISON SUMMARY")
    logger.info("=" * 80)
    
    summary = comparison_results['summary']
    total_keys = summary['keys_in_kv'] + summary['only_in_transform']
    
    # Overall statistics
    logger.info(f"Total unique keys: {total_keys}")
    logger.info(f"Keys in KV: {summary['keys_in_kv']}")
    logger.info(f"Keys in transform results: {summary['keys_in_transform']}")
    
    match_rate = (summary['matches'] / total_keys) * 100 if total_keys > 0 else 0
    logger.info(f"Matching keys: {summary['matches']} ({match_rate:.1f}%)")
    logger.info(f"Mismatched keys: {summary['mismatches']}")
    logger.info(f"Only in KV: {summary['only_in_kv']}")
    logger.info(f"Only in transform results: {summary['only_in_transform']}")
    
    # Size difference statistics for mismatches
    if summary['mismatches'] > 0:
        logger.info("\nSize difference analysis for mismatches:")
        
        size_diffs = [m['size_diff'] for m in comparison_results['mismatches']]
        percent_diffs = [m['size_diff_percent'] for m in comparison_results['mismatches']]
        
        avg_diff = sum(size_diffs) / len(size_diffs)
        avg_percent = sum(percent_diffs) / len(percent_diffs)
        max_diff = max(percent_diffs, key=abs)
        
        logger.info(f"  Average size difference: {avg_diff:.0f} bytes ({avg_percent:.2f}%)")
        logger.info(f"  Maximum size difference: {max_diff:.2f}%")
        
        # Create a histogram of percent differences
        ranges = [(-100, -10), (-10, -5), (-5, -1), (-1, 1), (1, 5), (5, 10), (10, 100)]
        histogram = {f"{r[0]}% to {r[1]}%": 0 for r in ranges}
        
        for diff in percent_diffs:
            for r in ranges:
                if r[0] <= diff < r[1]:
                    histogram[f"{r[0]}% to {r[1]}%"] += 1
                    break
        
        logger.info("\nDistribution of percentage differences:")
        for range_str, count in histogram.items():
            if count > 0:
                percent = (count / len(percent_diffs)) * 100
                logger.info(f"  {range_str}: {count} ({percent:.1f}%)")
    
    # Show some examples of mismatches if they exist
    if summary['mismatches'] > 0:
        logger.info("\nExample mismatches (first 5):")
        mismatches_to_show = min(5, len(comparison_results['mismatches']))
        
        headers = ["Key", "Transform Size", "KV Size", "Diff %"]
        table_data = []
        
        for i in range(mismatches_to_show):
            mismatch = comparison_results['mismatches'][i]
            transform_size_mb = mismatch['transform_size'] / (1024 * 1024)
            kv_size_mb = mismatch['kv_size'] / (1024 * 1024)
            
            # Only show the last part of long keys
            key_parts = mismatch['key'].split(':')
            short_key = f"...:{key_parts[-1]}" if len(key_parts) > 2 else mismatch['key']
            
            table_data.append([
                short_key,
                f"{transform_size_mb:.2f} MB",
                f"{kv_size_mb:.2f} MB",
                f"{mismatch['size_diff_percent']:.2f}%"
            ])
        
        logger.info(tabulate(table_data, headers, tablefmt="grid"))
    
    logger.info("=" * 80)

def generate_markdown_summary(comparison_results, logger):
    """Generate a detailed markdown summary of the comparison results."""
    summary = comparison_results['summary']
    total_keys = summary['keys_in_kv'] + summary['only_in_transform']
    match_rate = (summary['matches'] / total_keys) * 100 if total_keys > 0 else 0
    
    # Calculate total sizes
    total_size_kv = sum(m.get('kv_size', 0) for m in comparison_results.get('matches', []))
    total_size_transform = sum(m.get('transform_size', 0) for m in comparison_results.get('matches', []))
    
    # Determine verification status
    verification_status = "✅ SUCCESSFUL" if summary['mismatches'] == 0 and summary['only_in_kv'] == 0 else "❌ FAILED"
    
    # Format timestamp for the report
    timestamp = comparison_results.get('timestamp', time.strftime("%Y-%m-%d %H:%M:%S"))
    
    # Build the markdown content
    md = [
        "# Video Asset Transformation Verification Report",
        f"Generated: {timestamp}",
        "",
        f"## Verification Status: {verification_status}",
        "",
        "## Summary",
        f"- **Total unique keys**: {total_keys}",
        f"- **Keys in KV**: {summary['keys_in_kv']}",
        f"- **Keys in transform results**: {summary['keys_in_transform']}",
        f"- **Match rate**: {summary['matches']}/{total_keys} ({match_rate:.1f}%)",
        "",
        "## Size Verification",
        f"- **Total size in KV**: {total_size_kv/1024/1024:.2f} MB",
        f"- **Total size in transform results**: {total_size_transform/1024/1024:.2f} MB",
        f"- **Size difference**: {(total_size_transform - total_size_kv)/1024/1024:.2f} MB ({(total_size_transform - total_size_kv)/total_size_kv*100 if total_size_kv > 0 else 0:.3f}%)",
    ]
    
    # Add mismatch information if any exist
    if summary['mismatches'] > 0:
        md.extend([
            "",
            "## Mismatches",
            f"**{summary['mismatches']} keys have size mismatches:**",
            "",
            "| Key | Transform Size (MB) | KV Size (MB) | Difference (%) |",
            "| --- | ----------------- | ----------- | ------------- |"
        ])
        
        # Add up to 10 mismatches
        for i, mismatch in enumerate(comparison_results['mismatches'][:10]):
            transform_size_mb = mismatch['transform_size'] / (1024 * 1024)
            kv_size_mb = mismatch['kv_size'] / (1024 * 1024)
            md.append(f"| {mismatch['key']} | {transform_size_mb:.2f} | {kv_size_mb:.2f} | {mismatch['size_diff_percent']:.2f} |")
        
        if len(comparison_results['mismatches']) > 10:
            md.append(f"| ... and {len(comparison_results['mismatches']) - 10} more | | | |")
    
    # Add missing keys information
    if summary['only_in_kv'] > 0:
        md.extend([
            "",
            "## Keys Only in KV",
            f"**{summary['only_in_kv']} keys exist only in KV:**",
            "",
            "| Key | KV Size (MB) |",
            "| --- | ----------- |"
        ])
        
        # Add up to 10 KV-only keys
        for i, kv_only in enumerate(comparison_results['only_in_kv'][:10]):
            kv_size_mb = kv_only['kv_size'] / (1024 * 1024)
            md.append(f"| {kv_only['key']} | {kv_size_mb:.2f} |")
        
        if len(comparison_results['only_in_kv']) > 10:
            md.append(f"| ... and {len(comparison_results['only_in_kv']) - 10} more | |")
    
    if summary['only_in_transform'] > 0:
        md.extend([
            "",
            "## Keys Only in Transform Results",
            f"**{summary['only_in_transform']} keys exist only in transform results:**",
            "",
            "| Key | Transform Size (MB) |",
            "| --- | ------------------ |"
        ])
        
        # Add up to 10 transform-only keys
        for i, transform_only in enumerate(comparison_results['only_in_transform'][:10]):
            transform_size_mb = transform_only['transform_size'] / (1024 * 1024)
            md.append(f"| {transform_only['key']} | {transform_size_mb:.2f} |")
        
        if len(comparison_results['only_in_transform']) > 10:
            md.append(f"| ... and {len(comparison_results['only_in_transform']) - 10} more | |")
    
    # Add verification conclusion
    md.extend([
        "",
        "## Conclusion",
    ])
    
    if verification_status == "✅ SUCCESSFUL":
        md.append(f"All {summary['matches']} keys found in KV store match the transformation results exactly.")
        if summary['only_in_transform'] > 0:
            md.append(f"There are {summary['only_in_transform']} additional keys in transformation results that are not yet in KV.")
    else:
        if summary['mismatches'] > 0:
            md.append(f"❌ {summary['mismatches']} keys have size mismatches between KV and transformation results.")
        if summary['only_in_kv'] > 0:
            md.append(f"❌ {summary['only_in_kv']} keys exist in KV but were not found in transformation results.")
    
    return "\n".join(md)

def generate_json_summary(comparison_results):
    """Generate a simplified JSON summary of the comparison results."""
    summary = comparison_results['summary']
    total_keys = summary['keys_in_kv'] + summary['only_in_transform']
    match_rate = (summary['matches'] / total_keys) * 100 if total_keys > 0 else 0
    
    # Calculate total sizes
    total_size_kv = sum(m.get('kv_size', 0) for m in comparison_results.get('matches', []))
    total_size_transform = sum(m.get('transform_size', 0) for m in comparison_results.get('matches', []))
    
    # Determine verification status
    verification_successful = summary['mismatches'] == 0 and summary['only_in_kv'] == 0
    
    # Create a simplified summary object
    json_summary = {
        "timestamp": comparison_results.get('timestamp', time.strftime("%Y-%m-%d %H:%M:%S")),
        "verification_successful": verification_successful,
        "summary": {
            "total_unique_keys": total_keys,
            "keys_in_kv": summary['keys_in_kv'],
            "keys_in_transform": summary['keys_in_transform'],
            "matching_keys": summary['matches'],
            "match_rate_percent": round(match_rate, 2),
            "mismatched_keys": summary['mismatches'],
            "only_in_kv": summary['only_in_kv'],
            "only_in_transform": summary['only_in_transform']
        },
        "size_verification": {
            "total_size_kv_bytes": total_size_kv,
            "total_size_kv_mb": round(total_size_kv/1024/1024, 2),
            "total_size_transform_bytes": total_size_transform,
            "total_size_transform_mb": round(total_size_transform/1024/1024, 2),
            "size_difference_bytes": total_size_transform - total_size_kv,
            "size_difference_mb": round((total_size_transform - total_size_kv)/1024/1024, 2),
            "size_difference_percent": round((total_size_transform - total_size_kv)/total_size_kv*100 if total_size_kv > 0 else 0, 3)
        },
        "mismatch_examples": [],
        "kv_only_examples": [],
        "transform_only_examples": []
    }
    
    # Add examples of mismatches (limited to 5)
    if summary['mismatches'] > 0:
        for mismatch in comparison_results['mismatches'][:5]:
            json_summary["mismatch_examples"].append({
                "key": mismatch['key'],
                "transform_size_bytes": mismatch['transform_size'],
                "transform_size_mb": round(mismatch['transform_size']/1024/1024, 2),
                "kv_size_bytes": mismatch['kv_size'],
                "kv_size_mb": round(mismatch['kv_size']/1024/1024, 2),
                "difference_percent": round(mismatch['size_diff_percent'], 2)
            })
    
    # Add examples of keys only in KV (limited to 5)
    if summary['only_in_kv'] > 0:
        for kv_only in comparison_results['only_in_kv'][:5]:
            json_summary["kv_only_examples"].append({
                "key": kv_only['key'],
                "kv_size_bytes": kv_only['kv_size'],
                "kv_size_mb": round(kv_only['kv_size']/1024/1024, 2)
            })
    
    # Add examples of keys only in transform results (limited to 5)
    if summary['only_in_transform'] > 0:
        for transform_only in comparison_results['only_in_transform'][:5]:
            json_summary["transform_only_examples"].append({
                "key": transform_only['key'],
                "transform_size_bytes": transform_only['transform_size'],
                "transform_size_mb": round(transform_only['transform_size']/1024/1024, 2)
            })
    
    return json_summary

def save_summary(comparison_results, output_path, format_type, logger):
    """Save the comparison summary in the specified format."""
    try:
        if format_type == 'markdown':
            content = generate_markdown_summary(comparison_results, logger)
        else:  # json
            content = generate_json_summary(comparison_results)
            
        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
            
        # Write to file
        if format_type == 'markdown':
            with open(output_path, 'w') as f:
                f.write(content)
        else:  # json
            with open(output_path, 'w') as f:
                json.dump(content, f, indent=2)
                
        logger.info(f"Saved {format_type} summary to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save summary: {str(e)}")
        return False

def generate_error_report_from_results(results_file, logger):
    """Generate or regenerate error reports from an existing results file."""
    logger.info(f"Generating error reports from existing results file: {results_file}")
    
    try:
        # Load the results file
        with open(results_file, 'r') as f:
            data = json.load(f)
        
        if 'results' not in data:
            logger.error(f"Invalid results file format. Expected 'results' key.")
            return False
            
        # Extract metadata for the report generation
        metadata = data.get('metadata', {})
        metadata['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Make sure base_url is included in the metadata for error reports
        if 'base_url' not in metadata:
            logger.warning("Base URL not found in metadata for error report generation")
            # Try to use one from command line arguments if available
            import sys
            for i, arg in enumerate(sys.argv):
                if arg == '--base-url' and i + 1 < len(sys.argv):
                    metadata['base_url'] = sys.argv[i + 1]
                    logger.info(f"Using base URL from command line: {metadata['base_url']}")
        
        # Call the write_results_to_file function which will generate the error reports
        processed = metadata.get('processed', 0)
        total = metadata.get('total', processed)
        write_results_to_file(data['results'], metadata, results_file, processed, total, logger)
        
        return True
    except Exception as e:
        logger.error(f"Failed to generate error reports: {str(e)}")
        return False

# Global variables for graceful shutdown
shutdown_event = None
executor = None
logger = None

# Other global variables
executors = {}

def signal_handler(sig, frame):
    """Handle interrupt signals for graceful shutdown."""
    global shutdown_event, logger
    if logger:
        logger.info("\nReceived interrupt signal. Initiating graceful shutdown...")
        logger.info("Please wait while running tasks complete (this may take a few seconds)")
    if shutdown_event:
        shutdown_event.set()
    else:
        # If shutdown_event is not initialized yet, exit immediately
        sys.exit(0)

def cleanup_resources():
    """Clean up resources before exiting."""
    global executor, executors, logger
    
    # Clean up the main executor if it exists
    if executor and not executor._shutdown:
        if logger:
            logger.info("Shutting down thread pool executor...")
        executor.shutdown(wait=False)
        if logger:
            logger.info("Thread pool executor shutdown complete.")
            
    # Clean up any category-specific executors
    for category, exe in executors.items():
        if exe and not exe._shutdown:
            if logger:
                logger.info(f"Shutting down {category} executor...")
            exe.shutdown(wait=False)

def main():
    """Main function."""
    global shutdown_event, executor, executors, logger
    
    # Set up shutdown event
    shutdown_event = threading.Event()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    args = parse_arguments()
    
    # Set up logging
    logger = setup_logging(args.verbose)
    
    # Log script startup and configuration
    logger.info(f"=== Video Transformation Script Started ===")
    logger.info(f"Configuration:")
    for arg, value in vars(args).items():
        logger.info(f"  {arg}: {value}")
    
    # Check if we're only generating error reports
    if args.generate_error_report:
        if not os.path.exists(args.output):
            logger.error(f"Results file not found: {args.output}")
            return
            
        success = generate_error_report_from_results(args.output, logger)
        if success:
            logger.info(f"Successfully generated error reports from {args.output}")
        else:
            logger.error(f"Failed to generate error reports from {args.output}")
        return
    
    # Check if we're only doing comparison
    if args.only_compare:
        if not args.compare or not os.path.exists(args.compare):
            logger.error(f"Comparison file not found: {args.compare}")
            return
        
        if not os.path.exists(args.output):
            logger.error(f"Transform results file not found: {args.output}")
            return
        
        # Load the two data sources
        kv_data = load_kv_data(args.compare, logger)
        transform_data = load_transform_results(args.output, logger)
        
        if not kv_data or not transform_data:
            logger.error("Failed to load required data for comparison")
            return
        
        # Run comparison
        comparison_results = compare_transformation_with_kv(transform_data['results'], kv_data, logger)
        
        # Print summary
        print_comparison_summary(comparison_results, logger)
        
        # Save comparison results
        with open(args.comparison_output, 'w') as f:
            json.dump(comparison_results, f, indent=2)
        
        logger.info(f"Comparison results saved to {args.comparison_output}")
        
        # Generate and save summary in requested format
        save_summary(comparison_results, args.summary_output, args.summary_format, logger)
        
        return
    
    # Verify required parameters for processing mode
    if not args.remote or not args.bucket:
        logger.error("Missing required parameters: --remote and --bucket are required")
        return
    
    # If list-files mode is enabled, only list files and generate size report
    if args.list_files:
        logger.info(f"Running in list-files mode to generate size report")
        
        # List objects in the bucket/directory with sizes
        logger.info(f"Listing objects from {args.remote}:{args.bucket}/{args.directory}")
        objects = list_objects(
            args.remote,
            args.bucket, 
            args.directory, 
            args.extension, 
            args.limit, 
            logger,
            args.use_aws_cli,
            get_sizes=True  # Make sure we get sizes from list_objects
        )
        
        if not objects:
            logger.warning(f"No {args.extension} objects found. Check your bucket and directory settings.")
            return
        
        # Convert objects with path/size to the format expected by generate_size_report
        file_sizes = [(obj['path'], obj['size']) for obj in objects]
        
        # Generate size report
        generate_size_report(
            file_sizes,
            args.size_threshold,
            args.size_report_output,
            logger
        )
        
        logger.info(f"Size report generation completed successfully")
        logger.info(f"Report written to {args.size_report_output}")
        return
    
    # For normal processing mode, base_url is required
    if not args.base_url:
        logger.error("Missing required parameter: --base-url is required for processing mode")
        return
        
    derivatives = [d.strip() for d in args.derivatives.split(',')]
    logger.info(f"Processing derivatives: {derivatives}")
    
    try:
        # List objects in the bucket/directory
        logger.info(f"Listing objects from {args.remote}:{args.bucket}/{args.directory}")
        objects = list_objects(
            args.remote,
            args.bucket, 
            args.directory, 
            args.extension, 
            args.limit, 
            logger,
            args.use_aws_cli
        )
        
        if not objects:
            logger.warning(f"No {args.extension} objects found. Check your bucket and directory settings.")
            return
        
        # Process objects concurrently with size-based optimization
        total_objects = len(objects)
        processed = 0
        all_results = {}
        
        # Check if we're using size-based optimization
        using_size_based_optimization = args.optimize_by_size
        
        # Log processing information
        logger.info(f"Starting to process {total_objects} objects with {len(derivatives)} derivatives each")
        logger.info(f"Using {args.workers} concurrent workers with {args.timeout}s timeout")
        logger.info(f"Connection close delay: {args.connection_close_delay}s")
        if using_size_based_optimization:
            logger.info(f"Size-based optimization enabled")
            logger.info(f"Size categories: Small <{args.small_file_threshold}MB, Medium <{args.medium_file_threshold}MB, Large ≥{args.medium_file_threshold}MB")
        logger.info(f"Press Ctrl+C to initiate graceful shutdown")
        
        start_time = time.time()
        
        # Group objects by size category if we have size information and optimization is enabled
        if using_size_based_optimization and isinstance(objects[0], dict) and 'size' in objects[0]:
            # Calculate size thresholds in bytes
            small_threshold = args.small_file_threshold * 1024 * 1024
            medium_threshold = args.medium_file_threshold * 1024 * 1024
            
            # Group by size category
            size_grouped_objects = {
                'small': [obj for obj in objects if obj['size'] < small_threshold],
                'medium': [obj for obj in objects if small_threshold <= obj['size'] < medium_threshold],
                'large': [obj for obj in objects if obj['size'] >= medium_threshold]
            }
            
            # Log size distribution
            for category, items in size_grouped_objects.items():
                category_size = sum(item['size'] for item in items)
                logger.info(f"{category.upper()} files: {len(items)} objects, "
                           f"Total size: {format_file_size(category_size)}")
            
            # Calculate optimal worker allocation if not specified by user
            total_workers = args.workers
            
            # Use user-specified worker counts if provided, otherwise auto-calculate
            if args.small_file_workers > 0 and args.medium_file_workers > 0 and args.large_file_workers > 0:
                # Use user-specified worker counts
                worker_allocation = {
                    'small': args.small_file_workers,
                    'medium': args.medium_file_workers,
                    'large': args.large_file_workers
                }
            else:
                # Auto-calculate based on file count and size distribution
                # Start with a basic allocation based on file count percentages
                total_count = sum(len(items) for items in size_grouped_objects.values())
                
                if total_count == 0:
                    # Fallback if no files (shouldn't happen)
                    worker_allocation = {'small': 0, 'medium': 0, 'large': 0}
                else:
                    # Base allocation on file count percentages, but give more weight to large files
                    small_pct = len(size_grouped_objects['small']) / total_count
                    medium_pct = len(size_grouped_objects['medium']) / total_count
                    large_pct = len(size_grouped_objects['large']) / total_count
                    
                    # Adjust percentages to give more resources to large files and less to small files
                    adjusted_small_pct = small_pct * 0.6  # 40% reduction for small files (can be processed quickly)
                    adjusted_medium_pct = medium_pct * 1.0  # No adjustment for medium files
                    adjusted_large_pct = large_pct * 2.0  # Double the allocation for large files
                    
                    # Normalize adjusted percentages
                    total_adjusted = adjusted_small_pct + adjusted_medium_pct + adjusted_large_pct
                    if total_adjusted > 0:
                        adjusted_small_pct /= total_adjusted
                        adjusted_medium_pct /= total_adjusted
                        adjusted_large_pct /= total_adjusted
                    
                    # Calculate worker counts (ensure at least 1 worker per category if files exist)
                    worker_allocation = {
                        'small': max(1, int(total_workers * adjusted_small_pct)) if size_grouped_objects['small'] else 0,
                        'medium': max(1, int(total_workers * adjusted_medium_pct)) if size_grouped_objects['medium'] else 0,
                        'large': max(1, int(total_workers * adjusted_large_pct)) if size_grouped_objects['large'] else 0
                    }
            
            # Adjust to ensure we don't exceed total workers
            allocated = sum(worker_allocation.values())
            if allocated > total_workers:
                # Scale down proportionally
                for category in worker_allocation:
                    worker_allocation[category] = int(worker_allocation[category] * (total_workers / allocated))
                
                # Assign any remaining workers to categories with files
                remaining = total_workers - sum(worker_allocation.values())
                categories_with_files = [cat for cat in ['large', 'medium', 'small'] 
                                       if len(size_grouped_objects[cat]) > 0]
                
                for i in range(remaining):
                    if categories_with_files:
                        worker_allocation[categories_with_files[i % len(categories_with_files)]] += 1
            
            # Log worker allocation
            logger.info(f"Worker allocation: SMALL: {worker_allocation['small']}, "
                       f"MEDIUM: {worker_allocation['medium']}, LARGE: {worker_allocation['large']}")
            
            # Process each size category with its own thread pool
            logger.info("Starting size-optimized processing pools")
            global executor
            
            # Create a queue for collecting results from all pools
            results_queue = queue.Queue()
            
            # Create a lock to protect shared data structures
            results_lock = Lock()
            
            # Define a function to process a single derivative and put results in the queue
            def process_derivative_to_queue(obj_data, derivative, queue_obj, **kwargs):
                try:
                    rel_path, derivative_key, result_data, original_size, size_category = process_single_derivative(
                        obj_data, 
                        derivative, 
                        **kwargs
                    )
                    queue_obj.put(('success', {
                        'object': obj_data,
                        'rel_path': rel_path,
                        'derivative': derivative,
                        'derivative_key': derivative_key,
                        'result_data': result_data
                    }))
                except Exception as e:
                    queue_obj.put(('error', {
                        'object': obj_data,
                        'derivative': derivative,
                        'error': str(e)
                    }))
            
            # Create and start thread pools for each size category
            executors = {}  # Reset the executors dictionary
            futures_by_category = {}
            
            # Keep track of derivatives per file
            file_to_derivatives = {}
            total_derivatives = 0
            
            for category, worker_count in worker_allocation.items():
                if worker_count > 0 and size_grouped_objects[category]:
                    logger.info(f"Starting {worker_count} workers for {category.upper()} files "
                               f"({len(size_grouped_objects[category])} files, "
                               f"{len(size_grouped_objects[category])*len(derivatives)} derivatives)")
                    
                    # Create thread pool for this category
                    executors[category] = concurrent.futures.ThreadPoolExecutor(
                        max_workers=worker_count,
                        thread_name_prefix=f"{category}_worker"
                    )
                    
                    # Submit tasks for this category - one task per derivative
                    futures = {}
                    for obj in size_grouped_objects[category]:
                        obj_path = obj['path'] if isinstance(obj, dict) else obj
                        if obj_path not in file_to_derivatives:
                            file_to_derivatives[obj_path] = []
                            
                        # Submit each derivative separately
                        for derivative in derivatives:
                            future = executors[category].submit(
                                process_derivative_to_queue, 
                                obj, 
                                derivative,
                                results_queue,
                                base_url=args.base_url, 
                                bucket=args.bucket, 
                                directory=args.directory,
                                timeout=args.timeout,
                                retry_attempts=args.retry,
                                connection_close_delay=args.connection_close_delay,
                                logger=logger,
                                small_threshold_mib=args.small_file_threshold,
                                medium_threshold_mib=args.medium_file_threshold
                            )
                            futures[future] = (obj, derivative)
                            file_to_derivatives[obj_path].append(derivative)
                            total_derivatives += 1
                    
                    futures_by_category[category] = futures
                    
            logger.info(f"Submitted {total_derivatives} derivative tasks for {len(file_to_derivatives)} files across all categories")
            
            # Create a thread to collect results from the queue
            def result_collector():
                nonlocal processed, all_results
                
                # Track completed derivatives per file
                completed_derivatives = {file_path: 0 for file_path in file_to_derivatives.keys()}
                file_results = {file_path: {"key": file_path.split('/')[-1], "derivatives": {}} 
                             for file_path in file_to_derivatives.keys()}
                file_completed = set()
                derivative_processed = 0
                
                while not shutdown_event.is_set() and processed < total_objects:
                    try:
                        # Wait for a result with timeout to check shutdown event periodically
                        result_type, result_data = results_queue.get(timeout=1.0)
                        
                        if result_type == 'success':
                            # Process successful result for one derivative
                            obj = result_data['object']
                            rel_path = result_data['rel_path']
                            derivative = result_data['derivative']
                            derivative_key = result_data['derivative_key']
                            result = result_data['result_data']
                            
                            # Determine the object path
                            obj_path = obj['path'] if isinstance(obj, dict) else obj
                            
                            # Use lock to safely update shared data structures
                            with results_lock:
                                # Add to all_results dictionary
                                all_results[derivative_key] = result
                                derivative_processed += 1
                                
                                # Add to file-specific results dictionary
                                file_results[obj_path]["derivatives"][derivative_key] = result
                                
                                # Update completion counters
                                completed_derivatives[obj_path] += 1
                            
                            logger.debug(f"Completed derivative {derivative} for {rel_path} - "
                                        f"{completed_derivatives[obj_path]}/{len(file_to_derivatives[obj_path])} derivatives done")
                            
                            # Check if all derivatives for this file are completed
                            if completed_derivatives[obj_path] == len(file_to_derivatives[obj_path]):
                                file_completed.add(obj_path)
                                processed += 1
                                
                                # Log summary for the entire file
                                derivative_keys = file_results[obj_path]["derivatives"].keys()
                                derivatives_successful = sum(1 for k in derivative_keys 
                                                          if isinstance(file_results[obj_path]["derivatives"][k].get('status'), int) 
                                                          and file_results[obj_path]["derivatives"][k].get('status') == 200)
                                
                                progress_pct = (processed / total_objects) * 100
                                derivatives_pct = (derivative_processed / total_derivatives) * 100
                                logger.info(f"[{processed}/{total_objects} ({progress_pct:.1f}%) files, "
                                           f"{derivative_processed}/{total_derivatives} ({derivatives_pct:.1f}%) derivatives] "
                                           f"Completed {rel_path} - "
                                           f"{derivatives_successful}/{len(derivative_keys)} derivatives successful")
                        else:
                            # Process error result
                            obj = result_data['object']
                            derivative = result_data['derivative']
                            error_msg = result_data['error']
                            
                            # Determine the object path
                            obj_path = obj['path'] if isinstance(obj, dict) else obj
                            
                            # Use lock to safely update shared data structures
                            with results_lock:
                                # Update completion counter for this derivative
                                completed_derivatives[obj_path] += 1
                                derivative_processed += 1
                            
                            logger.error(f"Error processing derivative {derivative} for {obj_path}: {error_msg}")
                            
                            # Check if all derivatives for this file are completed
                            if completed_derivatives[obj_path] == len(file_to_derivatives[obj_path]):
                                file_completed.add(obj_path)
                                processed += 1
                                logger.info(f"[{processed}/{total_objects}] All derivatives processed for {obj_path} (with errors)")
                        
                        # Every 5 objects or at the end, write intermediate results
                        if processed % 5 == 0 or processed == total_objects or shutdown_event.is_set():
                            current_time = time.time()
                            elapsed = current_time - start_time
                            estimated_total = (elapsed / processed) * total_objects if processed > 0 else 0
                            remaining = estimated_total - elapsed if processed > 0 else 0
                            
                            metadata = {
                                "processed": processed,
                                "total": total_objects,
                                "derivatives_requested": derivatives,
                                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "elapsed_seconds": elapsed,
                                "estimated_total_seconds": estimated_total,
                                "estimated_remaining_seconds": remaining,
                                "base_url": args.base_url,
                                "size_based_optimization": using_size_based_optimization,
                                "worker_allocation": worker_allocation if using_size_based_optimization else None,
                                "size_categories": {
                                    "small_threshold_mib": args.small_file_threshold,
                                    "medium_threshold_mib": args.medium_file_threshold
                                }
                            }
                            
                            logger.info(f"Progress: {processed}/{total_objects} objects processed "
                                       f"({elapsed:.1f}s elapsed, ~{remaining:.1f}s remaining)")
                            
                            write_results_to_file(all_results, metadata, args.output, processed, total_objects, logger)
                        
                    except queue.Empty:
                        # Queue was empty, just continue waiting
                        continue
                    except Exception as e:
                        logger.error(f"Error in result collector: {str(e)}", exc_info=True)
            
            # Start the result collector thread
            collector_thread = threading.Thread(target=result_collector, daemon=True)
            collector_thread.start()
            
            # Wait for all tasks to complete or shutdown to be requested
            for category, futures in futures_by_category.items():
                try:
                    # Wait for futures to complete while checking for shutdown
                    while futures and not shutdown_event.is_set():
                        done, futures[category] = concurrent.futures.wait(
                            futures, timeout=1.0, 
                            return_when=concurrent.futures.FIRST_COMPLETED
                        )
                except Exception as e:
                    logger.error(f"Error waiting for {category} tasks: {str(e)}", exc_info=True)
            
            # Wait for result collector to finish
            try:
                collector_thread.join(timeout=60.0)  # Wait up to 60 seconds for collector to finish
            except Exception as e:
                logger.error(f"Error waiting for result collector: {str(e)}", exc_info=True)
            
            # Shutdown executors
            for category, exe in executors.items():
                logger.debug(f"Shutting down {category} executor")
                exe.shutdown(wait=False)
            
            executors = {}  # Clear the executors dictionary
            executor = None  # Clear the global executor
            
        else:
            # Standard non-size-optimized processing, but with per-derivative tasks
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
            try:
                logger.debug(f"Thread pool executor initialized with {args.workers} workers")
                
                # Create a lock to protect shared data structures
                results_lock = Lock()
                
                # Submit derivative-level tasks (each derivative is a separate task)
                futures = {}
                file_to_derivatives = {}  # Keep track of which derivatives belong to which file
                total_derivatives = 0
                
                for obj in objects:
                    # Create record to track derivatives for this file
                    obj_path = obj['path'] if isinstance(obj, dict) else obj
                    file_to_derivatives[obj_path] = []
                    
                    # Submit each derivative as a separate task
                    for derivative in derivatives:
                        logger.debug(f"Submitting derivative '{derivative}' for object: {obj_path}")
                        future = executor.submit(
                            process_single_derivative, 
                            obj, 
                            derivative,
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
                        futures[future] = (obj, derivative)
                        file_to_derivatives[obj_path].append(derivative)
                        total_derivatives += 1
                
                logger.info(f"Submitted {len(futures)} derivative tasks for {len(objects)} files to thread pool")
                
                # Track completed derivatives per file
                completed_derivatives = {file_path: 0 for file_path in file_to_derivatives.keys()}
                file_results = {file_path: {"key": file_path.split('/')[-1], "derivatives": {}} 
                             for file_path in file_to_derivatives.keys()}
                file_completed = set()
                
                # Process results as they complete
                derivative_processed = 0
                for future in concurrent.futures.as_completed(futures):
                    # Check if shutdown was requested
                    if shutdown_event.is_set():
                        logger.info("Shutdown requested. Stopping processing of new results.")
                        break
                    
                    obj, derivative = futures[future]
                    obj_path = obj['path'] if isinstance(obj, dict) else obj
                    derivative_processed += 1
                    
                    try:
                        logger.debug(f"Processing completed for derivative '{derivative}' of object: {obj_path}")
                        rel_path, derivative_key, result_data, _, _ = future.result()
                        
                        # Use lock to safely update shared data structures
                        with results_lock:
                            # Add to all_results dictionary
                            all_results[derivative_key] = result_data
                            
                            # Add to file-specific results dictionary
                            file_results[obj_path]["derivatives"][derivative_key] = result_data
                            
                            # Update completion counters
                            completed_derivatives[obj_path] += 1
                        
                        # Check if all derivatives for this file are completed
                        if completed_derivatives[obj_path] == len(file_to_derivatives[obj_path]):
                            file_completed.add(obj_path)
                            processed += 1
                            
                            # Log summary for the entire file
                            derivative_keys = file_results[obj_path]["derivatives"].keys()
                            derivatives_successful = sum(1 for k in derivative_keys 
                                                      if isinstance(file_results[obj_path]["derivatives"][k].get('status'), int) 
                                                      and file_results[obj_path]["derivatives"][k].get('status') == 200)
                            
                            progress_pct = (processed / total_objects) * 100
                            derivatives_pct = (derivative_processed / total_derivatives) * 100
                            logger.info(f"[{processed}/{total_objects} ({progress_pct:.1f}%) files, "
                                       f"{derivative_processed}/{total_derivatives} ({derivatives_pct:.1f}%) derivatives] "
                                       f"Completed {rel_path} - "
                                       f"{derivatives_successful}/{len(derivative_keys)} derivatives successful")
                        
                        # Every 5 objects or at the end, write intermediate results
                        if processed % 5 == 0 or processed == total_objects or shutdown_event.is_set():
                            current_time = time.time()
                            elapsed = current_time - start_time
                            estimated_total = (elapsed / processed) * total_objects if processed > 0 else 0
                            remaining = estimated_total - elapsed if processed > 0 else 0
                            
                            metadata = {
                                "processed": processed,
                                "total": total_objects,
                                "derivatives_requested": derivatives,
                                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "elapsed_seconds": elapsed,
                                "estimated_total_seconds": estimated_total,
                                "estimated_remaining_seconds": remaining,
                                "base_url": args.base_url,
                                "size_categories": {
                                    "small_threshold_mib": args.small_file_threshold,
                                    "medium_threshold_mib": args.medium_file_threshold
                                }
                            }
                            
                            logger.info(f"Progress: {processed}/{total_objects} objects processed "
                                       f"({elapsed:.1f}s elapsed, ~{remaining:.1f}s remaining)")
                            
                            write_results_to_file(all_results, metadata, args.output, processed, total_objects, logger)
                                
                    except Exception as e:
                        logger.error(f"Error processing {obj}: {str(e)}", exc_info=True)
            
            finally:
                # Save results if shutdown was requested
                if shutdown_event.is_set():
                    # Write final results file with shutdown status
                    metadata = {
                        "processed": processed,
                        "total": total_objects,
                        "derivatives_requested": derivatives,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "elapsed_seconds": time.time() - start_time,
                        "early_shutdown": True,
                        "base_url": args.base_url
                    }
                    
                    logger.info(f"Saving results before shutdown ({processed}/{total_objects} objects processed)")
                    write_results_to_file(all_results, metadata, args.output, processed, total_objects, logger)
                    logger.info(f"Results saved to {args.output}")
                
                # Ensure thread pool is shutdown properly
                cleanup_resources()
        
            # Calculate and log statistics
            elapsed_time = time.time() - start_time
            if shutdown_event.is_set():
                logger.info(f"Processing stopped early: {processed} of {total_objects} objects in {elapsed_time:.2f} seconds")
            else:
                logger.info(f"Processing completed: {processed} of {total_objects} objects in {elapsed_time:.2f} seconds")
            
            # Print comprehensive summary statistics
            print_summary_statistics(all_results, elapsed_time, total_objects, logger)
        
        logger.info(f"Final results written to {args.output}")
        
        # If comparison is requested, perform comparison with KV data
        if args.compare and os.path.exists(args.compare):
            logger.info(f"Comparing results with KV data from {args.compare}")
            
            # Load KV data
            kv_data = load_kv_data(args.compare, logger)
            if kv_data:
                # Run comparison
                comparison_results = compare_transformation_with_kv(all_results, kv_data, logger)
                
                # Print summary
                print_comparison_summary(comparison_results, logger)
                
                # Save comparison results
                with open(args.comparison_output, 'w') as f:
                    json.dump(comparison_results, f, indent=2)
                
                logger.info(f"Comparison results saved to {args.comparison_output}")
                
                # Generate and save summary in requested format
                save_summary(comparison_results, args.summary_output, args.summary_format, logger)
            else:
                logger.error("Failed to load KV data for comparison")
        
        logger.info(f"=== Video Transformation Script Completed ===")
        
    except Exception as e:
        logger.error(f"An error occurred in the main process: {str(e)}", exc_info=True)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # This is a fallback in case the signal handler doesn't catch the interrupt
        print("\nInterrupted by user. Exiting.")
        sys.exit(1)
    except Exception as e:
        if logger:
            logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        else:
            print(f"Unhandled exception: {str(e)}")
        sys.exit(1)
