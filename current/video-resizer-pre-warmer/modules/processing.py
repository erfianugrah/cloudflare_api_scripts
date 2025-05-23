"""
Core processing module for video resizer pre-warmer.
Contains functions for URL processing and network requests.
"""
import requests
import time
import logging
import concurrent.futures
import threading
import json
import re
import os
from threading import Lock
from urllib.parse import urljoin
from datetime import datetime
from tabulate import tabulate

# Set up module logger
logger = logging.getLogger(__name__)

# Global locks for thread safety
derivatives_lock = Lock()
errors_lock = Lock()
results_lock = Lock()
processed_locks = {}
failed_locks = {}
stats_lock = Lock()

def get_derivative_dimensions(derivative, logger=None):
    """
    Get the requested dimensions for a specific derivative.
    
    Args:
        derivative: The derivative name (desktop, tablet, mobile)
        logger: Logger instance
        
    Returns:
        dict: Dictionary with width and height keys
    """
    dimensions = {
        'desktop': {'width': 1920, 'height': 1080},
        'tablet': {'width': 1280, 'height': 720},
        'mobile': {'width': 854, 'height': 640}
    }
    
    if derivative in dimensions:
        if logger:
            logger.debug(f"Using standard dimensions for '{derivative}': {dimensions[derivative]}")
        return dimensions[derivative]
    else:
        if logger:
            logger.warning(f"Unknown derivative '{derivative}', using default dimensions")
        return {'width': 1280, 'height': 720}  # Default dimensions

def log_response_details(response, url, logger=None):
    """
    Log detailed information about a response.
    
    Args:
        response: Response object
        url: URL that was requested
        logger: Logger instance
    """
    if not logger:
        return
        
    # Log basic response info
    logger.debug(f"Response for {url}: {response.status_code}")
    
    # Log headers (with some filtering for sensitive info)
    filtered_headers = {k: v for k, v in response.headers.items() 
                       if k.lower() not in ('authorization', 'cookie', 'set-cookie')}
    logger.debug(f"Response headers: {filtered_headers}")
    
    # Log more details for error responses
    if response.status_code >= 400:
        try:
            content_preview = response.text[:200] + ('...' if len(response.text) > 200 else '')
            logger.debug(f"Error response content: {content_preview}")
        except Exception:
            logger.debug("Could not get response content")

def process_single_derivative(obj_data, derivative, base_url, bucket, directory, timeout, 
                            retry_attempts=2, connection_close_delay=10, logger=None,
                            small_threshold_mib=50, medium_threshold_mib=200):
    """
    Process a single derivative for a video object.
    
    Args:
        obj_data: Dictionary containing object metadata
        derivative: The derivative to process
        base_url: Base URL to prepend to object paths
        bucket: S3 bucket name
        directory: Directory path within bucket
        timeout: Request timeout in seconds
        retry_attempts: Number of retry attempts for failed requests
        connection_close_delay: Additional delay in seconds before closing connections
        logger: Logger instance
        small_threshold_mib: Threshold for small files in MiB
        medium_threshold_mib: Threshold for medium files in MiB
        
    Returns:
        Dictionary with processing results for this derivative
    """
    obj_path = obj_data.path
    obj_size = obj_data.size_bytes
    
    # Mark the start of processing this derivative
    obj_data.start_derivative_processing(derivative)
    
    # Prepare result data structure
    result = {
        'status': 'unknown',
        'status_code': None,
        'time_to_first_byte': None,
        'total_time': None,
        'response_size_bytes': None,
        'retries': 0,
        'start_time': time.time(),
        'end_time': None,
    }
    
    # For simplicity, just use the key directly as specified by the user
    file_key = obj_path.split('/')[-1]  # Just get the filename without any path
    
    # Build final URL - just base_url and file_key as specified by user  
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    
    # Use simple URL format by default (just base_url/file_key)
    url = f"{base_url}/{file_key}"
    
    # Add imwidth parameter for all cases - if derivative is empty/blank, use desktop dimensions
    dimensions = get_derivative_dimensions(derivative or "desktop", logger)
    url = f"{url}?imwidth={dimensions['width']}"
    
    # Note: We're using just imwidth parameter, not derivative parameter
    
    # Store URL in the result for all cases, not just errors
    result['url'] = url
    
    if logger:
        logger.debug(f"Processing URL: {url}")
    
    # Attempt request with retries
    for attempt in range(retry_attempts + 1):
        if attempt > 0:
            if logger:
                logger.debug(f"Retry {attempt}/{retry_attempts} for {url}")
            result['retries'] = attempt
            time.sleep(2 * attempt)  # Back off with each retry
        
        try:
            with requests.get(url, stream=True, timeout=timeout) as response:
                result['status_code'] = response.status_code
                
                # Check for errors
                if response.status_code >= 400:
                    result['status'] = 'error'
                    error_content = response.text[:1000]  # Get start of error response for logging
                    if logger:
                        logger.debug(f"Error {response.status_code} for {url}: {error_content}")
                    
                    # Check for specific error types
                    if response.status_code == 500:
                        result['error_type'] = 'server_error'
                    elif response.status_code == 404:
                        result['error_type'] = 'not_found'
                    elif response.status_code == 403:
                        result['error_type'] = 'forbidden'
                    elif response.status_code == 429:
                        result['error_type'] = 'rate_limited'
                    else:
                        result['error_type'] = 'other'
                        
                    # Store error details - keep for backward compatibility
                    result['error_details'] = {
                        'url': url,
                        'response': error_content
                    }
                    
                    # If not a 5xx error, don't retry
                    if response.status_code < 500:
                        break
                    
                # Successful response
                else:
                    # Record time to first byte
                    ttfb = response.elapsed.total_seconds()
                    result['time_to_first_byte'] = ttfb
                    
                    # Always stream the content to get actual size
                    # Even if Content-Length is present, we need to download the content
                    download_start = time.time()
                    content_size = 0
                    
                    # Stream response content
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            content_size += len(chunk)
                    
                    download_time = time.time() - download_start
                    
                    # Log if Content-Length doesn't match actual size
                    if 'Content-Length' in response.headers:
                        expected_size = int(response.headers['Content-Length'])
                        if expected_size != content_size:
                            if logger:
                                logger.warning(f"Content-Length mismatch for {url}: header={expected_size}, actual={content_size}")
                    
                    # Log details about content size
                    if logger:
                        logger.debug(f"Response for {url}: size={content_size}, headers={response.headers.get('Content-Type')}, content-length={response.headers.get('Content-Length')}")
                    
                    # Calculate total download time
                    result['total_time'] = ttfb + download_time
                    result['response_size_bytes'] = content_size
                    result['status'] = 'success'
                    
                    # Add size comparison metrics if we have the original size and a valid response size
                    if hasattr(obj_data, 'size_bytes') and obj_data.size_bytes > 0 and content_size > 0:
                        result['original_size_bytes'] = obj_data.size_bytes
                        # Calculate size reduction
                        size_diff = obj_data.size_bytes - content_size
                        reduction_percent = (size_diff / obj_data.size_bytes) * 100 if obj_data.size_bytes > 0 else 0
                        result['size_reduction_bytes'] = size_diff
                        result['size_reduction_percent'] = reduction_percent
                    elif content_size == 0:
                        # If content size is 0, there was likely a problem with the response
                        # Just store original size without reduction calculations
                        result['original_size_bytes'] = obj_data.size_bytes
                        result['size_reduction_note'] = "Could not calculate reduction - empty response"
                    
                    # Additional delay to ensure connection closure
                    if connection_close_delay > 0:
                        if logger:
                            logger.debug(f"Waiting {connection_close_delay}s for connection close")
                        time.sleep(connection_close_delay)
                    
                    break  # Success, no need for further attempts
                    
        except requests.exceptions.Timeout:
            result['status'] = 'timeout'
            result['error_type'] = 'timeout'
            if logger:
                logger.debug(f"Timeout for {url}")
            
        except requests.exceptions.ConnectionError as e:
            result['status'] = 'connection_error'
            result['error_type'] = 'connection_error'
            result['error_details'] = str(e)
            if logger:
                logger.debug(f"Connection error for {url}: {e}")
            
        except Exception as e:
            result['status'] = 'exception'
            result['error_type'] = 'unknown'
            result['error_details'] = str(e)
            if logger:
                logger.debug(f"Unexpected error for {url}: {str(e)}")
    
    # Record end time
    result['end_time'] = time.time()
    result['duration'] = result['end_time'] - result['start_time']
    
    # Mark the completion of processing this derivative
    obj_data.complete_derivative_processing(derivative)
    
    return result

def process_object_without_derivatives(obj_data, base_url, bucket, directory, timeout,
                               retry_attempts=2, connection_close_delay=10, logger=None,
                               small_threshold_mib=50, medium_threshold_mib=200):
    """
    Process a video object without any derivatives - just simple URL request.
    
    Args:
        obj_data: Dictionary containing object metadata
        base_url: Base URL to prepend to object paths
        bucket: S3 bucket name
        directory: Directory path within bucket
        timeout: Request timeout in seconds
        retry_attempts: Number of retry attempts for failed requests
        connection_close_delay: Additional delay in seconds before closing connections
        logger: Logger instance
        small_threshold_mib: Threshold for small files in MiB
        medium_threshold_mib: Threshold for medium files in MiB
        
    Returns:
        Dictionary with processing results
    """
    # Mark the start of processing
    obj_data.start_processing()
    
    # Process results - make a single request without derivative
    results = {
        "default": process_single_derivative(
            obj_data, "", base_url, bucket, directory, timeout,
            retry_attempts, connection_close_delay, logger,
            small_threshold_mib, medium_threshold_mib
        )
    }
    
    # Mark the completion of processing
    obj_data.complete_processing()
    
    return results

def process_object(obj_data, base_url, bucket, directory, derivatives, timeout,
                retry_attempts=2, connection_close_delay=10, logger=None, 
                small_threshold_mib=50, medium_threshold_mib=200):
    """
    Process a video object with all its derivatives.
    
    Args:
        obj_data: Dictionary containing object metadata
        base_url: Base URL to prepend to object paths
        bucket: S3 bucket name
        directory: Directory path within bucket
        derivatives: List of derivatives to process
        timeout: Request timeout in seconds
        retry_attempts: Number of retry attempts for failed requests
        connection_close_delay: Additional delay in seconds before closing connections
        logger: Logger instance
        small_threshold_mib: Threshold for small files in MiB
        medium_threshold_mib: Threshold for medium files in MiB
        
    Returns:
        Dictionary with processing results
    """
    # Mark the start of processing
    obj_data.start_processing()
    
    # Process results
    results = {}
    
    # Process each derivative
    for derivative in derivatives:
        results[derivative] = process_single_derivative(
            obj_data, derivative, base_url, bucket, directory, timeout,
            retry_attempts, connection_close_delay, logger,
            small_threshold_mib, medium_threshold_mib
        )
    
    # Mark the completion of processing
    obj_data.complete_processing()
    
    return results

def update_processing_stats(stats, obj_data, results, derivatives):
    """
    Update processing statistics with object results.
    
    Args:
        stats: Statistics dictionary to update
        obj_data: Object metadata
        results: Processing results
        derivatives: List of derivatives (can be empty)
        
    Returns:
        Updated statistics dictionary
    """
    with stats_lock:
        # Update global stats
        stats['total_processed'] += 1
        stats['total_size_bytes'] += obj_data.size_bytes
        
        # Update size category stats
        size_cat = obj_data.size_category
        stats['by_size_category'][size_cat]['count'] += 1
        stats['by_size_category'][size_cat]['size_bytes'] += obj_data.size_bytes
        
        # Get the list of keys to process - either derivatives or "default"
        result_keys = derivatives if derivatives else ["default"]
        
        # Update stats for each result
        for key in result_keys:
            result = results.get(key, {})
            status = result.get('status', 'unknown')
            
            # Track derivative-specific success/failure
            if status == 'success':
                if f'{key}_success' in stats:
                    stats[f'{key}_success'] += 1
                
                stats['success_count'] += 1
                stats['success_bytes'] += obj_data.size_bytes
                
                # Record timing stats only for successful requests
                if result.get('time_to_first_byte'):
                    stats['ttfb_values'].append(result['time_to_first_byte'])
                
                if result.get('total_time'):
                    stats['total_time_values'].append(result['total_time'])
                
                # Track size-specific timing stats
                if 'time_to_first_byte' in result:
                    stats['by_size_category'][size_cat]['ttfb_values'].append(result['time_to_first_byte'])
                
                if 'total_time' in result:
                    stats['by_size_category'][size_cat]['total_time_values'].append(result['total_time'])
            else:
                if f'{key}_errors' in stats:
                    stats[f'{key}_errors'] += 1
                    
                stats['error_count'] += 1
                stats['error_bytes'] += obj_data.size_bytes
                
                # Track error types
                error_type = result.get('error_type', 'unknown')
                if error_type not in stats['errors_by_type']:
                    stats['errors_by_type'][error_type] = 0
                stats['errors_by_type'][error_type] += 1
        
        return stats