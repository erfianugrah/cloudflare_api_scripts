import argparse
import subprocess
import concurrent.futures
import requests
import logging
import json
import time
import os
import re
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
    parser.add_argument('--remote', required=True, help='rclone remote name')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--directory', default='', help='Directory path within bucket')
    parser.add_argument('--base-url', required=True, help='Base URL to prepend to object paths')
    parser.add_argument('--derivatives', default='desktop,tablet,mobile', help='Comma-separated list of derivatives')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers')
    parser.add_argument('--timeout', type=int, default=120, help='Request timeout in seconds')
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
    return parser.parse_args()

def list_objects(remote, bucket, directory, extension, limit=0, logger=None):
    """List objects in the specified rclone remote bucket and directory."""
    path = f"{remote}:{bucket}/{directory}"
    path = path.rstrip('/') # Remove trailing slash if present
    
    logger.info(f"Listing objects from rclone path: {path}")
    logger.debug(f"Using filter extension: {extension}")
    
    try:
        # Use recursive listing to get all objects
        cmd = ['rclone', 'lsf', '--recursive', path]
        logger.debug(f"Executing command: {' '.join(cmd)}")
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        duration = time.time() - start_time
        
        # Parse the output and filter
        all_items = result.stdout.splitlines()
        logger.debug(f"rclone returned {len(all_items)} total items in {duration:.2f} seconds")
        
        # Filter out directories and by extension
        objects = [line for line in all_items 
                  if not line.endswith('/') and line.lower().endswith(extension.lower())]
        
        logger.info(f"Found {len(objects)} {extension} files out of {len(all_items)} total items")
        
        # Apply limit if specified
        if limit > 0 and len(objects) > limit:
            logger.info(f"Limiting to {limit} objects (from {len(objects)} found)")
            objects = objects[:limit]
            logger.debug(f"First {min(5, len(objects))} limited objects: {objects[:5]}")
        else:
            logger.debug(f"First {min(5, len(objects))} objects: {objects[:5]}")
        
        return objects
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing rclone command: {' '.join(cmd)}")
        logger.error(f"rclone stderr: {e.stderr}")
        logger.error(f"rclone exit code: {e.returncode}")
        raise

def get_derivative_dimensions(derivative, logger=None):
    """Get the requested dimensions for a specific derivative."""
    dimensions = {
        'desktop': {'width': 1920, 'height': 1080},
        'tablet': {'width': 1000, 'height': 720},
        'mobile': {'width': 800, 'height': 640}
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

def process_object(obj_path, base_url, derivatives, bucket, directory, timeout, retry_attempts=2, logger=None):
    """Process a single video object with different derivatives."""
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
    
    results = {
        "key": rel_path,
        "derivatives": {}
    }
    
    # Process each derivative
    for derivative in derivatives:
        logger.info(f"Requesting derivative '{derivative}' for {rel_path}")
        dimensions = get_derivative_dimensions(derivative, logger)
        
        # Construct URL with both dimensions
        url = f"{base_obj_url}?derivative={derivative}&width={dimensions['width']}&height={dimensions['height']}"
        logger.debug(f"Request URL: {url}")
        
        # Keep track of retry attempts
        attempt = 0
        while attempt <= retry_attempts:
            attempt += 1
            try:
                logger.debug(f"Starting request (attempt {attempt}/{retry_attempts+1}): {url}")
                start_time = time.time()
                
                response = requests.get(url, timeout=timeout)
                duration = time.time() - start_time
                
                logger.debug(f"Request completed in {duration:.2f} seconds with status {response.status_code}")
                log_response_details(response, url, logger)
                
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
                else:
                    logger.warning(f"Non-200 response: {derivative} for {rel_path} - Status: {status}")
                
                results["derivatives"][derivative_key] = {
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
                    'requestDimensions': f"{dimensions['width']}x{dimensions['height']}",
                    'etag': response.headers.get('ETag', '').strip('"'),
                    'attempt': attempt
                }
                
                # If successful, break the retry loop
                break
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt}/{retry_attempts+1}): {url}")
                logger.error(f"Error details: {str(e)}")
                
                # If we've exhausted retries, record the error
                if attempt > retry_attempts:
                    logger.error(f"All retry attempts failed for {url}")
                    derivative_key = f"video:{rel_path}:derivative={derivative}"
                    results["derivatives"][derivative_key] = {
                        'status': 'Error',
                        'error': str(e),
                        'contentLength': 0,
                        'actualTotalVideoSize': 0,
                        'derivative': derivative,
                        'attempts': attempt
                    }
                else:
                    # Calculate backoff time (exponential backoff)
                    backoff = 2 ** (attempt - 1)
                    logger.info(f"Retrying in {backoff} seconds...")
                    time.sleep(backoff)
    
    return results

def write_results_to_file(all_results, metadata, output_file, processed, total_objects, logger=None):
    """Write current results to the output file."""
    try:
        logger.debug(f"Writing results to {output_file} ({processed}/{total_objects} processed)")
        with open(output_file, 'w') as f:
            output_data = {
                "metadata": metadata,
                "results": all_results
            }
            json.dump(output_data, f, indent=2)
        logger.debug(f"Successfully wrote {len(all_results)} results to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write results to {output_file}: {str(e)}")

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
        
        # Track status codes
        status = data.get('status')
        if status not in status_codes:
            status_codes[status] = 0
        status_codes[status] += 1
        
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
    for status, count in sorted(status_codes.items()):
        logger.info(f"  {status}: {count} ({count/len(all_results)*100:.1f}%)")
    
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

def main():
    """Main function."""
    args = parse_arguments()
    
    # Set up logging
    logger = setup_logging(args.verbose)
    
    # Log script startup and configuration
    logger.info(f"=== Video Transformation Script Started ===")
    logger.info(f"Configuration:")
    for arg, value in vars(args).items():
        logger.info(f"  {arg}: {value}")
    
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
    
    derivatives = [d.strip() for d in args.derivatives.split(',')]
    logger.info(f"Processing derivatives: {derivatives}")
    
    try:
        # List objects in the bucket/directory
        logger.info(f"Listing objects from {args.remote}:{args.bucket}/{args.directory}")
        objects = list_objects(args.remote, args.bucket, args.directory, args.extension, args.limit, logger)
        
        if not objects:
            logger.warning(f"No {args.extension} objects found. Check your bucket and directory settings.")
            return
        
        # Process objects concurrently
        total_objects = len(objects)
        processed = 0
        all_results = {}
        logger.info(f"Starting to process {total_objects} objects with {len(derivatives)} derivatives each")
        logger.info(f"Using {args.workers} concurrent workers with {args.timeout}s timeout")
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            logger.debug(f"Thread pool executor initialized with {args.workers} workers")
            
            # Submit all tasks
            futures = {}
            for obj in objects:
                logger.debug(f"Submitting object for processing: {obj}")
                future = executor.submit(
                    process_object, 
                    obj, 
                    args.base_url, 
                    derivatives,
                    args.bucket, 
                    args.directory,
                    args.timeout,
                    args.retry,
                    logger
                )
                futures[future] = obj
            
            logger.info(f"Submitted {len(futures)} tasks to thread pool")
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                processed += 1
                obj = futures[future]
                try:
                    logger.debug(f"Processing completed for object: {obj}")
                    result = future.result()
                    
                    # Add to results dictionary
                    derivatives_processed = 0
                    for derivative_key, derivative_data in result["derivatives"].items():
                        all_results[derivative_key] = derivative_data
                        derivatives_processed += 1
                    
                    # Log summary of progress
                    derivatives_successful = sum(1 for d in result["derivatives"].values() 
                                              if isinstance(d.get('status'), int) and d.get('status') == 200)
                    
                    progress_pct = (processed / total_objects) * 100
                    logger.info(f"[{processed}/{total_objects} ({progress_pct:.1f}%)] Processed {result['key']} - "
                               f"{derivatives_successful}/{derivatives_processed} derivatives successful")
                    
                    # Every 5 objects or at the end, write intermediate results
                    if processed % 5 == 0 or processed == total_objects:
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
                            "estimated_remaining_seconds": remaining
                        }
                        
                        logger.info(f"Progress: {processed}/{total_objects} objects processed "
                                   f"({elapsed:.1f}s elapsed, ~{remaining:.1f}s remaining)")
                        
                        write_results_to_file(all_results, metadata, args.output, processed, total_objects, logger)
                            
                except Exception as e:
                    logger.error(f"Error processing {obj}: {str(e)}", exc_info=True)
        
        # Calculate and log statistics
        elapsed_time = time.time() - start_time
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
    main()
