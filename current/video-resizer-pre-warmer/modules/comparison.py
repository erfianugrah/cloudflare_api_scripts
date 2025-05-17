"""
Comparison module for video resizer pre-warmer.
Handles comparing results with Cloudflare KV data.
"""
import json
import re
import logging
import math
import os
from datetime import datetime
from tabulate import tabulate
from modules import video_utils

# Set up module logger
logger = logging.getLogger(__name__)

def extract_video_path_and_derivative(key_name):
    """
    Extract the video path and derivative from a KV key name.
    
    Args:
        key_name: The KV key name string
        
    Returns:
        tuple: (path, derivative, is_chunk, chunk_index)
    """
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

def load_cloudflare_kv_data(kv_file, logger=None):
    """
    Load Cloudflare KV data from a JSON file.
    
    Args:
        kv_file: Path to the KV JSON file
        logger: Logger instance
        
    Returns:
        dict: The KV data or None if loading failed
    """
    if not os.path.exists(kv_file):
        if logger:
            logger.error(f"KV file does not exist: {kv_file}")
        return None
    
    try:
        with open(kv_file) as f:
            kv_data = json.load(f)
        
        if not isinstance(kv_data, dict) or 'keys' not in kv_data:
            if logger:
                logger.error(f"Invalid KV data format. Expected object with 'keys' property.")
            return None
        
        if logger:
            logger.info(f"Successfully loaded KV data with {len(kv_data['keys'])} keys")
        return kv_data
    except Exception as e:
        if logger:
            logger.error(f"Failed to load KV data: {str(e)}")
        return None

def load_transform_results(results_file, logger=None):
    """
    Load transform results from a JSON file.
    
    Args:
        results_file: Path to the results JSON file
        logger: Logger instance
        
    Returns:
        dict: The transform results or None if loading failed
    """
    if not os.path.exists(results_file):
        if logger:
            logger.error(f"Results file does not exist: {results_file}")
        return None
    
    try:
        with open(results_file) as f:
            transform_data = json.load(f)
        
        if not isinstance(transform_data, dict) or 'results' not in transform_data:
            if logger:
                logger.error(f"Invalid transform results format. Expected object with 'results' property.")
            return None
        
        if logger:
            logger.info(f"Successfully loaded transform results with {len(transform_data['results'])} entries")
        return transform_data
    except Exception as e:
        if logger:
            logger.error(f"Failed to load transform results: {str(e)}")
        return None

def compare_results_with_kv(transform_results, kv_data, logger=None):
    """
    Compare transform results with Cloudflare KV data.
    
    Args:
        transform_results: The transform results dictionary
        kv_data: The KV data dictionary
        logger: Logger instance
        
    Returns:
        dict: Comparison results
    """
    if not transform_results or not kv_data:
        if logger:
            logger.error("Cannot compare results: missing data")
        return None
    
    comparison = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'transform_file_count': len(transform_results['results']),
        'kv_key_count': len(kv_data['keys']),
        'matches': [],
        'only_in_transform': [],
        'only_in_kv': [],
        'stats': {
            'match_count': 0,
            'only_transform_count': 0,
            'only_kv_count': 0,
            'match_rate': 0.0
        }
    }
    
    # Create lookup dictionaries
    transform_lookup = {}
    for path, result in transform_results['results'].items():
        for derivative, details in result['derivatives'].items():
            key = f"{path}:{derivative}"
            transform_lookup[key] = details
    
    kv_lookup = {}
    for key_info in kv_data['keys']:
        key_name = key_info['name']
        path, derivative, is_chunk, chunk_index = extract_video_path_and_derivative(key_name)
        if path and derivative:
            kv_key = f"{path}:{derivative}"
            if kv_key not in kv_lookup:
                kv_lookup[kv_key] = []
            kv_lookup[kv_key].append(key_info)
    
    # Find matches and mismatches
    all_keys = set(transform_lookup.keys()) | set(kv_lookup.keys())
    
    for key in all_keys:
        if key in transform_lookup and key in kv_lookup:
            path, derivative = key.split(':', 1)
            transform_details = transform_lookup[key]
            kv_entries = kv_lookup[key]
            
            comparison['matches'].append({
                'path': path,
                'derivative': derivative,
                'transform_details': transform_details,
                'kv_entries': kv_entries
            })
            comparison['stats']['match_count'] += 1
        
        elif key in transform_lookup:
            path, derivative = key.split(':', 1)
            comparison['only_in_transform'].append({
                'path': path, 
                'derivative': derivative,
                'details': transform_lookup[key]
            })
            comparison['stats']['only_transform_count'] += 1
        
        elif key in kv_lookup:
            path, derivative = key.split(':', 1)
            comparison['only_in_kv'].append({
                'path': path,
                'derivative': derivative,
                'entries': kv_lookup[key]
            })
            comparison['stats']['only_kv_count'] += 1
    
    # Calculate match rate
    total_count = len(all_keys)
    if total_count > 0:
        comparison['stats']['match_rate'] = (comparison['stats']['match_count'] / total_count) * 100
    
    return comparison

def generate_comparison_report(comparison_results, format_type='markdown'):
    """
    Generate a report comparing transform results with KV data.
    
    Args:
        comparison_results: The comparison results dictionary
        format_type: Output format ('markdown' or 'json')
        
    Returns:
        str: The formatted report
    """
    if format_type == 'json':
        return json.dumps(comparison_results, indent=2)
    
    # Generate markdown report
    timestamp = comparison_results['timestamp']
    transform_count = comparison_results['transform_file_count']
    kv_count = comparison_results['kv_key_count']
    match_count = comparison_results['stats']['match_count']
    only_transform = comparison_results['stats']['only_transform_count']
    only_kv = comparison_results['stats']['only_kv_count']
    match_rate = comparison_results['stats']['match_rate']
    
    md_report = [
        f"# Video Transform Comparison Report",
        f"Generated: {timestamp}",
        "",
        f"## Summary",
        f"- Transform result entries: {transform_count}",
        f"- KV key entries: {kv_count}",
        f"- Matches: {match_count} ({match_rate:.1f}%)",
        f"- Only in transform results: {only_transform}",
        f"- Only in KV data: {only_kv}",
        "",
    ]
    
    # Add detailed sections if they're not too large
    if len(comparison_results['only_in_transform']) > 0 and len(comparison_results['only_in_transform']) <= 100:
        md_report.extend([
            f"## Files in Transform Results but Not in KV",
            "",
            "| # | Path | Derivative |",
            "|---|------|------------|",
        ])
        
        for i, item in enumerate(comparison_results['only_in_transform']):
            md_report.append(f"| {i+1} | {item['path']} | {item['derivative']} |")
        
        md_report.append("")
    
    if len(comparison_results['only_in_kv']) > 0 and len(comparison_results['only_in_kv']) <= 100:
        md_report.extend([
            f"## Files in KV but Not in Transform Results",
            "",
            "| # | Path | Derivative |",
            "|---|------|------------|",
        ])
        
        for i, item in enumerate(comparison_results['only_in_kv']):
            md_report.append(f"| {i+1} | {item['path']} | {item['derivative']} |")
    
    return "\n".join(md_report)