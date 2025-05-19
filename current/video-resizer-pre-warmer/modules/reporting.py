"""
Reporting module for video resizer pre-warmer.
Handles report generation, statistics, and output formatting.
"""
import json
import time
import logging
import os
import numpy as np
import math
from datetime import datetime
from tabulate import tabulate
from modules import video_utils

# Set up module logger
logger = logging.getLogger(__name__)

def write_results_to_file(all_results, metadata, output_file, processed, total_objects, logger=None):
    """
    Write current results to the output file, with special sections for errors and performance metrics.
    
    Args:
        all_results: Dictionary of processing results
        metadata: Dictionary of metadata to include
        output_file: Path to output file
        processed: Number of processed objects
        total_objects: Total number of objects
        logger: Logger instance
    """
    try:
        if logger:
            logger.debug(f"Writing results to {output_file} ({processed}/{total_objects} processed)")
        
        # Create lists for errors of different types
        http_500_errors = []
        all_errors = []  # Track all errors regardless of type
        
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
            'by_size_category': {'small': [], 'medium': [], 'large': []}
        }
        
        # Process all results to generate these special sections
        for obj_path, obj_results in all_results.items():
            size_category = obj_results.get('size_category', 'unknown')
            size_bytes = obj_results.get('size_bytes', 0)
            
            # Track timing data by size category
            if size_category in performance_data['by_size_category'] and obj_results.get('processing_time'):
                performance_data['by_size_category'][size_category].append({
                    'path': obj_path,
                    'size_bytes': size_bytes,
                    'processing_time': obj_results.get('processing_time')
                })
                
                size_data['by_size_category'][size_category].append(size_bytes)
            
            if obj_results.get('processing_time'):
                performance_data['overall'].append({
                    'path': obj_path,
                    'size_bytes': size_bytes,
                    'size_category': size_category,
                    'processing_time': obj_results.get('processing_time')
                })
            
            # Check for errors of all types
            derivatives = obj_results.get('derivatives', {})
            has_error = False
            
            for derivative, derivative_results in derivatives.items():
                # Initialize derivative-specific performance data if needed
                if derivative not in performance_data['by_derivative']:
                    performance_data['by_derivative'][derivative] = []
                
                # Add derivative performance data
                if 'duration' in obj_results.get('derivatives', {}).get(derivative, {}):
                    performance_data['by_derivative'][derivative].append({
                        'path': obj_path,
                        'size_bytes': size_bytes,
                        'size_category': size_category,
                        'duration': obj_results['derivatives'][derivative]['duration']
                    })
                
                # Check for errors - track both 500 errors specifically and all errors
                # Error statuses can be: 'error', 'timeout', 'connection_error', 'exception'
                status = derivative_results.get('status', '')
                if status != 'success' and status != 'unknown':
                    # Add to all errors list - extract full error details
                    # Preserve all details from the API response
                    error_details = derivative_results.copy()
                    
                    # Ensure we capture the complete original API error if available
                    if 'error_details' in derivative_results and isinstance(derivative_results['error_details'], dict):
                        # Extract raw API response and error data
                        if 'raw_response' in derivative_results['error_details']:
                            error_details['raw_api_response'] = derivative_results['error_details']['raw_response']
                        if 'original_error' in derivative_results['error_details']:
                            error_details['original_api_error'] = derivative_results['error_details']['original_error']
                    
                    all_errors.append({
                        'path': obj_path,
                        'size_bytes': size_bytes,
                        'size_category': size_category,
                        'derivative': derivative,
                        'details': error_details
                    })
                    has_error = True
                    
                    # Additionally track 500 errors separately for backward compatibility
                    if derivative_results.get('status_code') == 500:
                        http_500_errors.append({
                            'path': obj_path,
                            'size_bytes': size_bytes,
                            'size_category': size_category,
                            'derivative': derivative,
                            'details': derivative_results
                        })
            
            # Track sizes for correlation analysis
            if has_error:
                size_data['error_sizes'].append(size_bytes)
            else:
                size_data['success_sizes'].append(size_bytes)
        
        # Calculate stats based on collected data
        performance_stats = calculate_performance_stats(performance_data, size_data, logger)
        
        # Assemble the complete results object
        results_obj = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'parameters': metadata,
            'summary': {
                'total_processed': processed,
                'total_count': total_objects,
                'percent_complete': (processed / total_objects) * 100 if total_objects > 0 else 0,
                'http_500_error_count': len(http_500_errors),
                'all_error_count': len(all_errors)
            },
            'results': all_results,
            'http_500_errors': http_500_errors,
            'all_errors': all_errors,  # Include all errors in the results
            'performance_metrics': performance_stats
        }
        
        # Write to file
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(results_obj, f, indent=2, default=str)
        
        if logger:
            logger.info(f"Results written to {output_file}")
    
    except Exception as e:
        if logger:
            logger.error(f"Error writing results to file: {str(e)}", exc_info=True)

def calculate_performance_stats(performance_data, size_data, logger=None):
    """
    Calculate performance statistics from collected data.
    
    Args:
        performance_data: Dictionary of performance data
        size_data: Dictionary of size data
        logger: Logger instance
        
    Returns:
        Dictionary of calculated statistics
    """
    stats = {
        'overall': {},
        'by_size_category': {},
        'by_derivative': {},
        'correlation': {}
    }
    
    try:
        # Calculate overall stats
        if performance_data['overall']:
            times = [entry['processing_time'] for entry in performance_data['overall'] 
                    if entry.get('processing_time') is not None]
            
            if times:
                stats['overall'] = {
                    'count': len(times),
                    'min_time': min(times),
                    'max_time': max(times),
                    'avg_time': sum(times) / len(times),
                    'median_time': np.median(times) if times else None,
                    'p90_time': np.percentile(times, 90) if times else None,
                    'p95_time': np.percentile(times, 95) if times else None,
                    'std_dev': np.std(times) if times else None
                }
        
        # Calculate stats by size category
        for category, entries in performance_data['by_size_category'].items():
            times = [entry['processing_time'] for entry in entries 
                    if entry.get('processing_time') is not None]
            
            if times:
                stats['by_size_category'][category] = {
                    'count': len(times),
                    'min_time': min(times),
                    'max_time': max(times),
                    'avg_time': sum(times) / len(times),
                    'median_time': np.median(times) if times else None,
                    'p90_time': np.percentile(times, 90) if times else None,
                    'p95_time': np.percentile(times, 95) if times else None,
                    'std_dev': np.std(times) if times else None
                }
        
        # Calculate stats by derivative
        for derivative, entries in performance_data['by_derivative'].items():
            times = [entry['duration'] for entry in entries 
                    if entry.get('duration') is not None]
            
            if times:
                stats['by_derivative'][derivative] = {
                    'count': len(times),
                    'min_time': min(times),
                    'max_time': max(times),
                    'avg_time': sum(times) / len(times),
                    'median_time': np.median(times) if times else None,
                    'p90_time': np.percentile(times, 90) if times else None,
                    'p95_time': np.percentile(times, 95) if times else None,
                    'std_dev': np.std(times) if times else None
                }
        
        # Calculate size correlation metrics
        # Correlation between size and processing time
        overall_sizes = [entry['size_bytes'] for entry in performance_data['overall'] 
                        if entry.get('size_bytes') is not None and entry.get('processing_time') is not None]
        overall_times = [entry['processing_time'] for entry in performance_data['overall'] 
                        if entry.get('size_bytes') is not None and entry.get('processing_time') is not None]
        
        if overall_sizes and overall_times and len(overall_sizes) == len(overall_times):
            # Calculate Pearson correlation coefficient
            correlation_coef = np.corrcoef(overall_sizes, overall_times)[0, 1]
            stats['correlation']['size_time_pearson'] = correlation_coef
            
            # Calculate regression line (slope, intercept)
            slope, intercept = np.polyfit(overall_sizes, overall_times, 1)
            stats['correlation']['regression_slope'] = slope
            stats['correlation']['regression_intercept'] = intercept
            
            # Estimate time for different file sizes
            for size_mb in [10, 50, 100, 500, 1000]:
                size_bytes = size_mb * 1024 * 1024
                estimated_time = slope * size_bytes + intercept
                stats['correlation'][f'estimated_time_{size_mb}MB'] = estimated_time
        
        # Error distribution analysis
        error_sizes = size_data.get('error_sizes', [])
        success_sizes = size_data.get('success_sizes', [])
        
        if error_sizes:
            stats['error_distribution'] = {
                'count': len(error_sizes),
                'min_size': min(error_sizes) if error_sizes else None,
                'max_size': max(error_sizes) if error_sizes else None,
                'avg_size': sum(error_sizes) / len(error_sizes) if error_sizes else None,
                'median_size': np.median(error_sizes) if error_sizes else None
            }
        
        if success_sizes:
            stats['success_distribution'] = {
                'count': len(success_sizes),
                'min_size': min(success_sizes) if success_sizes else None,
                'max_size': max(success_sizes) if success_sizes else None,
                'avg_size': sum(success_sizes) / len(success_sizes) if success_sizes else None,
                'median_size': np.median(success_sizes) if success_sizes else None
            }
        
        # Calculate error rate by size quartiles
        all_sizes = error_sizes + success_sizes
        if all_sizes:
            quartiles = np.percentile(all_sizes, [25, 50, 75, 100])
            quartile_ranges = [
                (0, quartiles[0]),
                (quartiles[0], quartiles[1]),
                (quartiles[1], quartiles[2]),
                (quartiles[2], quartiles[3])
            ]
            
            stats['error_by_size_quartile'] = []
            for i, (lower, upper) in enumerate(quartile_ranges):
                errors_in_range = sum(1 for size in error_sizes if lower <= size < upper)
                total_in_range = sum(1 for size in all_sizes if lower <= size < upper)
                error_rate = (errors_in_range / total_in_range) * 100 if total_in_range > 0 else 0
                
                stats['error_by_size_quartile'].append({
                    'quartile': i + 1,
                    'size_range': {
                        'lower_bytes': lower,
                        'upper_bytes': upper,
                        'lower_formatted': video_utils.format_file_size(lower),
                        'upper_formatted': video_utils.format_file_size(upper)
                    },
                    'error_count': errors_in_range,
                    'total_count': total_in_range,
                    'error_rate': error_rate
                })
                
    except Exception as e:
        if logger:
            logger.error(f"Error calculating performance stats: {str(e)}", exc_info=True)
    
    return stats

def print_summary_statistics(all_results, elapsed_time, total_objects, logger=None):
    """
    Print summary statistics about the processing results.
    
    Args:
        all_results: Dictionary of all results
        elapsed_time: Total elapsed time
        total_objects: Total number of objects
        logger: Logger instance
    """
    try:
        # Count successes and errors
        total_processed = len(all_results)
        
        success_count = 0
        error_count = 0
        derivative_stats = {}
        
        for obj_path, obj_results in all_results.items():
            derivatives = obj_results.get('derivatives', {})
            
            for derivative, derivative_results in derivatives.items():
                # Initialize derivative stats if needed
                if derivative not in derivative_stats:
                    derivative_stats[derivative] = {
                        'success': 0,
                        'error': 0,
                        'total': 0
                    }
                
                status = derivative_results.get('status', 'unknown')
                derivative_stats[derivative]['total'] += 1
                
                if status == 'success':
                    derivative_stats[derivative]['success'] += 1
                    success_count += 1
                else:
                    derivative_stats[derivative]['error'] += 1
                    error_count += 1
        
        # Format statistics for display
        overall_stats = [
            ['Total Objects', total_objects],
            ['Processed', total_processed],
            ['Progress', f"{(total_processed / total_objects) * 100:.1f}%" if total_objects > 0 else "0%"],
            ['Time Elapsed', f"{elapsed_time:.1f} seconds"],
            ['Average Per Object', f"{(elapsed_time / total_processed):.2f} seconds" if total_processed > 0 else "N/A"]
        ]
        
        result_stats = [
            ['Success', success_count],
            ['Error', error_count],
            ['Success Rate', f"{(success_count / (success_count + error_count)) * 100:.1f}%" if (success_count + error_count) > 0 else "N/A"]
        ]
        
        derivative_table = []
        for derivative, stats in derivative_stats.items():
            success_rate = (stats['success'] / stats['total']) * 100 if stats['total'] > 0 else 0
            derivative_table.append([
                derivative,
                stats['success'],
                stats['error'],
                stats['total'],
                f"{success_rate:.1f}%"
            ])
        
        # Print the statistics
        if logger:
            logger.info("\n=== Processing Summary ===")
            logger.info("\nOverall Statistics:")
            logger.info(tabulate(overall_stats, tablefmt="simple"))
            
            logger.info("\nResults:")
            logger.info(tabulate(result_stats, tablefmt="simple"))
            
            logger.info("\nBy Derivative:")
            logger.info(tabulate(
                derivative_table,
                headers=["Derivative", "Success", "Error", "Total", "Success Rate"],
                tablefmt="simple"
            ))
    
    except Exception as e:
        if logger:
            logger.error(f"Error printing summary statistics: {str(e)}", exc_info=True)

def generate_stats_report(stats, format_type='markdown'):
    """
    Generate a statistics report in markdown or JSON format.
    
    Args:
        stats: Statistics dictionary
        format_type: Output format ('markdown' or 'json')
        
    Returns:
        String containing the formatted report
    """
    if format_type == 'json':
        return json.dumps(stats, indent=2)
    
    # Calculate aggregated statistics
    total_count = stats['total_processed']
    success_rate = (stats['success_count'] / total_count) * 100 if total_count > 0 else 0
    
    ttfb_values = stats.get('ttfb_values', [])
    total_time_values = stats.get('total_time_values', [])
    
    # Calculate timing percentiles if we have values
    ttfb_percentiles = {}
    total_time_percentiles = {}
    
    if ttfb_values:
        ttfb_percentiles = {
            'min': min(ttfb_values),
            'p50': np.percentile(ttfb_values, 50),
            'p90': np.percentile(ttfb_values, 90),
            'p95': np.percentile(ttfb_values, 95),
            'p99': np.percentile(ttfb_values, 99),
            'max': max(ttfb_values)
        }
    
    if total_time_values:
        total_time_percentiles = {
            'min': min(total_time_values),
            'p50': np.percentile(total_time_values, 50),
            'p90': np.percentile(total_time_values, 90),
            'p95': np.percentile(total_time_values, 95),
            'p99': np.percentile(total_time_values, 99),
            'max': max(total_time_values)
        }
    
    # Calculate size category statistics
    size_category_rows = []
    for category, category_stats in stats['by_size_category'].items():
        cat_count = category_stats['count']
        if cat_count == 0:
            continue
            
        cat_ttfb = category_stats.get('ttfb_values', [])
        cat_total_time = category_stats.get('total_time_values', [])
        
        row = [
            category.capitalize(),
            cat_count,
            video_utils.format_file_size(category_stats['size_bytes']),
            f"{(cat_count / total_count) * 100:.1f}%",
        ]
        
        # Add timing stats if available
        if cat_ttfb:
            row.append(f"{np.median(cat_ttfb):.3f}s")
            row.append(f"{np.percentile(cat_ttfb, 95):.3f}s")
        else:
            row.extend(["-", "-"])
            
        if cat_total_time:
            row.append(f"{np.median(cat_total_time):.3f}s")
            row.append(f"{np.percentile(cat_total_time, 95):.3f}s")
        else:
            row.extend(["-", "-"])
            
        size_category_rows.append(row)
    
    # Create error type breakdown
    error_rows = []
    for error_type, count in stats['errors_by_type'].items():
        error_rows.append([
            error_type,
            count,
            f"{(count / stats['error_count']) * 100:.1f}%" if stats['error_count'] > 0 else "0%"
        ])
    
    # Build markdown report
    md_report = [
        f"# Video Transform Processing Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"## Summary",
        f"- Total files processed: {total_count}",
        f"- Total size: {video_utils.format_file_size(stats['total_size_bytes'])}",
        f"- Success rate: {success_rate:.1f}%",
        f"- Success count: {stats['success_count']}",
        f"- Error count: {stats['error_count']}",
        "",
        f"## Response Time Statistics (successful requests)",
    ]
    
    # Add timing tables if we have values
    if ttfb_values:
        md_report.extend([
            "",
            "### Time to First Byte (TTFB)",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Minimum | {ttfb_percentiles['min']:.3f}s |",
            f"| Median (p50) | {ttfb_percentiles['p50']:.3f}s |",
            f"| p90 | {ttfb_percentiles['p90']:.3f}s |",
            f"| p95 | {ttfb_percentiles['p95']:.3f}s |",
            f"| p99 | {ttfb_percentiles['p99']:.3f}s |",
            f"| Maximum | {ttfb_percentiles['max']:.3f}s |",
            "",
        ])
    
    if total_time_values:
        md_report.extend([
            "",
            "### Total Download Time",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Minimum | {total_time_percentiles['min']:.3f}s |",
            f"| Median (p50) | {total_time_percentiles['p50']:.3f}s |",
            f"| p90 | {total_time_percentiles['p90']:.3f}s |",
            f"| p95 | {total_time_percentiles['p95']:.3f}s |",
            f"| p99 | {total_time_percentiles['p99']:.3f}s |",
            f"| Maximum | {total_time_percentiles['max']:.3f}s |",
            "",
        ])
    
    # Add size category table
    if size_category_rows:
        md_report.extend([
            "",
            "## Results by File Size Category",
            "",
            "| Category | Count | Total Size | % of Files | Median TTFB | p95 TTFB | Median Download | p95 Download |",
            "|----------|-------|------------|------------|-------------|----------|----------------|--------------|",
        ])
        
        for row in size_category_rows:
            md_report.append("| " + " | ".join(str(col) for col in row) + " |")
    
    # Add error breakdown
    if error_rows:
        md_report.extend([
            "",
            "## Error Breakdown",
            "",
            "| Error Type | Count | Percentage |",
            "|------------|-------|------------|",
        ])
        
        for row in error_rows:
            md_report.append("| " + " | ".join(str(col) for col in row) + " |")
    
    return "\n".join(md_report)

def generate_size_report(file_list, size_threshold_mib, format_type='markdown'):
    """
    Generate a report of file sizes.
    
    Args:
        file_list: List of (file_path, size_bytes) tuples
        size_threshold_mib: Size threshold in MiB for reporting
        format_type: Output format ('markdown' or 'json')
        
    Returns:
        String containing the formatted report
    """
    if not file_list:
        return "No files found matching criteria."
    
    # Sort files by size (largest first)
    file_list.sort(key=lambda x: x[1], reverse=True)
    
    # Find files above threshold
    threshold_bytes = size_threshold_mib * 1024 * 1024
    files_above_threshold = [(path, size) for path, size in file_list if size > threshold_bytes]
    
    if format_type == 'json':
        report_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "size_threshold_mib": size_threshold_mib,
            "total_files": len(file_list),
            "total_size_bytes": sum(size for _, size in file_list),
            "files_above_threshold": {
                "count": len(files_above_threshold),
                "total_size_bytes": sum(size for _, size in files_above_threshold),
                "files": [
                    {"path": path, "size_bytes": size, "size_mib": size / (1024 * 1024)}
                    for path, size in files_above_threshold
                ]
            },
            "files": [
                {"path": path, "size_bytes": size, "size_mib": size / (1024 * 1024)}
                for path, size in file_list
            ]
        }
        return json.dumps(report_data, indent=2)
    
    # Generate markdown report
    total_size = sum(size for _, size in file_list)
    threshold_size = sum(size for _, size in files_above_threshold)
    
    md_report = [
        f"# File Size Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"## Summary",
        f"- Size threshold: {size_threshold_mib} MiB",
        f"- Total files: {len(file_list)}",
        f"- Total size: {video_utils.format_file_size(total_size)}",
    ]
    
    # Add threshold summary section
    if files_above_threshold:
        md_report.extend([
            f"",
            f"### Files Above Threshold ({size_threshold_mib} MiB)",
            f"- Files above threshold: {len(files_above_threshold)} of {len(file_list)} ({len(files_above_threshold)/len(file_list)*100:.1f}%)",
            f"- Total size of large files: {video_utils.format_file_size(threshold_size)} ({threshold_size/total_size*100:.1f}% of total)",
            f"",
            f"| # | File Path | Size |",
            f"|---|----------|------|",
        ])
        
        for i, (path, size) in enumerate(files_above_threshold):
            md_report.append(f"| {i+1} | {path} | {video_utils.format_file_size(size)} |")
    else:
        md_report.append(f"\n**No files above the {size_threshold_mib} MiB threshold found.**")
    
    # Add complete file list
    md_report.extend([
        "",
        "## All Files by Size (Largest First)",
        "",
        "| # | File Path | Size |",
        "|---|----------|------|",
    ])
    
    for i, (path, size) in enumerate(file_list):
        md_report.append(f"| {i+1} | {path} | {video_utils.format_file_size(size)} |")
    
    return "\n".join(md_report)

def generate_optimization_report(optimization_results, include_replacement_status=False):
    """
    Generate a report for video optimization results.
    
    Args:
        optimization_results: List of optimization result dictionaries
        include_replacement_status: Whether to include in-place replacement status
        
    Returns:
        String containing the formatted report
    """
    if not optimization_results:
        return "No optimization results available."
    
    # Calculate totals
    total_original_size = sum(result.get('original_size', 0) for result in optimization_results)
    total_new_size = sum(result.get('new_size', 0) for result in optimization_results)
    total_reduction = total_original_size - total_new_size
    percent_reduction = (total_reduction / total_original_size) * 100 if total_original_size > 0 else 0
    
    # Check if we have WebM results
    has_webm = any('webm_size' in result for result in optimization_results)
    
    # Generate markdown report
    md_report = [
        f"# Video Optimization Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"## Summary",
        f"- Files processed: {len(optimization_results)}",
    ]
    
    # Add replacement stats if needed
    if include_replacement_status:
        replaced_count = sum(1 for r in optimization_results if r.get('replaced_in_place', False))
        md_report.append(f"- Files replaced in-place: {replaced_count} of {len(optimization_results)} ({(replaced_count/len(optimization_results))*100:.1f}%)")
    
    md_report.extend([
        f"- Total original size: {video_utils.format_file_size(total_original_size)}",
        f"- Total optimized size: {video_utils.format_file_size(total_new_size)}",
        f"- Total reduction: {video_utils.format_file_size(total_reduction)} ({percent_reduction:.1f}%)",
        "",
        "## Individual File Results",
        "",
    ])
    
    # Create table header based on whether we have WebM results and replacement status
    if has_webm and include_replacement_status:
        md_report.extend([
            "| File | Original Size | Optimized Size | Reduction | WebM Size | WebM Reduction | Codec | Resolution | Fit Mode | Replaced In-place |",
            "|------|---------------|----------------|-----------|-----------|---------------|-------|------------|----------|-------------------|",
        ])
    elif has_webm:
        md_report.extend([
            "| File | Original Size | Optimized Size | Reduction | WebM Size | WebM Reduction | Codec | Resolution | Fit Mode |",
            "|------|---------------|----------------|-----------|-----------|---------------|-------|------------|----------|",
        ])
    elif include_replacement_status:
        md_report.extend([
            "| File | Original Size | Optimized Size | Reduction | Codec | Resolution | Fit Mode | Replaced In-place |",
            "|------|---------------|----------------|-----------|-------|------------|----------|-------------------|",
        ])
    else:
        md_report.extend([
            "| File | Original Size | Optimized Size | Reduction | Codec | Resolution | Fit Mode |",
            "|------|---------------|----------------|-----------|-------|------------|----------|",
        ])
    
    # Add rows for each file
    for result in optimization_results:
        output_path = os.path.basename(result.get('output_path', 'unknown'))
        original_size = video_utils.format_file_size(result.get('original_size', 0))
        new_size = video_utils.format_file_size(result.get('new_size', 0))
        reduction = result.get('reduction_percent', 0)
        codec = result.get('codec', 'unknown')
        resolution = result.get('resolution', 'unknown')
        fit_mode = result.get('fit_mode', 'contain')
        replaced = "✅ Yes" if result.get('replaced_in_place', False) else "❌ No"
        
        if has_webm and include_replacement_status and 'webm_size' in result:
            webm_size = video_utils.format_file_size(result.get('webm_size', 0))
            webm_reduction = result.get('webm_reduction_percent', 0)
            md_report.append(
                f"| {output_path} | {original_size} | {new_size} | {reduction:.1f}% | "
                f"{webm_size} | {webm_reduction:.1f}% | {codec} | {resolution} | {fit_mode} | {replaced} |"
            )
        elif has_webm and 'webm_size' in result:
            webm_size = video_utils.format_file_size(result.get('webm_size', 0))
            webm_reduction = result.get('webm_reduction_percent', 0)
            md_report.append(
                f"| {output_path} | {original_size} | {new_size} | {reduction:.1f}% | "
                f"{webm_size} | {webm_reduction:.1f}% | {codec} | {resolution} | {fit_mode} |"
            )
        elif include_replacement_status:
            # Add a row with replacement status
            md_report.append(
                f"| {output_path} | {original_size} | {new_size} | {reduction:.1f}% | "
                f"{codec} | {resolution} | {fit_mode} | {replaced} |"
            )
        else:
            # Add a row without WebM data or replacement status
            md_report.append(
                f"| {output_path} | {original_size} | {new_size} | {reduction:.1f}% | "
                f"{codec} | {resolution} | {fit_mode} |"
            )
    
    return "\n".join(md_report)
def generate_error_report(results_data, format_type='markdown'):
    """
    Generate a detailed error report from results file.
    
    Args:
        results_data: Loaded results data dictionary
        format_type: Output format ('markdown' or 'json')
        
    Returns:
        String containing the formatted report
    """
    from collections import Counter
    import numpy as np
    
    # Use the all_errors list if available, otherwise extract from results
    if 'all_errors' in results_data and results_data['all_errors']:
        # Use the pre-extracted list of all errors
        all_error_entries = results_data['all_errors']
        errors = []
        
        # Convert to the expected format
        for entry in all_error_entries:
            errors.append({
                'file': entry.get('path', ''),
                'derivative': entry.get('derivative', 'default'),
                'status_code': entry.get('details', {}).get('status_code'),
                'error_type': entry.get('details', {}).get('error_type', 'unknown'),
                'url': entry.get('details', {}).get('url', ''),
                'error_msg': entry.get('details', {}).get('error_details', {}).get('response', '') or 
                             entry.get('details', {}).get('raw_api_response', '') or 
                             entry.get('details', {}).get('original_api_error', '') or 
                             entry.get('details', {}).get('error_message', ''),
                'size_bytes': entry.get('size_bytes', 0),
                'size_category': entry.get('size_category', 'unknown'),
                'ttfb': entry.get('details', {}).get('ttfb'),
                'duration': entry.get('details', {}).get('duration')
            })
    else:
        # Extract all errors from the results (fallback for backward compatibility)
        errors = []
        
        for obj_path, obj_result in results_data.get('results', {}).items():
            size_bytes = obj_result.get('size_bytes', 0)
            size_category = obj_result.get('size_category', 'unknown')
            derivatives_dict = obj_result.get('derivatives', {})
            
            # If structure has derivatives
            if derivatives_dict:
                for deriv, deriv_result in derivatives_dict.items():
                    # Error statuses can be: 'error', 'timeout', 'connection_error', 'exception'
                    status = deriv_result.get('status', '')
                    if status != 'success' and status != 'unknown':
                        errors.append({
                            'file': obj_path,
                            'derivative': deriv,
                            'status_code': deriv_result.get('status_code'),
                            'error_type': deriv_result.get('error_type', 'unknown'),
                            'url': deriv_result.get('url', ''),
                            'error_msg': deriv_result.get('error_details', {}).get('response', '') or
                                 deriv_result.get('raw_api_response', '') or
                                 deriv_result.get('original_api_error', '') or
                                 deriv_result.get('error_message', ''),
                            'size_bytes': size_bytes,
                            'size_category': size_category,
                            'ttfb': deriv_result.get('ttfb'),
                            'duration': deriv_result.get('duration')
                        })
            # Alternative structure without derivatives wrapper
            # Error statuses can be: 'error', 'timeout', 'connection_error', 'exception'
            else:
                status = obj_result.get('status', '')
                if status != 'success' and status != 'unknown':
                    errors.append({
                        'file': obj_path,
                        'derivative': 'default',
                        'status_code': obj_result.get('status_code'),
                        'error_type': obj_result.get('error_type', 'unknown'),
                        'url': obj_result.get('url', ''),
                        'error_msg': obj_result.get('error_details', {}).get('response', '') or
                             obj_result.get('raw_api_response', '') or
                             obj_result.get('original_api_error', '') or
                             obj_result.get('error_message', ''),
                        'size_bytes': size_bytes,
                        'size_category': size_category,
                        'ttfb': obj_result.get('ttfb'),
                        'duration': obj_result.get('duration')
                    })
    
    # Analyze errors for more specific error types based on error messages
    for error in errors:
        # Try to identify more specific error types from error messages
        error_msg = error.get('error_msg', '').lower()
        
        # Default error type is what's in the data
        specific_error_type = error.get('error_type', 'unknown')
        
        # Look for more specific error indicators in the message
        if 'timeout' in error_msg or 'timed out' in error_msg:
            specific_error_type = 'timeout_error'
        elif 'memory' in error_msg or 'out of memory' in error_msg:
            specific_error_type = 'memory_error'
        elif 'format' in error_msg and ('invalid' in error_msg or 'unsupported' in error_msg):
            specific_error_type = 'format_error'
        elif 'corrupt' in error_msg:
            specific_error_type = 'corrupt_file_error'
        elif 'bad request' in error_msg or '400' in error_msg:
            specific_error_type = 'bad_request_error'
        elif 'throttl' in error_msg or 'rate limit' in error_msg:
            specific_error_type = 'rate_limit_error'
        elif 'permission' in error_msg or 'access denied' in error_msg or 'unauthorized' in error_msg:
            specific_error_type = 'permission_error'
        
        # Update the error with more specific type if we found one
        error['specific_error_type'] = specific_error_type
    
    # Extract file sizes for analysis
    error_file_sizes = [e['size_bytes'] for e in errors if 'size_bytes' in e]
    
    # Calculate success file sizes by subtracting error files from all files
    success_file_sizes = []
    for obj_path, obj_result in results_data.get('results', {}).items():
        size_bytes = obj_result.get('size_bytes', 0)
        if size_bytes > 0:
            # Check if this file is in errors
            is_error = any(e['file'] == obj_path for e in errors)
            if not is_error:
                success_file_sizes.append(size_bytes)
    
    # If no errors found
    if not errors:
        if format_type == 'json':
            return json.dumps({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'error_count': 0,
                'message': 'No errors found in the results file'
            }, indent=2)
        return "# Error Report\n\nNo errors found in the results file."
    
    # Count errors by type using both generic and specific types
    error_types = Counter([e['error_type'] for e in errors])
    
    # Also track the more specific error types we identified
    specific_error_types = Counter([e.get('specific_error_type', 'unknown') for e in errors])
    
    # Initialize combined error types with the original types
    combined_error_types = {}
    
    # Initialize recommendations lists for both output formats
    recommendations = []
    troubleshooting_recommendations = []
    
    # Map of generic error types to more specific types
    error_type_mapping = {
        'server_error': ['bad_request_error', 'timeout_error', 'memory_error', 
                        'format_error', 'corrupt_file_error', 'rate_limit_error', 
                        'permission_error'],
        'error': ['bad_request_error', 'timeout_error', 'memory_error', 
                'format_error', 'corrupt_file_error', 'rate_limit_error', 
                'permission_error']
    }
    
    # Track which errors have been assigned a specific type
    errors_with_specific_type = set()
    
    # First add all specific error types
    for i, error in enumerate(errors):
        spec_type = error.get('specific_error_type')
        if spec_type and spec_type != 'unknown':
            # This error has a specific type
            combined_error_types[spec_type] = combined_error_types.get(spec_type, 0) + 1
            errors_with_specific_type.add(i)
    
    # Then add generic types only for errors that don't have a specific type
    for i, error in enumerate(errors):
        if i not in errors_with_specific_type:
            gen_type = error.get('error_type', 'unknown')
            combined_error_types[gen_type] = combined_error_types.get(gen_type, 0) + 1
    
    # Count errors by status code
    status_codes = Counter([e['status_code'] for e in errors])
    
    # Find common error messages
    error_msgs = Counter([e['error_msg'] for e in errors if e['error_msg']])
    
    # Count errors by size category
    size_categories = Counter([e['size_category'] for e in errors])
    
    # Generate error counts by derivative type
    derivatives = Counter([e['derivative'] for e in errors])
    
    # Calculate stats for error files
    size_stats = {}
    if error_file_sizes:
        size_stats['error_files'] = {
            'count': len(error_file_sizes),
            'min_size': min(error_file_sizes),
            'max_size': max(error_file_sizes),
            'avg_size': sum(error_file_sizes) / len(error_file_sizes),
            'median_size': np.median(error_file_sizes),
            'total_size': sum(error_file_sizes)
        }
    
    if success_file_sizes:
        size_stats['success_files'] = {
            'count': len(success_file_sizes),
            'min_size': min(success_file_sizes),
            'max_size': max(success_file_sizes),
            'avg_size': sum(success_file_sizes) / len(success_file_sizes),
            'median_size': np.median(success_file_sizes),
            'total_size': sum(success_file_sizes)
        }
    
    # Prepare detailed error information
    detailed_errors = []
    for e in errors:
        detailed_errors.append({
            'file': e['file'],
            'derivative': e['derivative'],
            'status_code': e['status_code'],
            'error_type': e['error_type'],
            'url': e['url'],
            'error_msg': e['error_msg'],
            'size_bytes': e['size_bytes'],
            'size_formatted': video_utils.format_file_size(e['size_bytes']),
            'size_category': e['size_category']
        })
    
    # Group errors by type with examples
    error_examples = {}
    for error_type in error_types.keys():
        examples = [e for e in errors if e['error_type'] == error_type]
        if examples:
            error_examples[error_type] = examples[0]
    
    # JSON output format
    if format_type == 'json':
        # Force add recommendations based on what we know exists in the data
        if '400' in ''.join([e.get('error_msg', '') for e in errors[:5]]) and not troubleshooting_recommendations:
            troubleshooting_recommendations.append("Bad Request (400) Errors: Check input file formats and parameters to ensure they meet API requirements.")
            
        if any(e.get('status_code') == 500 for e in errors) and not any("Server Errors (500)" in rec for rec in troubleshooting_recommendations):
            troubleshooting_recommendations.append("Server Errors (500): These indicate issues on the server side. Consider retrying these files at a later time or contacting the API provider with specific error details.")
            
        if any(e.get('size_bytes', 0) > 100*1024*1024 for e in errors) and not any("Large File Errors" in rec for rec in troubleshooting_recommendations):
            troubleshooting_recommendations.append("Large File Errors: Consider pre-processing large files (>100 MB) to reduce size before transformation.")
    
        error_report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'original_timestamp': results_data.get('timestamp', 'unknown'),
            'summary': {
                'total_errors': len(errors),
                'total_processed': results_data.get('stats', {}).get('total_processed', 0),
                'error_rate': len(errors) / results_data.get('stats', {}).get('total_processed', 1) * 100,
                'error_source': 'Extracted from results' if 'all_errors' not in results_data else 'Pre-collected errors',
                'original_all_errors_count': len(results_data.get('all_errors', [])) if 'all_errors' in results_data else 0
            },
            'error_types': dict(error_types),
            'specific_error_types': dict(specific_error_types),
            'combined_error_types': combined_error_types,
            'status_codes': dict(status_codes),
            'size_categories': dict(size_categories),
            'derivatives': dict(derivatives),
            'common_error_messages': dict(error_msgs.most_common(10)),
            'size_statistics': size_stats,
            'detailed_errors': detailed_errors,
            'troubleshooting_recommendations': troubleshooting_recommendations,
            'error_examples': {k: {
                'file': v['file'],
                'url': v['url'],
                'status_code': v['status_code'],
                'error_msg': v['error_msg'],
                'size_bytes': v['size_bytes'],
                'size_formatted': video_utils.format_file_size(v['size_bytes'])
            } for k, v in error_examples.items()}
        }
        
        return json.dumps(error_report, indent=2, default=str)
    
    # Generate markdown report
    lines = [
        "# Video Transformation Error Report",
        f"\nGenerated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Original data timestamp: {results_data.get('timestamp', 'unknown')}\n",
        "## Summary",
        f"- **Total errors**: {len(errors)}",
        f"- **Error source**: {'Extracted from results' if 'all_errors' not in results_data else f'Pre-collected errors'}",
        f"- **Total processed files**: {results_data.get('stats', {}).get('total_processed', 'unknown')}",
        f"- **Error rate**: {len(errors) / results_data.get('stats', {}).get('total_processed', 1) * 100:.2f}%\n",
        "## Error Types",
        tabulate([(k, v, f"{(v / len(errors)) * 100:.1f}%") for k, v in combined_error_types.items()], 
                headers=["Error Type", "Count", "Percentage"],
                tablefmt="pipe"),
                
        "\n## Specific Error Types (Based on Error Message Analysis)",
        tabulate([(k, v, f"{(v / len(errors)) * 100:.1f}%") for k, v in specific_error_types.items() 
                if k != 'unknown' and v > 0], 
                headers=["Detailed Error Type", "Count", "Percentage"],
                tablefmt="pipe"),
        "\n## Status Codes",
        tabulate([(k, v, f"{(v / len(errors)) * 100:.1f}%") for k, v in status_codes.items()], 
                headers=["Status Code", "Count", "Percentage"],
                tablefmt="pipe"),
        "\n## Errors by Size Category",
        tabulate([(k, v, f"{(v / len(errors)) * 100:.1f}%") for k, v in size_categories.items()], 
                headers=["Size Category", "Count", "Percentage"],
                tablefmt="pipe"),
        "\n## Errors by Derivative",
        tabulate([(k, v, f"{(v / len(errors)) * 100:.1f}%") for k, v in derivatives.items()], 
                headers=["Derivative", "Count", "Percentage"],
                tablefmt="pipe"),
    ]
    
    # Add file size statistics
    if size_stats:
        lines.extend(["\n## File Size Statistics"])
        
        if 'error_files' in size_stats:
            stats = size_stats['error_files']
            lines.extend([
                "\n### Error Files",
                f"- Count: {stats['count']}", 
                f"- Min size: {video_utils.format_file_size(stats['min_size'])}",
                f"- Max size: {video_utils.format_file_size(stats['max_size'])}",
                f"- Average size: {video_utils.format_file_size(stats['avg_size'])}",
                f"- Median size: {video_utils.format_file_size(stats['median_size'])}",
                f"- Total size: {video_utils.format_file_size(stats['total_size'])}"
            ])
        
        if 'success_files' in size_stats:
            stats = size_stats['success_files']
            lines.extend([
                "\n### Success Files",
                f"- Count: {stats['count']}", 
                f"- Min size: {video_utils.format_file_size(stats['min_size'])}",
                f"- Max size: {video_utils.format_file_size(stats['max_size'])}",
                f"- Average size: {video_utils.format_file_size(stats['avg_size'])}",
                f"- Median size: {video_utils.format_file_size(stats['median_size'])}",
                f"- Total size: {video_utils.format_file_size(stats['total_size'])}"
            ])
    
    # Add common error messages section if we have data
    if error_msgs:
        lines.extend([
            "\n## Common Error Messages",
            tabulate([(msg[:100] + "..." if len(msg) > 100 else msg, count, f"{(count / len(errors)) * 100:.1f}%") 
                     for msg, count in error_msgs.most_common(10)],
                    headers=["Error Message", "Count", "Percentage"],
                    tablefmt="pipe")
        ])
    
    # Add a detailed list of errors
    lines.extend([
        "\n## Detailed Error List",
        tabulate([(e['file'], e['derivative'], e['status_code'], 
                   e['error_type'], video_utils.format_file_size(e['size_bytes']), e['size_category']) 
                 for e in errors[:50]],  # Limit to first 50 errors for readability
                headers=["File", "Derivative", "Status Code", "Error Type", "File Size", "Size Category"],
                tablefmt="pipe")
    ])
    
    if len(errors) > 50:
        lines.append(f"\n*Note: Showing only the first 50 of {len(errors)} errors*")
    
    # Generate troubleshooting recommendations based on error types
    # (variables already initialized above)
    
    # Debug to check specific error types
    if format_type == 'json':
        for e in errors[:3]:
            print(f"DEBUG: Error type: {e.get('error_type')}, Specific error type: {e.get('specific_error_type')}")
    
    # Check for specific error patterns and add recommendations
    if any(e.get('specific_error_type') == 'bad_request_error' for e in errors):
        recommendations.append("- **Bad Request (400) Errors**: Check input file formats and parameters to ensure they meet API requirements.")
        troubleshooting_recommendations.append("Bad Request (400) Errors: Check input file formats and parameters to ensure they meet API requirements.")
        
        # Debug print for troubleshooting this issue
        if format_type == 'json':
            print(f"DEBUG: Adding bad_request_error recommendation, list now has {len(troubleshooting_recommendations)} items")
        
    if any('timeout_error' == e.get('specific_error_type') for e in errors):
        recommendations.append("- **Timeout Errors**: Consider increasing timeout settings or optimizing large files before processing.")
        troubleshooting_recommendations.append("Timeout Errors: Consider increasing timeout settings or optimizing large files before processing.")
        
    if any('memory_error' == e.get('specific_error_type') for e in errors):
        recommendations.append("- **Memory Errors**: Try processing files in smaller batches or with lower resolution settings.")
        troubleshooting_recommendations.append("Memory Errors: Try processing files in smaller batches or with lower resolution settings.")
        
    if any('format_error' == e.get('specific_error_type') for e in errors):
        recommendations.append("- **Format Errors**: Verify input files are valid video formats supported by the transformation service.")
        troubleshooting_recommendations.append("Format Errors: Verify input files are valid video formats supported by the transformation service.")
        
    if any('corrupt_file_error' == e.get('specific_error_type') for e in errors):
        recommendations.append("- **Corrupt File Errors**: Run a validation pass on source files to identify and repair corrupted videos.")
        troubleshooting_recommendations.append("Corrupt File Errors: Run a validation pass on source files to identify and repair corrupted videos.")
        
    if any(e.get('size_bytes', 0) > 100*1024*1024 for e in errors):
        recommendations.append("- **Large File Errors**: Consider pre-processing large files (>100 MB) to reduce size before transformation.")
        troubleshooting_recommendations.append("Large File Errors: Consider pre-processing large files (>100 MB) to reduce size before transformation.")
        
    # Generic catch-all recommendation if we have server errors
    if any(e.get('status_code') == 500 for e in errors):
        recommendations.append("- **Server Errors (500)**: These indicate issues on the server side. Consider retrying these files at a later time or contacting the API provider with specific error details.")
        troubleshooting_recommendations.append("Server Errors (500): These indicate issues on the server side. Consider retrying these files at a later time or contacting the API provider with specific error details.")
    
    # Add troubleshooting section if we have recommendations
    if recommendations:
        lines.extend([
            "\n## Troubleshooting Recommendations",
            "Based on the error patterns observed, consider the following recommendations:\n"
        ])
        lines.extend(recommendations)
    
    # Add specific error examples 
    if errors:
        lines.extend([
            "\n## Error Examples",
            "Here are examples of each error type:\n"
        ])
        
        # Add examples for each error type
        for error_type, example in error_examples.items():
            lines.extend([
                f"### {error_type}",
                f"File: `{example['file']}`",
                f"URL: `{example['url']}`",
                f"Status Code: {example['status_code']}",
                f"File Size: {video_utils.format_file_size(example['size_bytes'])} ({example['size_category']})",
                f"Error Message:",
                "```",
                example['error_msg'] if example['error_msg'] else "No message",
                "```\n"
            ])
    
    # Add a comprehensive list of all errors with complete details
    lines.extend([
        "\n## Complete Error Details",
        "Below is a comprehensive list of all errors with complete details:\n"
    ])
    
    # Add each error with full details
    for i, error in enumerate(errors):
        lines.extend([
            f"### Error #{i+1}: {error.get('file', 'Unknown file')}",
            f"- **Derivative**: {error.get('derivative', 'unknown')}",
            f"- **URL**: {error.get('url', 'N/A')}",
            f"- **Status Code**: {error.get('status_code', 'unknown')}",
            f"- **Error Type**: {error.get('error_type', 'unknown')}",
            f"- **Specific Error Type**: {error.get('specific_error_type', 'unknown')}",
            f"- **File Size**: {video_utils.format_file_size(error.get('size_bytes', 0))} ({error.get('size_category', 'unknown')})",
            f"- **TTFB**: {error.get('ttfb', 'N/A')}",
            f"- **Duration**: {error.get('duration', 'N/A')}",
            f"- **Error Message**:",
            "```",
            error.get('error_msg', 'No message'),
            "```\n"
        ])
    
    return '\n'.join(lines)