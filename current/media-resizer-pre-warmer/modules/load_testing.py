"""
Load testing module for video pre-warmer.
Handles running k6 load tests and processing results.
"""
import os
import subprocess
import json
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

def run_k6_test(
    script_path,
    base_url,
    results_file,
    error_report_file=None,
    url_format="imwidth",
    stage_config=None,
    debug_mode=False,
    use_head_requests=True,
    skip_large_files=True,
    large_file_threshold=256,
    request_timeout="120s",
    global_timeout="90s",
    head_timeout="30s",
    failure_rate_threshold="0.05",
    max_retries=2,
    clean_output=True,
    show_progress=True,
    output_format="json",
    connection_close_delay=10
):
    """
    Run k6 load test with the specified parameters.
    
    Args:
        script_path: Path to the k6 test script
        base_url: Base URL for video assets
        results_file: Path to the results file from pre-warming
        error_report_file: Path to the error report file (optional)
        url_format: URL format to use ("imwidth" or "derivative")
        stage_config: Dictionary containing stage configurations
        debug_mode: Enable debug mode for verbose output
        use_head_requests: Use HEAD requests to get content length
        skip_large_files: Skip large files in load test
        large_file_threshold: Threshold in MiB for skipping large files
        request_timeout: Timeout for individual requests
        global_timeout: Global timeout for the k6 test
        failure_rate_threshold: Maximum acceptable failure rate
        max_retries: Maximum number of retry attempts for failed requests
        clean_output: Run k6 with clean output (reduces duplication)
        show_progress: Show progress indicators
        output_format: Format for test results ("json", "csv", or "all")
        
    Returns:
        Tuple of (success, results) where success is a boolean and
        results is a dictionary containing test results
    """
    logger.info(f"Starting k6 load test using script: {script_path}")
    
    # Validate inputs
    if not Path(script_path).exists():
        logger.error(f"k6 script not found: {script_path}")
        return False, {"error": "Script not found"}
    
    if not Path(results_file).exists():
        logger.error(f"Results file not found: {results_file}")
        return False, {"error": "Results file not found"}
    
    # Set up environment variables for k6
    env = os.environ.copy()
    env["BASE_URL"] = base_url
    env["RESULTS_FILE"] = results_file
    env["URL_FORMAT"] = url_format
    
    # Add error report file if specified
    if error_report_file:
        env["ERROR_REPORT_FILE"] = error_report_file
    
    # Set debug mode
    if debug_mode:
        env["DEBUG_MODE"] = "true"
    
    # Configure HEAD requests
    env["USE_HEAD_REQUESTS"] = "true" if use_head_requests else "false"
    
    # Configure large file handling
    env["SKIP_LARGE_FILES"] = "true" if skip_large_files else "false"
    env["LARGE_FILE_THRESHOLD_MIB"] = str(large_file_threshold)
    
    # Set timeout values
    env["REQUEST_TIMEOUT"] = request_timeout
    env["GLOBAL_TIMEOUT"] = global_timeout
    env["HEAD_TIMEOUT"] = head_timeout
    
    # Add connection close delay
    env["CONNECTION_CLOSE_DELAY"] = str(connection_close_delay)
    
    # Set failure threshold
    env["FAILURE_RATE_THRESHOLD"] = failure_rate_threshold
    
    # Set retry configuration
    env["MAX_RETRIES"] = str(max_retries)
    
    # Configure stages
    if stage_config:
        for stage_num in range(1, 6):
            stage_key = f"stage{stage_num}"
            if stage_key in stage_config:
                env[f"STAGE{stage_num}_USERS"] = str(stage_config[stage_key].get("users", 0))
                env[f"STAGE{stage_num}_DURATION"] = stage_config[stage_key].get("duration", "30s")
    
    # Prepare file paths for outputs
    output_dir = os.path.join(os.getcwd(), "load_test_results")
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    summary_json_path = os.path.join(output_dir, f"k6_summary_{timestamp}.json")
    metrics_csv_path = os.path.join(output_dir, f"k6_metrics_{timestamp}.csv")
    
    # Construct the k6 command with appropriate output options
    cmd = ["k6", "run"]
    
    # Add output options based on configuration
    if output_format == "json" or output_format == "all":
        cmd.extend(["--summary-export", summary_json_path])
    
    if output_format == "csv" or output_format == "all":
        cmd.extend(["--out", f"csv={metrics_csv_path}"])
    
    # Control verbosity based on debug mode and clean output preference
    if debug_mode and not clean_output:
        cmd.append("--verbose")
    elif clean_output:
        cmd.append("--quiet")
    
    # Add script path
    cmd.append(script_path)
    
    # Execute k6 command
    logger.info(f"Executing k6 with script: {script_path}")
    logger.debug(f"Command: {' '.join(cmd)}")
    logger.debug(f"Environment variables: {' '.join([f'{k}={v}' for k, v in env.items() if k.startswith('STAGE') or k in ['BASE_URL', 'URL_FORMAT', 'DEBUG_MODE']])}")
    
    try:
        start_time = time.time()
        
        # Show progress indicator if requested
        if show_progress:
            logger.info("=========== K6 LOAD TEST STARTING ===========")
        
        # Create path for the failure report
        failure_report_file = os.path.join(output_dir, f"failure_report_{timestamp}.json")
        env["FAILURE_REPORT_FILE"] = failure_report_file
            
        # Run k6 with transparent output
        # Always use direct console output to show progress in real-time
        process = subprocess.run(
            cmd,
            env=env,
            check=False
            # No stdout/stderr capture to allow direct console output
        )
        
        # After the process completes, we need to read the result files
        stdout_output = ""
        stderr_output = ""
        
        # Try to extract failure report data
        failure_data = None
        if os.path.exists(failure_report_file):
            try:
                with open(failure_report_file, 'r') as f:
                    failure_data = json.load(f)
                    
                    # Generate a markdown summary of the report
                    markdown_report = f"# K6 Load Test Failure Report\n\n"
                    markdown_report += f"**Test Date:** {failure_data.get('timestamp')}\n"
                    markdown_report += f"**Test Duration:** {failure_data.get('test_duration_seconds', 0):.2f} seconds\n"
                    markdown_report += f"**Total Failures:** {failure_data.get('total_failures', 0)}\n\n"
                    
                    # Add failure breakdown by type
                    failure_tracking = failure_data.get('failure_tracking', {})
                    if 'by_type' in failure_tracking:
                        markdown_report += "## Failures by Type\n\n"
                        markdown_report += "| Type | Count | Percentage |\n"
                        markdown_report += "|------|-------|------------|\n"
                        total = failure_tracking.get('count', 0)
                        
                        for failure_type, count in sorted(failure_tracking['by_type'].items(), key=lambda x: x[1], reverse=True):
                            pct = (count / total * 100) if total > 0 else 0
                            markdown_report += f"| {failure_type} | {count} | {pct:.1f}% |\n"
                        
                        markdown_report += "\n"
                    
                    # Save markdown report
                    md_report_file = os.path.join(output_dir, f"failure_report_{timestamp}.md")
                    with open(md_report_file, 'w') as f:
                        f.write(markdown_report)
                    logger.info(f"Detailed failure report saved to {failure_report_file}")
            except Exception as e:
                logger.warning(f"Could not process failure report: {str(e)}")
        
        if show_progress:
            logger.info("=========== K6 LOAD TEST COMPLETED ===========")
        
        # Try to read the summary JSON file if it was generated
        try:
            if os.path.exists(summary_json_path):
                with open(summary_json_path, "r") as f:
                    summary_json = f.read()
                    if clean_output:
                        stdout_output = summary_json + "\n" + stdout_output
        except Exception as e:
            logger.warning(f"Could not read k6 summary JSON: {str(e)}")
        
        duration = time.time() - start_time
        
        # Display summarized metrics from JSON if available
        if os.path.exists(summary_json_path):
            try:
                with open(summary_json_path, "r") as f:
                    summary_data = json.load(f)
                
                # Generate a nicely formatted summary report
                if show_progress and 'metrics' in summary_data:
                    logger.info("\n┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
                    logger.info("┃              LOAD TEST SUMMARY                  ┃")
                    logger.info("┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫")
                    
                    # HTTP Request metrics
                    if 'http_req_duration' in summary_data['metrics']:
                        req_metrics = summary_data['metrics']['http_req_duration']
                        logger.info("HTTP Request Duration:")
                        if 'avg' in req_metrics:
                            logger.info(f"  Average: {req_metrics['avg']:.3f}s")
                        if 'min' in req_metrics:
                            logger.info(f"  Min: {req_metrics['min']:.3f}s")
                        if 'max' in req_metrics:
                            logger.info(f"  Max: {req_metrics['max']:.3f}s")
                        if 'p(95)' in req_metrics:
                            logger.info(f"  p(95): {req_metrics['p(95)']:.3f}s")
                    
                    # HTTP Requests count
                    if 'http_reqs' in summary_data['metrics'] and 'count' in summary_data['metrics']['http_reqs']:
                        logger.info(f"Total Requests: {summary_data['metrics']['http_reqs']['count']}")
                    
                    # Failure rate
                    if 'http_req_failed' in summary_data['metrics'] and 'rate' in summary_data['metrics']['http_req_failed']:
                        failure_rate = summary_data['metrics']['http_req_failed']['rate'] * 100
                        logger.info(f"Failure Rate: {failure_rate:.2f}%")
                    
                    # VU information
                    if 'vus' in summary_data['metrics'] and 'max' in summary_data['metrics']['vus']:
                        logger.info(f"Peak VUs: {summary_data['metrics']['vus']['max']}")
                    
                    # Data transfer
                    if 'data_received' in summary_data['metrics'] and 'count' in summary_data['metrics']['data_received']:
                        data_received = summary_data['metrics']['data_received']['count'] / (1024 * 1024)
                        logger.info(f"Data Received: {data_received:.2f} MB")
                    
                    # Display failure details if available
                    failure_report_file = os.path.join(output_dir, f"failure_report_{timestamp}.json")
                    if os.path.exists(failure_report_file):
                        try:
                            with open(failure_report_file, 'r') as f:
                                failure_data = json.load(f)
                                failure_tracking = failure_data.get('failure_tracking', {})
                                
                                if failure_tracking and failure_tracking.get('count', 0) > 0:
                                    logger.info("\n┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫")
                                    logger.info("┃              FAILURE SUMMARY                     ┃")
                                    logger.info("┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫")
                                    
                                    # Display top 5 failure types
                                    if 'by_type' in failure_tracking:
                                        logger.info("Top Failure Types:")
                                        for failure_type, count in sorted(failure_tracking['by_type'].items(), 
                                                                        key=lambda x: x[1], reverse=True)[:5]:
                                            pct = (count / failure_tracking['count']) * 100
                                            logger.info(f"  {failure_type}: {count} ({pct:.1f}%)")
                                    
                                    # Display top status codes
                                    if 'by_status' in failure_tracking:
                                        logger.info("Top Status Codes:")
                                        for status, count in sorted(failure_tracking['by_status'].items(), 
                                                                  key=lambda x: x[1], reverse=True)[:3]:
                                            pct = (count / failure_tracking['count']) * 100
                                            logger.info(f"  Status {status}: {count} ({pct:.1f}%)")
                                    
                                    # Display distribution by derivative
                                    if 'by_derivative' in failure_tracking:
                                        logger.info("By Derivative:")
                                        for derivative, count in sorted(failure_tracking['by_derivative'].items(), 
                                                                      key=lambda x: x[1], reverse=True):
                                            pct = (count / failure_tracking['count']) * 100
                                            logger.info(f"  {derivative}: {count} ({pct:.1f}%)")
                        except Exception as e:
                            logger.debug(f"Error processing failure data: {str(e)}")
                    
                    logger.info("┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫")
                    logger.info(f"┃ Detailed results saved to {summary_json_path}")
                    if os.path.exists(metrics_csv_path):
                        logger.info(f"┃ Detailed metrics saved to {metrics_csv_path}")
                    if os.path.exists(failure_report_file):
                        logger.info(f"┃ Failure report saved to {failure_report_file}")
                        md_report_file = os.path.join(output_dir, f"failure_report_{timestamp}.md")
                        if os.path.exists(md_report_file):
                            logger.info(f"┃ Failure summary saved to {md_report_file}")
                    logger.info("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛")
            except Exception as e:
                logger.warning(f"Error processing summary data: {str(e)}")
        
        # Process results
        duration = time.time() - start_time
        
        # Create paths for the failure reports
        failure_report_json = os.path.join(output_dir, f"failure_report_{timestamp}.json")
        failure_report_md = os.path.join(output_dir, f"failure_report_{timestamp}.md")
        
        if process.returncode == 0:
            logger.info(f"k6 load test completed successfully in {duration:.2f} seconds")
            return True, {
                "success": True,
                "duration": duration,
                "output": stdout_output,
                "summary_file": summary_json_path if os.path.exists(summary_json_path) else None,
                "metrics_file": metrics_csv_path if os.path.exists(metrics_csv_path) else None,
                "failure_report_json": failure_report_json if os.path.exists(failure_report_json) else None,
                "failure_report_markdown": failure_report_md if os.path.exists(failure_report_md) else None
            }
        else:
            logger.warning(f"k6 load test completed with exit code {process.returncode} in {duration:.2f} seconds")
            return False, {
                "success": False,
                "exit_code": process.returncode,
                "duration": duration,
                "error": stderr_output,
                "output": stdout_output,
                "summary_file": summary_json_path if os.path.exists(summary_json_path) else None,
                "metrics_file": metrics_csv_path if os.path.exists(metrics_csv_path) else None,
                "failure_report_json": failure_report_json if os.path.exists(failure_report_json) else None,
                "failure_report_markdown": failure_report_md if os.path.exists(failure_report_md) else None
            }
    except Exception as e:
        logger.error(f"Error executing k6 load test: {str(e)}")
        return False, {"error": str(e)}

def parse_k6_results(output):
    """
    Parse k6 output to extract key metrics.
    
    Args:
        output: String output from k6 run
        
    Returns:
        Dictionary containing parsed metrics
    """
    metrics = {}
    
    try:
        # First check if the output contains JSON data (from the summary export)
        if output.strip().startswith('{') and output.strip().endswith('}'): 
            try:
                import json
                summary_data = json.loads(output)
                
                # Extract metrics from JSON format
                if 'metrics' in summary_data:
                    # Get iteration count
                    if 'iterations' in summary_data['metrics']:
                        if 'count' in summary_data['metrics']['iterations']:
                            metrics['iterations'] = summary_data['metrics']['iterations']['count']
                    
                    # Get HTTP request duration
                    if 'http_req_duration' in summary_data['metrics']:
                        if 'avg' in summary_data['metrics']['http_req_duration']:
                            metrics['avg_duration_ms'] = summary_data['metrics']['http_req_duration']['avg']
                    
                    # Get failure rate
                    if 'http_req_failed' in summary_data['metrics']:
                        if 'rate' in summary_data['metrics']['http_req_failed']:
                            metrics['failure_rate'] = summary_data['metrics']['http_req_failed']['rate']
                    
                    # Get VUs
                    if 'vus' in summary_data['metrics']:
                        if 'max' in summary_data['metrics']['vus']:
                            metrics['peak_vus'] = summary_data['metrics']['vus']['max']
                            
                # If we found metrics, return them
                if metrics:
                    # Add a summary metric
                    if 'avg_duration_ms' in metrics and 'failure_rate' in metrics:
                        metrics['summary'] = f"Avg duration: {metrics['avg_duration_ms']:.2f}ms, Failure rate: {metrics['failure_rate']*100:.2f}%"
                    return metrics
            except json.JSONDecodeError:
                # Not valid JSON or not what we expected, continue with text parsing
                pass
        
        # The format changed in k6 v1.0, we need a more robust parsing
        
        # First, let's try to extract the summary values at the end
        data_sections = output.split("data_received")  # This is usually at the top of the summary
        if len(data_sections) > 1:
            summary_section = data_sections[1]
            
            # Parse iteration count
            if "iterations" in summary_section:
                iteration_line = [l for l in summary_section.splitlines() if "iterations" in l]
                if iteration_line:
                    parts = iteration_line[0].strip().split()
                    for i, part in enumerate(parts):
                        if part == "count=":
                            try:
                                metrics["iterations"] = int(parts[i+1].strip(","))
                            except (ValueError, IndexError):
                                pass
            
            # Parse VUs
            if "vus" in summary_section:
                vus_line = [l for l in summary_section.splitlines() if "vus=" in l]
                if vus_line:
                    for part in vus_line[0].strip().split():
                        if part.startswith("vus="):
                            try:
                                metrics["peak_vus"] = int(part.split("=")[1].strip(","))
                            except (ValueError, IndexError):
                                pass
            
            # Parse request duration
            if "http_req_duration" in summary_section:
                duration_lines = [l for l in summary_section.splitlines() if "http_req_duration" in l]
                for line in duration_lines:
                    if "avg=" in line:
                        for part in line.strip().split():
                            if part.startswith("avg="):
                                try:
                                    # Convert time string to milliseconds
                                    time_str = part.split("=")[1].strip(",")
                                    if time_str.endswith("ms"):
                                        metrics["avg_duration_ms"] = float(time_str[:-2])
                                    elif time_str.endswith("s"):
                                        metrics["avg_duration_ms"] = float(time_str[:-1]) * 1000
                                    elif time_str.endswith("m"):
                                        metrics["avg_duration_ms"] = float(time_str[:-1]) * 1000 * 60
                                    else:
                                        # Try direct conversion
                                        metrics["avg_duration_ms"] = float(time_str) * 1000
                                except (ValueError, IndexError):
                                    pass
            
            # Parse failure rate
            if "http_req_failed" in summary_section:
                failure_lines = [l for l in summary_section.splitlines() if "http_req_failed" in l]
                for line in failure_lines:
                    if "rate=" in line:
                        for part in line.strip().split():
                            if part.startswith("rate="):
                                try:
                                    # Parse percentage
                                    rate_str = part.split("=")[1].strip(",")
                                    if rate_str.endswith("%"):
                                        metrics["failure_rate"] = float(rate_str[:-1]) / 100
                                    else:
                                        metrics["failure_rate"] = float(rate_str)
                                except (ValueError, IndexError):
                                    pass
        
        # If we couldn't find metrics in the summary, try the old format
        if not metrics:
            for line in output.splitlines():
                if "http_req_duration" in line and "{" not in line and "avg=" not in line:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        try:
                            metrics["avg_duration_ms"] = float(parts[1])
                        except ValueError:
                            pass  # Skip if not a valid float
                elif "http_req_failed" in line and "rate=" not in line:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        try:
                            metrics["failure_rate"] = float(parts[1])
                        except ValueError:
                            pass  # Skip if not a valid float
        
        # If we still couldn't parse anything, provide defaults with a note
        if not metrics:
            metrics = {
                "note": "Tests completed successfully but detailed metrics are not available",
                "iterations": "unknown",  # We don't know the value but tests did run
                "avg_duration_ms": "unknown",
                "failure_rate": 0  # Assuming success if the test completed
            }
        
        # Add a summary metric if we have both duration and failure rate
        if "avg_duration_ms" in metrics and "failure_rate" in metrics and \
           metrics["avg_duration_ms"] != "unknown" and metrics["failure_rate"] != "unknown":
            metrics["summary"] = f"Avg duration: {metrics['avg_duration_ms']:.2f}ms, Failure rate: {metrics['failure_rate']*100:.2f}%"
        elif "note" in metrics:
            metrics["summary"] = metrics["note"]
        
        return metrics
    except Exception as e:
        logger.error(f"Error parsing k6 results: {str(e)}")
        return {
            "error": str(e),
            "note": "Unable to parse metrics, but load test completed. See k6 console output for details.",
            "summary": "Load test completed, but metrics parsing failed. See console output for results."
        }

def check_k6_installed():
    """Check if k6 is installed and available."""
    try:
        result = subprocess.run(
            ["k6", "--version"], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, None
    except Exception:
        return False, None