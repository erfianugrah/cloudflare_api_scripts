# reporter.py
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Union
import logging
from pathlib import Path
import json
import pandas as pd
import numpy as np
from prettytable import PrettyTable
import traceback

logger = logging.getLogger(__name__)

from .origin_reporter import OriginReporter
class Reporter:
    """Comprehensive reporter for Cloudflare analytics."""
    
    def __init__(self, config):
        self.config = config
        self.report_dir = self.config.reports_dir
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize specialized reporters
        self.origin_reporter = OriginReporter(config)

    def generate_summary(self, results: List[Dict], start_time: datetime) -> Optional[str]:
        """Generate overall analysis summary."""
        try:
            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time
            
            successful_zones = [r for r in results if self._validate_result(r)]
            
            if not successful_zones:
                logger.warning("No successful zone analysis completed")
                return self._format_empty_summary()

            # Calculate overall metrics
            total_metrics = self._calculate_total_metrics(successful_zones)
            performance_metrics = self._calculate_performance_metrics(successful_zones)
            cache_metrics = self._calculate_cache_metrics(successful_zones)
            error_metrics = self._calculate_error_metrics(successful_zones)
            
            # Generate main summary
            summary = self._format_detailed_summary(
                duration=duration,
                total_metrics=total_metrics,
                performance_metrics=performance_metrics,
                cache_metrics=cache_metrics,
                error_metrics=error_metrics,
                zones=successful_zones
            )

            # Add origin analysis summary if available
            for result in successful_zones:
                if 'origin_analysis' in result and 'raw_data' in result:
                    origin_report = self.origin_reporter.generate_origin_report(
                        df=result['raw_data'],
                        analysis_results=result['origin_analysis'],
                        zone_name=result['zone_name']
                    )
                    if origin_report:
                        summary += "\n\nOrigin Performance Summary\n" + "=" * 25 + "\n"
                        summary += origin_report

            # Save summary to file
            self._save_summary(summary)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            logger.error(traceback.format_exc())
            return self._format_empty_summary()

    def _validate_result(self, result: Dict) -> bool:
        """Validate analysis result structure."""
        try:
            required_keys = ['cache_analysis', 'latency_analysis', 'error_analysis', 'sampling_metrics']
            return all(
                key in result and result[key] is not None
                for key in required_keys
            )
        except Exception as e:
            logger.error(f"Error validating result: {str(e)}")
            return False

    def _calculate_total_metrics(self, zones: List[Dict]) -> Dict:
        """Calculate total metrics across all zones."""
        try:
            total_requests = sum(
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            )
            total_bytes = sum(
                z['cache_analysis']['overall']['total_bytes']
                for z in zones
            )
            avg_sampling_rate = np.mean([
                z['sampling_metrics']['sampling_rates']['mean']
                for z in zones
            ])

            return {
                'total_requests': total_requests,
                'total_bytes_gb': total_bytes,
                'avg_sampling_rate': avg_sampling_rate,
                'zone_count': len(zones)
            }
        except Exception as e:
            logger.error(f"Error calculating total metrics: {str(e)}")
            return {
                'total_requests': 0,
                'total_bytes_gb': 0,
                'avg_sampling_rate': 0,
                'zone_count': 0
            }

    def _calculate_performance_metrics(self, zones: List[Dict]) -> Dict:
        """Calculate weighted performance metrics."""
        try:
            total_requests = sum(
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            )
            
            if total_requests == 0:
                return {
                    'avg_ttfb': 0,
                    'avg_origin_time': 0,
                    'p95_ttfb': 0,
                    'p95_origin_time': 0
                }

            # Calculate weighted averages based on request volume
            weighted_ttfb = sum(
                z['latency_analysis']['basic_metrics']['ttfb']['avg'] *
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            ) / total_requests

            weighted_origin = sum(
                z['latency_analysis']['basic_metrics']['origin_time']['avg'] *
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            ) / total_requests

            # Calculate 95th percentiles
            p95_ttfb = np.percentile([
                z['latency_analysis']['basic_metrics']['ttfb']['p95']
                for z in zones
            ], 95)

            p95_origin = np.percentile([
                z['latency_analysis']['basic_metrics']['origin_time']['p95']
                for z in zones
            ], 95)

            return {
                'avg_ttfb': weighted_ttfb,
                'avg_origin_time': weighted_origin,
                'p95_ttfb': p95_ttfb,
                'p95_origin_time': p95_origin
            }
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {str(e)}")
            return {
                'avg_ttfb': 0,
                'avg_origin_time': 0,
                'p95_ttfb': 0,
                'p95_origin_time': 0
            }

    def _calculate_cache_metrics(self, zones: List[Dict]) -> Dict:
        """Calculate cache performance metrics."""
        try:
            total_requests = sum(
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            )
            
            if total_requests == 0:
                return {
                    'hit_ratio': 0,
                    'bandwidth_saving': 0
                }

            # Calculate weighted hit ratio
            weighted_hit_ratio = sum(
                z['cache_analysis']['overall']['hit_ratio'] *
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            ) / total_requests

            # Calculate overall bandwidth saving
            weighted_bandwidth_saving = sum(
                z['cache_analysis']['overall']['bandwidth_saving'] *
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            ) / total_requests

            return {
                'hit_ratio': weighted_hit_ratio,
                'bandwidth_saving': weighted_bandwidth_saving
            }
        except Exception as e:
            logger.error(f"Error calculating cache metrics: {str(e)}")
            return {
                'hit_ratio': 0,
                'bandwidth_saving': 0
            }

    def _calculate_error_metrics(self, zones: List[Dict]) -> Dict:
        """Calculate error rate metrics."""
        try:
            total_requests = sum(
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            )
            
            if total_requests == 0:
                return {
                    'error_rate_4xx': 0,
                    'error_rate_5xx': 0
                }

            # Calculate weighted error rates
            weighted_4xx = sum(
                z['error_analysis']['overall']['error_rate_4xx'] *
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            ) / total_requests

            weighted_5xx = sum(
                z['error_analysis']['overall']['error_rate_5xx'] *
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            ) / total_requests

            return {
                'error_rate_4xx': weighted_4xx,
                'error_rate_5xx': weighted_5xx
            }
        except Exception as e:
            logger.error(f"Error calculating error metrics: {str(e)}")
            return {
                'error_rate_4xx': 0,
                'error_rate_5xx': 0
            }

    def _format_detailed_summary(
        self,
        duration: timedelta,
        total_metrics: Dict,
        performance_metrics: Dict,
        cache_metrics: Dict,
        error_metrics: Dict,
        zones: List[Dict]
    ) -> str:
        """Format detailed analysis summary with explanations."""
        try:
            summary = f"""
Cloudflare Analytics Summary Report
=================================
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
Analysis Duration: {duration}

Overall Metrics
--------------
These metrics provide a high-level overview of your site's traffic and resource usage.

Zones Analyzed: {total_metrics['zone_count']}
• Each zone represents a domain configured on Cloudflare

Total Requests: {total_metrics['total_requests']:,}
• Total number of HTTP requests processed
• A lower number might indicate:
  - Low traffic period
  - Filtering/sampling issues
  - Monitoring window limitations

Total Bandwidth: {total_metrics['total_bytes_gb']:.2f} GB
• Total data transferred through Cloudflare
• Includes both cached and origin traffic

Average Sampling Rate: {total_metrics['avg_sampling_rate']:.2f}%
• Percentage of requests being analyzed
• 100% means all requests are included
• Lower rates may affect metric accuracy

Performance Metrics
-----------------
These metrics indicate how quickly your site responds to visitors.

Average TTFB: {performance_metrics['avg_ttfb']:.2f} ms
• Time To First Byte - initial response speed
• Target: < 200ms
• Current Status: {"Good" if performance_metrics['avg_ttfb'] < 200 else "Needs Improvement"}

95th Percentile TTFB: {performance_metrics['p95_ttfb']:.2f} ms
• 95% of requests were faster than this
• Shows worst-case performance excluding outliers

Average Origin Time: {performance_metrics['avg_origin_time']:.2f} ms
• Time your server takes to respond
• Target: < 100ms
• Current Status: {"Good" if performance_metrics['avg_origin_time'] < 100 else "Needs Improvement"}

95th Percentile Origin Time: {performance_metrics['p95_origin_time']:.2f} ms
• Indicates consistent server performance
• High values suggest server-side issues

Cache Performance
---------------
These metrics show caching effectiveness and efficiency.

Overall Hit Ratio: {cache_metrics['hit_ratio']:.2f}%
• Percentage of requests served from cache
• Target: > 80%
• Current Status: {"Good" if cache_metrics['hit_ratio'] > 80 else "Needs Improvement"}
• Low ratios suggest caching optimization opportunities

Bandwidth Savings: {cache_metrics['bandwidth_saving']:.2f}%
• Data served from cache vs. origin
• Higher values reduce origin server load
• Target: > 60%
• Current Status: {"Good" if cache_metrics['bandwidth_saving'] > 60 else "Needs Improvement"}

Error Rates
----------
These metrics indicate site reliability and potential issues.

4xx Error Rate: {error_metrics['error_rate_4xx']:.2f}%
• Client-side errors (e.g., 404 Not Found)
• Target: < 5%
• Current Status: {"Good" if error_metrics['error_rate_4xx'] < 5 else "Needs Investigation"}
• High rates may indicate:
  - Broken links
  - Invalid requests
  - Access control issues

5xx Error Rate: {error_metrics['error_rate_5xx']:.2f}%
• Server-side errors (e.g., 500 Internal Server Error)
• Target: < 1%
• Current Status: {"Good" if error_metrics['error_rate_5xx'] < 1 else "Needs Urgent Investigation"}
• High rates indicate:
  - Server problems
  - Resource limitations
  - Configuration issues

Zone Details
-----------"""

            # Add individual zone details with explanations
            for zone in zones:
                confidence_score = zone['sampling_metrics']['confidence_scores']['mean']
                error_rate = (zone['error_analysis']['overall']['error_rate_4xx'] + 
                            zone['error_analysis']['overall']['error_rate_5xx'])
                
                summary += f"""

{zone['zone_name']}:
• Requests: {zone['cache_analysis']['overall']['total_requests']:,}
• Hit Ratio: {zone['cache_analysis']['overall']['hit_ratio']:.2f}%
• Avg TTFB: {zone['latency_analysis']['basic_metrics']['ttfb']['avg']:.2f} ms
• Error Rate: {error_rate:.2f}%
• Sampling Rate: {zone['sampling_metrics']['sampling_rates']['mean']:.2f}%
• Confidence Score: {confidence_score:.3f}
  - Score Interpretation:
    * 0.9-1.0: Very High Confidence
    * 0.7-0.9: High Confidence
    * 0.5-0.7: Moderate Confidence
    * <0.5: Low Confidence - Consider increasing sample size

  - Performance Assessment:
    * Cache Performance: {"Good" if zone['cache_analysis']['overall']['hit_ratio'] > 80 else "Needs Improvement"}
    * Response Time: {"Good" if zone['latency_analysis']['basic_metrics']['ttfb']['avg'] < 200 else "Needs Improvement"}
    * Error Rate: {"Good" if error_rate < 5 else "Needs Investigation"}"""

                # Add specific recommendations based on metrics
                if zone['cache_analysis']['overall']['hit_ratio'] < 80:
                    summary += """
    
    Cache Optimization Suggestions:
    • Review cache settings and TTL values
    • Implement browser caching headers
    • Consider using Cloudflare's Cache Everything option
    • Check cache bypass conditions"""

                if zone['latency_analysis']['basic_metrics']['ttfb']['avg'] > 200:
                    summary += """
    
    Performance Optimization Suggestions:
    • Enable Cloudflare Auto-Minify
    • Optimize server response time
    • Consider using Cloudflare Argo
    • Review origin server resources"""

                if error_rate > 5:
                    summary += """
    
    Error Rate Optimization Suggestions:
    • Check server logs for error patterns
    • Review and update broken links
    • Monitor server resources
    • Verify server configuration"""

            if total_metrics['total_requests'] < 100:
                summary += """

Note on Low Request Count:
------------------------
The analysis shows a relatively low number of requests. This could be due to:
• Normal low-traffic period
• Limited analysis time window
• Filtering or sampling configuration
• Potential monitoring gaps

Consider:
• Extending the analysis time window
• Verifying traffic patterns in Cloudflare dashboard
• Checking for any request filtering
• Confirming proper zone configuration"""

            return summary

        except Exception as e:
            logger.error(f"Error formatting detailed summary: {str(e)}")
            logger.error(traceback.format_exc())
            return "Error generating analysis summary. Please check logs for details."


    def _format_empty_summary(self) -> str:
        """Format summary for failed analysis."""
        return """
Cloudflare Analytics Summary
==========================
No successful zone analysis completed.

Possible reasons:
• API connectivity issues
• Invalid zone configuration
• No data available in the selected time range
• Rate limiting
• Authentication problems

Troubleshooting steps:
1. Verify your Cloudflare API credentials
2. Check zone configuration
3. Confirm the analysis time range
4. Review API access permissions
5. Check the logs for specific error messages

Please check the logs for detailed error information and try again.
"""

    def _save_summary(self, summary: str) -> None:
        """Save summary to file."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = self.report_dir / f"analysis_summary_{timestamp}.txt"
            
            filepath.write_text(summary)
            logger.info(f"Summary saved to {filepath}")
            
            # Also save as markdown for better formatting
            md_filepath = filepath.with_suffix('.md')
            md_filepath.write_text(summary)
            
        except Exception as e:
            logger.error(f"Error saving summary: {str(e)}")

    def generate_zone_report(self, zone_result: Dict, output_format: str = 'text') -> Optional[str]:
        """Generate detailed report for a single zone."""
        try:
            if not self._validate_result(zone_result):
                logger.error(f"Invalid zone result for {zone_result.get('zone_name', 'unknown zone')}")
                return None

            if output_format == 'json':
                return self._generate_json_report(zone_result)
            else:
                return self._generate_text_report(zone_result)

        except Exception as e:
            logger.error(f"Error generating zone report: {str(e)}")
            return None

    def _generate_text_report(self, zone_result: Dict) -> str:
        """Generate text format report for a zone."""
        try:
            zone_name = zone_result['zone_name']
            cache = zone_result['cache_analysis']['overall']
            perf = zone_result['latency_analysis']['basic_metrics']
            errors = zone_result['error_analysis']['overall']
            sampling = zone_result['sampling_metrics']

            report = f"""
Detailed Analysis Report for {zone_name}
======================================

Cache Performance
---------------
Hit Ratio: {cache['hit_ratio']:.2f}%
• Percentage of requests served from cache
• {'Good' if cache['hit_ratio'] > 80 else 'Needs improvement'} (Target: >80%)

Bandwidth Saving: {cache['bandwidth_saving']:.2f}%
• Data served from cache instead of origin
• {'Good' if cache['bandwidth_saving'] > 60 else 'Could be improved'} (Target: >60%)

Total Requests: {cache['total_requests']:,}
Total Bandwidth: {cache['total_bytes']:.2f} GB

Performance Metrics
-----------------
TTFB (Time To First Byte):
• Average: {perf['ttfb']['avg']:.2f} ms {'(Good)' if perf['ttfb']['avg'] < 200 else '(Needs improvement)'}
• P50: {perf['ttfb']['p50']:.2f} ms
• P95: {perf['ttfb']['p95']:.2f} ms
• P99: {perf['ttfb']['p99']:.2f} ms

Origin Response Time:
• Average: {perf['origin_time']['avg']:.2f} ms {'(Good)' if perf['origin_time']['avg'] < 100 else '(Needs improvement)'}
• P50: {perf['origin_time']['p50']:.2f} ms
• P95: {perf['origin_time']['p95']:.2f} ms
• P99: {perf['origin_time']['p99']:.2f} ms

Error Analysis
-------------
4xx Error Rate: {errors['error_rate_4xx']:.2f}%
• Client-side errors
• {'Good' if errors['error_rate_4xx'] < 5 else 'Needs investigation'} (Target: <5%)

5xx Error Rate: {errors['error_rate_5xx']:.2f}%
• Server-side errors
• {'Good' if errors['error_rate_5xx'] < 1 else 'Needs urgent attention'} (Target: <1%)

Total Errors: {errors['total_errors']:,}

Sampling Information
------------------
Average Rate: {sampling['sampling_rates']['mean']:.2f}%
• Percentage of traffic analyzed

Confidence Score: {sampling['confidence_scores']['mean']:.3f}
• Reliability of the analysis
• {'High' if sampling['confidence_scores']['mean'] > 0.8 else 'Moderate' if sampling['confidence_scores']['mean'] > 0.5 else 'Low'}

Total Samples: {sampling['sample_counts']['total_samples']:,}
Estimated Total: {sampling['sample_counts']['estimated_total']:,}
"""
            return report

        except Exception as e:
            logger.error(f"Error generating text report: {str(e)}")
            return "Error generating report. Please check logs for details."

    def _generate_json_report(self, zone_result: Dict) -> str:
        """Generate JSON format report for a zone."""
        try:
            # Convert numpy types to native Python types
            def convert_to_serializable(obj):
                if isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64)):
                    return int(obj)
                elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, datetime):
                    return obj.isoformat()
                return obj

            converted_result = json.loads(
                json.dumps(zone_result, default=convert_to_serializable)
            )
            
            return json.dumps(converted_result, indent=2)

        except Exception as e:
            logger.error(f"Error generating JSON report: {str(e)}")
            return "{}"
