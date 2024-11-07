from typing import Dict, List, Optional, Union
from datetime import datetime, timezone, timedelta
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

            # Add performance alerts if needed
            alerts = self._generate_performance_alerts(
                performance_metrics,
                cache_metrics,
                error_metrics
            )
            if alerts:
                summary += "\n\nPerformance Alerts\n" + "=" * 17 + "\n" + alerts

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

            # Add recommendations
            recommendations = self._generate_recommendations(
                performance_metrics,
                cache_metrics,
                error_metrics,
                successful_zones
            )
            if recommendations:
                summary += "\n\nRecommendations\n" + "=" * 15 + "\n" + recommendations

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
            total_visits = sum(
                z['cache_analysis']['overall']['total_visits']
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
                'total_visits': total_visits,
                'total_bytes_gb': total_bytes,
                'avg_sampling_rate': avg_sampling_rate,
                'zone_count': len(zones)
            }
        except Exception as e:
            logger.error(f"Error calculating total metrics: {str(e)}")
            return {
                'total_requests': 0,
                'total_visits': 0,
                'total_bytes_gb': 0,
                'avg_sampling_rate': 0,
                'zone_count': 0
            }

    def _calculate_performance_metrics(self, zones: List[Dict]) -> Dict:
        """Calculate weighted performance metrics based on request counts."""
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
        """Calculate cache performance metrics weighted by requests."""
        try:
            total_requests = sum(
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            )
            
            if total_requests == 0:
                return {
                    'hit_ratio': 0,
                    'bandwidth_saving': 0,
                    'cache_status_distribution': {}
                }

            # Calculate weighted hit ratio by requests
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

            # Aggregate cache status distribution
            cache_distribution = {}
            for zone in zones:
                for status, metrics in zone['cache_analysis']['status_distribution'].items():
                    if status not in cache_distribution:
                        cache_distribution[status] = {
                            'requests': 0,
                            'bytes': 0,
                            'percentage': 0
                        }
                    cache_distribution[status]['requests'] += metrics['requests']
                    cache_distribution[status]['bytes'] += metrics['bytes']

            # Calculate percentages
            for status in cache_distribution:
                cache_distribution[status]['percentage'] = (
                    cache_distribution[status]['requests'] / total_requests * 100
                )

            return {
                'hit_ratio': weighted_hit_ratio,
                'bandwidth_saving': weighted_bandwidth_saving,
                'cache_status_distribution': cache_distribution
            }
            
        except Exception as e:
            logger.error(f"Error calculating cache metrics: {str(e)}")
            return {
                'hit_ratio': 0,
                'bandwidth_saving': 0,
                'cache_status_distribution': {}
            }

    def _calculate_error_metrics(self, zones: List[Dict]) -> Dict:
        """Calculate error rate metrics weighted by requests."""
        try:
            total_requests = sum(
                z['cache_analysis']['overall']['total_requests']
                for z in zones
            )
            
            total_visits = sum(
                z['cache_analysis']['overall']['total_visits']
                for z in zones
            )
            
            if total_requests == 0:
                return {
                    'error_rate_4xx': 0,
                    'error_rate_5xx': 0,
                    'total_errors': 0,
                    'error_request_percentage': 0,
                    'error_visit_percentage': 0
                }

            # Calculate weighted error rates by requests
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

            # Calculate total errors and percentages
            total_errors = sum(
                z['error_analysis']['overall']['total_errors']
                for z in zones
            )

            error_request_percentage = (total_errors / total_requests * 100) if total_requests > 0 else 0
            error_visit_percentage = (total_errors / total_visits * 100) if total_visits > 0 else 0

            return {
                'error_rate_4xx': weighted_4xx,
                'error_rate_5xx': weighted_5xx,
                'total_errors': total_errors,
                'error_request_percentage': error_request_percentage,
                'error_visit_percentage': error_visit_percentage
            }
            
        except Exception as e:
            logger.error(f"Error calculating error metrics: {str(e)}")
            return {
                'error_rate_4xx': 0,
                'error_rate_5xx': 0,
                'total_errors': 0,
                'error_request_percentage': 0,
                'error_visit_percentage': 0
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
        """Format detailed analysis summary."""
        try:
            zone_summaries = []
            for zone in zones:
                zone_summary = f"""
Zone: {zone['zone_name']}
------------------
• Requests: {zone['cache_analysis']['overall']['total_requests']:,}
• Visits: {zone['cache_analysis']['overall']['total_visits']:,}
• Cache Hit Ratio: {zone['cache_analysis']['overall']['hit_ratio']:.2f}%
• Avg TTFB: {zone['latency_analysis']['basic_metrics']['ttfb']['avg']:.2f}ms
• Error Rate: {error_metrics['error_rate_4xx'] + error_metrics['error_rate_5xx']:.2f}%
"""
                zone_summaries.append(zone_summary)

            return f"""
Cloudflare Analytics Summary
==========================
Analysis Duration: {str(duration).split('.')[0]}
Zones Analyzed: {len(zones)}

Overall Metrics
--------------
• Total Requests: {total_metrics['total_requests']:,}
• Total Visits: {total_metrics['total_visits']:,}
• Total Bandwidth: {total_metrics['total_bytes_gb']:.2f} GB
• Average Sampling Rate: {total_metrics['avg_sampling_rate']:.1f}%

Performance Metrics
-----------------
• Average TTFB: {performance_metrics['avg_ttfb']:.2f}ms
• 95th Percentile TTFB: {performance_metrics['p95_ttfb']:.2f}ms
• Average Origin Time: {performance_metrics['avg_origin_time']:.2f}ms
• 95th Percentile Origin Time: {performance_metrics['p95_origin_time']:.2f}ms

Cache Performance
---------------
• Overall Hit Ratio: {cache_metrics['hit_ratio']:.2f}%
• Bandwidth Savings: {cache_metrics['bandwidth_saving']:.2f}%

Error Analysis
------------
• 4xx Error Rate: {error_metrics['error_rate_4xx']:.2f}%
• 5xx Error Rate: {error_metrics['error_rate_5xx']:.2f}%
• Total Errors: {error_metrics['total_errors']:,}
• Error/Request Ratio: {error_metrics['error_request_percentage']:.2f}%

Zone Details
-----------
{"".join(zone_summaries)}
"""
        except Exception as e:
            logger.error(f"Error formatting detailed summary: {str(e)}")
            return self._format_empty_summary()

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

    def _generate_performance_alerts(
        self,
        performance_metrics: Dict,
        cache_metrics: Dict,
        error_metrics: Dict
    ) -> str:
        """Generate performance alerts based on metrics."""
        alerts = []
        
        # Performance alerts
        if performance_metrics['avg_ttfb'] > 200:
            alerts.append("⚠ High average TTFB (>200ms)")
        if performance_metrics['p95_ttfb'] > 1000:
            alerts.append("⚠ High 95th percentile TTFB (>1000ms)")
        
        # Cache alerts
        if cache_metrics['hit_ratio'] < 50:
            alerts.append("⚠ Low cache hit ratio (<50%)")
        
        # Error alerts
        if error_metrics['error_rate_5xx'] > 1:
            alerts.append("⚠ High 5xx error rate (>1%)")
        if error_metrics['error_rate_4xx'] > 5:
            alerts.append("⚠ High 4xx error rate (>5%)")
        
        if not alerts:
            return "\n".join(alerts)

    def _generate_recommendations(
        self,
        performance_metrics: Dict,
        cache_metrics: Dict,
        error_metrics: Dict,
        zones: List[Dict]
    ) -> str:
        """Generate actionable recommendations based on analysis results."""
        recommendations = []

        # Performance recommendations
        if performance_metrics['avg_ttfb'] > 200:
            recommendations.append("""
• Performance Optimization Needed
  - Current average TTFB: {:.2f}ms (target: <200ms)
  - Consider enabling Argo Smart Routing
  - Review origin server performance
  - Consider implementing server-side caching""".format(performance_metrics['avg_ttfb']))

        # Cache recommendations
        if cache_metrics['hit_ratio'] < 50:
            recommendations.append("""
• Cache Optimization Required
  - Current hit ratio: {:.2f}% (target: >70%)
  - Review cache configuration rules
  - Implement browser cache control headers
  - Consider increasing cache TTLs
  - Enable cache everything rules where appropriate""".format(cache_metrics['hit_ratio']))

        # Error handling recommendations
        total_error_rate = error_metrics['error_rate_4xx'] + error_metrics['error_rate_5xx']
        if total_error_rate > 5:
            recommendations.append("""
• High Error Rates Detected
  - 4xx errors: {:.2f}%
  - 5xx errors: {:.2f}%
  - Review server error logs
  - Implement proper error handling
  - Consider setting up custom error pages
  - Monitor application health metrics""".format(
                error_metrics['error_rate_4xx'],
                error_metrics['error_rate_5xx']
            ))

        # Zone-specific recommendations
        for zone in zones:
            zone_name = zone['zone_name']
            origin_metrics = zone.get('origin_analysis', {}).get('overall_metrics', {})
            
            if origin_metrics:
                response_time = origin_metrics.get('response_time', {})
                if response_time.get('avg', 0) > 500:
                    recommendations.append(f"""
• Origin Optimization for {zone_name}
  - High origin response time: {response_time.get('avg', 0):.2f}ms
  - Consider origin server optimization
  - Review database query performance
  - Implement server-side caching
  - Monitor server resources""")

        if not recommendations:
            recommendations.append("""
• No Critical Issues Found
  - Continue monitoring performance metrics
  - Regularly review cache configuration
  - Keep security rules updated
  - Monitor for changes in traffic patterns""")

        return "\n".join(recommendations)

    def _save_summary(self, summary: str) -> None:
        """Save summary to file with proper error handling."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = self.report_dir / f"analysis_summary_{timestamp}.txt"
            
            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as text
            filepath.write_text(summary)
            logger.info(f"Summary saved to {filepath}")
            
            # Save as markdown for better formatting
            md_filepath = filepath.with_suffix('.md')
            md_filepath.write_text(summary)
            
            # Save as JSON for programmatic access
            json_filepath = filepath.with_suffix('.json')
            summary_dict = {
                'timestamp': timestamp,
                'content': summary,
                'sections': self._parse_summary_sections(summary)
            }
            with open(json_filepath, 'w') as f:
                json.dump(summary_dict, f, indent=2)
            
        except Exception as e:
            logger.error(f"Error saving summary: {str(e)}")
            logger.error(traceback.format_exc())

    def _parse_summary_sections(self, summary: str) -> Dict:
        """Parse summary into structured sections for JSON export."""
        try:
            sections = {}
            current_section = None
            current_content = []
            
            for line in summary.split('\n'):
                if line.strip() and all(c in '=' for c in line.strip()):
                    # Section header found in previous line
                    if current_section and current_content:
                        sections[current_section] = '\n'.join(current_content).strip()
                    current_section = current_content[-1].strip()
                    current_content = []
                else:
                    current_content.append(line)
            
            # Add final section
            if current_section and current_content:
                sections[current_section] = '\n'.join(current_content).strip()
                
            return sections
            
        except Exception as e:
            logger.error(f"Error parsing summary sections: {str(e)}")
            return {'error': 'Failed to parse summary sections'}

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

    def _generate_json_report(self, zone_result: Dict) -> str:
        """Generate JSON format report for a zone."""
        try:
            # Convert numpy types to native Python types
            def convert_to_serializable(obj):
                if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                                 np.int16, np.int32, np.int64)):
                    return int(obj)
                elif isinstance(obj, (np.float_, np.float16, np.float32,
                                   np.float64)):
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

    def _generate_text_report(self, zone_result: Dict) -> str:
        """Generate human-readable text report for a zone."""
        try:
            # Generate basic metrics table
            metrics_table = PrettyTable()
            metrics_table.field_names = ["Metric", "Value"]
            metrics_table.align["Metric"] = "l"
            metrics_table.align["Value"] = "r"
            
            cache_metrics = zone_result['cache_analysis']['overall']
            latency_metrics = zone_result['latency_analysis']['basic_metrics']
            error_metrics = zone_result['error_analysis']['overall']
            
            metrics_table.add_rows([
                ["Total Requests", f"{cache_metrics['total_requests']:,}"],
                ["Total Visits", f"{cache_metrics['total_visits']:,}"],
                ["Cache Hit Ratio", f"{cache_metrics['hit_ratio']:.2f}%"],
                ["Avg TTFB", f"{latency_metrics['ttfb']['avg']:.2f}ms"],
                ["P95 TTFB", f"{latency_metrics['ttfb']['p95']:.2f}ms"],
                ["Error Rate", f"{error_metrics['error_rate_4xx'] + error_metrics['error_rate_5xx']:.2f}%"]
            ])
            
            return f"""
Zone Performance Report
=====================
Zone: {zone_result['zone_name']}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

Basic Metrics
------------
{metrics_table.get_string()}

Cache Performance
---------------
• Hit Ratio: {cache_metrics['hit_ratio']:.2f}%
• Bandwidth Savings: {cache_metrics['bandwidth_saving']:.2f}%
• Total Bandwidth: {cache_metrics['total_bytes'] / (1024**3):.2f} GB

Error Analysis
------------
• 4xx Error Rate: {error_metrics['error_rate_4xx']:.2f}%
• 5xx Error Rate: {error_metrics['error_rate_5xx']:.2f}%
• Total Errors: {error_metrics['total_errors']:,}
"""

        except Exception as e:
            logger.error(f"Error generating text report: {str(e)}")
            return "Error generating report. Please check logs for details."
