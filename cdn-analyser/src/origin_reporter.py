from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from pathlib import Path
import json
import traceback
from prettytable import PrettyTable

logger = logging.getLogger(__name__)

class OriginReporter:
    """Enhanced reporter for origin server performance analysis."""
    
    def __init__(self, config):
        self.config = config
        self.report_dir = self.config.reports_dir / 'origin'
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        self.thresholds = {
            'response_time': {
                'good': 200,     # ms
                'warning': 500,  # ms
                'critical': 1000 # ms
            },
            'error_rate': {
                'good': 1,     # %
                'warning': 5,  # %
                'critical': 10 # %
            },
            'timeout_rate': {
                'good': 0.1,    # %
                'warning': 1.0,  # %
                'critical': 5.0  # %
            }
        }

        # Unicode symbols for consistent formatting
        self.symbols = {
            'bullet': "•",
            'check': "✓",
            'warning': "⚠"
        }

    def generate_origin_report(
        self,
        df: pd.DataFrame,
        analysis_results: Dict,
        zone_name: str
    ) -> Optional[str]:
        """Generate comprehensive origin performance report."""
        try:
            if df is None or df.empty or not analysis_results:
                logger.error("No data available for report generation")
                return None

            logger.info(f"""
Report Generation Starting:
----------------------
Zone: {zone_name}
Analysis Results Keys: {list(analysis_results.keys())}
DataFrame Shape: {df.shape}
""")

            report = self._create_report_sections(df, analysis_results, zone_name)
            self._save_report(report, zone_name, analysis_results)
            return report

        except Exception as e:
            logger.error(f"Error generating origin report: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _create_report_sections(
        self,
        df: pd.DataFrame,
        analysis: Dict,
        zone_name: str
    ) -> str:
        """Create detailed report sections."""
        try:
            sections = [
                self._create_header(zone_name),
                self._create_executive_summary(analysis),
                self._create_performance_section(analysis),
                self._create_reliability_section(analysis),
                self._create_geographic_section(analysis),
                self._create_endpoint_section(analysis),
                self._create_recommendations(analysis)
            ]

            return "\n\n".join(filter(None, sections))

        except Exception as e:
            logger.error(f"Error creating report sections: {str(e)}")
            return "Error generating report. Please check logs for details."

    def _create_header(self, zone_name: str) -> str:
        """Create report header."""
        return f"""Origin Performance Analysis Report
================================
Zone: {zone_name}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"""

    def _create_executive_summary(self, analysis: Dict) -> str:
        """Create executive summary section."""
        try:
            metrics = analysis.get('overall_metrics', {})
            response_time = metrics.get('response_time', {})
            failure_rates = metrics.get('failure_rates', {})
            request_vol = metrics.get('request_volume', {})
            bandwidth = metrics.get('bandwidth', {})
            health_status = metrics.get('health_status', 'unknown')

            bullet = self.symbols['bullet']
            check = self.symbols['check']
            warning = self.symbols['warning']

            # Create status indicators
            status = {
                'response_time': f'{check} Good' if response_time.get('avg', 0) < self.thresholds['response_time']['warning']
                               else f'{warning} Attention Needed',
                'error_rate': f'{check} Good' if failure_rates.get('error_rate', 0) < self.thresholds['error_rate']['warning']
                             else f'{warning} Attention Needed',
                'timeout': f'{check} Good' if failure_rates.get('timeout_rate', 0) < self.thresholds['timeout_rate']['warning']
                          else f'{warning} Attention Needed'
            }

            return f"""
Executive Summary
----------------
{bullet} Average Origin Response Time: {response_time.get('avg', 0):.2f}ms
{bullet} 95th Percentile Response Time: {response_time.get('p95', 0):.2f}ms
{bullet} Total Origin Requests: {request_vol.get('total_requests', 0):,}
{bullet} Overall Error Rate: {failure_rates.get('error_rate', 0):.2f}%
{bullet} Total Bandwidth Processed: {bandwidth.get('total_bytes', 0) / (1024**3):.2f} GB
{bullet} Health Status: {health_status.upper()}

Key Findings:
{bullet} {status['response_time']} - Origin Response Time
{bullet} {status['error_rate']} - Error Rate
{bullet} {status['timeout']} - Timeout Rate"""

        except Exception as e:
            logger.error(f"Error creating executive summary: {str(e)}")
            return "Executive Summary: Error generating summary"

    def _create_performance_section(self, analysis: Dict) -> str:
        """Create performance analysis section."""
        try:
            perf = analysis.get('overall_metrics', {}).get('response_time', {})
            temporal = analysis.get('temporal_analysis', {})
            peak_periods = temporal.get('peak_periods', {})
            bullet = self.symbols['bullet']

            return f"""
Performance Analysis
------------------
Response Time Metrics:
{bullet} Average: {perf.get('avg', 0):.2f}ms
{bullet} Median: {perf.get('median', 0):.2f}ms
{bullet} 95th Percentile: {perf.get('p95', 0):.2f}ms
{bullet} 99th Percentile: {perf.get('p99', 0):.2f}ms
{bullet} Standard Deviation: {perf.get('std_dev', 0):.2f}ms

Peak Performance:
{bullet} Busiest Period: {peak_periods.get('highest_load', {}).get('timestamp', 'Unknown')}
  - Requests: {peak_periods.get('highest_load', {}).get('requests', 0):,}
  - Response Time: {peak_periods.get('highest_load', {}).get('response_time', 0):.2f}ms

{bullet} Worst Performance Period: {peak_periods.get('worst_performance', {}).get('timestamp', 'Unknown')}
  - Response Time: {peak_periods.get('worst_performance', {}).get('response_time', 0):.2f}ms
  - Requests: {peak_periods.get('worst_performance', {}).get('requests', 0):,}"""

        except Exception as e:
            logger.error(f"Error creating performance section: {str(e)}")
            return "Performance Analysis: Error generating analysis"

    def _create_status_table(self, status_dist: Dict) -> str:
        """Create formatted table for status code distribution."""
        if not status_dist:
            return "No status distribution data available"

        table = PrettyTable()
        table.field_names = ["Status", "Count", "Percentage"]
        table.align = "r"
        
        for status, data in sorted(status_dist.items()):
            if isinstance(data, dict):
                table.add_row([
                    status,
                    f"{data.get('count', 0):,}",
                    f"{data.get('percentage', 0):.2f}%"
                ])
            else:
                # Handle case where data might be just the count
                total = sum(status_dist.values())
                percentage = (data / total * 100) if total > 0 else 0
                table.add_row([
                    status,
                    f"{data:,}",
                    f"{percentage:.2f}%"
                ])

        return table.get_string()

    def _create_reliability_section(self, analysis: Dict) -> str:
        """Create reliability analysis section with proper status distribution."""
        try:
            reliability = analysis.get('overall_metrics', {}).get('failure_rates', {})
            error_analysis = analysis.get('error_analysis', {})
            error_timing = error_analysis.get('error_timing', {})
            error_percentiles = error_timing.get('error_percentiles', {})
            bullet = self.symbols['bullet']

            # Add status distribution table
            status_table = self._create_status_table(error_analysis.get('error_distribution', {}))

            return f"""
Reliability Analysis
------------------
Error Metrics:
{bullet} Overall Error Rate: {reliability.get('error_rate', 0):.2f}%
{bullet} Error Rate (4xx): {reliability.get('error_rate_4xx', 0):.2f}%
{bullet} Error Rate (5xx): {reliability.get('error_rate_5xx', 0):.2f}%
{bullet} Timeout Rate: {reliability.get('timeout_rate', 0):.2f}%

Error Response Times:
{bullet} Average Time to Error: {error_timing.get('avg_time_to_error', 0):.2f}ms
{bullet} 50th Percentile: {error_percentiles.get('p50', 0):.2f}ms
{bullet} 95th Percentile: {error_percentiles.get('p95', 0):.2f}ms
{bullet} 99th Percentile: {error_percentiles.get('p99', 0):.2f}ms

Status Distribution:
{status_table}"""

        except Exception as e:
            logger.error(f"Error creating reliability section: {str(e)}")
            return "Reliability Analysis: Error generating analysis"

    def _create_geographic_section(self, analysis: Dict) -> str:
        """Create geographic analysis section."""
        try:
            geo = analysis.get('geographic_analysis', {})
            regional = geo.get('regional_summary', {})

            return f"""
Geographic Analysis
-----------------
Best Performing Regions:
{self._format_list(regional.get('fastest_regions', []))}

Worst Performing Regions:
{self._format_list(regional.get('slowest_regions', []))}

Regions with Highest Error Rates:
{self._format_list(regional.get('highest_error_regions', []))}"""

        except Exception as e:
            logger.error(f"Error creating geographic section: {str(e)}")
            return "Geographic Analysis: Error generating analysis"

    def _create_endpoint_section(self, analysis: Dict) -> str:
        """Create endpoint analysis section."""
        try:
            endpoint_analysis = analysis.get('endpoint_analysis', {})
            endpoints = endpoint_analysis.get('endpoints', {})
            top_endpoints = endpoint_analysis.get('top_impacting_endpoints', [])
            bullet = self.symbols['bullet']

            if not endpoints or not top_endpoints:
                return "Endpoint Analysis\n---------------\nNo endpoint data available"

            result = ["Endpoint Analysis", "---------------", "Top Impacting Endpoints:"]
            
            for endpoint in top_endpoints[:5]:  # Show top 5
                metrics = endpoints.get(endpoint, {})
                perf = metrics.get('performance', {})
                load = metrics.get('load', {})
                reliability = metrics.get('reliability', {})
                
                result.append(f"""
{bullet} {endpoint}
  - Response Time: {perf.get('avg_response_time', 0):.2f}ms
  - Requests: {load.get('requests', 0):,}
  - Error Rate: {(reliability.get('error_rate_4xx', 0) + reliability.get('error_rate_5xx', 0)):.2f}%
  - Impact Score: {load.get('load_impact_score', 0):.2f}""")

            return "\n".join(result)

        except Exception as e:
            logger.error(f"Error creating endpoint section: {str(e)}")
            return "Endpoint Analysis: Error generating analysis"

    def _create_recommendations(self, analysis: Dict) -> str:
        """Create actionable recommendations based on analysis results."""
        try:
            metrics = analysis.get('overall_metrics', {})
            response_time = metrics.get('response_time', {})
            failure_rates = metrics.get('failure_rates', {})
            recommendations = []
            bullet = self.symbols['bullet']

            # Performance recommendations
            avg_time = response_time.get('avg', 0)
            if avg_time > self.thresholds['response_time']['critical']:
                recommendations.append(
                    f"{bullet} Critical Performance Issues\n"
                    f"  - Current average response time: {avg_time:.2f}ms\n"
                    f"  - Immediate optimization required\n"
                    f"  - Review origin server resources\n"
                    f"  - Implement aggressive caching"
                )
            elif avg_time > self.thresholds['response_time']['warning']:
                recommendations.append(
                    f"{bullet} Performance Optimization Required\n"
                    f"  - Current average response time: {avg_time:.2f}ms\n"
                    f"  - Review origin configuration\n"
                    f"  - Consider Argo Smart Routing\n"
                    f"  - Implement edge computing"
                )

            # Error rate recommendations
            error_rate_4xx = failure_rates.get('error_rate_4xx', 0)
            error_rate_5xx = failure_rates.get('error_rate_5xx', 0)
            if error_rate_5xx > self.thresholds['error_rate']['warning']:
                recommendations.append(
                    f"{bullet} Critical Error Rate\n"
                    f"  - 5xx error rate: {error_rate_5xx:.2f}%\n"
                    f"  - Review server stability\n"
                    f"  - Implement circuit breakers\n"
                    f"  - Set up error monitoring"
                )
            if error_rate_4xx > self.thresholds['error_rate']['warning']:
                recommendations.append(
                    f"{bullet} High Client Error Rate\n"
                    f"  - 4xx error rate: {error_rate_4xx:.2f}%\n"
                    f"  - Review request validation\n"
                    f"  - Check client implementations"
                )

            # Geographic recommendations
            geo = analysis.get('geographic_analysis', {})
            slow_regions = geo.get('regional_summary', {}).get('slowest_regions', [])
            if slow_regions:
                recommendations.append(
                    f"{bullet} Geographic Optimization Required\n"
                    f"  - Optimize for: {', '.join(slow_regions[:3])}\n"
                    f"  - Review regional routing\n"
                    f"  - Consider additional PoPs"
                )

            if not recommendations:
                recommendations.append(
                    f"{bullet} No Critical Issues\n"
                    f"  - Continue monitoring metrics\n"
                    f"  - Implement proactive alerting\n"
                    f"  - Regular performance reviews"
                )

            return "Recommendations\n--------------\n" + "\n\n".join(recommendations)

        except Exception as e:
            logger.error(f"Error creating recommendations: {str(e)}")
            return "Recommendations: Error generating recommendations"

    def _format_list(self, items: List[str]) -> str:
        """Format a list of items with bullets."""
        bullet = self.symbols['bullet']
        if not items:
            return f"{bullet} None available"
        return "\n".join(f"{bullet} {item}" for item in items)

    def _save_report(self, report: str, zone_name: str, analysis_results: Dict) -> None:
        """Save report to file with proper formatting."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_path = self.report_dir / f"origin_analysis_{zone_name}_{timestamp}"
            
            # Save as text
            with open(f"{base_path}.txt", 'w', encoding='utf-8') as f:
                f.write(report)
            
            # Save as markdown
            with open(f"{base_path}.md", 'w', encoding='utf-8') as f:
                f.write(report)
            
            # Save raw analysis results
            with open(f"{base_path}_raw.json", 'w', encoding='utf-8') as f:
                json.dump(analysis_results, f, indent=2, default=str)
            
            logger.info(f"Origin analysis report saved to {base_path}")
            
        except Exception as e:
            logger.error(f"Error saving report: {str(e)}")
            logger.error(traceback.format_exc())
