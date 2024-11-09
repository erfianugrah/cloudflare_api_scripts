from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from pathlib import Path
import json
import traceback

logger = logging.getLogger(__name__)

class OriginReporter:
    """Reporter for origin server performance analysis."""
    
    def __init__(self, config):
        self.config = config
        self.report_dir = self.config.reports_dir / 'origin'
        self.report_dir.mkdir(parents=True, exist_ok=True)

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

            report = self._create_report_sections(df, analysis_results, zone_name)
            self._save_report(report, zone_name)
            return report

        except Exception as e:
            logger.error(f"Error generating origin report: {str(e)}")
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

            return "\n\n".join(sections)

        except Exception as e:
            logger.error(f"Error creating report sections: {str(e)}")
            return "Error generating report. Please check logs for details."

    def _create_header(self, zone_name: str) -> str:
        """Create report header."""
        return f"""
Origin Performance Analysis Report
================================
Zone: {zone_name}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
"""

    def _create_executive_summary(self, analysis: Dict) -> str:
        """Create executive summary section."""
        try:
            metrics = analysis.get('overall_metrics', {})
            response_time = metrics.get('response_time', {})
            failure_rates = metrics.get('failure_rates', {})
            request_vol = metrics.get('request_volume', {})
            bandwidth = metrics.get('bandwidth', {})

            return f"""
Executive Summary
----------------
• Average Origin Response Time: {response_time.get('avg', 0):.2f}ms
• 95th Percentile Response Time: {response_time.get('p95', 0):.2f}ms
• Total Origin Requests: {request_vol.get('total_requests', 0):,}
• Overall Error Rate: {failure_rates.get('error_rate', 0):.2f}%
• Total Bandwidth Processed: {bandwidth.get('total_bytes', 0) / (1024**3):.2f} GB

Key Findings:
• {'✓ Good' if response_time.get('avg', 0) < 200 else '⚠ Attention Needed'} - Origin Response Time
• {'✓ Good' if failure_rates.get('error_rate', 0) < 1 else '⚠ Attention Needed'} - Error Rate
• {'✓ Good' if failure_rates.get('timeout_rate', 0) < 0.1 else '⚠ Attention Needed'} - Timeout Rate
"""
        except Exception as e:
            logger.error(f"Error creating executive summary: {str(e)}")
            return "Executive Summary: Error generating summary\n"

    def _create_performance_section(self, analysis: Dict) -> str:
        """Create performance analysis section."""
        try:
            perf = analysis.get('overall_metrics', {}).get('response_time', {})
            temporal = analysis.get('temporal_analysis', {})
            peak_periods = temporal.get('peak_periods', {})

            return f"""
Performance Analysis
------------------
Response Time Metrics:
• Average: {perf.get('avg', 0):.2f}ms
• Median: {perf.get('median', 0):.2f}ms
• 95th Percentile: {perf.get('p95', 0):.2f}ms
• 99th Percentile: {perf.get('p99', 0):.2f}ms
• Standard Deviation: {perf.get('std_dev', 0):.2f}ms

Peak Performance:
• Busiest Period: {peak_periods.get('highest_load', 'Unknown')}
• Worst Performance Period: {peak_periods.get('worst_performance', 'Unknown')}
"""
        except Exception as e:
            logger.error(f"Error creating performance section: {str(e)}")
            return "Performance Analysis: Error generating analysis\n"

    def _create_reliability_section(self, analysis: Dict) -> str:
        """Create reliability analysis section."""
        try:
            reliability = analysis.get('overall_metrics', {}).get('failure_rates', {})
            error_analysis = analysis.get('error_analysis', {})
            error_timing = error_analysis.get('error_timing', {})
            error_percentiles = error_timing.get('error_percentiles', {})

            return f"""
Reliability Analysis
------------------
Error Metrics:
• Overall Error Rate: {reliability.get('error_rate', 0):.2f}%
• Timeout Rate: {reliability.get('timeout_rate', 0):.2f}%

Error Response Times:
• Average Time to Error: {error_timing.get('avg_time_to_error', 0):.2f}ms
• 50th Percentile: {error_percentiles.get('p50', 0):.2f}ms
• 95th Percentile: {error_percentiles.get('p95', 0):.2f}ms
• 99th Percentile: {error_percentiles.get('p99', 0):.2f}ms
"""
        except Exception as e:
            logger.error(f"Error creating reliability section: {str(e)}")
            return "Reliability Analysis: Error generating analysis\n"

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
{self._format_list(regional.get('highest_error_regions', []))}
"""
        except Exception as e:
            logger.error(f"Error creating geographic section: {str(e)}")
            return "Geographic Analysis: Error generating analysis\n"

    def _create_endpoint_section(self, analysis: Dict) -> str:
        """Create endpoint analysis section."""
        try:
            endpoint_analysis = analysis.get('endpoint_analysis', {})
            endpoints = endpoint_analysis.get('endpoints', {})
            top_endpoints = endpoint_analysis.get('top_impacting_endpoints', [])

            if not endpoints or not top_endpoints:
                return "Endpoint Analysis\n---------------\nNo endpoint data available"

            result = "Endpoint Analysis\n---------------\n"
            result += "Top Impacting Endpoints:\n"
            
            for endpoint in top_endpoints[:5]:  # Show top 5
                metrics = endpoints.get(endpoint, {})
                perf = metrics.get('performance', {})
                load = metrics.get('load', {})
                reliability = metrics.get('reliability', {})
                
                result += f"""
• {endpoint}
  - Response Time: {perf.get('avg_response_time', 0):.2f}ms
  - Requests: {load.get('requests', 0):,}
  - Error Rate: {reliability.get('error_rate', 0):.2f}%
  - Impact Score: {load.get('load_impact_score', 0):.2f}
"""
            return result

        except Exception as e:
            logger.error(f"Error creating endpoint section: {str(e)}")
            return "Endpoint Analysis: Error generating analysis\n"

    def _create_recommendations(self, analysis: Dict) -> str:
        """Create actionable recommendations based on analysis results."""
        try:
            metrics = analysis.get('overall_metrics', {})
            response_time = metrics.get('response_time', {})
            failure_rates = metrics.get('failure_rates', {})
            recommendations = []

            # Performance recommendations
            avg_time = response_time.get('avg', 0)
            if avg_time > 200:
                recommendations.append(
                    "• Consider origin server optimization - average response time exceeds 200ms"
                )
                if response_time.get('p95', 0) > 1000:
                    recommendations.append(
                        "• Implement request queuing or rate limiting - high p95 latency indicates potential overload"
                    )

            # Error rate recommendations
            error_rate = failure_rates.get('error_rate', 0)
            if error_rate > 1:
                recommendations.append(
                    "• Investigate origin server errors - error rate exceeds 1%"
                )
            if failure_rates.get('timeout_rate', 0) > 0.1:
                recommendations.append(
                    "• Review origin timeout settings - timeout rate indicates connectivity issues"
                )

            # Geographic recommendations
            geo = analysis.get('geographic_analysis', {})
            slow_regions = geo.get('regional_summary', {}).get('slowest_regions', [])
            if slow_regions:
                recommendations.append(
                    f"• Consider additional edge locations or CDN optimization for: {', '.join(slow_regions[:3])}"
                )

            if not recommendations:
                recommendations.append("• No critical issues identified - continue monitoring")

            return f"""
Recommendations
--------------
{chr(10).join(recommendations)}
"""
        except Exception as e:
            logger.error(f"Error creating recommendations: {str(e)}")
            return "Recommendations: Error generating recommendations\n"

    def _format_list(self, items: List[str], bullet: str = "• ") -> str:
        """Format a list of items with bullets."""
        if not items:
            return f"{bullet}None available"
        return "\n".join(f"{bullet}{item}" for item in items)

    def _save_report(self, report: str, zone_name: str) -> None:
        """Save report to file with proper formatting."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = self.report_dir / f"origin_analysis_{zone_name}_{timestamp}.txt"
            
            filepath.write_text(report)
            logger.info(f"Origin analysis report saved to {filepath}")
            
            # Also save as markdown for better formatting
            md_filepath = filepath.with_suffix('.md')
            md_filepath.write_text(report)
            
        except Exception as e:
            logger.error(f"Error saving report: {str(e)}")
