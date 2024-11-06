# origin_reporter.py
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from pathlib import Path
import json

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
        metrics = analysis['overall_metrics']
        return f"""
Executive Summary
----------------
• Average Origin Response Time: {metrics['response_time']['avg']:.2f}ms
• 95th Percentile Response Time: {metrics['response_time']['p95']:.2f}ms
• Total Origin Requests: {metrics['request_volume']['total_requests']:,}
• Overall Error Rate: {metrics['failure_rates']['error_rate']:.2f}%
• Total Bandwidth Processed: {metrics['bandwidth']['total_bytes'] / (1024**3):.2f} GB

Key Findings:
• {'✓ Good' if metrics['response_time']['avg'] < 200 else '⚠ Attention Needed'} - Origin Response Time
• {'✓ Good' if metrics['failure_rates']['error_rate'] < 1 else '⚠ Attention Needed'} - Error Rate
• {'✓ Good' if metrics['failure_rates']['timeout_rate'] < 0.1 else '⚠ Attention Needed'} - Timeout Rate
"""

    def _create_performance_section(self, analysis: Dict) -> str:
        """Create performance analysis section."""
        perf = analysis['overall_metrics']['response_time']
        return f"""
Performance Analysis
------------------
Response Time Metrics:
• Average: {perf['avg']:.2f}ms
• Median: {perf['median']:.2f}ms
• 95th Percentile: {perf['p95']:.2f}ms
• 99th Percentile: {perf['p99']:.2f}ms
• Standard Deviation: {perf['std_dev']:.2f}ms

Performance Distribution:
• Fast Responses (<100ms): {self._calculate_percentage(analysis, lambda x: x < 100):.1f}%
• Normal Responses (100-500ms): {self._calculate_percentage(analysis, lambda x: 100 <= x < 500):.1f}%
• Slow Responses (>500ms): {self._calculate_percentage(analysis, lambda x: x >= 500):.1f}%

Peak Performance:
• Busiest Hour: {self._get_busiest_period(analysis)}
• Best Performance Period: {self._get_best_performance_period(analysis)}
• Worst Performance Period: {self._get_worst_performance_period(analysis)}
"""

    def _create_reliability_section(self, analysis: Dict) -> str:
        """Create reliability analysis section."""
        reliability = analysis['overall_metrics']['failure_rates']
        return f"""
Reliability Analysis
------------------
Error Metrics:
• Overall Error Rate: {reliability['error_rate']:.2f}%
• Timeout Rate: {reliability['timeout_rate']:.2f}%

Error Distribution:
{self._format_error_distribution(analysis)}

Reliability Patterns:
• Most Reliable Period: {self._get_most_reliable_period(analysis)}
• Most Errors: {self._get_highest_error_period(analysis)}
"""

    def _create_geographic_section(self, analysis: Dict) -> str:
        """Create geographic analysis section."""
        geo = analysis['geographic_analysis']
        return f"""
Geographic Analysis
-----------------
Best Performing Regions:
{self._format_top_regions(geo['regional_summary']['fastest_regions'])}

Worst Performing Regions:
{self._format_top_regions(geo['regional_summary']['slowest_regions'])}

Regions with Highest Error Rates:
{self._format_top_regions(geo['regional_summary']['highest_error_regions'])}
"""

    def _create_endpoint_section(self, analysis: Dict) -> str:
        """Create endpoint analysis section."""
        endpoints = analysis['endpoint_analysis']['endpoints']
        return f"""
Endpoint Analysis
---------------
Top Impacting Endpoints:
{self._format_top_endpoints(endpoints)}

Performance by Endpoint:
{self._format_endpoint_performance(endpoints)}
"""

    def _create_recommendations(self, analysis: Dict) -> str:
        """Create actionable recommendations based on analysis results."""
        metrics = analysis['overall_metrics']
        recommendations = []

        # Performance Recommendations
        if metrics['response_time']['avg'] > 200:
            recommendations.append(
                "• Consider origin server optimization - average response time exceeds 200ms"
            )
            if metrics['response_time']['p95'] > 1000:
                recommendations.append(
                    "• Implement request queuing or rate limiting - high p95 latency indicates potential overload"
                )

        # Error Rate Recommendations
        if metrics['failure_rates']['error_rate'] > 1:
            recommendations.append(
                "• Investigate origin server errors - error rate exceeds 1%"
            )
        if metrics['failure_rates']['timeout_rate'] > 0.1:
            recommendations.append(
                "• Review origin timeout settings - timeout rate indicates connectivity issues"
            )

        # Geographic Recommendations
        geo_metrics = analysis.get('geographic_analysis', {})
        if geo_metrics:
            slow_regions = geo_metrics.get('regional_summary', {}).get('slowest_regions', [])
            if slow_regions:
                recommendations.append(
                    f"• Consider additional edge locations or CDN optimization for: {', '.join(slow_regions[:3])}"
                )

        # Load Distribution Recommendations
        endpoints = analysis.get('endpoint_analysis', {}).get('endpoints', {})
        if endpoints:
            high_load_endpoints = [
                endpoint for endpoint, data in endpoints.items()
                if data['load']['load_impact_score'] > 0.8
            ]
            if high_load_endpoints:
                recommendations.append(
                    "• Implement caching or load balancing for high-impact endpoints:\n  " +
                    "\n  ".join(high_load_endpoints[:3])
                )

        # Protocol Recommendations
        protocol_impact = analysis.get('protocol_impact', {}).get('protocols', {})
        if protocol_impact:
            http1_metrics = protocol_impact.get('HTTP/1.1', {})
            http2_metrics = protocol_impact.get('HTTP/2', {})
            if http1_metrics and http2_metrics:
                if http1_metrics['performance']['avg_response_time'] > http2_metrics['performance']['avg_response_time']:
                    recommendations.append(
                        "• Consider upgrading to HTTP/2 for improved performance"
                    )

        if not recommendations:
            recommendations.append("• No critical issues identified - continue monitoring")

        return f"""
Recommendations
--------------
{chr(10).join(recommendations)}

Priority Actions:
{self._prioritize_recommendations(recommendations)}
"""

    def _prioritize_recommendations(self, recommendations: List[str]) -> str:
        """Prioritize recommendations based on impact and urgency."""
        def get_priority(recommendation: str) -> int:
            if any(keyword in recommendation.lower() 
                  for keyword in ['error', 'timeout', 'critical']):
                return 1
            elif any(keyword in recommendation.lower() 
                    for keyword in ['performance', 'optimize', 'slow']):
                return 2
            elif any(keyword in recommendation.lower() 
                    for keyword in ['consider', 'implement']):
                return 3
            return 4

        prioritized = sorted(recommendations, key=get_priority)
        result = []
        for i, rec in enumerate(prioritized[:3], 1):
            priority = get_priority(rec)
            urgency = "HIGH" if priority == 1 else "MEDIUM" if priority == 2 else "LOW"
            result.append(f"{i}. [{urgency}] {rec.lstrip('• ')}")

        return "\n".join(result)

    def _calculate_percentage(self, analysis: Dict, condition) -> float:
        """Calculate percentage of requests meeting a condition."""
        try:
            metrics = analysis.get('overall_metrics', {})
            total_requests = metrics.get('request_volume', {}).get('total_requests', 0)
            if total_requests == 0:
                return 0.0

            filtered_requests = sum(1 for rt in metrics.get('response_time', {}).values()
                                 if condition(rt))
            return (filtered_requests / total_requests) * 100
        except Exception as e:
            logger.error(f"Error calculating percentage: {str(e)}")
            return 0.0

    def _format_error_distribution(self, analysis: Dict) -> str:
        """Format error distribution details."""
        try:
            error_analysis = analysis.get('error_analysis', {})
            if not error_analysis:
                return "No error distribution data available"

            error_dist = error_analysis.get('error_distribution', {})
            result = []
            for status, count in sorted(error_dist.items()):
                percentage = count / analysis['overall_metrics']['request_volume']['total_requests'] * 100
                result.append(f"• {status}: {count:,} ({percentage:.2f}%)")

            return "\n".join(result) if result else "No errors recorded"
        except Exception as e:
            logger.error(f"Error formatting error distribution: {str(e)}")
            return "Error distribution data unavailable"

    def _format_top_regions(self, regions: List[str]) -> str:
        """Format top regions with performance metrics."""
        try:
            return "\n".join(f"• {region}" for region in regions[:5])
        except Exception as e:
            logger.error(f"Error formatting top regions: {str(e)}")
            return "Region data unavailable"

    def _format_top_endpoints(self, endpoints: Dict) -> str:
        """Format top endpoints by impact."""
        try:
            sorted_endpoints = sorted(
                endpoints.items(),
                key=lambda x: x[1]['load']['load_impact_score'],
                reverse=True
            )[:5]

            result = []
            for endpoint, data in sorted_endpoints:
                result.append(
                    f"• {endpoint}\n"
                    f"  - Response Time: {data['performance']['avg_response_time']:.2f}ms\n"
                    f"  - Requests: {data['load']['requests']:,}\n"
                    f"  - Error Rate: {data['reliability']['error_rate']:.2f}%"
                )
            return "\n".join(result)
        except Exception as e:
            logger.error(f"Error formatting top endpoints: {str(e)}")
            return "Endpoint data unavailable"

    def _format_endpoint_performance(self, endpoints: Dict) -> str:
        """Format detailed endpoint performance metrics."""
        try:
            rows = []
            for endpoint, data in sorted(
                endpoints.items(),
                key=lambda x: x[1]['load']['requests'],
                reverse=True
            )[:10]:
                rows.append(
                    f"• {endpoint[:50]}...\n"
                    f"  Avg: {data['performance']['avg_response_time']:.1f}ms | "
                    f"Errors: {data['reliability']['error_rate']:.1f}% | "
                    f"Requests: {data['load']['requests']:,}"
                )
            return "\n".join(rows)
        except Exception as e:
            logger.error(f"Error formatting endpoint performance: {str(e)}")
            return "Performance data unavailable"

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

    def _get_busiest_period(self, analysis: Dict) -> str:
        """Get the busiest time period from the analysis."""
        try:
            time_analysis = analysis.get('temporal_analysis', {})
            peak_periods = time_analysis.get('peak_periods', {})
            return peak_periods.get('highest_load', 'Data unavailable')
        except Exception as e:
            logger.error(f"Error getting busiest period: {str(e)}")
            return 'Data unavailable'

    def _get_best_performance_period(self, analysis: Dict) -> str:
        """Get the period with best performance."""
        try:
            time_analysis = analysis.get('temporal_analysis', {})
            performance_periods = time_analysis.get('performance_periods', {})
            return performance_periods.get('best_performance', 'Data unavailable')
        except Exception as e:
            logger.error(f"Error getting best performance period: {str(e)}")
            return 'Data unavailable'

    def _get_worst_performance_period(self, analysis: Dict) -> str:
        """Get the period with worst performance."""
        try:
            time_analysis = analysis.get('temporal_analysis', {})
            performance_periods = time_analysis.get('performance_periods', {})
            return performance_periods.get('worst_performance', 'Data unavailable')
        except Exception as e:
            logger.error(f"Error getting worst performance period: {str(e)}")
            return 'Data unavailable'
