from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import json
from prettytable import PrettyTable
import traceback
import pandas as pd
import numpy as np
from .types import OriginPerformanceMetrics, OriginPathMetrics
from .data_processor import DataProcessor

logger = logging.getLogger(__name__)

class OriginReporter:
    """Enhanced reporter for origin server performance analysis"""
    
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
            'warning': "⚠",
            'critical': "🔴",
            'up_arrow': "↑",
            'down_arrow': "↓",
            'right_arrow': "→",
            'database': "🗄",
            'clock': "⏱",
            'lightning': "⚡"
        }

    def generate_origin_report(
        self,
        df: pd.DataFrame,
        analysis_results: Dict,
        zone_name: str
    ) -> Optional[str]:
        """Generate comprehensive origin performance report"""
        try:
            if df is None or df.empty or not analysis_results:
                logger.error("No data available for origin report generation")
                return None

            logger.info(f"""
Origin Report Generation Starting:
----------------------------
Zone: {zone_name}
Analysis Results Keys: {list(analysis_results.keys())}
DataFrame Shape: {df.shape}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
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
        """Create detailed report sections"""
        try:
            sections = [
                self._create_header(zone_name),
                self._create_executive_summary(analysis),
                self._create_performance_section(analysis),
                self._create_network_path_section(analysis),
                self._create_reliability_section(analysis),
                self._create_geographic_section(analysis),
                self._create_endpoint_section(analysis),
                self._create_content_section(analysis),
                self._create_recommendations(analysis)
            ]

            return "\n\n".join(filter(None, sections))

        except Exception as e:
            logger.error(f"Error creating report sections: {str(e)}")
            return "Error generating origin performance report. Please check logs for details."

    def _create_header(self, zone_name: str) -> str:
        """Create report header with metadata"""
        return f"""Origin Performance Analysis Report
================================
Zone: {zone_name}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
Analysis Type: Origin Server Performance"""

    def _create_executive_summary(self, analysis: Dict) -> str:
        """Create executive summary with enhanced status indicators"""
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
            critical = self.symbols['critical']
            clock = self.symbols['clock']

            # Calculate status indicators
            avg_response = response_time.get('avg', 0)
            error_rate = failure_rates.get('error_rate', 0)
            timeout_rate = failure_rates.get('timeout_rate', 0)

            status = {
                'performance': (
                    f'{check} Good' if avg_response < self.thresholds['response_time']['warning']
                    else f'{critical} Critical' if avg_response >= self.thresholds['response_time']['critical']
                    else f'{warning} Attention Needed'
                ),
                'reliability': (
                    f'{check} Good' if error_rate < self.thresholds['error_rate']['warning']
                    else f'{critical} Critical' if error_rate >= self.thresholds['error_rate']['critical']
                    else f'{warning} Attention Needed'
                ),
                'timeouts': (
                    f'{check} Good' if timeout_rate < self.thresholds['timeout_rate']['warning']
                    else f'{critical} Critical' if timeout_rate >= self.thresholds['timeout_rate']['critical']
                    else f'{warning} Attention Needed'
                )
            }

            # Network path analysis
            network_analysis = analysis.get('network_analysis', {}).get('summary', {})
            path_metrics = request_vol.get('path_latency_avg', 0)

            return f"""
Executive Summary
----------------
{bullet} Overall Health Status: {health_status.upper()}

Performance Metrics:
{bullet} Average Response Time: {avg_response:.2f}ms
{bullet} P95 Response Time: {response_time.get('p95', 0):.2f}ms
{bullet} Response Time Variation: {response_time.get('std_dev', 0):.2f}ms

Traffic Overview:
{bullet} Total Origin Requests: {request_vol.get('total', 0):,}
{bullet} Requests/Second: {request_vol.get('per_second', 0):.2f}
{bullet} Average Response Size: {bandwidth.get('avg_response_size', 0) / 1024:.2f} KB

Network Paths:
{bullet} Path Distribution:
  - Direct Paths: {network_analysis.get('direct_paths', 0)}
  - Tiered Paths: {network_analysis.get('tiered_paths', 0)}
{bullet} Average Path Latency: {path_metrics:.2f}ms

Reliability:
{bullet} Error Rate: {error_rate:.2f}%
{bullet} Timeout Rate: {timeout_rate:.2f}%
{bullet} 4xx Rate: {failure_rates.get('error_rate_4xx', 0):.2f}%
{bullet} 5xx Rate: {failure_rates.get('error_rate_5xx', 0):.2f}%

Status Indicators:
{bullet} Response Time: {status['performance']}
{bullet} Error Rate: {status['reliability']}
{bullet} Timeout Rate: {status['timeouts']}"""

        except Exception as e:
            logger.error(f"Error creating executive summary: {str(e)}")
            return "Executive Summary: Error generating summary"

    def _create_performance_section(self, analysis: Dict) -> str:
        """Create origin performance analysis section with enhanced metrics"""
        try:
            metrics = analysis.get('overall_metrics', {})
            response_time = metrics.get('response_time', {})
            temporal = analysis.get('temporal_analysis', {})
            peak_periods = temporal.get('peak_periods', {})
            bullet = self.symbols['bullet']
            clock = self.symbols['clock']

            # Add trend analysis
            trend_data = temporal.get('time_series', {})
            if trend_data:
                trend_values = [data['performance']['response_time'] 
                              for data in trend_data.values()]
                trend_direction = (
                    f"{self.symbols['up_arrow']} Increasing" 
                    if trend_values[-1] > trend_values[0]
                    else f"{self.symbols['down_arrow']} Decreasing"
                )
                trend_pct = ((trend_values[-1] / trend_values[0]) - 1) * 100
                trend = f"{trend_direction} ({trend_pct:.1f}%)"
            else:
                trend = "No trend data available"

            # Format peak periods
            highest_load = peak_periods.get('highest_load', {})
            worst_perf = peak_periods.get('worst_performance', {})

            return f"""
Origin Performance Analysis
------------------------
Response Time Metrics:
{bullet} Basic Metrics:
  - Average: {response_time.get('avg', 0):.2f}ms
  - Median: {response_time.get('median', 0):.2f}ms
  - P95: {response_time.get('p95', 0):.2f}ms
  - P99: {response_time.get('p99', 0):.2f}ms
  - Standard Deviation: {response_time.get('std_dev', 0):.2f}ms
  - Total Response Time: {response_time.get('total', 0):.2f}ms

{bullet} Response Time Trend: {trend}

{bullet} Peak Load Period: {highest_load.get('timestamp', 'Unknown')}
  - Requests: {highest_load.get('requests', 0):,}
  - Response Time: {highest_load.get('response_time', 0):.2f}ms
  - Path Latency: {highest_load.get('path_latency', 0):.2f}ms

{bullet} Worst Performance Period: {worst_perf.get('timestamp', 'Unknown')}
  - Response Time: {worst_perf.get('response_time', 0):.2f}ms
  - Requests: {worst_perf.get('requests', 0):,}
  - Path Latency: {worst_perf.get('path_latency', 0):.2f}ms"""

        except Exception as e:
            logger.error(f"Error creating performance section: {str(e)}")
            return "Performance Analysis: Error generating analysis"

    def _create_network_path_section(self, analysis: Dict) -> str:
        """Create network path analysis section with detailed metrics"""
        try:
            network = analysis.get('network_analysis', {})
            paths = network.get('paths', {})
            summary = network.get('summary', {})
            bullet = self.symbols['bullet']
            arrow = self.symbols['right_arrow']

            if not paths:
                return f"""
Network Path Analysis
------------------
No network path data available"""

            # Create path performance table
            path_table = PrettyTable()
            path_table.field_names = [
                "Path", "Requests", "Response Time", "Latency", "Error Rate"
            ]
            path_table.align = "r"
            path_table.align["Path"] = "l"

            for path_id, metrics in paths.items():
                path_table.add_row([
                    path_id,
                    f"{metrics.get('requests', 0):,}",
                    f"{metrics.get('avg_response_time', 0):.2f}ms",
                    f"{metrics.get('path_latency', 0):.2f}ms",
                    f"{sum(metrics.get('error_rates', {}).values()):.2f}%"
                ])

            # Get performance comparison
            perf_comparison = summary.get('performance_comparison', {})
            tiered = perf_comparison.get('tiered', {})
            direct = perf_comparison.get('direct', {})

            return f"""
Network Path Analysis
------------------
Path Distribution:
{bullet} Total Paths: {summary.get('total_paths', 0)}
{bullet} Direct Paths: {summary.get('direct_paths', 0)}
{bullet} Tiered Paths: {summary.get('tiered_paths', 0)}

Traffic Distribution:
{bullet} Direct Requests: {summary.get('path_distribution', {}).get('direct_percentage', 0):.1f}%
{bullet} Tiered Requests: {summary.get('path_distribution', {}).get('tiered_percentage', 0):.1f}%

Performance Comparison:
{bullet} Direct Paths:
  - Average Response Time: {direct.get('avg_response_time', 0):.2f}ms
  - Average Latency: {direct.get('avg_latency', 0):.2f}ms
  - Error Rate: {direct.get('error_rate', 0):.2f}%

{bullet} Tiered Paths:
  - Average Response Time: {tiered.get('avg_response_time', 0):.2f}ms
  - Average Latency: {tiered.get('avg_latency', 0):.2f}ms
  - Error Rate: {tiered.get('error_rate', 0):.2f}%

Path Performance Details:
{path_table.get_string()}"""

        except Exception as e:
            logger.error(f"Error creating network path section: {str(e)}")
            return "Network Path Analysis: Error generating analysis"

    def _create_reliability_section(self, analysis: Dict) -> str:
        """Create reliability analysis section with detailed error patterns"""
        try:
            error_analysis = analysis.get('error_analysis', {})
            error_dist = error_analysis.get('error_distribution', {})
            error_timing = error_analysis.get('timing', {})
            bullet = self.symbols['bullet']
            warning = self.symbols['warning']

            # Create error distribution table
            error_table = PrettyTable()
            error_table.field_names = ["Status", "Count", "Percentage", "Avg Response", "Avg Size"]
            error_table.align = "r"
            error_table.align["Status"] = "l"

            for status, metrics in error_dist.items():
                error_table.add_row([
                    status,
                    f"{metrics.get('count', 0):,}",
                    f"{metrics.get('percentage', 0):.2f}%",
                    f"{metrics.get('avg_response_time', 0):.2f}ms",
                    f"{metrics.get('avg_size', 0) / 1024:.2f}KB"
                ])

            # Get impact analysis
            network_impact = error_analysis.get('network_impact', {})
            content_impact = error_analysis.get('content_impact', {})

            # Format error patterns
            patterns = {
                'Network Impact': self._format_error_network_impact(network_impact),
                'Content Impact': self._format_error_content_impact(content_impact)
            }

            return f"""
Reliability Analysis
------------------
Error Response Times:
{bullet} Average Response Time: {error_timing.get('avg_response_time', 0):.2f}ms
{bullet} P50 Response Time: {error_timing.get('percentiles', {}).get('p50', 0):.2f}ms
{bullet} P95 Response Time: {error_timing.get('percentiles', {}).get('p95', 0):.2f}ms
{bullet} P99 Response Time: {error_timing.get('percentiles', {}).get('p99', 0):.2f}ms
{bullet} Average Path Latency: {error_timing.get('path_latency', 0):.2f}ms

Error Distribution:
{error_table.get_string()}

Network Impact on Errors:
{patterns['Network Impact']}

Content Type Impact on Errors:
{patterns['Content Impact']}

Error Correlations:
{self._format_error_correlations(error_analysis.get('correlations', {}))}"""

        except Exception as e:
            logger.error(f"Error creating reliability section: {str(e)}")
            return "Reliability Analysis: Error generating analysis"

    def _format_error_network_impact(self, network_impact: Dict) -> str:
        """Format network impact on errors with detailed metrics"""
        try:
            bullet = self.symbols['bullet']
            paths = network_impact.get('paths', {})
            summary = network_impact.get('summary', {})

            if not paths:
                return f"{bullet} No network impact data available"

            # Format high-error paths
            high_error_paths = [
                (path, metrics) for path, metrics in paths.items()
                if sum(metrics.get('error_rates', {}).values()) > 5  # 5% error rate threshold
            ]

            path_impacts = []
            for path, metrics in sorted(
                high_error_paths,
                key=lambda x: sum(x[1].get('error_rates', {}).values()),
                reverse=True
            )[:3]:  # Top 3 problematic paths
                error_rates = metrics.get('error_rates', {})
                requests = metrics.get('requests', 0)
                path_impacts.append(
                    f"{bullet} {path}:\n"
                    f"  - Error Rate: {sum(error_rates.values()):.2f}%\n"
                    f"  - Response Time: {metrics.get('avg_response_time', 0):.2f}ms\n"
                    f"  - Requests: {requests:,}"
                )

            return "\n".join(path_impacts) if path_impacts else f"{bullet} No significant network-related error patterns detected"

        except Exception as e:
            logger.error(f"Error formatting network impact: {str(e)}")
            return f"{self.symbols['bullet']} Error analyzing network impact"

    def _format_error_content_impact(self, content_impact: Dict) -> str:
        """Format content type impact on errors with enhanced metrics"""
        try:
            bullet = self.symbols['bullet']
            content_types = content_impact.get('content_types', {})
            summary = content_impact.get('summary', {})

            if not content_types:
                return f"{bullet} No content impact data available"

            # Format high-error content types
            high_error_types = [
                (ctype, metrics) for ctype, metrics in content_types.items()
                if sum(metrics.get('error_rates', {}).values()) > 5  # 5% error rate threshold
            ]

            # Add summary metrics first
            content_impacts = [
                f"{bullet} Static Content Error Rate: {summary.get('static_error_rate', 0):.2f}%",
                f"{bullet} Dynamic Content Error Rate: {summary.get('dynamic_error_rate', 0):.2f}%"
            ]

            # Add detailed content type impacts
            for ctype, metrics in sorted(
                high_error_types,
                key=lambda x: sum(x[1].get('error_rates', {}).values()),
                reverse=True
            )[:3]:  # Top 3 problematic content types
                error_rates = metrics.get('error_rates', {})
                content_impacts.append(
                    f"{bullet} {ctype}:\n"
                    f"  - Error Rate: {sum(error_rates.values()):.2f}%\n"
                    f"  - Type: {metrics.get('content_type', 'unknown').upper()}\n"
                    f"  - Avg Size: {metrics.get('avg_size', 0) / 1024:.2f}KB"
                )

            return "\n".join(content_impacts)

        except Exception as e:
            logger.error(f"Error formatting content impact: {str(e)}")
            return f"{self.symbols['bullet']} Error analyzing content impact"

    def _format_error_correlations(self, correlations: Dict) -> str:
        """Format error correlations with enhanced analysis"""
        try:
            bullet = self.symbols['bullet']

            if not correlations:
                return f"{bullet} No correlation data available"

            sections = []
            
            # Performance correlations
            perf = correlations.get('performance', {})
            if perf:
                sections.append(f"{bullet} Performance Correlations:")
                for metric, value in perf.items():
                    if abs(value) > 0.1:  # Only show significant correlations
                        direction = "positive" if value > 0 else "negative"
                        strength = (
                            "strong" if abs(value) > 0.7
                            else "moderate" if abs(value) > 0.4
                            else "weak"
                        )
                        sections.append(
                            f"  - {metric}: {strength} {direction} "
                            f"correlation ({value:.2f})"
                        )

            # Traffic correlations
            traffic = correlations.get('traffic', {})
            if traffic:
                sections.append(f"{bullet} Traffic Correlations:")
                for metric, value in traffic.items():
                    if abs(value) > 0.1:
                        direction = "positive" if value > 0 else "negative"
                        strength = (
                            "strong" if abs(value) > 0.7
                            else "moderate" if abs(value) > 0.4
                            else "weak"
                        )
                        sections.append(
                            f"  - {metric}: {strength} {direction} "
                            f"correlation ({value:.2f})"
                        )

            return "\n".join(sections) if sections else f"{bullet} No significant correlations found"

        except Exception as e:
            logger.error(f"Error formatting error correlations: {str(e)}")
            return f"{self.symbols['bullet']} Error analyzing correlations"

    def _create_geographic_section(self, analysis: Dict) -> str:
        """Create geographic analysis section with enhanced regional analysis"""
        try:
            geo = analysis.get('geographic_analysis', {})
            countries = geo.get('countries', {})
            summary = geo.get('summary', {})
            bullet = self.symbols['bullet']

            if not countries:
                return f"""
Geographic Analysis
-----------------
No geographic data available"""

            # Create performance table
            perf_table = PrettyTable()
            perf_table.field_names = [
                "Region", "Response Time", "P95", "Error Rate", "Path Latency"
            ]
            perf_table.align = "r"
            perf_table.align["Region"] = "l"

            # Get top countries by request volume
            top_countries = sorted(
                [(k, v) for k, v in countries.items() if isinstance(v, dict)],
                key=lambda x: x[1].get('traffic', {}).get('requests', 0),
                reverse=True
            )[:10]

            for country, metrics in top_countries:
                country = self._clean_string_value(country)
                perf = metrics.get('performance', {})
                reliability = metrics.get('reliability', {})

                perf_table.add_row([
                    country,
                    f"{perf.get('avg_response_time', 0):.2f}ms",
                    f"{perf.get('p95_response_time', 0):.2f}ms",
                    f"{reliability.get('total_error_rate', 0):.2f}%",
                    f"{perf.get('path_latency', {}).get('avg', 0):.2f}ms"
                ])

            # Get global metrics
            global_metrics = summary.get('global_metrics', {})
            regional_dist = summary.get('top_countries', {})

            return f"""
Geographic Analysis
-----------------
Performance by Region:
{perf_table.get_string()}

Regional Patterns:
{bullet} Fastest Regions: {', '.join(regional_dist.get('fastest', ['None']))}
{bullet} Slowest Regions: {', '.join(regional_dist.get('slowest', ['None']))}
{bullet} Highest Error Regions: {', '.join(regional_dist.get('highest_errors', ['None']))}
{bullet} Highest Traffic: {', '.join(regional_dist.get('by_traffic', ['None']))}

Global Metrics:
{bullet} Total Countries: {global_metrics.get('total_countries', 0)}
{bullet} Average Response Time: {global_metrics.get('avg_response_time', 0):.2f}ms
{bullet} Average Path Latency: {global_metrics.get('avg_path_latency', 0):.2f}ms
{bullet} Global Error Rate: {global_metrics.get('avg_error_rate', 0):.2f}%"""

        except Exception as e:
            logger.error(f"Error creating geographic section: {str(e)}")
            return "Geographic Analysis: Error generating analysis"

    def _create_endpoint_section(self, analysis: Dict) -> str:
        """Create endpoint analysis section with comprehensive metrics"""
        try:
            endpoint_analysis = analysis.get('endpoint_analysis', {})
            endpoints = endpoint_analysis.get('endpoints', {})
            summary = endpoint_analysis.get('summary', {})
            bullet = self.symbols['bullet']
            warning = self.symbols['warning']

            if not endpoints:
                return f"""
Endpoint Analysis
--------------
No endpoint data available"""

            # Create endpoint performance table
            endpoint_table = PrettyTable()
            endpoint_table.field_names = [
                "Endpoint", "Response Time", "P95", "Error Rate", "Type"
            ]
            endpoint_table.align = "r"
            endpoint_table.align["Endpoint"] = "l"
            endpoint_table.align["Type"] = "l"

            # Get top endpoints by impact (requests * error rate)
            top_endpoints = sorted(
                endpoints.items(),
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )[:10]

            for endpoint, metrics in top_endpoints:
                perf = metrics.get('performance', {})
                reliability = metrics.get('reliability', {})

                endpoint_table.add_row([
                    endpoint[:50] + "..." if len(endpoint) > 50 else endpoint,
                    f"{perf.get('avg_response_time', 0):.2f}ms",
                    f"{perf.get('p95_response_time', 0):.2f}ms",
                    f"{reliability.get('error_rate', 0):.2f}%",
                    metrics.get('content_type', 'unknown').upper()
                ])

            # Format problematic endpoints
            problematic = []
            for endpoint in summary.get('problematic_endpoints', []):
                if endpoint in endpoints:
                    metrics = endpoints[endpoint]
                    perf = metrics.get('performance', {})
                    reliability = metrics.get('reliability', {})
                    problematic.append(
                        f"{warning} {endpoint}:\n"
                        f"  - Response Time: {perf.get('avg_response_time', 0):.2f}ms\n"
                        f"  - Error Rate: {reliability.get('error_rate', 0):.2f}%\n"
                        f"  - Type: {metrics.get('content_type', 'unknown').upper()}"
                    )

            return f"""
Endpoint Analysis
--------------
Summary:
{bullet} Total Endpoints: {summary.get('total_endpoints', 0)}
{bullet} Static Endpoints: {summary.get('static_endpoints', 0)}
{bullet} Dynamic Endpoints: {summary.get('dynamic_endpoints', 0)}

Top Endpoints by Traffic:
{endpoint_table.get_string()}

Problematic Endpoints:
{chr(10).join(problematic) if problematic else f"{bullet} No problematic endpoints detected"}"""

        except Exception as e:
            logger.error(f"Error creating endpoint section: {str(e)}")
            return "Endpoint Analysis: Error generating analysis"

    def _create_content_section(self, analysis: Dict) -> str:
        """Create content type analysis section with enhanced performance metrics"""
        try:
            content_analysis = analysis.get('content_analysis', {})
            content_types = content_analysis.get('content_types', {})
            summary = content_analysis.get('summary', {})
            bullet = self.symbols['bullet']

            if not content_types:
                return ""  # Skip section if no content type data

            # Create content type performance table
            content_table = PrettyTable()
            content_table.field_names = [
                "Content Type", "Response Time", "Error Rate", "Avg Size", "Type"
            ]
            content_table.align = "r"
            content_table.align["Content Type"] = "l"
            content_table.align["Type"] = "l"

            for ctype, metrics in sorted(
                content_types.items(),
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )[:10]:  # Show top 10 content types by volume
                perf = metrics['performance']
                reliability = metrics['reliability']
                
                content_table.add_row([
                    ctype[:40] + "..." if len(ctype) > 40 else ctype,
                    f"{perf['avg_response_time']:.2f}ms",
                    f"{reliability['error_rate']:.2f}%",
                    f"{perf['avg_size'] / 1024:.2f}KB",
                    metrics['type'].upper()
                ])

            category_perf = summary.get('performance_by_category', {})

            return f"""
Content Type Analysis
------------------
Content Distribution:
{bullet} Static Content Types: {summary.get('static_types', 0)}
{bullet} Dynamic Content Types: {summary.get('dynamic_types', 0)}

Performance by Category:
{bullet} Static Content:
  - Average Response Time: {category_perf.get('static', {}).get('avg_response_time', 0):.2f}ms
  - Error Rate: {category_perf.get('static', {}).get('error_rate', 0):.2f}%

{bullet} Dynamic Content:
  - Average Response Time: {category_perf.get('dynamic', {}).get('avg_response_time', 0):.2f}ms
  - Error Rate: {category_perf.get('dynamic', {}).get('error_rate', 0):.2f}%

Content Type Details:
{content_table.get_string()}"""

        except Exception as e:
            logger.error(f"Error creating content section: {str(e)}")
            return ""

    def _create_recommendations(self, analysis: Dict) -> str:
        """Create actionable recommendations based on analysis results"""
        try:
            recommendations = []
            bullet = self.symbols['bullet']
            warning = self.symbols['warning']
            critical = self.symbols['critical']
            database = self.symbols['database']
            clock = self.symbols['clock']

            # Extract key metrics
            metrics = analysis.get('overall_metrics', {})
            response_time = metrics.get('response_time', {})
            failure_rates = metrics.get('failure_rates', {})
            path_latency = metrics.get('request_volume', {}).get('path_latency_avg', 0)
            network = analysis.get('network_analysis', {}).get('summary', {})

            # Performance recommendations
            avg_response = response_time.get('avg', 0)
            p95_response = response_time.get('p95', 0)
            if avg_response > self.thresholds['response_time']['critical']:
                recommendations.append(f"""
{critical} Critical Performance Issues:
- Current average response time: {avg_response:.2f}ms (threshold: {self.thresholds['response_time']['critical']}ms)
- P95 response time: {p95_response:.2f}ms
- Path latency: {path_latency:.2f}ms

Recommended Actions:
{bullet} Immediate server optimization required
{bullet} Review and scale server resources
{bullet} Optimize database queries and interactions
{bullet} Review application code performance
{bullet} Consider implementing caching strategies
{bullet} Monitor server resource utilization""")
            elif avg_response > self.thresholds['response_time']['warning']:
                recommendations.append(f"""
{warning} Performance Optimization Required:
- Current average response time: {avg_response:.2f}ms (threshold: {self.thresholds['response_time']['warning']}ms)
- P95 response time: {p95_response:.2f}ms
- Path latency: {path_latency:.2f}ms

Recommended Actions:
{bullet} Review application performance
{bullet} Optimize database operations
{bullet} Consider implementing caching
{bullet} Monitor resource utilization
{bullet} Review code-level optimizations""")

            # Error rate recommendations
            error_rate = failure_rates.get('error_rate', 0)
            timeout_rate = failure_rates.get('timeout_rate', 0)
            if error_rate > self.thresholds['error_rate']['critical'] or timeout_rate > self.thresholds['timeout_rate']['critical']:
                recommendations.append(f"""
{critical} Critical Error Rate Issues:
- Current error rate: {error_rate:.2f}% (threshold: {self.thresholds['error_rate']['critical']}%)
- Timeout rate: {timeout_rate:.2f}% (threshold: {self.thresholds['timeout_rate']['critical']}%)
- 4xx rate: {failure_rates.get('error_rate_4xx', 0):.2f}%
- 5xx rate: {failure_rates.get('error_rate_5xx', 0):.2f}%

Recommended Actions:
{bullet} Implement comprehensive error monitoring
{bullet} Review error logs and patterns
{bullet} Implement circuit breakers
{bullet} Add request timeout handling
{bullet} Review error handling strategy
{bullet} Set up automated alerting""")
            elif error_rate > self.thresholds['error_rate']['warning'] or timeout_rate > self.thresholds['timeout_rate']['warning']:
                recommendations.append(f"""
{warning} Error Rate Concerns:
- Current error rate: {error_rate:.2f}% (threshold: {self.thresholds['error_rate']['warning']}%)
- Timeout rate: {timeout_rate:.2f}% (threshold: {self.thresholds['timeout_rate']['warning']}%)

Recommended Actions:
{bullet} Monitor error patterns
{bullet} Review timeout configurations
{bullet} Implement error tracking
{bullet} Consider retry strategies
{bullet} Review error handling""")

            # Network path recommendations
            tiered_pct = network.get('path_distribution', {}).get('tiered_percentage', 0)
            if path_latency > 100 or tiered_pct > 80:
                recommendations.append(f"""
{warning} Network Path Optimization Required:
- Current path latency: {path_latency:.2f}ms
- Tiered requests: {tiered_pct:.1f}%

Recommended Actions:
{bullet} Review network routing configuration
{bullet} Optimize load balancer settings
{bullet} Consider direct connection paths
{bullet} Monitor network performance
{bullet} Review geographic distribution""")

            # Add maintenance recommendations if no critical issues
            if not recommendations:
                recommendations.append(f"""
{bullet} No Critical Issues Detected
Current Performance:
- Response Time: {avg_response:.2f}ms
- Error Rate: {error_rate:.2f}%
- Path Latency: {path_latency:.2f}ms

Maintenance Recommendations:
{bullet} Continue monitoring key metrics
{bullet} Implement proactive alerting
{bullet} Schedule regular performance reviews
{bullet} Document baseline performance
{bullet} Plan capacity testing
{bullet} Maintain error tracking
{bullet} Review backup strategies""")

            return "\nRecommendations\n---------------" + "\n".join(recommendations)

        except Exception as e:
            logger.error(f"Error creating recommendations: {str(e)}")
            return "\nRecommendations: Error generating recommendations"

    def _save_report(self, report: str, zone_name: str, analysis_results: Dict) -> None:
        """Save report to multiple formats with robust error handling"""
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

    def _clean_string_value(self, value: str) -> str:
        """Clean string value by removing DataFrame metadata"""
        try:
            if not isinstance(value, str):
                return str(value)
            
            value = str(value).split('\n')[0].strip()
            value = value.split('Name:')[0].strip()
            value = value.split('dtype:')[0].strip()
            
            return value
            
        except Exception as e:
            logger.error(f"Error cleaning string value: {str(e)}")
            return str(value)

    def _format_list(self, items: List[str]) -> str:
        """Format a list of items with bullets"""
        if not items:
            return f"{self.symbols['bullet']} None available"
        return "\n".join(f"{self.symbols['bullet']} {item}" for item in items)

    def _format_bandwidth(self, bytes_val: float) -> str:
        """Format bandwidth in appropriate units with proper scaling"""
        try:
            if bytes_val >= 1024**3:
                return f"{bytes_val / (1024**3):.2f} GB"
            elif bytes_val >= 1024**2:
                return f"{bytes_val / (1024**2):.2f} MB"
            else:
                return f"{bytes_val / 1024:.2f} KB"
        except Exception as e:
            logger.error(f"Error formatting bandwidth: {str(e)}")
            return "0.00 KB"

    def _format_error_rate(self, error_rate: float) -> str:
        """Format error rate with appropriate severity indicators"""
        try:
            if error_rate >= self.thresholds['error_rate']['critical']:
                return f"{self.symbols['critical']} {error_rate:.2f}%"
            elif error_rate >= self.thresholds['error_rate']['warning']:
                return f"{self.symbols['warning']} {error_rate:.2f}%"
            else:
                return f"{self.symbols['check']} {error_rate:.2f}%"
        except Exception as e:
            logger.error(f"Error formatting error rate: {str(e)}")
            return "0.00%"

    def _format_response_time(self, response_time: float) -> str:
        """Format response time with appropriate severity indicators"""
        try:
            if response_time >= self.thresholds['response_time']['critical']:
                return f"{self.symbols['critical']} {response_time:.2f}ms"
            elif response_time >= self.thresholds['response_time']['warning']:
                return f"{self.symbols['warning']} {response_time:.2f}ms"
            else:
                return f"{self.symbols['check']} {response_time:.2f}ms"
        except Exception as e:
            logger.error(f"Error formatting response time: {str(e)}")
            return "0.00ms"

    def __str__(self) -> str:
        """String representation of the reporter"""
        return f"OriginReporter(thresholds={self.thresholds})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"OriginReporter(report_dir='{self.report_dir}', "
                f"thresholds={self.thresholds})")
