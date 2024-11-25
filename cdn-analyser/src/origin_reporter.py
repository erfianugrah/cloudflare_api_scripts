from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import json
from prettytable import PrettyTable
import traceback
import pandas as pd
from .types import (
    OriginPerformanceMetrics, OriginPathMetrics, NetworkPathMetrics,
    PerformanceMetrics, ErrorMetrics, ProcessedMetrics, SamplingMetrics
)

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
            'down_arrow': "↓"
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
                self._create_recommendations(analysis)
            ]

            return "\n\n".join(filter(None, sections))

        except Exception as e:
            logger.error(f"Error creating report sections: {str(e)}")
            return "Error generating report. Please check logs for details."

    def _create_header(self, zone_name: str) -> str:
        """Create report header with metadata"""
        return f"""Origin Performance Analysis Report
================================
Zone: {zone_name}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"""

    def _create_executive_summary(self, analysis: Dict) -> str:
        """Create executive summary with status indicators"""
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

            # Create status indicators with proper thresholds
            status = {
                'response_time': (
                    f'{check} Good' if response_time.get('avg', 0) < self.thresholds['response_time']['warning']
                    else f'{critical} Critical' if response_time.get('avg', 0) >= self.thresholds['response_time']['critical']
                    else f'{warning} Attention Needed'
                ),
                'error_rate': (
                    f'{check} Good' if failure_rates.get('error_rate', 0) < self.thresholds['error_rate']['warning']
                    else f'{critical} Critical' if failure_rates.get('error_rate', 0) >= self.thresholds['error_rate']['critical']
                    else f'{warning} Attention Needed'
                ),
                'timeout': (
                    f'{check} Good' if failure_rates.get('timeout_rate', 0) < self.thresholds['timeout_rate']['warning']
                    else f'{critical} Critical' if failure_rates.get('timeout_rate', 0) >= self.thresholds['timeout_rate']['critical']
                    else f'{warning} Attention Needed'
                )
            }

            # Network path analysis
            network_analysis = analysis.get('network_analysis', {}).get('summary', {})
            path_metrics = (
                f"{bullet} Network Paths: {network_analysis.get('total_paths', 0)} total "
                f"({network_analysis.get('tiered_paths', 0)} tiered, "
                f"{network_analysis.get('direct_paths', 0)} direct)\n"
                f"{bullet} Average Path Latency: {network_analysis.get('avg_path_latency', 0):.2f}ms"
            )

            return f"""
Executive Summary
----------------
{bullet} Overall Health Status: {health_status.upper()}
{bullet} Average Origin Response Time: {response_time.get('avg', 0):.2f}ms
{bullet} 95th Percentile Response Time: {response_time.get('p95', 0):.2f}ms
{bullet} Total Origin Requests: {request_vol.get('total_requests', 0):,}
{bullet} Error Rate: {failure_rates.get('error_rate', 0):.2f}%
{bullet} Total Bandwidth Processed: {bandwidth.get('total_bytes', 0) / (1024**3):.2f} GB

Network Path Analysis:
{path_metrics}

Status Indicators:
{bullet} {status['response_time']} - Origin Response Time
{bullet} {status['error_rate']} - Error Rate
{bullet} {status['timeout']} - Timeout Rate"""

        except Exception as e:
            logger.error(f"Error creating executive summary: {str(e)}")
            return "Executive Summary: Error generating summary"

    def _create_performance_section(self, analysis: Dict) -> str:
        """Create performance analysis section with detailed metrics"""
        try:
            perf = analysis.get('overall_metrics', {}).get('response_time', {})
            temporal = analysis.get('temporal_analysis', {})
            peak_periods = temporal.get('peak_periods', {})
            bullet = self.symbols['bullet']

            # Add trend indicators
            trend_data = temporal.get('time_series', {})
            if trend_data:
                trend_values = [data['response_time'] for data in trend_data.values()]
                trend_direction = (
                    f"{self.symbols['up_arrow']} Increasing" if trend_values[-1] > trend_values[0]
                    else f"{self.symbols['down_arrow']} Decreasing"
                )
            else:
                trend_direction = "No trend data available"

            return f"""
Performance Analysis
------------------
Response Time Metrics:
{bullet} Average: {perf.get('avg', 0):.2f}ms
{bullet} Median: {perf.get('median', 0):.2f}ms
{bullet} 95th Percentile: {perf.get('p95', 0):.2f}ms
{bullet} 99th Percentile: {perf.get('p99', 0):.2f}ms
{bullet} Standard Deviation: {perf.get('std_dev', 0):.2f}ms
{bullet} Response Time Trend: {trend_direction}

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

    def _create_network_path_section(self, analysis: Dict) -> str:
        """Create network path analysis section"""
        try:
            network = analysis.get('network_analysis', {})
            paths = network.get('paths', {})
            summary = network.get('summary', {})
            bullet = self.symbols['bullet']

            if not paths:
                return f"""
Network Path Analysis
------------------
No network path data available"""

            path_table = PrettyTable()
            path_table.field_names = ["Path Type", "Requests", "Avg Latency"]
            path_table.align["Path Type"] = "l"
            path_table.align["Requests"] = "r"
            path_table.align["Avg Latency"] = "r"

            for path_id, metrics in paths.items():
                path_type = "Tiered" if metrics['upper_tier'] else "Direct"
                requests = metrics['tiered_requests'] + metrics['direct_requests']
                path_table.add_row([
                    path_type,
                    f"{requests:,}",
                    f"{metrics['path_latency']:.2f}ms"
                ])

            return f"""
Network Path Analysis
------------------
Summary:
{bullet} Total Paths: {summary.get('total_paths', 0)}
{bullet} Tiered Paths: {summary.get('tiered_paths', 0)}
{bullet} Direct Paths: {summary.get('direct_paths', 0)}
{bullet} Average Path Latency: {summary.get('avg_path_latency', 0):.2f}ms

Path Performance:
{path_table.get_string()}"""

        except Exception as e:
            logger.error(f"Error creating network path section: {str(e)}")
            return "Network Path Analysis: Error generating analysis"

    def _create_reliability_section(self, analysis: Dict) -> str:
        """Create reliability analysis section with error patterns"""
        try:
            reliability = analysis.get('overall_metrics', {}).get('failure_rates', {})
            error_analysis = analysis.get('error_analysis', {})
            error_timing = error_analysis.get('timing', {})
            error_patterns = error_analysis.get('patterns', {})
            bullet = self.symbols['bullet']

            # Create error distribution table
            error_table = self._create_status_table(error_analysis.get('error_distribution', {}))

            # Format temporal error patterns
            hourly_patterns = error_patterns.get('by_hour', {})
            high_error_hours = sorted(
                [(timestamp, data) for timestamp, data in hourly_patterns.items()],
                key=lambda x: x[1]['error_rate'],
                reverse=True
            )[:3]

            return f"""
Reliability Analysis
------------------
Error Metrics:
{bullet} Overall Error Rate: {reliability.get('error_rate', 0):.2f}%
{bullet} Timeout Rate: {reliability.get('timeout_rate', 0):.2f}%

Error Response Times:
{bullet} Average Time to Error: {error_timing.get('avg_time_to_error', 0):.2f}ms
{bullet} 50th Percentile: {error_timing.get('percentiles', {}).get('p50', 0):.2f}ms
{bullet} 95th Percentile: {error_timing.get('percentiles', {}).get('p95', 0):.2f}ms
{bullet} 99th Percentile: {error_timing.get('percentiles', {}).get('p99', 0):.2f}ms

Error Status Distribution:
{error_table}

High Error Periods:
{self._format_error_periods(high_error_hours)}"""

        except Exception as e:
            logger.error(f"Error creating reliability section: {str(e)}")
            return "Reliability Analysis: Error generating analysis"

    def _create_status_table(self, status_dist: Dict) -> str:
        """Create formatted table for status code distribution"""
        if not status_dist:
            return "No status distribution data available"

        table = PrettyTable()
        table.field_names = ["Status", "Count", "Percentage"]
        table.align = "r"
        
        for status, data in sorted(status_dist.items()):
            table.add_row([
                status,
                f"{data.get('count', 0):,}",
                f"{data.get('percentage', 0):.2f}%"
            ])

        return table.get_string()

    def _format_error_periods(self, periods: List[Tuple[str, Dict]]) -> str:
        """Format high error periods for display"""
        if not periods:
            return "No significant error periods detected"

        bullet = self.symbols['bullet']
        return "\n".join(
            f"{bullet} {timestamp}:"
            f"  - Error Rate: {data['error_rate']:.2f}%"
            f"  - Requests: {data['request_count']:,}"
            for timestamp, data in periods
        )

    def _create_geographic_section(self, analysis: Dict) -> str:
        """Create geographic analysis section with regional performance"""
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
            perf_table.field_names = ["Region", "Avg Response", "Error Rate", "Requests"]
            perf_table.align = "r"
            perf_table.align["Region"] = "l"

            for country, metrics in sorted(
                countries.items(),
                key=lambda x: x[1]['performance']['avg_response_time']
            )[:5]:
                perf_table.add_row([
                    country,
                    f"{metrics['performance']['avg_response_time']:.2f}ms",
                    f"{(metrics['reliability']['error_rate_4xx'] + metrics['reliability']['error_rate_5xx']):.2f}%",
                    f"{metrics['traffic']['requests']:,}"
                ])

            return f"""
Geographic Analysis
-----------------
Best Performing Regions:
{perf_table.get_string()}

Regional Issues:
{bullet} Slowest Regions: {', '.join(summary.get('slowest_regions', ['None']))}
{bullet} Highest Error Rates: {', '.join(summary.get('highest_error_regions', ['None']))}

Performance by Region Details:
{self._create_region_details(countries)}"""

        except Exception as e:
            logger.error(f"Error creating geographic section: {str(e)}")
            return "Geographic Analysis: Error generating analysis"

    def _create_region_details(self, countries: Dict) -> str:
        """Create detailed region-by-region analysis"""
        try:
            details = []
            bullet = self.symbols['bullet']
            
            for country, metrics in sorted(
                countries.items(),
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )[:10]:  # Top 10 by traffic
                perf = metrics['performance']
                traffic = metrics['traffic']
                reliability = metrics['reliability']
                
                details.append(f"""
{bullet} {country}:
  - Response Time: {perf['avg_response_time']:.2f}ms (P95: {perf['p95_response_time']:.2f}ms)
  - Requests: {traffic['requests']:,}
  - Error Rate: {(reliability['error_rate_4xx'] + reliability['error_rate_5xx']):.2f}%
  - Performance Score: {metrics['perf_score']:.2f}""")
                
            return "\n".join(details)
        except Exception as e:
            logger.error(f"Error creating region details: {str(e)}")
            return "Error generating region details"

    def _create_endpoint_section(self, analysis: Dict) -> str:
        """Create endpoint analysis section with performance metrics"""
        try:
            endpoint_analysis = analysis.get('endpoint_analysis', {})
            endpoints = endpoint_analysis.get('endpoints', {})
            summary = endpoint_analysis.get('summary', {})
            bullet = self.symbols['bullet']
            warning = self.symbols['warning']

            if not endpoints:
                return "Endpoint Analysis\n---------------\nNo endpoint data available"

            # Create endpoint performance table
            perf_table = PrettyTable()
            perf_table.field_names = ["Endpoint", "Avg Response", "Requests", "Error Rate"]
            perf_table.align["Endpoint"] = "l"
            perf_table.align["Avg Response"] = "r"
            perf_table.align["Requests"] = "r"
            perf_table.align["Error Rate"] = "r"

            # Add top impacting endpoints
            for endpoint in summary.get('high_impact_endpoints', [])[:5]:
                metrics = endpoints.get(endpoint, {})
                perf = metrics.get('performance', {})
                load = metrics.get('load', {})
                reliability = metrics.get('reliability', {})
                
                perf_table.add_row([
                    endpoint[:50] + "..." if len(endpoint) > 50 else endpoint,
                    f"{perf['avg_response_time']:.2f}ms",
                    f"{load['requests']:,}",
                    f"{(reliability['error_rate_4xx'] + reliability['error_rate_5xx']):.2f}%"
                ])

            # Add problematic endpoints section
            problematic = summary.get('problematic_endpoints', [])
            problematic_section = "\nProblematic Endpoints:" if problematic else ""
            for endpoint in problematic:
                metrics = endpoints.get(endpoint, {})
                perf = metrics.get('performance', {})
                reliability = metrics.get('reliability', {})
                
                problematic_section += f"""
{warning} {endpoint}
  - Response Time: {perf['avg_response_time']:.2f}ms
  - Error Rate: {(reliability['error_rate_4xx'] + reliability['error_rate_5xx']):.2f}%
  - Reliability Score: {reliability['reliability_score']:.2f}"""

            return f"""
Endpoint Analysis
---------------
Summary:
{bullet} Total Endpoints: {summary.get('total_endpoints', 0)}
{bullet} Average Response Time: {summary.get('avg_response_time', 0):.2f}ms

High Impact Endpoints:
{perf_table.get_string()}{problematic_section}"""

        except Exception as e:
            logger.error(f"Error creating endpoint section: {str(e)}")
            return "Endpoint Analysis: Error generating analysis"

    def _create_recommendations(self, analysis: Dict) -> str:
        """Create actionable recommendations based on analysis results"""
        try:
            recommendations = []
            bullet = self.symbols['bullet']
            warning = self.symbols['warning']
            critical = self.symbols['critical']

            # Extract key metrics
            metrics = analysis.get('overall_metrics', {})
            error_analysis = analysis.get('error_analysis', {})
            network_analysis = analysis.get('network_analysis', {})
            
            # Response time recommendations
            response_time = metrics.get('response_time', {}).get('avg', 0)
            if response_time > self.thresholds['response_time']['critical']:
                recommendations.append(f"""
{critical} Critical Performance Issues
- Current average response time: {response_time:.2f}ms (threshold: {self.thresholds['response_time']['critical']}ms)
- Actions:
    {bullet} Immediate origin server optimization required
    {bullet} Review server resources and scaling
    {bullet} Check database query performance
    {bullet} Implement aggressive caching
    {bullet} Consider CDN configuration optimization""")
            elif response_time > self.thresholds['response_time']['warning']:
                recommendations.append(f"""
{warning} Performance Optimization Required
- Current average response time: {response_time:.2f}ms (threshold: {self.thresholds['response_time']['warning']}ms)
- Actions:
    {bullet} Review origin server configuration
    {bullet} Optimize application code
    {bullet} Consider implementing edge computing
    {bullet} Monitor server resources""")

            # Error rate recommendations
            error_rates = metrics.get('failure_rates', {})
            error_rate = error_rates.get('error_rate', 0)
            timeout_rate = error_rates.get('timeout_rate', 0)
            
            if error_rate > self.thresholds['error_rate']['warning']:
                recommendations.append(f"""
{warning} High Error Rate Detected
- Current error rate: {error_rate:.2f}% (threshold: {self.thresholds['error_rate']['warning']}%)
- Actions:
    {bullet} Review server error logs
    {bullet} Implement circuit breakers
    {bullet} Set up automated error monitoring
    {bullet} Review error handling logic""")

            # Network recommendations
            network_summary = network_analysis.get('summary', {})
            if network_summary.get('tiered_paths', 0) > 0:
                avg_latency = network_summary.get('avg_path_latency', 0)
                if avg_latency > 200:  # 200ms threshold for network latency
                    recommendations.append(f"""
{warning} High Network Latency
- Current average path latency: {avg_latency:.2f}ms
- Actions:
    {bullet} Review network path configuration
    {bullet} Consider additional edge locations
    {bullet} Optimize routing configuration
    {bullet} Monitor network performance""")

            # Health check recommendations
            health_status = metrics.get('health_status', 'unknown')
            if health_status.lower() == 'critical':
                recommendations.append(f"""
{critical} Critical Health Status
- Actions:
    {bullet} Implement immediate health checks
    {bullet} Set up automated alerts
    {bullet} Review system capacity
    {bullet} Prepare incident response plan""")

            # If no issues detected, provide maintenance recommendations
            if not recommendations:
                recommendations.append(f"""
{bullet} No Critical Issues Detected
- Current Performance:
    {bullet} Response Time: {response_time:.2f}ms
    {bullet} Error Rate: {error_rate:.2f}%
    {bullet} Timeout Rate: {timeout_rate:.2f}%
    
- Recommendations for Maintenance:
    {bullet} Continue monitoring key metrics
    {bullet} Implement proactive alerting
    {bullet} Schedule regular performance reviews
    {bullet} Document baseline performance""")

            return "\n".join(recommendations)

        except Exception as e:
            logger.error(f"Error creating recommendations: {str(e)}")
            logger.error(traceback.format_exc())
            return "Error generating recommendations"
        
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
                self._create_recommendations(analysis)
            ]

            # Filter out None sections and join with double newlines
            return "\n\n".join(filter(None, sections))

        except Exception as e:
            logger.error(f"Error creating report sections: {str(e)}")
            return "Error generating report. Please check logs for details."
        
    def _save_report(self, report: str, zone_name: str, analysis_results: Dict) -> None:
        """Save report to multiple formats with proper error handling"""
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

    def _format_list(self, items: List[str]) -> str:
        """Format a list of items with bullets"""
        if not items:
            return f"{self.symbols['bullet']} None available"
        return "\n".join(f"{self.symbols['bullet']} {item}" for item in items)
