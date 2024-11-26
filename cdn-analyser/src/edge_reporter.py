from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import json
from prettytable import PrettyTable
import traceback
import pandas as pd
from .types import (
    NetworkPathMetrics, PerformanceMetrics, ErrorMetrics, ProcessedMetrics, SamplingMetrics
)

logger = logging.getLogger(__name__)

class EdgeReporter:
    """Reporter for edge/CDN performance analysis"""
    
    def __init__(self, config):
        self.config = config
        self.report_dir = self.config.reports_dir / 'edge'
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        self.thresholds = {
            'ttfb': {
                'good': 100,     # ms
                'warning': 200,  # ms
                'critical': 500  # ms
            },
            'error_rate': {
                'good': 1,     # %
                'warning': 5,  # %
                'critical': 10 # %
            },
            'cache_hit': {
                'good': 80,    # %
                'warning': 60,  # %
                'critical': 40  # %
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

    def generate_edge_report(
        self,
        df: pd.DataFrame,
        analysis_results: Dict,
        zone_name: str
    ) -> Optional[str]:
        """Generate comprehensive edge/CDN performance report"""
        try:
            if df is None or df.empty or not analysis_results:
                logger.error("No data available for edge report generation")
                return None

            logger.info(f"""
Edge Report Generation Starting:
---------------------------
Zone: {zone_name}
Analysis Results Keys: {list(analysis_results.keys())}
DataFrame Shape: {df.shape}
""")

            report = self._create_report_sections(df, analysis_results, zone_name)
            self._save_report(report, zone_name, analysis_results)
            return report

        except Exception as e:
            logger.error(f"Error generating edge report: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _create_report_sections(
        self,
        df: pd.DataFrame,
        analysis: Dict,
        zone_name: str
    ) -> str:
        """Create detailed edge performance report sections"""
        try:
            sections = [
                self._create_header(zone_name),
                self._create_executive_summary(analysis),
                self._create_edge_performance_section(analysis),
                self._create_cache_section(analysis),
                self._create_reliability_section(analysis),
                self._create_geographic_section(analysis),
                self._create_protocol_section(analysis),
                self._create_recommendations(analysis)
            ]

            return "\n\n".join(filter(None, sections))

        except Exception as e:
            logger.error(f"Error creating report sections: {str(e)}")
            return "Error generating edge performance report. Please check logs for details."

    def _create_header(self, zone_name: str) -> str:
        """Create report header with metadata"""
        return f"""Edge Performance Analysis Report
================================
Zone: {zone_name}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"""

    def _create_executive_summary(self, analysis: Dict) -> str:
        """Create executive summary with edge performance indicators"""
        try:
            metrics = analysis.get('edge_metrics', {})
            cache_metrics = analysis.get('cache_metrics', {})
            failure_rates = metrics.get('error_rates', {})
            request_vol = metrics.get('request_metrics', {})
            bandwidth = metrics.get('bandwidth', {})
            health_status = metrics.get('health_status', 'unknown')

            bullet = self.symbols['bullet']
            check = self.symbols['check']
            warning = self.symbols['warning']
            critical = self.symbols['critical']

            # Create status indicators
            status = {
                'ttfb': (
                    f'{check} Good' if metrics.get('edge_response_time', {}).get('avg', 0) < self.thresholds['ttfb']['warning']
                    else f'{critical} Critical' if metrics.get('edge_response_time', {}).get('avg', 0) >= self.thresholds['ttfb']['critical']
                    else f'{warning} Attention Needed'
                ),
                'cache': (
                    f'{check} Good' if cache_metrics.get('overall', {}).get('hit_ratio', 0) >= self.thresholds['cache_hit']['good']
                    else f'{critical} Critical' if cache_metrics.get('overall', {}).get('hit_ratio', 0) < self.thresholds['cache_hit']['critical']
                    else f'{warning} Attention Needed'
                ),
                'error_rate': (
                    f'{check} Good' if failure_rates.get('total_error_rate', 0) < self.thresholds['error_rate']['warning']
                    else f'{critical} Critical' if failure_rates.get('total_error_rate', 0) >= self.thresholds['error_rate']['critical']
                    else f'{warning} Attention Needed'
                )
            }

            # Protocol analysis
            protocol_analysis = analysis.get('protocol_metrics', {}).get('summary', {})
            protocol_metrics = (
                f"{bullet} Protocol Distribution:\n"
                f"  - HTTP/2: {protocol_analysis.get('http2_percentage', 0):.1f}%\n"
                f"  - HTTP/3: {protocol_analysis.get('http3_percentage', 0):.1f}%\n"
                f"  - HTTP/1.x: {protocol_analysis.get('http1_percentage', 0):.1f}%"
            )

            content_mix = cache_metrics.get('overall', {}).get('content_mix', {})
            cache_status = "NORMAL - Mostly Dynamic Content" if content_mix.get('dynamic_percentage', 0) > 80 else status['cache']

            return f"""
Executive Summary
----------------
{bullet} Overall Edge Health Status: {health_status.upper()}

{bullet} Edge Performance:
  - Average TTFB: {metrics.get('edge_response_time', {}).get('avg', 0):.2f}ms
  - P95 TTFB: {metrics.get('edge_response_time', {}).get('p95', 0):.2f}ms
  - Total Requests: {request_vol.get('total', 0):,}
  - Bandwidth Served: {bandwidth.get('total', 0) / (1024**3):.2f} GB

{bullet} Cache Performance:
  - Hit Ratio: {cache_metrics.get('overall', {}).get('hit_ratio', 0):.2f}%
  - Bandwidth Savings: {cache_metrics.get('overall', {}).get('bandwidth_saving', 0):.2f}%
  - Content Mix: {content_mix.get('static_percentage', 0):.1f}% static, {content_mix.get('dynamic_percentage', 0):.1f}% dynamic

{bullet} Error Rates:
  - 4xx Errors: {failure_rates.get('error_rate_4xx', 0):.2f}%
  - 5xx Errors: {failure_rates.get('error_rate_5xx', 0):.2f}%

{protocol_metrics}

Status Indicators:
{bullet} {status['ttfb']} - Edge Response Time
{bullet} {cache_status} - Cache Performance
{bullet} {status['error_rate']} - Error Rate"""

        except Exception as e:
            logger.error(f"Error creating executive summary: {str(e)}")
            return "Executive Summary: Error generating summary"

    def _create_edge_performance_section(self, analysis: Dict) -> str:
        """Create edge performance analysis section"""
        try:
            perf = analysis.get('edge_metrics', {}).get('edge_response_time', {})
            temporal = analysis.get('temporal_analysis', {})
            peak_periods = temporal.get('peak_periods', {})
            bullet = self.symbols['bullet']

            # Add trend indicators
            trend_data = temporal.get('time_series', {})
            if trend_data:
                trend_values = [
                    data.get('performance', {}).get('ttfb', 0) 
                    for data in trend_data.values()
                ]
                trend_direction = (
                    f"{self.symbols['up_arrow']} Increasing" 
                    if trend_values[-1] > trend_values[0]
                    else f"{self.symbols['down_arrow']} Decreasing"
                )
            else:
                trend_direction = "No trend data available"

            # Calculate protocol impact
            protocol_metrics = analysis.get('protocol_metrics', {}).get('protocols', {})
            protocol_comparison = "\n".join(
                f"  - {protocol}: {metrics.get('performance', {}).get('avg_ttfb', 0):.2f}ms "
                f"(P95: {metrics.get('performance', {}).get('p95_ttfb', 0):.2f}ms)"
                for protocol, metrics in protocol_metrics.items()
            )

            return f"""
Edge Performance Analysis
----------------------
Response Time Metrics:
{bullet} Basic Metrics:
  - Average TTFB: {perf.get('avg', 0):.2f}ms
  - Median TTFB: {perf.get('median', 0):.2f}ms
  - P95 TTFB: {perf.get('p95', 0):.2f}ms
  - P99 TTFB: {perf.get('p99', 0):.2f}ms
  - Standard Deviation: {perf.get('std', 0):.2f}ms
  - TTFB Trend: {trend_direction}

{bullet} Protocol Performance:
{protocol_comparison if protocol_comparison else '  - No protocol performance data available'}

{bullet} Peak Performance:
  - Busiest Period: {peak_periods.get('highest_load', {}).get('timestamp', 'Unknown')}
    • Requests: {peak_periods.get('highest_load', {}).get('requests', 0):,}
    • TTFB: {peak_periods.get('highest_load', {}).get('ttfb', 0):.2f}ms
  
  - Slowest Period: {peak_periods.get('worst_edge_latency', {}).get('timestamp', 'Unknown')}
    • TTFB: {peak_periods.get('worst_edge_latency', {}).get('ttfb', 0):.2f}ms
    • Requests: {peak_periods.get('worst_edge_latency', {}).get('requests', 0):,}"""

        except Exception as e:
            logger.error(f"Error creating edge performance section: {str(e)}")
            logger.error(traceback.format_exc())
            return "Edge Performance Analysis: Error generating analysis"

    def _create_cache_section(self, analysis: Dict) -> str:
        """Create cache performance analysis section"""
        try:
            cache = analysis.get('cache_metrics', {})
            overall = cache.get('overall', {})
            status_dist = cache.get('status_distribution', {})
            content_mix = overall.get('content_mix', {})
            bullet = self.symbols['bullet']

            # Determine if this is primarily dynamic content
            is_mostly_dynamic = content_mix.get('dynamic_percentage', 0) > 80
            cache_context = "Dynamic Content Workload" if is_mostly_dynamic else "Mixed Content Workload"

            # Create cache status table
            cache_table = PrettyTable()
            cache_table.field_names = ["Cache Status", "Requests %", "Bandwidth %", "Avg TTFB"]
            cache_table.align["Cache Status"] = "l"
            cache_table.align["Requests %"] = "r"
            cache_table.align["Bandwidth %"] = "r"
            cache_table.align["Avg TTFB"] = "r"

            for status, metrics in status_dist.items():
                if not isinstance(metrics, dict):
                    continue
                cache_table.add_row([
                    status,
                    f"{metrics.get('requests_percentage', 0):.2f}%",
                    f"{metrics.get('bytes_percentage', 0):.2f}%",
                    f"{metrics.get('avg_ttfb', 0):.2f}ms"
                ])

            # Get performance impact metrics with safe access
            perf_impact = cache.get('edge_performance_impact', {})
            if not isinstance(perf_impact, dict):
                perf_impact = {}

            # Add content type analysis if available
            content_analysis = cache.get('content_type_analysis', {})
            content_type_section = self._format_content_type_analysis(content_analysis) if content_analysis else ""

            return f"""
Cache Performance Analysis
-----------------------
{bullet} Overall Metrics:
  - Hit Ratio: {overall.get('hit_ratio', 0):.2f}%
  - Static Content Hit Ratio: {overall.get('static_hit_ratio', 0):.2f}%
  - Dynamic Content Hit Ratio: {overall.get('dynamic_hit_ratio', 0):.2f}%
  - Bandwidth Savings: {overall.get('bandwidth_saving', 0):.2f}%
  - Total Bandwidth Served: {overall.get('total_bytes', 0) / (1024**3):.2f} GB
  - Workload Type: {cache_context}

{bullet} Cache Impact on Edge Performance:
  - Cache Hit TTFB: {perf_impact.get('cache_hit_ttfb', 0):.2f}ms
  - Cache Miss TTFB: {perf_impact.get('cache_miss_ttfb', 0):.2f}ms
  - Cache Hit P95 TTFB: {perf_impact.get('cache_hit_ttfb_p95', 0):.2f}ms
  - Cache Miss P95 TTFB: {perf_impact.get('cache_miss_ttfb_p95', 0):.2f}ms

{bullet} Cache Status Distribution:
{cache_table.get_string()}

{content_type_section}"""

        except Exception as e:
            logger.error(f"Error creating cache section: {str(e)}")
            return "Cache Performance Analysis: Error generating analysis"

    def _format_content_type_analysis(self, content_types: Dict) -> str:
        """Format content type analysis section"""
        try:
            if not content_types:
                return ""

            content_table = PrettyTable()
            content_table.field_names = ["Content Type", "Requests %", "Hit Ratio", "Category"]
            content_table.align["Content Type"] = "l"
            content_table.align["Requests %"] = "r"
            content_table.align["Hit Ratio"] = "r"
            content_table.align["Category"] = "l"

            for content_type, metrics in sorted(
                content_types.items(),
                key=lambda x: x[1].get('percentage', 0),
                reverse=True
            ):
                if isinstance(metrics, dict):
                    content_table.add_row([
                        content_type[:40] + "..." if len(content_type) > 40 else content_type,
                        f"{metrics.get('percentage', 0):.2f}%",
                        f"{metrics.get('hit_ratio', 0):.2f}%",
                        metrics.get('type', 'unknown').upper()
                    ])

            return f"\nContent Type Analysis:\n{content_table.get_string()}"

        except Exception as e:
            logger.error(f"Error formatting content type analysis: {str(e)}")
            return ""

    def _create_reliability_section(self, analysis: Dict) -> str:
        """Create reliability analysis section focusing on edge errors"""
        try:
            metrics = analysis.get('edge_metrics', {})
            error_rates = metrics.get('error_rates', {})
            error_timing = analysis.get('error_timing', {})
            error_dist = analysis.get('error_distribution', {})
            bullet = self.symbols['bullet']

            # Create error distribution table
            error_table = PrettyTable()
            error_table.field_names = ["Status", "Requests", "Percentage", "Avg TTFB"]
            error_table.align["Status"] = "r"
            error_table.align["Requests"] = "r"
            error_table.align["Percentage"] = "r"
            error_table.align["Avg TTFB"] = "r"

            for status, data in sorted(error_dist.items()):
                if isinstance(data, dict):
                    error_table.add_row([
                        status,
                        f"{data.get('count', 0):,}",
                        f"{data.get('percentage', 0):.2f}%",
                        f"{data.get('avg_ttfb', 0):.2f}ms"
                    ])

            # Add error trends analysis
            error_trends = analysis.get('temporal_analysis', {}).get('error_trends', {})
            peak_error_time = max(error_trends.items(), key=lambda x: x[1]['total_error_rate'])[0] if error_trends else "Unknown"
            peak_error_rate = max([t['total_error_rate'] for t in error_trends.values()]) if error_trends else 0

            return f"""
Reliability Analysis
------------------
{bullet} Error Rates:
  - 4xx Error Rate: {error_rates.get('error_rate_4xx', 0):.2f}%
  - 5xx Error Rate: {error_rates.get('error_rate_5xx', 0):.2f}%
  - Total Error Rate: {error_rates.get('total_error_rate', 0):.2f}%

{bullet} Edge Error Response Times:
  - Average TTFB: {error_timing.get('edge', {}).get('avg_ttfb', 0):.2f}ms
  - P95 TTFB: {error_timing.get('edge', {}).get('p95_ttfb', 0):.2f}ms
  - P99 TTFB: {error_timing.get('edge', {}).get('p99_ttfb', 0):.2f}ms

{bullet} Error Patterns:
  - Peak Error Time: {peak_error_time}
  - Peak Error Rate: {peak_error_rate:.2f}%

{bullet} Error Status Distribution:
{error_table.get_string()}"""

        except Exception as e:
            logger.error(f"Error creating reliability section: {str(e)}")
            return "Reliability Analysis: Error generating analysis"

    def _create_geographic_section(self, analysis: Dict) -> str:
        """Create geographic analysis section with proper data cleaning"""
        try:
            geo = analysis.get('geographic_metrics', {})
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
            perf_table.field_names = ["Region", "Avg TTFB", "Error Rate", "Requests"]
            perf_table.align["Region"] = "l"
            for align in ["Avg TTFB", "Error Rate", "Requests"]:
                perf_table.align[align] = "r"

            # Get top 10 countries by request volume
            top_countries = sorted(
                [(k, v) for k, v in countries.items() if isinstance(v, dict)],
                key=lambda x: x[1].get('traffic', {}).get('requests', 0),
                reverse=True
            )[:10]

            for country, metrics in top_countries:
                # Clean country name
                country = self._clean_country_name(country)
                traffic = metrics.get('traffic', {})
                performance = metrics.get('performance', {})
                reliability = metrics.get('reliability', {})

                # Calculate total error rate
                error_rate = (
                    reliability.get('error_rate_4xx', 0) + 
                    reliability.get('error_rate_5xx', 0)
                )

                perf_table.add_row([
                    country,
                    f"{performance.get('ttfb_avg', 0):.2f}ms",
                    f"{error_rate:.2f}%",
                    f"{traffic.get('requests', 0):,}"
                ])

            # Clean and format summary lists
            slowest_regions = [self._clean_country_name(r) for r in summary.get('slowest_edge', [])]
            highest_errors = [self._clean_country_name(r) for r in summary.get('highest_errors', [])]
            
            # Remove duplicates while preserving order
            slowest_regions = list(dict.fromkeys(slowest_regions))
            highest_errors = list(dict.fromkeys(highest_errors))

            return f"""
Geographic Analysis
-----------------
Performance by Region:
{perf_table.get_string()}

Regional Issues:
{bullet} Slowest Regions: {', '.join(slowest_regions) if slowest_regions else 'None'}
{bullet} Highest Error Rates: {', '.join(highest_errors) if highest_errors else 'None'}"""

            
        except Exception as e:
            logger.error(f"Error creating geographic section: {str(e)}")
            return "Geographic Analysis: Error generating analysis"

    def _clean_country_name(self, country: str) -> str:
        """Clean country name from DataFrame metadata"""
        try:
            if not isinstance(country, str):
                return str(country)
                
            # Remove pandas Series metadata
            country = country.split('\n')[0].strip()
            country = country.split('Name:')[0].strip()
            country = country.split('dtype:')[0].strip()
            
            # Additional cleanup
            country = country.replace('object', '').strip()
            country = ' '.join(country.split())  # Normalize whitespace
            
            return country
            
        except Exception as e:
            logger.error(f"Error cleaning country name: {str(e)}")
            return str(country)

    def _create_protocol_section(self, analysis: Dict) -> str:
        """Create protocol analysis section with robust error handling"""
        try:
            protocol_metrics = analysis.get('protocol_metrics', {})
            protocols = protocol_metrics.get('protocols', {})
            summary = protocol_metrics.get('summary', {})
            bullet = self.symbols['bullet']

            if not protocols:
                return f"""
    Protocol Analysis
    ---------------
    No protocol data available or all protocols filtered out."""

            # Create protocol performance table with null handling
            protocol_table = PrettyTable()
            protocol_table.field_names = ["Protocol", "Requests %", "Avg TTFB", "P95 TTFB", "Cache Hit"]
            protocol_table.align["Protocol"] = "l"
            protocol_table.align["Requests %"] = "r"
            protocol_table.align["Avg TTFB"] = "r"
            protocol_table.align["P95 TTFB"] = "r"
            protocol_table.align["Cache Hit"] = "r"

            for protocol, metrics in protocols.items():
                # Handle missing or invalid perf metrics
                perf = metrics.get('performance', {})
                if not isinstance(perf, dict):
                    perf = {}

                protocol_table.add_row([
                    protocol,
                    f"{metrics.get('requests_percentage', 0):.1f}%",
                    f"{perf.get('avg_ttfb', 0):.2f}ms",
                    f"{perf.get('p95_ttfb', 0):.2f}ms",
                    f"{metrics.get('cache_hit_ratio', 0):.1f}%"
                ])

            # Get protocol distribution with safe defaults
            distribution = {
                'http2': summary.get('http2_percentage', 0),
                'http3': summary.get('http3_percentage', 0),
                'http1': summary.get('http1_percentage', 0)
            }

            # Add protocol comparisons if available
            comparisons = summary.get('comparisons', {})
            comparison_text = self._format_protocol_comparisons(comparisons) if comparisons else ""

            return f"""
    Protocol Analysis
    ---------------
    {bullet} Protocol Distribution:
      - HTTP/2: {distribution['http2']:.1f}%
      - HTTP/3: {distribution['http3']:.1f}%
      - HTTP/1.x: {distribution['http1']:.1f}%

    {bullet} Performance by Protocol:
    {protocol_table.get_string()}

    {bullet} Protocol Trends:
      - Fastest Protocol: {summary.get('fastest_protocol', 'Unknown')}
      - Most Reliable: {summary.get('most_reliable_protocol', 'Unknown')}
      - Best Cache Performance: {summary.get('best_cache_protocol', 'Unknown')}
      
    {comparison_text}"""

        except Exception as e:
            logger.error(f"Error creating protocol section: {str(e)}")
            logger.error(traceback.format_exc())
            return "Protocol Analysis: Error generating analysis"

    def _format_protocol_comparisons(self, comparisons: Dict) -> str:
        """Format protocol comparison data"""
        try:
            if not comparisons:
                return ""

            comparison_lines = []
            bullet = self.symbols['bullet']
            
            for comparison_key, data in comparisons.items():
                protocols = comparison_key.split('_vs_')
                if len(protocols) != 2:
                    continue
                    
                ttfb_diff = data.get('ttfb_difference', 0)
                error_diff = data.get('error_rate_difference', 0)
                cache_diff = data.get('cache_difference', 0)
                
                comparison_lines.append(f"""
    {bullet} {protocols[0]} vs {protocols[1]}:
      - Response Time: {abs(ttfb_diff):.2f}ms faster on {data.get('performance_winner', 'Unknown')}
      - Error Rate: {abs(error_diff):.2f}% lower on {data.get('reliability_winner', 'Unknown')}
      - Cache Performance: {abs(cache_diff):.2f}% better on {data.get('cache_winner', 'Unknown')}""")

            if comparison_lines:
                return "\nProtocol Comparisons:" + "".join(comparison_lines)
            return ""

        except Exception as e:
            logger.error(f"Error formatting protocol comparisons: {str(e)}")
            return ""

    def _create_recommendations(self, analysis: Dict) -> str:
        """Create actionable recommendations based on edge performance"""
        try:
            recommendations = []
            bullet = self.symbols['bullet']
            warning = self.symbols['warning']
            
            metrics = analysis.get('edge_metrics', {})
            cache_metrics = analysis.get('cache_metrics', {})
            protocol_metrics = analysis.get('protocol_metrics', {})
            content_mix = cache_metrics.get('overall', {}).get('content_mix', {})

            # Edge performance recommendations
            edge_ttfb = metrics.get('edge_response_time', {}).get('avg', 0)
            p95_ttfb = metrics.get('edge_response_time', {}).get('p95', 0)
            if edge_ttfb > self.thresholds['ttfb']['warning']:
                recommendations.append(f"""
    {warning} Edge Performance Optimization Required
    - Current average TTFB: {edge_ttfb:.1f}ms
    - P95 TTFB: {p95_ttfb:.1f}ms
    Actions:
        {bullet} Review Cloudflare configuration
        {bullet} Consider enabling Argo Smart Routing
        {bullet} Optimize request routing
        {bullet} Review browser cache settings""")

            # Cache performance recommendations
            hit_ratio = cache_metrics.get('overall', {}).get('hit_ratio', 0)
            is_dynamic = content_mix.get('dynamic_percentage', 0) > 80
            
            if not is_dynamic and hit_ratio < self.thresholds['cache_hit']['warning']:
                recommendations.append(f"""
    {warning} Cache Optimization Required
    - Current hit ratio: {hit_ratio:.1f}%
    Actions:
        {bullet} Review cache configuration rules
        {bullet} Implement browser cache control headers
        {bullet} Consider increasing edge cache TTLs
        {bullet} Review cache bypass conditions""")
            elif is_dynamic and hit_ratio < 2:  # Lower threshold for dynamic content
                recommendations.append(f"""
    {warning} Dynamic Content Caching Opportunities
    - Current hit ratio: {hit_ratio:.1f}%
    Actions:
        {bullet} Review API response caching potential
        {bullet} Implement cache-control headers for semi-dynamic content
        {bullet} Consider implementing stale-while-revalidate
        {bullet} Evaluate edge computing options""")

            # Protocol recommendations
            http2_pct = protocol_metrics.get('summary', {}).get('http2_percentage', 0)
            http3_pct = protocol_metrics.get('summary', {}).get('http3_percentage', 0)
            if http2_pct + http3_pct < 80:
                recommendations.append(f"""
    {warning} Protocol Optimization Recommended
    - Current HTTP/2+HTTP/3 usage: {http2_pct + http3_pct:.1f}%
    Actions:
        {bullet} Enable HTTP/3 (QUIC) if not enabled
        {bullet} Review TLS configuration
        {bullet} Update origin server protocols
        {bullet} Monitor client protocol support""")

            if not recommendations:
                recommendations.append(f"""
    {bullet} No Critical Edge Issues Detected
    Maintenance Recommendations:
        {bullet} Continue monitoring edge metrics
        {bullet} Regular cache configuration review
        {bullet} Monitor protocol distribution
        {bullet} Track geographic performance""")

            return "\n".join(recommendations)

        except Exception as e:
            logger.error(f"Error creating recommendations: {str(e)}")
            return "Error generating recommendations"

    def _save_report(self, report: str, zone_name: str, analysis_results: Dict) -> None:
        """Save report to multiple formats with proper error handling"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_path = self.report_dir / f"edge_analysis_{zone_name}_{timestamp}"
            
            # Save as text
            with open(f"{base_path}.txt", 'w', encoding='utf-8') as f:
                f.write(report)
            
            # Save as markdown
            with open(f"{base_path}.md", 'w', encoding='utf-8') as f:
                f.write(report)
            
            # Save raw analysis results
            with open(f"{base_path}_raw.json", 'w', encoding='utf-8') as f:
                json.dump(analysis_results, f, indent=2, default=str)
            
            logger.info(f"Edge analysis report saved to {base_path}")
            
        except Exception as e:
            logger.error(f"Error saving report: {str(e)}")
            logger.error(traceback.format_exc())

    def _format_list(self, items: List[str]) -> str:
        """Format a list of items with bullets"""
        if not items:
            return f"{self.symbols['bullet']} None available"
        return "\n".join(f"{self.symbols['bullet']} {item}" for item in items)

    def _validate_metrics(self, metrics: Dict) -> bool:
        """Validate metrics data structure"""
        try:
            required_sections = [
                'edge_metrics', 'cache_metrics', 'protocol_metrics',
                'geographic_metrics', 'temporal_analysis', 'error_analysis'
            ]
            
            if not all(section in metrics for section in required_sections):
                missing = [s for s in required_sections if s not in metrics]
                logger.error(f"Missing required sections in metrics: {missing}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating metrics: {str(e)}")
            return False

    def _format_bandwidth(self, bytes_val: float) -> str:
        """Format bandwidth in appropriate units"""
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

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format"""
        try:
            if seconds < 60:
                return f"{seconds:.1f}s"
            elif seconds < 3600:
                minutes = seconds / 60
                return f"{minutes:.1f}m"
            else:
                hours = seconds / 3600
                return f"{hours:.1f}h"
        except Exception as e:
            logger.error(f"Error formatting duration: {str(e)}")
            return "0.0s"

    def _format_percentage(self, value: float, total: float) -> str:
        """Format percentage with proper handling of zero division"""
        try:
            if total <= 0:
                return "0.00%"
            return f"{(value / total * 100):.2f}%"
        except Exception as e:
            logger.error(f"Error formatting percentage: {str(e)}")
            return "0.00%"

    def _format_error_rate(self, errors: int, total: int) -> str:
        """Format error rate with threshold-based coloring"""
        try:
            if total <= 0:
                return "0.00%"
                
            error_rate = (errors / total) * 100
            if error_rate >= self.thresholds['error_rate']['critical']:
                return f"{self.symbols['critical']} {error_rate:.2f}%"
            elif error_rate >= self.thresholds['error_rate']['warning']:
                return f"{self.symbols['warning']} {error_rate:.2f}%"
            return f"{error_rate:.2f}%"
            
        except Exception as e:
            logger.error(f"Error formatting error rate: {str(e)}")
            return "0.00%"

    def _safe_series_value(self, series: pd.Series, default: Any = 0) -> Any:
        """Safely get value from a Series without deprecation warning"""
        try:
            if isinstance(series, pd.Series):
                if len(series) > 0:
                    return series.iloc[0]
                return default
            return series
        except Exception:
            return default

    def _clean_string_value(self, value: str) -> str:
        """Clean string value by removing DataFrame metadata"""
        try:
            if not isinstance(value, str):
                return str(value)
            
            # Remove DataFrame metadata
            value = value.split('\n')[0].strip()
            value = value.split('Name:')[0].strip()
            value = value.split('dtype:')[0].strip()
            
            return value
            
        except Exception as e:
            logger.error(f"Error cleaning string value: {str(e)}")
            return str(value)

    def __str__(self) -> str:
        """String representation of EdgeReporter"""
        return f"EdgeReporter(thresholds={self.thresholds})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"EdgeReporter(report_dir={self.report_dir}, "
                f"thresholds={self.thresholds})")
