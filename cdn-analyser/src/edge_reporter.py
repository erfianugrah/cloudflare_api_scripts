from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import json
from prettytable import PrettyTable
import traceback
import pandas as pd
import numpy as np
from .data_processor import DataProcessor

logger = logging.getLogger(__name__)

class EdgeReporter:
    """Enhanced reporter for edge/CDN performance analysis"""
    
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
                'static': {
                    'good': 80,    # %
                    'warning': 60,  # %
                    'critical': 40  # %
                },
                'dynamic': {
                    'good': 5,     # %
                    'warning': 2,   # %
                    'critical': 1   # %
                }
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
            'lightning': "⚡"
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
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
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
        """Create detailed report sections"""
        try:
            sections = [
                self._create_header(zone_name),
                self._create_executive_summary(analysis),
                self._create_edge_performance_section(analysis),
                self._create_cache_section(analysis),
                self._create_reliability_section(analysis),
                self._create_geographic_section(analysis),
                self._create_protocol_section(analysis),
                self._create_content_section(analysis),
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
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
Analysis Type: Edge/CDN Performance"""

    def _create_executive_summary(self, analysis: Dict) -> str:
        """Create executive summary with enhanced status indicators"""
        try:
            metrics = analysis.get('edge_metrics', {})
            cache = analysis.get('cache_metrics', {}).get('overall', {})
            content_mix = cache.get('content_mix', {})
            error_rates = metrics.get('error_rates', {})
            bandwidth = metrics.get('bandwidth', {})
            health_status = metrics.get('health_status', 'unknown')

            bullet = self.symbols['bullet']
            check = self.symbols['check']
            warning = self.symbols['warning']
            critical = self.symbols['critical']
            
            # Calculate status indicators
            ttfb_avg = metrics.get('edge_response_time', {}).get('avg', 0)
            error_total = error_rates.get('total_error_rate', 0)
            hit_ratio = cache.get('hit_ratio', 0)
            
            status = {
                'performance': (
                    f'{check} Good' if ttfb_avg < self.thresholds['ttfb']['warning']
                    else f'{critical} Critical' if ttfb_avg >= self.thresholds['ttfb']['critical']
                    else f'{warning} Attention Needed'
                ),
                'reliability': (
                    f'{check} Good' if error_total < self.thresholds['error_rate']['warning']
                    else f'{critical} Critical' if error_total >= self.thresholds['error_rate']['critical']
                    else f'{warning} Attention Needed'
                ),
                'cache': (
                    f'{check} Good' if hit_ratio >= self.thresholds['cache_hit']['static']['good']
                    else f'{critical} Critical' if hit_ratio < self.thresholds['cache_hit']['static']['critical']
                    else f'{warning} Attention Needed'
                ) if content_mix.get('static_percentage', 0) > 50 else 'Optimized for Dynamic Content'
            }

            # Get protocol metrics
            protocol_metrics = analysis.get('protocol_metrics', {}).get('summary', {})
            
            return f"""
Executive Summary
----------------
{bullet} Overall Health Status: {health_status.upper()}

Performance Metrics:
{bullet} Edge Response Time: {ttfb_avg:.2f}ms
{bullet} P95 Response Time: {metrics.get('edge_response_time', {}).get('p95', 0):.2f}ms
{bullet} Path Latency: {metrics.get('path_latency', {}).get('avg', 0):.2f}ms

Traffic Overview:
{bullet} Total Requests: {metrics.get('request_metrics', {}).get('total', 0):,}
{bullet} Requests/Second: {metrics.get('request_metrics', {}).get('per_second', 0):.2f}
{bullet} Total Bandwidth: {bandwidth.get('total', 0) / (1024**3):.2f} GB

Cache Performance:
{bullet} Hit Ratio: {hit_ratio:.2f}%
{bullet} Content Mix: {content_mix.get('static_percentage', 0):.1f}% static, {content_mix.get('dynamic_percentage', 0):.1f}% dynamic
{bullet} Bandwidth Savings: {cache.get('bandwidth_saving', 0):.2f}%

Error Rates:
{bullet} 4xx Errors: {error_rates.get('error_rate_4xx', 0):.2f}%
{bullet} 5xx Errors: {error_rates.get('error_rate_5xx', 0):.2f}%

Protocol Distribution:
{bullet} HTTP/2: {protocol_metrics.get('http2_percentage', 0):.1f}%
{bullet} HTTP/3: {protocol_metrics.get('http3_percentage', 0):.1f}%
{bullet} HTTP/1.x: {protocol_metrics.get('http1_percentage', 0):.1f}%

Status Indicators:
{bullet} Edge Performance: {status['performance']}
{bullet} Reliability: {status['reliability']}
{bullet} Cache Efficiency: {status['cache']}"""

        except Exception as e:
            logger.error(f"Error creating executive summary: {str(e)}")
            return "Executive Summary: Error generating summary"

    def _create_edge_performance_section(self, analysis: Dict) -> str:
        """Create edge performance analysis section with enhanced metrics"""
        try:
            edge_metrics = analysis.get('edge_metrics', {})
            response_time = edge_metrics.get('edge_response_time', {})
            path_latency = edge_metrics.get('path_latency', {})
            bandwidth = edge_metrics.get('bandwidth', {})
            temporal = analysis.get('temporal_analysis', {})
            bullet = self.symbols['bullet']

            # Calculate performance trends
            time_series = temporal.get('time_series', {})
            trend = "No trend data available"
            
            if time_series:
                times = list(time_series.values())
                start_perf = times[0]['performance']['ttfb']
                end_perf = times[-1]['performance']['ttfb']
                trend_direction = self.symbols['up_arrow'] if end_perf > start_perf else self.symbols['down_arrow']
                trend_pct = ((end_perf/start_perf) - 1) * 100
                trend = f"{trend_direction} {'Increasing' if end_perf > start_perf else 'Decreasing'} ({trend_pct:.1f}%)"

            # Get peak periods
            peak_periods = temporal.get('peak_periods', {})
            highest_load = peak_periods.get('highest_load', {})
            highest_latency = peak_periods.get('highest_latency', {})
            return f"""
Edge Performance Analysis
----------------------
Response Time Metrics:
{bullet} Average TTFB: {response_time.get('avg', 0):.2f}ms
{bullet} Median TTFB: {response_time.get('median', 0):.2f}ms
{bullet} P95 TTFB: {response_time.get('p95', 0):.2f}ms
{bullet} P99 TTFB: {response_time.get('p99', 0):.2f}ms
{bullet} Standard Deviation: {response_time.get('std', 0):.2f}ms

Path Latency Metrics:
{bullet} Average Latency: {path_latency.get('avg', 0):.2f}ms
{bullet} Min Latency: {path_latency.get('min', 0):.2f}ms
{bullet} Max Latency: {path_latency.get('max', 0):.2f}ms
{bullet} Latency Variation: {path_latency.get('std', 0):.2f}ms

Bandwidth Metrics:
{bullet} Average Response Size: {bandwidth.get('avg_per_request', 0) / 1024:.2f} KB
{bullet} P95 Response Size: {bandwidth.get('p95', 0) / 1024:.2f} KB
{bullet} P99 Response Size: {bandwidth.get('p99', 0) / 1024:.2f} KB

Performance Trends:
{bullet} TTFB Trend: {trend}
{bullet} Busiest Period: {highest_load.get('timestamp', 'Unknown')}
  - Requests: {highest_load.get('requests', 0):,}
  - TTFB: {highest_load.get('ttfb', 0):.2f}ms

{bullet} Slowest Period: {highest_latency.get('timestamp', 'Unknown')}
  - TTFB: {highest_latency.get('ttfb', 0):.2f}ms
  - Requests: {highest_latency.get('requests', 0):,}"""

        except Exception as e:
            logger.error(f"Error creating edge performance section: {str(e)}")
            return "Edge Performance Analysis: Error generating analysis"

    def _create_cache_section(self, analysis: Dict) -> str:
        """Create cache performance analysis section with detailed metrics"""
        try:
            cache = analysis.get('cache_metrics', {})
            overall = cache.get('overall', {})
            dist = cache.get('status_distribution', {})
            impact = cache.get('edge_performance_impact', {})
            bullet = self.symbols['bullet']

            # Create cache status distribution table
            cache_table = PrettyTable()
            cache_table.field_names = ["Status", "Requests %", "Bytes %", "Avg TTFB"]
            cache_table.align = "r"
            cache_table.align["Status"] = "l"

            for status, metrics in dist.items():
                if isinstance(metrics, dict):
                    cache_table.add_row([
                        status,
                        f"{metrics.get('requests_percentage', 0):.2f}%",
                        f"{metrics.get('bytes_percentage', 0):.2f}%",
                        f"{metrics.get('avg_ttfb', 0):.2f}ms"
                    ])

            # Format content type analysis
            content_analysis = ""
            if cache.get('content_type_analysis'):
                content_table = PrettyTable()
                content_table.field_names = ["Content Type", "Requests %", "Hit Ratio", "Category"]
                content_table.align = "r"
                content_table.align["Content Type"] = "l"
                content_table.align["Category"] = "l"

                for ctype, metrics in cache.get('content_type_analysis', {}).get('content_types', {}).items():
                    content_table.add_row([
                        ctype[:40] + "..." if len(ctype) > 40 else ctype,
                        f"{metrics.get('traffic', {}).get('percentage', 0):.2f}%",
                        f"{metrics.get('cache_hit_ratio', 0):.2f}%",
                        metrics.get('type', 'unknown').upper()
                    ])
                content_analysis = f"\nContent Type Analysis:\n{content_table.get_string()}"

            return f"""
Cache Performance Analysis
-----------------------
Overall Metrics:
{bullet} Hit Ratio: {overall.get('hit_ratio', 0):.2f}%
{bullet} Static Content Hit Ratio: {overall.get('static_hit_ratio', 0):.2f}%
{bullet} Dynamic Content Hit Ratio: {overall.get('dynamic_hit_ratio', 0):.2f}%
{bullet} Bandwidth Savings: {overall.get('bandwidth_saving', 0):.2f}%
{bullet} Total Bandwidth Served: {overall.get('total_bytes', 0) / (1024**3):.2f} GB

Content Mix:
{bullet} Static Content: {overall.get('content_mix', {}).get('static_percentage', 0):.1f}%
{bullet} Dynamic Content: {overall.get('content_mix', {}).get('dynamic_percentage', 0):.1f}%
{bullet} Unknown Content: {overall.get('content_mix', {}).get('unknown_percentage', 0):.1f}%

Cache Impact on Performance:
{bullet} Cache Hit TTFB: {impact.get('cache_hit_ttfb', 0):.2f}ms
{bullet} Cache Miss TTFB: {impact.get('cache_miss_ttfb', 0):.2f}ms
{bullet} Cache Hit P95 TTFB: {impact.get('cache_hit_ttfb_p95', 0):.2f}ms
{bullet} Cache Miss P95 TTFB: {impact.get('cache_miss_ttfb_p95', 0):.2f}ms
{bullet} Cache Hit Avg Size: {impact.get('cache_hit_bytes', 0) / 1024:.2f} KB
{bullet} Cache Miss Avg Size: {impact.get('cache_miss_bytes', 0) / 1024:.2f} KB

Cache Status Distribution:
{cache_table.get_string()}
{content_analysis}"""

        except Exception as e:
            logger.error(f"Error creating cache section: {str(e)}")
            return "Cache Performance Analysis: Error generating analysis"

    def _create_reliability_section(self, analysis: Dict) -> str:
        """Create reliability analysis section with comprehensive error analysis"""
        try:
            metrics = analysis.get('edge_metrics', {})
            error_rates = metrics.get('error_rates', {})
            error_analysis = analysis.get('error_analysis', {})
            bullet = self.symbols['bullet']
            warning = self.symbols['warning']

            # Create error distribution table
            error_table = PrettyTable()
            error_table.field_names = ["Status", "Count", "Percentage", "Avg TTFB", "Avg Size"]
            error_table.align = "r"
            error_table.align["Status"] = "l"

            error_dist = error_analysis.get('error_distribution', {})
            for status, data in error_dist.items():
                if isinstance(data, dict):
                    error_table.add_row([
                        status,
                        f"{data.get('count', 0):,}",
                        f"{data.get('percentage', 0):.2f}%",
                        f"{data.get('avg_ttfb', 0):.2f}ms",
                        f"{data.get('avg_size', 0) / 1024:.2f}KB"
                    ])

            # Get error patterns
            patterns = error_analysis.get('patterns', {})
            error_timing = error_analysis.get('timing', {})

            # Get impact analysis
            impact = error_analysis.get('impact_analysis', {})
            cache_impact = impact.get('cache_impact', {})
            protocol_impact = impact.get('protocol_impact', {})

            return f"""
Reliability Analysis
------------------
Error Rates:
{bullet} 4xx Error Rate: {error_rates.get('error_rate_4xx', 0):.2f}%
{bullet} 5xx Error Rate: {error_rates.get('error_rate_5xx', 0):.2f}%
{bullet} Total Error Rate: {error_rates.get('total_error_rate', 0):.2f}%

Error Response Times:
{bullet} Average TTFB: {error_timing.get('avg_ttfb', 0):.2f}ms
{bullet} P50 TTFB: {error_timing.get('percentiles', {}).get('p50', 0):.2f}ms
{bullet} P95 TTFB: {error_timing.get('percentiles', {}).get('p95', 0):.2f}ms
{bullet} P99 TTFB: {error_timing.get('percentiles', {}).get('p99', 0):.2f}ms

Cache Impact on Errors:
{bullet} Error Rate (Cached): {cache_impact.get('error_rate_cached', 0):.2f}%
{bullet} Error Rate (Uncached): {cache_impact.get('error_rate_uncached', 0):.2f}%

Protocol Impact on Errors:
{bullet} HTTP/2: {protocol_impact.get('HTTP/2', 0):.2f}%
{bullet} HTTP/3: {protocol_impact.get('HTTP/3', 0):.2f}%
{bullet} HTTP/1.x: {protocol_impact.get('HTTP/1.1', 0):.2f}%

Error Status Distribution:
{error_table.get_string()}

High-Impact Error Patterns:{self._format_error_patterns(patterns) if patterns else ''}"""

        except Exception as e:
            logger.error(f"Error creating reliability section: {str(e)}")
            return "Reliability Analysis: Error generating analysis"

    def _format_error_patterns(self, patterns: Dict) -> str:
        """Format error patterns for display with enhanced metrics"""
        try:
            bullet = self.symbols['bullet']
            formatted_patterns = []

            # Format patterns by protocol
            if 'by_protocol' in patterns:
                protocol_patterns = []
                for protocol, data in patterns['by_protocol'].items():
                    if isinstance(data, dict) and data.get('error_percentage', 0) > 1:
                        protocol_patterns.append(
                            f"\n  - {protocol}: {data.get('error_percentage', 0):.2f}% "
                            f"(TTFB: {data.get('avg_ttfb', 0):.2f}ms)"
                        )
                if protocol_patterns:
                    formatted_patterns.append(f"\n{bullet} By Protocol:{''.join(protocol_patterns)}")

            # Format patterns by content type
            if 'by_content_type' in patterns:
                content_patterns = []
                for ctype, data in patterns['by_content_type'].items():
                    if isinstance(data, dict) and data.get('error_percentage', 0) > 1:
                        content_patterns.append(
                            f"\n  - {ctype}: {data.get('error_percentage', 0):.2f}% "
                            f"(TTFB: {data.get('avg_ttfb', 0):.2f}ms)"
                        )
                if content_patterns:
                    formatted_patterns.append(f"\n{bullet} By Content Type:{''.join(content_patterns)}")

            # Format patterns by cache status
            if 'by_cache_status' in patterns:
                cache_patterns = []
                for status, data in patterns['by_cache_status'].items():
                    if isinstance(data, dict) and data.get('error_percentage', 0) > 1:
                        cache_patterns.append(
                            f"\n  - {status}: {data.get('error_percentage', 0):.2f}% "
                            f"(TTFB: {data.get('avg_ttfb', 0):.2f}ms)"
                        )
                if cache_patterns:
                    formatted_patterns.append(f"\n{bullet} By Cache Status:{''.join(cache_patterns)}")

            return ''.join(formatted_patterns) if formatted_patterns else f"\n{bullet} No significant error patterns detected"

        except Exception as e:
            logger.error(f"Error formatting error patterns: {str(e)}")
            return f"\n{bullet} Error formatting patterns"

    def _create_geographic_section(self, analysis: Dict) -> str:
        """Create geographic analysis section with enhanced regional metrics"""
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
            perf_table.field_names = ["Region", "TTFB", "Error Rate", "Requests", "Cache Hit"]
            perf_table.align = "r"
            perf_table.align["Region"] = "l"

            # Get top 10 countries by request volume
            top_countries = sorted(
                [(k, v) for k, v in countries.items() if isinstance(v, dict)],
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )[:10]

            for country, metrics in top_countries:
                traffic = metrics.get('traffic', {})
                perf = metrics.get('performance', {})
                reliability = metrics.get('reliability', {})

                perf_table.add_row([
                    country,
                    f"{perf.get('ttfb', {}).get('avg', 0):.2f}ms",
                    f"{sum(reliability.values()):.2f}%",
                    f"{traffic.get('requests', 0):,}",
                    f"{metrics.get('cache_hit_ratio', 0):.1f}%"
                ])

            # Get performance patterns
            global_metrics = summary.get('global_metrics', {})
            patterns = summary.get('performance_patterns', {})

            return f"""
Geographic Analysis
-----------------
Performance by Region:
{perf_table.get_string()}

Regional Patterns:
{bullet} Fastest Regions: {', '.join(patterns.get('fastest_edge', ['None']))}
{bullet} Slowest Regions: {', '.join(patterns.get('slowest_edge', ['None']))}
{bullet} Highest Error Rates: {', '.join(patterns.get('highest_errors', ['None']))}
{bullet} Top Traffic Regions: {', '.join(patterns.get('top_traffic', ['None']))}

Global Metrics:
{bullet} Total Countries: {global_metrics.get('total_countries', 0)}
{bullet} Average TTFB: {global_metrics.get('avg_ttfb', 0):.2f}ms
{bullet} TTFB Variation: {global_metrics.get('ttfb_std', 0):.2f}ms
{bullet} Average Cache Hit Ratio: {global_metrics.get('avg_cache_hit_ratio', 0):.1f}%"""

        except Exception as e:
            logger.error(f"Error creating geographic section: {str(e)}")
            return "Geographic Analysis: Error generating analysis"

    def _create_protocol_section(self, analysis: Dict) -> str:
        """Create protocol analysis section with detailed performance metrics"""
        try:
            protocol_metrics = analysis.get('protocol_metrics', {})
            protocols = protocol_metrics.get('protocols', {})
            summary = protocol_metrics.get('summary', {})
            bullet = self.symbols['bullet']
            arrow = self.symbols['right_arrow']

            if not protocols:
                return f"""
Protocol Analysis
--------------
No protocol data available"""

            # Create protocol performance table
            protocol_table = PrettyTable()
            protocol_table.field_names = [
                "Protocol", "Requests %", "Avg TTFB", "P95 TTFB", 
                "Error Rate", "Cache Hit"
            ]
            protocol_table.align = "r"
            protocol_table.align["Protocol"] = "l"

            for protocol, metrics in protocols.items():
                perf = metrics.get('performance', {})
                reliability = metrics.get('reliability', {})
                
                protocol_table.add_row([
                    protocol,
                    f"{metrics.get('requests_percentage', 0):.1f}%",
                    f"{perf.get('ttfb', {}).get('avg', 0):.2f}ms",
                    f"{perf.get('ttfb', {}).get('p95', 0):.2f}ms",
                    f"{reliability.get('total_error_rate', 0):.2f}%",
                    f"{metrics.get('cache_hit_ratio', 0):.1f}%"
                ])
            # Get protocol comparisons and format them
            comparisons = summary.get('comparisons', {})
            comparison_text = self._format_protocol_comparisons(comparisons) if comparisons else ""

            return f"""
Protocol Analysis
--------------
Distribution:
{bullet} HTTP/2: {summary.get('http2_percentage', 0):.1f}%
{bullet} HTTP/3: {summary.get('http3_percentage', 0):.1f}%
{bullet} HTTP/1.x: {summary.get('http1_percentage', 0):.1f}%

Performance by Protocol:
{protocol_table.get_string()}

Protocol Insights:
{bullet} Fastest Protocol: {summary.get('fastest_protocol', 'Unknown')}
{bullet} Most Reliable: {summary.get('most_reliable_protocol', 'Unknown')}
{bullet} Best Cache Performance: {summary.get('best_cache_protocol', 'Unknown')}

{comparison_text}"""

        except Exception as e:
            logger.error(f"Error creating protocol section: {str(e)}")
            return "Protocol Analysis: Error generating analysis"

    def _format_protocol_comparisons(self, comparisons: Dict) -> str:
        """Format protocol comparison data with detailed metrics"""
        try:
            if not comparisons:
                return ""

            bullet = self.symbols['bullet']
            arrow = self.symbols['right_arrow']
            formatted = []

            for comparison_key, data in comparisons.items():
                protocols = comparison_key.split('_vs_')
                if len(protocols) != 2:
                    continue

                ttfb_diff = abs(data.get('ttfb_difference', 0))
                error_diff = abs(data.get('error_rate_difference', 0))
                cache_diff = abs(data.get('cache_difference', 0))
                latency_diff = abs(data.get('latency_difference', 0))

                formatted.append(f"""
{bullet} {protocols[0]} vs {protocols[1]}:
  - Response Time: {ttfb_diff:.2f}ms faster on {data.get('performance_winner', 'Unknown')}
  - Error Rate: {error_diff:.2f}% lower on {data.get('reliability_winner', 'Unknown')}
  - Cache Performance: {cache_diff:.2f}% better on {data.get('cache_winner', 'Unknown')}
  - Path Latency: {latency_diff:.2f}ms difference""")

            return "\nProtocol Comparisons:" + "".join(formatted) if formatted else ""

        except Exception as e:
            logger.error(f"Error formatting protocol comparisons: {str(e)}")
            return ""

    def _create_content_section(self, analysis: Dict) -> str:
        """Create content type analysis section with enhanced metrics"""
        try:
            content_analysis = analysis.get('content_type_analysis', {})
            content_types = content_analysis.get('content_types', {})
            summary = content_analysis.get('summary', {})
            bullet = self.symbols['bullet']

            if not content_types:
                return ""

            # Create content type performance table
            content_table = PrettyTable()
            content_table.field_names = [
                "Content Type", "Requests %", "Avg TTFB", 
                "Cache Hit", "Avg Size"
            ]
            content_table.align = "r"
            content_table.align["Content Type"] = "l"

            # Sort content types by request volume and show top 10
            sorted_types = sorted(
                content_types.items(),
                key=lambda x: x[1]['traffic']['percentage'],
                reverse=True
            )[:10]

            for ctype, metrics in sorted_types:
                traffic = metrics.get('traffic', {})
                perf = metrics.get('performance', {})
                
                content_table.add_row([
                    ctype[:40] + "..." if len(ctype) > 40 else ctype,
                    f"{traffic.get('percentage', 0):.1f}%",
                    f"{perf.get('avg_ttfb', 0):.2f}ms",
                    f"{metrics.get('cache_hit_ratio', 0):.1f}%",
                    f"{perf.get('avg_size', 0) / 1024:.1f}KB"
                ])

            # Get performance metrics by category
            perf_by_category = summary.get('performance_by_category', {})

            return f"""
Content Type Analysis
------------------
Content Distribution:
{bullet} Static Content Types: {summary.get('static_types', 0)}
{bullet} Dynamic Content Types: {summary.get('dynamic_types', 0)}

Performance by Category:
{bullet} Static Content:
  - Average TTFB: {perf_by_category.get('static', {}).get('avg_response_time', 0):.2f}ms
  - Error Rate: {perf_by_category.get('static', {}).get('error_rate', 0):.2f}%
  - Cache Hit Ratio: {perf_by_category.get('static', {}).get('cache_hit_ratio', 0):.2f}%

{bullet} Dynamic Content:
  - Average TTFB: {perf_by_category.get('dynamic', {}).get('avg_response_time', 0):.2f}ms
  - Error Rate: {perf_by_category.get('dynamic', {}).get('error_rate', 0):.2f}%
  - Cache Hit Ratio: {perf_by_category.get('dynamic', {}).get('cache_hit_ratio', 0):.2f}%

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
            lightning = self.symbols['lightning']

            # Extract key metrics
            metrics = analysis.get('edge_metrics', {})
            cache = analysis.get('cache_metrics', {}).get('overall', {})
            protocol = analysis.get('protocol_metrics', {}).get('summary', {})
            content_mix = cache.get('content_mix', {})

            # Performance recommendations
            ttfb_avg = metrics.get('edge_response_time', {}).get('avg', 0)
            ttfb_p95 = metrics.get('edge_response_time', {}).get('p95', 0)
            path_latency = metrics.get('path_latency', {}).get('avg', 0)

            if ttfb_avg > self.thresholds['ttfb']['critical']:
                recommendations.append(f"""
{critical} Critical Performance Issues:
- Current average TTFB: {ttfb_avg:.2f}ms (threshold: {self.thresholds['ttfb']['critical']}ms)
- P95 TTFB: {ttfb_p95:.2f}ms
- Path Latency: {path_latency:.2f}ms

Recommended Actions:
{bullet} Review and optimize origin server performance
{bullet} Implement aggressive edge caching where possible
{bullet} Consider enabling Argo Smart Routing
{bullet} Review CDN configuration and routing rules
{bullet} Analyze geographic distribution of traffic""")
            elif ttfb_avg > self.thresholds['ttfb']['warning']:
                recommendations.append(f"""
{warning} Performance Optimization Required:
- Current average TTFB: {ttfb_avg:.2f}ms (threshold: {self.thresholds['ttfb']['warning']}ms)
- P95 TTFB: {ttfb_p95:.2f}ms
- Path Latency: {path_latency:.2f}ms

Recommended Actions:
{bullet} Review cache configuration and TTLs
{bullet} Optimize static asset delivery
{bullet} Consider implementing edge computing
{bullet} Monitor path latency trends""")

            # Cache recommendations
            hit_ratio = cache.get('hit_ratio', 0)
            static_pct = content_mix.get('static_percentage', 0)
            dynamic_pct = content_mix.get('dynamic_percentage', 0)

            if static_pct > 50 and hit_ratio < self.thresholds['cache_hit']['static']['warning']:
                recommendations.append(f"""
{warning} Cache Optimization Required:
- Current hit ratio: {hit_ratio:.1f}% (threshold: {self.thresholds['cache_hit']['static']['warning']}%)
- Static content: {static_pct:.1f}%
- Bandwidth savings: {cache.get('bandwidth_saving', 0):.1f}%

Recommended Actions:
{bullet} Review and optimize cache rules
{bullet} Implement appropriate Cache-Control headers
{bullet} Consider increasing edge cache TTLs
{bullet} Analyze cache bypass conditions
{bullet} Implement browser cache directives""")
            elif dynamic_pct > 50 and hit_ratio < self.thresholds['cache_hit']['dynamic']['good']:
                recommendations.append(f"""
{lightning} Dynamic Content Optimization:
- Current hit ratio: {hit_ratio:.1f}%
- Dynamic content: {dynamic_pct:.1f}%

Recommended Actions:
{bullet} Implement API response caching where possible
{bullet} Use edge computing for dynamic content
{bullet} Configure cache revalidation strategies
{bullet} Implement stale-while-revalidate
{bullet} Consider JSON caching strategies""")

            # Protocol recommendations
            http2_pct = protocol.get('http2_percentage', 0)
            http3_pct = protocol.get('http3_percentage', 0)
            if http2_pct + http3_pct < 80:
                recommendations.append(f"""
{warning} Protocol Optimization Recommended:
- Current HTTP/2+HTTP/3 usage: {http2_pct + http3_pct:.1f}%
- HTTP/3: {http3_pct:.1f}%

Recommended Actions:
{bullet} Enable HTTP/3 (QUIC) if not enabled
{bullet} Review TLS configuration
{bullet} Update origin server protocols
{bullet} Monitor client protocol support
{bullet} Implement protocol-based routing rules""")
            # Error rate recommendations
            error_rates = metrics.get('error_rates', {})
            total_error = error_rates.get('total_error_rate', 0)
            if total_error > self.thresholds['error_rate']['warning']:
                recommendations.append(f"""
{critical if total_error > self.thresholds['error_rate']['critical'] else warning} High Error Rate Detected:
- Current error rate: {total_error:.1f}%
- 4xx rate: {error_rates.get('error_rate_4xx', 0):.1f}%
- 5xx rate: {error_rates.get('error_rate_5xx', 0):.1f}%

Recommended Actions:
{bullet} Review server error logs
{bullet} Implement error monitoring
{bullet} Set up automated alerts
{bullet} Consider implementing circuit breakers
{bullet} Review error handling logic""")

            # If no critical issues, provide maintenance recommendations
            if not recommendations:
                recommendations.append(f"""
{bullet} No Critical Issues Detected
Current Performance:
- Response Time: {ttfb_avg:.2f}ms
- Cache Hit Ratio: {hit_ratio:.1f}%
- Error Rate: {total_error:.2f}%
- Protocol Distribution: HTTP/2+HTTP/3={http2_pct + http3_pct:.1f}%

Maintenance Recommendations:
{bullet} Continue monitoring key metrics
{bullet} Implement proactive alerting
{bullet} Schedule regular performance reviews
{bullet} Document baseline performance
{bullet} Consider load testing exercises""")

            return "\nRecommendations\n---------------" + "\n".join(recommendations)

        except Exception as e:
            logger.error(f"Error creating recommendations: {str(e)}")
            return "\nRecommendations: Error generating recommendations"

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

    def __str__(self) -> str:
        """String representation of the reporter"""
        return f"EdgeReporter(thresholds={self.thresholds})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"EdgeReporter(report_dir='{self.report_dir}', "
                f"thresholds={self.thresholds})")
