from typing import Dict, List, Optional, Union
from datetime import datetime, timezone, timedelta
import logging
import json
import traceback
from .edge_reporter import EdgeReporter
from .origin_reporter import OriginReporter
from prettytable import PrettyTable
from .formatters import TableFormatter
logger = logging.getLogger(__name__)

class Reporter:
    """Main reporter that coordinates edge and origin reporting"""
    
    def __init__(self, config):
        self.config = config
        self.report_dir = self.config.reports_dir
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        self.edge_reporter = EdgeReporter(config)
        self.origin_reporter = OriginReporter(config)
        self.table_formatter = TableFormatter()

    def safe_division(self, numerator: float, denominator: float) -> float:
        """Safely perform division with proper error handling"""
        try:
            if pd.isna(numerator) or pd.isna(denominator):
                return 0.0
            if denominator == 0:
                return 0.0
            return float(numerator) / float(denominator)
        except Exception as e:
            logger.error(f"Error in safe division: {str(e)}")
            return 0.0

    def generate_summary(self, results: List[Dict], start_time: datetime) -> Optional[str]:
        """Generate overall analysis summary combining edge and origin reports"""
        try:
            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time

            # Filter for successful zone analyses
            successful_zones = [r for r in results if self._validate_result(r)]

            if not successful_zones:
                logger.warning("No successful zone analysis completed")
                return self._format_empty_summary()

            # Calculate overall metrics
            total_metrics = self._calculate_total_metrics(successful_zones)

            # Generate per-zone reports
            zone_reports = []
            for zone_result in successful_zones:
                try:
                    # Generate edge report
                    edge_report = self.edge_reporter.generate_edge_report(
                        zone_result.get('raw_data'),
                        zone_result.get('edge_analysis'),
                        zone_result.get('zone_name')
                    )

                    # Generate origin report
                    origin_report = self.origin_reporter.generate_origin_report(
                        zone_result.get('raw_data'),
                        zone_result.get('origin_analysis'),
                        zone_result.get('zone_name')
                    )

                    if edge_report and origin_report:
                        zone_reports.append({
                            'zone_name': zone_result.get('zone_name'),
                            'edge_report': edge_report,
                            'origin_report': origin_report,
                            'metrics': {
                                'cache': zone_result.get('cache_analysis', {}).get('overall', {}),
                                'sampling': zone_result.get('sampling_metrics', {})
                            }
                        })

                except Exception as e:
                    logger.error(f"Error generating report for zone {zone_result.get('zone_name')}: {str(e)}")
                    continue

            # Generate combined summary
            summary = self._format_combined_summary(
                duration=duration,
                zone_reports=zone_reports,
                total_metrics=total_metrics
            )

            # Save summary
            self._save_summary(summary)

            return summary

        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            logger.error(traceback.format_exc())
            return self._format_empty_summary()

    def _calculate_total_metrics(self, zones: List[Dict]) -> Dict:
        """Calculate total metrics across all zones with proper error handling"""
        try:
            total_requests = sum(
                zone.get('cache_analysis', {})
                .get('overall', {})
                .get('total_requests', 0) 
                for zone in zones
            )

            total_bytes = sum(
                zone.get('cache_analysis', {})
                .get('overall', {})
                .get('total_bytes', 0) 
                for zone in zones
            )

            # Calculate cache hits safely
            total_hits = sum(
                safe_division(
                    zone.get('cache_analysis', {})
                    .get('overall', {})
                    .get('total_requests', 0) *
                    zone.get('cache_analysis', {})
                    .get('overall', {})
                    .get('hit_ratio', 0),
                    100
                )
                for zone in zones
            )

            # Calculate weighted averages for performance metrics
            weighted_edge_ttfb = 0
            weighted_origin_time = 0
            
            for zone in zones:
                requests = zone.get('cache_analysis', {}).get('overall', {}).get('total_requests', 0)
                
                # Edge TTFB
                edge_ttfb = (
                    zone.get('edge_analysis', {})
                    .get('edge_metrics', {})
                    .get('edge_response_time', {})
                    .get('avg', 0)
                )
                weighted_edge_ttfb += edge_ttfb * requests
                
                # Origin time
                origin_time = (
                    zone.get('origin_analysis', {})
                    .get('overall_metrics', {})
                    .get('response_time', {})
                    .get('avg', 0)
                )
                weighted_origin_time += origin_time * requests

            weighted_edge_ttfb = safe_division(weighted_edge_ttfb, total_requests)
            weighted_origin_time = safe_division(weighted_origin_time, total_requests)

            return {
                'requests': {
                    'total': int(total_requests),
                    'cache_hits': int(total_hits),
                    'hit_ratio': safe_division(total_hits, total_requests) * 100
                },
                'bandwidth': {
                    'total_gb': safe_division(total_bytes, (1024**3)),
                    'saved_gb': safe_division(
                        total_bytes * sum(
                            zone.get('cache_analysis', {})
                            .get('overall', {})
                            .get('bandwidth_saving', 0)
                            for zone in zones
                        ),
                        (100 * len(zones) * (1024**3))
                    )
                },
                'performance': {
                    'edge_ttfb': float(weighted_edge_ttfb),
                    'origin_time': float(weighted_origin_time)
                },
                'zones_analyzed': len(zones)
            }

        except Exception as e:
            logger.error(f"Error calculating total metrics: {str(e)}")
            return {
                'requests': {'total': 0, 'cache_hits': 0, 'hit_ratio': 0},
                'bandwidth': {'total_gb': 0, 'saved_gb': 0},
                'performance': {'edge_ttfb': 0, 'origin_time': 0},
                'zones_analyzed': 0
            }

    def _format_zone_table(self, zone_reports: List[Dict]) -> PrettyTable:
        """Create properly formatted zone summary table"""
        try:
            table_data = []
            columns = ['Zone', 'Requests', 'Cache Hit Ratio', 'Sampling Rate']
            column_types = {
                'Zone': 'text',
                'Requests': 'numeric',
                'Cache Hit Ratio': 'percentage',
                'Sampling Rate': 'percentage'
            }

            for report in zone_reports:
                metrics = report.get('metrics', {})
                table_data.append({
                    'Zone': report.get('zone_name', 'Unknown'),
                    'Requests': metrics.get('cache', {}).get('total_requests', 0),
                    'Cache Hit Ratio': metrics.get('cache', {}).get('hit_ratio', 0),
                    'Sampling Rate': metrics.get('sampling', {}).get('sampling_rates', {}).get('mean', 0)
                })

            return self.table_formatter.format_table(table_data, columns, column_types)

        except Exception as e:
            logger.error(f"Error formatting zone table: {str(e)}")
            return PrettyTable()

    def _format_combined_summary(self, 
                               duration: timedelta,
                               zone_reports: List[Dict],
                               total_metrics: Dict) -> str:
        """Format comprehensive summary combining all reports"""
        try:
            # Create header with overall metrics
            summary = [
                "\nCloudflare Analytics Summary",
                "==========================",
                f"Analysis Duration: {str(duration).split('.')[0]}",
                f"Zones Analyzed: {total_metrics['zones_analyzed']}",
                "\nOverall Performance:",
                "------------------",
                f"• Total Requests: {total_metrics['requests']['total']:,}",
                f"• Cache Hit Ratio: {total_metrics['requests']['hit_ratio']:.2f}%",
                f"• Average Edge TTFB: {total_metrics['performance']['edge_ttfb']:.2f}ms",
                f"• Average Origin Time: {total_metrics['performance']['origin_time']:.2f}ms",
                f"• Total Bandwidth: {total_metrics['bandwidth']['total_gb']:.2f} GB",
                f"• Bandwidth Saved: {total_metrics['bandwidth']['saved_gb']:.2f} GB",
                "\nIndividual Zone Reports:",
                "---------------------"
            ]

            # Add zone-specific reports
            for zone_report in zone_reports:
                zone_metrics = zone_report['metrics']
                summary.extend([
                    f"\nZone: {zone_report['zone_name']}",
                    "-" * (len(zone_report['zone_name']) + 6),
                    f"• Requests: {zone_metrics['cache'].get('total_requests', 0):,}",
                    f"• Cache Hit Ratio: {zone_metrics['cache'].get('hit_ratio', 0):.2f}%",
                    f"• Sampling Rate: {zone_metrics['sampling'].get('sampling_rates', {}).get('mean', 0):.2f}%",
                    "\nEdge Analysis:",
                    zone_report['edge_report'],
                    "\nOrigin Analysis:",
                    zone_report['origin_report'],
                    ""
                ])

            return "\n".join(summary)

        except Exception as e:
            logger.error(f"Error formatting combined summary: {str(e)}")
            return self._format_empty_summary()

    def _validate_result(self, result: Dict) -> bool:
        """Validate analysis result structure"""
        try:
            required_keys = [
                'edge_analysis', 'origin_analysis', 'cache_analysis',
                'sampling_metrics', 'zone_name', 'raw_data'
            ]
            if not all(key in result and result[key] is not None for key in required_keys):
                missing_keys = [key for key in required_keys if key not in result or result[key] is None]
                logger.error(f"Missing required keys in result: {missing_keys}")
                return False
            return True

        except Exception as e:
            logger.error(f"Error validating result: {str(e)}")
            return False

    def _format_empty_summary(self) -> str:
        """Format summary for failed analysis"""
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

Please check the logs for detailed error information and try again.
"""

    def _parse_summary_sections(self, summary: str) -> Dict:
        """Parse summary into structured sections with proper handling"""
        try:
            sections = {}
            current_section = None
            current_subsection = None
            current_content = []
            
            for line in summary.split('\n'):
                line = line.strip()
                
                # Handle section headers
                if line and all(c == '=' for c in line):
                    if current_section and current_content:
                        if current_subsection:
                            if current_section not in sections:
                                sections[current_section] = {}
                            sections[current_section][current_subsection] = current_content
                        else:
                            sections[current_section] = current_content
                    current_section = current_content[-1] if current_content else None
                    current_subsection = None
                    current_content = []
                    
                # Handle subsection headers
                elif line and all(c == '-' for c in line):
                    if current_content:
                        current_subsection = current_content[-1]
                        current_content = []
                        
                # Collect content
                else:
                    if line or current_content:  # Preserve empty lines within content
                        current_content.append(line)
            
            # Handle final section
            if current_section and current_content:
                if current_subsection:
                    if current_section not in sections:
                        sections[current_section] = {}
                    sections[current_section][current_subsection] = current_content
                else:
                    sections[current_section] = current_content
                    
            return sections
            
        except Exception as e:
            logger.error(f"Error parsing summary sections: {str(e)}")
            return {'error': 'Failed to parse summary sections'}

    def _save_summary(self, summary: str) -> None:
        """Save summary to multiple formats with proper error handling"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_path = self.report_dir / f"analysis_summary_{timestamp}"
            
            # Save as text with proper line endings
            with open(f"{base_path}.txt", 'w', encoding='utf-8', newline='\n') as f:
                f.write(summary)
            
            # Save as markdown with proper formatting
            with open(f"{base_path}.md", 'w', encoding='utf-8', newline='\n') as f:
                f.write(summary)
            
            # Save as JSON for programmatic access
            summary_dict = {
                'timestamp': timestamp,
                'content': summary,
                'sections': self._parse_summary_sections(summary)
            }
            with open(f"{base_path}.json", 'w', encoding='utf-8', newline='\n') as f:
                json.dump(summary_dict, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Summary saved to {base_path}")
            
        except Exception as e:
            logger.error(f"Error saving summary: {str(e)}")
            logger.error(traceback.format_exc())

    def generate_zone_report(self, zone_result: Dict, output_format: str = 'text') -> Optional[str]:
        """Generate detailed report for a single zone"""
        try:
            if not self._validate_result(zone_result):
                logger.error(f"Invalid zone result for {zone_result.get('zone_name', 'unknown zone')}")
                return None

            # Generate component reports
            edge_report = self.edge_reporter.generate_edge_report(
                zone_result.get('raw_data'),
                zone_result.get('edge_analysis'),
                zone_result.get('zone_name')
            )
            
            origin_report = self.origin_reporter.generate_origin_report(
                zone_result.get('raw_data'),
                zone_result.get('origin_analysis'),
                zone_result.get('zone_name')
            )

            if not edge_report or not origin_report:
                return None

            if output_format == 'json':
                return self._generate_json_report(zone_result, edge_report, origin_report)
            else:
                return self._generate_text_report(zone_result, edge_report, origin_report)

        except Exception as e:
            logger.error(f"Error generating zone report: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _generate_json_report(
        self,
        zone_result: Dict,
        edge_report: str,
        origin_report: str
    ) -> str:
        """Generate JSON format report"""
        try:
            report = {
                'zone_name': zone_result['zone_name'],
                'edge_analysis': edge_report,
                'origin_analysis': origin_report,
                'cache_analysis': zone_result.get('cache_analysis', {}),
                'sampling_metrics': zone_result.get('sampling_metrics', {}),
                'metadata': zone_result.get('metadata', {})
            }
            return json.dumps(report, indent=2, ensure_ascii=False)

        except Exception as e:
            logger.error(f"Error generating JSON report: {str(e)}")
            return "{}"

    def _generate_text_report(
        self,
        zone_result: Dict,
        edge_report: str,
        origin_report: str
    ) -> str:
        """Generate text format report"""
        try:
            cache_metrics = zone_result.get('cache_analysis', {}).get('overall', {})
            sampling_metrics = zone_result.get('sampling_metrics', {})

            return f"""
Zone Performance Report
=====================
Zone: {zone_result['zone_name']}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

Cache Performance:
• Total Requests: {cache_metrics.get('total_requests', 0):,}
• Hit Ratio: {cache_metrics.get('hit_ratio', 0):.2f}%
• Bandwidth Savings: {cache_metrics.get('bandwidth_saving', 0):.2f}%

Sampling Metrics:
• Average Rate: {sampling_metrics.get('sampling_rates', {}).get('mean', 0):.2f}%
• Request Ratio: {sampling_metrics.get('requests', {}).get('ratio', 1):.2f}

{edge_report}

{origin_report}
"""
        except Exception as e:
            logger.error(f"Error generating text report: {str(e)}")
            return "Error generating report. Please check logs for details."
