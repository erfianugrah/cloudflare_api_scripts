from typing import Dict, List, Optional, Union
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import json
import traceback

from .edge_reporter import EdgeReporter
from .origin_reporter import OriginReporter

logger = logging.getLogger(__name__)

class Reporter:
    """Main reporter that coordinates edge and origin reporting"""
    
    def __init__(self, config):
        self.config = config
        self.report_dir = self.config.reports_dir
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
        self.edge_reporter = EdgeReporter(config)
        self.origin_reporter = OriginReporter(config)

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
        """Calculate total metrics across all zones"""
        try:
            total_requests = sum(z.get('cache_analysis', {}).get('overall', {}).get('total_requests', 0) for z in zones)
            total_bytes = sum(z.get('cache_analysis', {}).get('overall', {}).get('total_bytes', 0) for z in zones)
            total_hits = sum(
                z.get('cache_analysis', {}).get('overall', {}).get('total_requests', 0) * 
                z.get('cache_analysis', {}).get('overall', {}).get('hit_ratio', 0) / 100 
                for z in zones
            )

            # Calculate weighted averages
            weighted_edge_ttfb = sum(
                z.get('edge_analysis', {}).get('edge_metrics', {}).get('edge_response_time', {}).get('avg', 0) *
                z.get('cache_analysis', {}).get('overall', {}).get('total_requests', 0)
                for z in zones
            ) / (total_requests if total_requests > 0 else 1)

            weighted_origin_time = sum(
                z.get('origin_analysis', {}).get('overall_metrics', {}).response_time.get('avg', 0) *
                z.get('cache_analysis', {}).get('overall', {}).get('total_requests', 0)
                for z in zones
            ) / (total_requests if total_requests > 0 else 1)

            return {
                'requests': {
                    'total': total_requests,
                    'cache_hits': int(total_hits),
                    'hit_ratio': float(total_hits / total_requests * 100 if total_requests > 0 else 0)
                },
                'bandwidth': {
                    'total_gb': float(total_bytes / (1024**3)),
                    'saved_gb': float(total_bytes * sum(
                        z.get('cache_analysis', {}).get('overall', {}).get('bandwidth_saving', 0) 
                        for z in zones
                    ) / (100 * len(zones)) / (1024**3))
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

    def _format_combined_summary(
        self,
        duration: timedelta,
        zone_reports: List[Dict],
        total_metrics: Dict
    ) -> str:
        """Format comprehensive summary combining all reports"""
        try:
            # Create header with overall metrics
            summary = f"""
Cloudflare Analytics Summary
==========================
Analysis Duration: {str(duration).split('.')[0]}
Zones Analyzed: {total_metrics['zones_analyzed']}

Overall Performance:
------------------
• Total Requests: {total_metrics['requests']['total']:,}
• Cache Hit Ratio: {total_metrics['requests']['hit_ratio']:.2f}%
• Average Edge TTFB: {total_metrics['performance']['edge_ttfb']:.2f}ms
• Average Origin Time: {total_metrics['performance']['origin_time']:.2f}ms
• Total Bandwidth: {total_metrics['bandwidth']['total_gb']:.2f} GB
• Bandwidth Saved: {total_metrics['bandwidth']['saved_gb']:.2f} GB

Individual Zone Reports:
---------------------
"""
            # Add zone-specific reports
            for zone_report in zone_reports:
                zone_metrics = zone_report['metrics']
                summary += f"""
Zone: {zone_report['zone_name']}
{'-' * (len(zone_report['zone_name']) + 6)}
• Requests: {zone_metrics['cache'].get('total_requests', 0):,}
• Cache Hit Ratio: {zone_metrics['cache'].get('hit_ratio', 0):.2f}%
• Sampling Rate: {zone_metrics['sampling'].get('sampling_rates', {}).get('mean', 0):.2f}%

Edge Analysis:
{zone_report['edge_report']}

Origin Analysis:
{zone_report['origin_report']}

"""

            return summary

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

    def _save_summary(self, summary: str) -> None:
        """Save summary to file with proper error handling"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_path = self.report_dir / f"analysis_summary_{timestamp}"
            
            # Save as text
            with open(f"{base_path}.txt", 'w', encoding='utf-8') as f:
                f.write(summary)
            
            # Save as markdown
            with open(f"{base_path}.md", 'w', encoding='utf-8') as f:
                f.write(summary)
            
            # Save as JSON for programmatic access
            summary_dict = {
                'timestamp': timestamp,
                'content': summary,
                'sections': self._parse_summary_sections(summary)
            }
            with open(f"{base_path}.json", 'w', encoding='utf-8') as f:
                json.dump(summary_dict, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Summary saved to {base_path}")
            
        except Exception as e:
            logger.error(f"Error saving summary: {str(e)}")
            logger.error(traceback.format_exc())

    def _parse_summary_sections(self, summary: str) -> Dict:
        """Parse summary into structured sections"""
        try:
            sections = {}
            current_section = None
            current_content = []
            
            for line in summary.split('\n'):
                if line.strip() and all(c in '=' for c in line.strip()):
                    if current_section and current_content:
                        sections[current_section] = '\n'.join(current_content).strip()
                    current_section = current_content[-1].strip()
                    current_content = []
                elif line.strip() and all(c == '-' for c in line.strip()):
                    if len(current_content) > 0:
                        subsection = current_content[-1].strip()
                        if current_section:
                            if current_section not in sections:
                                sections[current_section] = {}
                            sections[current_section][subsection] = []
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
