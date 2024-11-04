# reporter.py
from datetime import datetime, timezone
from typing import Dict, Optional, List
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Reporter:
    def __init__(self, config):
        self.config = config
    
    def generate_summary(self, results: List[Dict], start_time: datetime) -> Optional[str]:
        """Generate overall analysis summary."""
        try:
            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time
            
            successful_zones = [r for r in results if r.get('cache_analysis') and r.get('perf_analysis')]
            
            if not successful_zones:
                summary = """
                Analysis Summary
                ===============
                No successful zone analysis completed.
                """
                return summary

            # Calculate overall metrics
            total_requests = sum(r['cache_analysis']['overall']['total_requests'] for r in successful_zones)
            avg_hit_ratio = sum(
                r['cache_analysis']['overall']['hit_ratio'] * r['cache_analysis']['overall']['total_requests']
                for r in successful_zones
            ) / total_requests if total_requests > 0 else 0

            avg_sampling_rate = sum(
                r['sampling_metrics']['avg_sampling_rate'] for r in successful_zones
            ) / len(successful_zones)

            summary = f"""
            Cloudflare Analytics Summary
            ==========================
            Analysis completed at: {end_time}
            Total duration: {duration}
            
            Overview:
            ---------
            Zones Analyzed: {len(successful_zones)}
            Total Requests: {total_requests:,}
            Average Hit Ratio: {avg_hit_ratio:.2f}%
            Average Sampling Rate: {avg_sampling_rate:.2f}%
            
            Zone Details:
            -------------"""

            for result in successful_zones:
                zone_name = result['zone_name']
                cache_analysis = result['cache_analysis']
                perf_analysis = result['perf_analysis']
                
                summary += f"""
                
                {zone_name}:
                - Requests: {cache_analysis['overall']['total_requests']:,}
                - Hit Ratio: {cache_analysis['overall']['hit_ratio']:.2f}%
                - Avg TTFB: {perf_analysis['overall']['avg_ttfb_ms']:.2f}ms
                - Sampling Rate: {result['sampling_metrics']['avg_sampling_rate']:.2f}%
                - Confidence Score: {result['sampling_metrics']['avg_confidence_score']:.3f}
                """
            
            # Save summary to file
            summary_file = self.config.reports_dir / "analysis_summary.txt"
            with open(summary_file, 'w') as f:
                f.write(summary)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            return None
