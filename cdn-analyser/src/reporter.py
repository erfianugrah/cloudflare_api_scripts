# src/reporter.py
from datetime import datetime, timezone
from typing import Dict, Optional
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Reporter:
    def __init__(self, config):
        self.config = config
    
    def generate_report(self, zone_name: str, cache_analysis: Dict, perf_analysis: Dict) -> Optional[str]:
        """Generate a comprehensive analysis report."""
        try:
            report = self._create_report_header(zone_name)
            report += self._create_cache_section(cache_analysis)
            report += self._create_performance_section(perf_analysis)
            report += self._create_recommendations(cache_analysis, perf_analysis)
            
            # Save report
            report_file = self.config.reports_dir / f"{zone_name}_report.txt"
            with open(report_file, 'w') as f:
                f.write(report)
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating report for {zone_name}: {str(e)}")
            return None
    
    def _create_report_header(self, zone_name: str) -> str:
        """Create the report header."""
        return f"""
        Cloudflare Analytics Report for {zone_name}
        {'=' * (24 + len(zone_name))}

        Generated at: {datetime.now(timezone.utc).isoformat()}
        """
    
    def _create_cache_section(self, analysis: Dict) -> str:
        """Create the cache analysis section."""
        try:
            header = """
            Cache Performance Summary
            -----------------------
            """
            
            overall = f"""
            Overall Statistics:
            - Cache Hit Ratio: {analysis['overall']['hit_ratio']:.2f}%
            - Total Requests: {analysis['overall']['total_samples']:,}
            - Total Bandwidth: {analysis['overall']['bandwidth_saved_gb']:.2f} GB
            """
            
            status_dist = """
            Cache Status Distribution:
            """
            for status, data in analysis['cache_status_distribution'].items():
                status_dist += f"- {status}: {data['percentage']:.2f}% ({data['count']:,} requests)\n"
            
            content_type = """
            Top Content Types by Hit Ratio:
            """
            sorted_types = sorted(
                analysis['by_content_type'].items(),
                key=lambda x: x[1]['hit_ratio'],
                reverse=True
            )[:5]
            for ctype, data in sorted_types:
                content_type += f"- {ctype}: {data['hit_ratio']:.2f}% hit ratio\n"
            
            return header + overall + status_dist + content_type
            
        except Exception as e:
            logger.error(f"Error creating cache section: {str(e)}")
            return "\nError generating cache analysis section\n"
    
    def _create_performance_section(self, analysis: Dict) -> str:
        """Create the performance analysis section."""
        try:
            header = """
            Performance Metrics Summary
            -------------------------
            """
            
            overall = f"""
            Overall Statistics:
            - Average TTFB: {analysis['overall']['avg_ttfb']:.2f} ms
            - Average Origin Response Time: {analysis['overall']['avg_origin_time']:.2f} ms
            - Error Rate: {analysis['overall']['error_rate']:.2f}%
            
            Performance Percentiles:
            - TTFB (p95): {analysis['percentiles']['ttfb']['p95']:.2f} ms
            - TTFB (p99): {analysis['percentiles']['ttfb']['p99']:.2f} ms
            - Origin Response (p95): {analysis['percentiles']['origin_time']['p95']:.2f} ms
            - Origin Response (p99): {analysis['percentiles']['origin_time']['p99']:.2f} ms
            """
            
            geo_perf = """
            Geographic Performance (Top 5 Countries):
            """
            sorted_countries = sorted(
                analysis['by_country'].items(),
                key=lambda x: x[1]['ttfb'],
            )[:5]
            for country, data in sorted_countries:
                geo_perf += f"- {country}: {data['ttfb']:.2f} ms TTFB, {data['error_rate']:.2f}% errors\n"
            
            return header + overall + geo_perf
            
        except Exception as e:
            logger.error(f"Error creating performance section: {str(e)}")
            return "\nError generating performance analysis section\n"
    
    def _create_recommendations(self, cache_analysis: Dict, perf_analysis: Dict) -> str:
        """Create recommendations based on the analysis."""
        try:
            recommendations = ["\nRecommendations", "-" * 15]
            
            # Cache-related recommendations
            hit_ratio = cache_analysis['overall']['hit_ratio']
            if hit_ratio < 70:
                recommendations.append(
                    f"- Current cache hit ratio ({hit_ratio:.1f}%) is below optimal levels. Consider:"
                )
                recommendations.append("  * Reviewing cache settings for static content")
                recommendations.append("  * Implementing browser cache settings")
                recommendations.append("  * Using appropriate Cache-Control headers")
            
            # Performance-related recommendations
            avg_ttfb = perf_analysis['overall']['avg_ttfb']
            if avg_ttfb > 100:
                recommendations.append(
                    f"- TTFB ({avg_ttfb:.1f}ms) is higher than recommended. Consider:"
                )
                recommendations.append("  * Enabling Argo Smart Routing")
                recommendations.append("  * Optimizing origin server response times")
                recommendations.append("  * Using Cloudflare Workers for edge computing")
            
            error_rate = perf_analysis['overall']['error_rate']
            if error_rate > 1:
                recommendations.append(
                    f"- Error rate ({error_rate:.1f}%) is above threshold. Suggested actions:"
                )
                recommendations.append("  * Review server logs for error patterns")
                recommendations.append("  * Check origin server health")
                recommendations.append("  * Verify DNS configuration")
            
            # Add general recommendations
            recommendations.extend([
                "",
                "General Recommendations:",
                "- Monitor bandwidth usage patterns for optimization opportunities",
                "- Review content types with low cache hit ratios",
                "- Consider implementing rate limiting for high-traffic endpoints",
                "- Enable Brotli compression if not already enabled"
            ])
            
            return "\n".join(recommendations)
            
        except Exception as e:
            logger.error(f"Error creating recommendations: {str(e)}")
            return "\nError generating recommendations\n"
    
    def generate_summary(self, zones: list, start_time: datetime) -> str:
        """Generate overall analysis summary."""
        end_time = datetime.now(timezone.utc)
        
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        
        duration = end_time - start_time
        
        summary = f"""
        Cloudflare Analytics Summary
        ==========================

        Analysis completed at: {end_time}
        Total duration: {duration}
        Zones analyzed: {len(zones)}

        Zone List:
        ---------
        """ + "\n".join([f"- {zone['name']}" for zone in zones])
        
        summary_file = self.config.reports_dir / "analysis_summary.txt"
        with open(summary_file, 'w') as f:
            f.write(summary)
        
        return summary
