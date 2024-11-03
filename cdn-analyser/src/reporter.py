from datetime import datetime, timezone
from typing import Dict, Optional, List
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
    
    def generate_summary(self, zones: List[Dict], start_time: datetime) -> str:
        """Generate overall analysis summary."""
        try:
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
            
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            return "Error generating analysis summary"
    
    def _create_report_header(self, zone_name: str) -> str:
        """Create the report header."""
        return f"""
        Cloudflare Analytics Report for {zone_name}
        {'=' * (24 + len(zone_name))}

        Generated at: {datetime.now(timezone.utc).isoformat()}
        """
    
    def _create_cache_section(self, analysis: Dict) -> str:
        """Create cache analysis section with sampling information."""
        try:
            header = """
            Cache Performance Summary
            -----------------------
            """
            
            sampling_info = f"""
            Sampling Information:
            - Total Requests (Estimated): {analysis['overall']['total_requests']:,}
            - Actually Sampled Requests: {analysis['overall']['sampled_requests']:,}
            - Average Sampling Rate: {analysis['overall']['average_sampling_rate']:.2f}%
            - Sampling Confidence: {analysis['overall']['sampling_confidence']:.2f}
            """
            
            overall = f"""
            Overall Statistics:
            - Cache Hit Ratio: {analysis['overall']['hit_ratio']:.2f}%
            - Total Bandwidth: {analysis['overall']['total_bytes'] / (1024 ** 3):.2f} GB
            """
            
            cache_distribution = """
            Cache Status Distribution:
            """
            for status, data in analysis.get('cache_status_distribution', {}).items():
                cache_distribution += (
                    f"- {status}: {data['percentage']:.2f}% "
                    f"({data['estimated_requests']:,} estimated requests, "
                    f"{data['sampled_requests']:,} sampled)\n"
                )
            
            time_analysis = """
            Temporal Analysis:
            """
            hourly_data = analysis.get('by_time', {}).get('hourly', {})
            for hour, data in sorted(hourly_data.items(), key=lambda x: int(x[0])):
                time_analysis += (
                    f"\nHour {hour}:"
                    f"\n  - Hit Ratio: {data['hit_ratio']:.2f}%"
                    f"\n  - Requests: {data['total_requests']:,} (estimated from {data['sampled_requests']:,} samples)"
                    f"\n  - Sampling Rate: {data['sampling_rate']:.2f}%"
                )
            
            return header + sampling_info + overall + cache_distribution + time_analysis
            
        except Exception as e:
            logger.error(f"Error creating cache section: {str(e)}")
            return "\nError generating cache analysis section\n"
    
    def _create_performance_section(self, analysis: Dict) -> str:
        """Create performance analysis section with sampling considerations."""
        try:
            header = """
            Performance Metrics Summary
            -------------------------
            """
            
            sampling_context = f"""
            Sampling Context:
            - Analysis based on {analysis.get('sampling_metrics', {}).get('total_samples', 0):,} sampled requests
            - Estimated total traffic: {analysis.get('sampling_metrics', {}).get('estimated_total', 0):,} requests
            - Overall sampling confidence: {analysis.get('sampling_metrics', {}).get('sampling_confidence', 0):.2f}
            """
            
            overall = f"""
            Overall Performance (weighted by sampling rate):
            - Average TTFB: {analysis.get('overall', {}).get('avg_ttfb', 0):.2f} ms
            - Average Origin Response Time: {analysis.get('overall', {}).get('avg_origin_time', 0):.2f} ms
            - Error Rate: {analysis.get('overall', {}).get('error_rate', 0):.2f}%
            
            Performance Percentiles (from sampled data):
            - TTFB (p95): {analysis.get('percentiles', {}).get('ttfb', {}).get('p95', 0):.2f} ms
            - TTFB (p99): {analysis.get('percentiles', {}).get('ttfb', {}).get('p99', 0):.2f} ms
            - Origin Response (p95): {analysis.get('percentiles', {}).get('origin_time', {}).get('p95', 0):.2f} ms
            - Origin Response (p99): {analysis.get('percentiles', {}).get('origin_time', {}).get('p99', 0):.2f} ms
            """
            
            geo_perf = """
            Geographic Performance (Top 5 Countries):
            """
            sorted_countries = sorted(
                analysis.get('by_country', {}).items(),
                key=lambda x: x[1].get('ttfb', float('inf')),
            )[:5]
            for country, data in sorted_countries:
                geo_perf += (
                    f"\n- {country}:"
                    f"\n  TTFB: {data.get('ttfb', 0):.2f} ms"
                    f"\n  Error Rate: {data.get('error_rate', 0):.2f}%"
                    f"\n  Sampling Rate: {data.get('sampling_rate', 0):.2f}%"
                    f"\n  Requests: {data.get('total_requests', 0):,} "
                    f"(estimated from {data.get('sampled_requests', 0):,} samples)"
                )
            
            return header + sampling_context + overall + geo_perf
            
        except Exception as e:
            logger.error(f"Error creating performance section: {str(e)}")
            return "\nError generating performance analysis section\n"
    
    def _create_recommendations(self, cache_analysis: Dict, perf_analysis: Dict) -> str:
        """Create recommendations based on analysis with sampling confidence."""
        try:
            recommendations = ["\nRecommendations", "-" * 15]
            
            # Add sampling quality assessment
            sampling_confidence = cache_analysis.get('sampling_metrics', {}).get('sampling_confidence', 0)
            recommendations.append(
                f"\nAnalysis Confidence Assessment:"
                f"\n- Overall sampling confidence: {sampling_confidence:.2f}"
            )
            
            if sampling_confidence < 0.9:
                recommendations.append(
                    "- Consider increasing sampling rate for more accurate analysis"
                    "\n- Focus on trends rather than exact numbers due to low sampling rate"
                )
            
            # Cache-related recommendations
            hit_ratio = cache_analysis.get('overall', {}).get('hit_ratio', 0)
            if hit_ratio < 70:
                recommendations.append(
                    f"\nCache Performance Recommendations:"
                    f"\n- Current cache hit ratio ({hit_ratio:.1f}%) is below optimal levels. Consider:"
                )
                recommendations.append("  * Reviewing cache settings for static content")
                recommendations.append("  * Implementing browser cache settings")
                recommendations.append("  * Using appropriate Cache-Control headers")
            
            # Performance-related recommendations
            avg_ttfb = perf_analysis.get('overall', {}).get('avg_ttfb', 0)
            if avg_ttfb > 100:
                recommendations.append(
                    f"\nPerformance Recommendations:"
                    f"\n- TTFB ({avg_ttfb:.1f}ms) is higher than recommended. Consider:"
                )
                recommendations.append("  * Enabling Argo Smart Routing")
                recommendations.append("  * Optimizing origin server response times")
                recommendations.append("  * Using Cloudflare Workers for edge computing")
            
            error_rate = perf_analysis.get('overall', {}).get('error_rate', 0)
            if error_rate > 1:
                recommendations.append(
                    f"\nError Rate Recommendations:"
                    f"\n- Error rate ({error_rate:.1f}%) is above threshold. Suggested actions:"
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
