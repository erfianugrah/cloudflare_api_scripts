import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging
from datetime import datetime, timezone
import traceback

from .edge_analyzer import EdgeAnalyzer
from .origin_analyzer import OriginAnalyzer
from .types import NetworkPathMetrics, PerformanceMetrics, ErrorMetrics, ProcessedMetrics

logger = logging.getLogger(__name__)

class Analyzer:
    """Main analyzer that coordinates edge and origin analysis"""
    
    def __init__(self, config):
        self.config = config
        self.edge_analyzer = EdgeAnalyzer()
        self.origin_analyzer = OriginAnalyzer()

    def analyze_metrics(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Main analysis entry point that coordinates edge and origin analysis"""
        try:
            if df is None or df.empty:
                logger.error(f"No data available for analysis of zone {zone_name}")
                return None

            # Log analysis start
            logger.info(f"""
Analysis Starting:
---------------
Zone: {zone_name}
Total Records: {len(df)}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
""")

            # Calculate sampling metrics first
            sampling_metrics = self._calculate_sampling_metrics(df)

            # Perform edge analysis
            edge_analysis = self.edge_analyzer.analyze_edge_performance(df)
            if not edge_analysis:
                logger.error("Edge analysis failed")
                return None

            # Perform origin analysis
            origin_analysis = self.origin_analyzer.analyze_origin_performance(df)
            if not origin_analysis:
                logger.error("Origin analysis failed")
                return None

            # Calculate cache metrics
            cache_analysis = self._calculate_cache_metrics(df)

            # Combine results
            analysis_result = {
                'zone_name': zone_name,
                'edge_analysis': edge_analysis,
                'origin_analysis': origin_analysis,
                'cache_analysis': cache_analysis,
                'sampling_metrics': sampling_metrics,
                'raw_data': df,
                'metadata': {
                    'analysis_time': datetime.now(timezone.utc).isoformat(),
                    'total_records': len(df),
                    'total_requests': int(df['requests_adjusted'].sum()),
                    'time_range': {
                        'start': df['timestamp'].min().isoformat(),
                        'end': df['timestamp'].max().isoformat()
                    }
                }
            }

            # Log analysis completion
            self._log_analysis_summary(analysis_result)

            return analysis_result

        except Exception as e:
            logger.error(f"Error in metrics analysis for {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _calculate_sampling_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate sampling-related metrics with confidence scores"""
        try:
            # Calculate sampling rates
            sampling_rates = df['sampling_rate'].agg(['min', 'max', 'mean', 'median'])
            total_raw = df['requests'].sum()
            total_adjusted = df['requests_adjusted'].sum()
            
            # Calculate confidence scores based on sampling and volume
            confidence_scores = df.apply(
                lambda row: self._calculate_confidence_score(
                    row['sampling_rate'],
                    row['requests']
                ),
                axis=1
            )
            
            return {
                'sampling_rates': {
                    'min': float(sampling_rates['min'] * 100),
                    'max': float(sampling_rates['max'] * 100),
                    'mean': float(sampling_rates['mean'] * 100),
                    'median': float(sampling_rates['median'] * 100)
                },
                'confidence_scores': {
                    'min': float(confidence_scores.min()),
                    'max': float(confidence_scores.max()),
                    'mean': float(confidence_scores.mean()),
                    'median': float(confidence_scores.median())
                },
                'requests': {
                    'raw': int(total_raw),
                    'adjusted': int(total_adjusted),
                    'ratio': float(total_adjusted / total_raw if total_raw > 0 else 1)
                }
            }
        except Exception as e:
            logger.error(f"Error calculating sampling metrics: {str(e)}")
            return {
                'sampling_rates': {'min': 0, 'max': 0, 'mean': 0, 'median': 0},
                'confidence_scores': {'min': 0, 'max': 0, 'mean': 0, 'median': 0},
                'requests': {'raw': 0, 'adjusted': 0, 'ratio': 1}
            }

    def _calculate_confidence_score(self, sampling_rate: float, requests: float) -> float:
        """Calculate confidence score based on sampling rate and request volume"""
        try:
            # Base confidence from sampling rate
            if sampling_rate >= 0.5:
                base_confidence = 0.99
            elif sampling_rate >= 0.1:
                base_confidence = 0.95 if requests >= 1000 else 0.90
            elif sampling_rate >= 0.01:
                base_confidence = 0.90 if requests >= 10000 else 0.85
            else:
                base_confidence = 0.80 if requests >= 100000 else 0.75
            
            # Adjust for sample size
            size_factor = min(1.0, np.log10(float(requests) + 1) / 6)
            return base_confidence * size_factor

        except Exception as e:
            logger.error(f"Error calculating confidence score: {str(e)}")
            return 0.0

    def _calculate_cache_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate cache-related metrics"""
        try:
            total_requests = df['requests_adjusted'].sum()
            cache_hits = df[df['cache_status'].str.lower() == 'hit']['requests_adjusted'].sum()
            total_bytes = df['bytes_adjusted'].sum()
            cache_hit_bytes = df[df['cache_status'].str.lower() == 'hit']['bytes_adjusted'].sum()

            return {
                'overall': {
                    'hit_ratio': float(cache_hits / total_requests * 100 if total_requests > 0 else 0),
                    'total_requests': int(total_requests),
                    'total_visits': int(df['visits_adjusted'].sum()),
                    'total_bytes': float(total_bytes),
                    'bandwidth_saving': float(cache_hit_bytes / total_bytes * 100 if total_bytes > 0 else 0)
                },
                'status_distribution': self._get_cache_distribution(df)
            }
        except Exception as e:
            logger.error(f"Error calculating cache metrics: {str(e)}")
            return {
                'overall': {
                    'hit_ratio': 0,
                    'total_requests': 0,
                    'total_visits': 0,
                    'total_bytes': 0,
                    'bandwidth_saving': 0
                },
                'status_distribution': {}
            }

    def _get_cache_distribution(self, df: pd.DataFrame) -> Dict:
        """Get cache status distribution"""
        try:
            total_requests = df['requests_adjusted'].sum()
            total_bytes = df['bytes_adjusted'].sum()

            distribution = {}
            for status in df['cache_status'].unique():
                status_df = df[df['cache_status'] == status]
                requests = status_df['requests_adjusted'].sum()
                bytes_val = status_df['bytes_adjusted'].sum()

                distribution[str(status)] = {
                    'requests': int(requests),
                    'requests_percentage': float(requests / total_requests * 100 if total_requests > 0 else 0),
                    'bytes': float(bytes_val),
                    'bytes_percentage': float(bytes_val / total_bytes * 100 if total_bytes > 0 else 0)
                }

            return distribution

        except Exception as e:
            logger.error(f"Error calculating cache distribution: {str(e)}")
            return {}

    def _log_analysis_summary(self, analysis: Dict) -> None:
        """Log analysis summary with both edge and origin metrics"""
        try:
            edge = analysis['edge_analysis'].get('edge_metrics', {})
            edge_response = edge.get('edge_response_time', {})
            cache = analysis['cache_analysis'].get('overall', {})
            origin = analysis['origin_analysis'].get('overall_metrics', {})
            
            logger.info(f"""
Analysis Complete:
---------------
Zone: {analysis['zone_name']}

Edge Performance:
• Avg TTFB: {edge_response.get('avg', 0):.2f}ms
• Cache Hit Ratio: {cache.get('hit_ratio', 0):.2f}%
• Edge Error Rate: {edge.get('error_rates', {}).get('total_error_rate', 0):.2f}%
• Protocol Distribution: {analysis['edge_analysis'].get('protocol_metrics', {}).get('summary', {})}

Origin Performance:
• Avg Response Time: {origin.response_time.get('avg', 0):.2f}ms
• Error Rate: {origin.failure_rates.get('error_rate', 0):.2f}%
• Health Status: {origin.health_status}

Traffic Metrics:
• Total Requests: {analysis['metadata']['total_requests']:,}
• Time Range: {analysis['metadata']['time_range']['start']} to {analysis['metadata']['time_range']['end']}
""")

        except Exception as e:
            logger.error(f"Error logging analysis summary: {str(e)}")
            logger.error(traceback.format_exc())

    def __str__(self) -> str:
        """String representation of the analyzer"""
        return f"Analyzer(edge_analyzer={self.edge_analyzer}, origin_analyzer={self.origin_analyzer})"
