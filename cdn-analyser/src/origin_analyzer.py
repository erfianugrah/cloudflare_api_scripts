import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from datetime import datetime, timezone
import traceback

logger = logging.getLogger(__name__)

def safe_series_conversion(series_or_value, convert_type):
    """Safely convert a Series or single value to float/int."""
    try:
        if isinstance(series_or_value, pd.Series):
            if len(series_or_value) > 0:
                return convert_type(series_or_value.iloc[0])
            return convert_type(0)
        return convert_type(series_or_value)
    except (ValueError, TypeError) as e:
        logger.warning(f"Error converting value {series_or_value}: {str(e)}")
        return convert_type(0)

class OriginAnalyzer:
    """Analyzer for origin server performance metrics."""
    
    def __init__(self):
        self.cache_categories = {
            'MISS': ['miss', 'expired', 'updating'],  # Only analyze uncached requests
            'ERROR': ['error']
        }

    def analyze_origin_performance(self, df: pd.DataFrame) -> Dict:
        """Main entry point for origin performance analysis."""
        try:
            if df is None or df.empty:
                logger.error("No data available for origin analysis")
                return self._empty_origin_metrics()

            # Filter for uncached requests
            origin_requests = df[df['cache_status'].isin([status 
                for category in ['MISS', 'ERROR']
                for status in self.cache_categories[category]]
            )]

            if origin_requests.empty:
                logger.warning("No origin requests found in dataset")
                return self._empty_origin_metrics()

            return {
                'overall_metrics': self._calculate_overall_metrics(origin_requests),
                'temporal_analysis': self._analyze_temporal_patterns(origin_requests),
                'geographic_analysis': self._analyze_geographic_patterns(origin_requests),
                'error_analysis': self._analyze_origin_errors(origin_requests),
                'endpoint_analysis': self._analyze_endpoint_performance(origin_requests),
                'protocol_impact': self._analyze_protocol_impact(origin_requests)
            }

        except Exception as e:
            logger.error(f"Error in origin performance analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_origin_metrics()

    def _calculate_overall_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate overall origin performance metrics."""
        try:
            total_requests = df['visits_adjusted'].sum()
            error_requests = df[df['status'] >= 500]['visits_adjusted'].sum()
            timeout_requests = df[df['origin_time_avg'] > 30000]['visits_adjusted'].sum()

            return {
                'response_time': {
                    'avg': float(df['origin_time_avg'].mean()),
                    'median': safe_series_conversion(df['origin_p50'], float),
                    'p95': safe_series_conversion(df['origin_p95'], float),
                    'p99': safe_series_conversion(df['origin_p99'], float),
                    'std_dev': float(df['origin_time_avg'].std())
                },
                'request_volume': {
                    'total_requests': int(total_requests),
                    'requests_per_second': float(total_requests / 
                        max((df['timestamp'].max() - df['timestamp'].min()).total_seconds(), 1))
                },
                'failure_rates': {
                    'error_rate': float(error_requests / max(total_requests, 1) * 100),
                    'timeout_rate': float(timeout_requests / max(total_requests, 1) * 100)
                },
                'bandwidth': {
                    'total_bytes': float(df['bytes_adjusted'].sum()),
                    'avg_response_size': float(df['bytes_adjusted'].mean())
                }
            }
        except Exception as e:
            logger.error(f"Error calculating overall metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_overall_metrics()

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns in origin performance."""
        try:
            # Resample to 5-minute intervals
            df_time = df.set_index('timestamp')
            temporal_metrics = df_time.resample('5min').agg({
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            }).reset_index()

            # Calculate moving averages
            window_size = 6  # 30-minute window
            temporal_metrics['origin_time_ma'] = temporal_metrics['origin_time_avg'].rolling(
                window=window_size, min_periods=1).mean()

            return {
                'time_series': {
                    str(row['timestamp']): {
                        'response_time': float(row['origin_time_avg']),
                        'response_time_ma': float(row['origin_time_ma']),
                        'requests': int(row['visits_adjusted']),
                        'error_rate': float(row['status']),
                        'bytes_transferred': float(row['bytes_adjusted'])
                    }
                    for _, row in temporal_metrics.iterrows()
                },
                'peak_periods': {
                    'highest_load': str(temporal_metrics.loc[
                        temporal_metrics['visits_adjusted'].idxmax(), 'timestamp'
                    ] if not temporal_metrics.empty else pd.Timestamp.now()),
                    'worst_performance': str(temporal_metrics.loc[
                        temporal_metrics['origin_time_avg'].idxmax(), 'timestamp'
                    ] if not temporal_metrics.empty else pd.Timestamp.now())
                }
            }
        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _analyze_geographic_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic patterns in origin performance."""
        try:
            geo_metrics = df.groupby('country').agg({
                'origin_time_avg': ['mean', 'std'],
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            })

            # Reset index to make country a column
            geo_metrics = geo_metrics.reset_index()

            # Flatten columns
            geo_metrics.columns = ['country', 'origin_time_mean', 'origin_time_std', 
                                 'visits', 'bytes', 'error_rate']

            return {
                'countries': {
                    str(row['country']): {
                        'response_time': {
                            'avg': float(row['origin_time_mean']),
                            'std_dev': float(row['origin_time_std'])
                        },
                        'requests': int(row['visits']),
                        'error_rate': float(row['error_rate']),
                        'bandwidth': float(row['bytes'])
                    }
                    for _, row in geo_metrics.iterrows()
                },
                'regional_summary': {
                    'fastest_regions': list(geo_metrics.nsmallest(5, 'origin_time_mean')['country']),
                    'slowest_regions': list(geo_metrics.nlargest(5, 'origin_time_mean')['country']),
                    'highest_error_regions': list(geo_metrics.nlargest(5, 'error_rate')['country'])
                }
            }
        except Exception as e:
            logger.error(f"Error analyzing geographic patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _analyze_origin_errors(self, df: pd.DataFrame) -> Dict:
        """Analyze origin server errors and failures."""
        try:
            error_df = df[df['status'] >= 500].copy()
            
            # Handle empty error DataFrame
            if error_df.empty:
                return {
                    'error_distribution': {},
                    'error_timing': {
                        'avg_time_to_error': 0.0,
                        'error_percentiles': {'p50': 0.0, 'p95': 0.0, 'p99': 0.0}
                    },
                    'error_patterns': {'by_country': {}, 'by_endpoint': {}}
                }

            return {
                'error_distribution': dict(error_df['status'].value_counts()),
                'error_timing': {
                    'avg_time_to_error': float(error_df['origin_time_avg'].mean()),
                    'error_percentiles': {
                        'p50': safe_series_conversion(error_df['origin_p50'], float),
                        'p95': safe_series_conversion(error_df['origin_p95'], float),
                        'p99': safe_series_conversion(error_df['origin_p99'], float)
                    }
                },
                'error_patterns': {
                    'by_country': dict(df.groupby('country')['status']
                        .apply(lambda x: (x >= 500).mean() * 100)),
                    'by_endpoint': dict(df.groupby('endpoint')['status']
                        .apply(lambda x: (x >= 500).mean() * 100)
                        .nlargest(10))
                }
            }
        except Exception as e:
            logger.error(f"Error analyzing origin errors: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _analyze_endpoint_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze origin performance by endpoint."""
        try:
            endpoint_metrics = df.groupby('endpoint').agg({
                'origin_time_avg': ['mean', 'std', 'count'],
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            })

            # Reset index to make endpoint a column
            endpoint_metrics = endpoint_metrics.reset_index()

            # Flatten columns
            endpoint_metrics.columns = ['endpoint', 'origin_time_mean', 'origin_time_std',
                                     'origin_time_count', 'visits', 'bytes', 'error_rate']

            # Calculate load impact score
            endpoint_metrics['load_score'] = (
                endpoint_metrics['visits'] * 
                endpoint_metrics['origin_time_mean'] * 
                endpoint_metrics['bytes']
            ).rank(pct=True)

            return {
                'endpoints': {
                    str(row['endpoint']): {
                        'performance': {
                            'avg_response_time': float(row['origin_time_mean']),
                            'std_dev': float(row['origin_time_std']),
                            'sample_size': int(row['origin_time_count'])
                        },
                        'load': {
                            'requests': int(row['visits']),
                            'bytes': float(row['bytes']),
                            'load_impact_score': float(row['load_score'])
                        },
                        'reliability': {
                            'error_rate': float(row['error_rate']),
                            'availability': float(100 - row['error_rate'])
                        }
                    }
                    for _, row in endpoint_metrics.iterrows()
                },
                'top_impacting_endpoints': list(
                    endpoint_metrics.nlargest(10, 'load_score')['endpoint']
                )
            }
        except Exception as e:
            logger.error(f"Error analyzing endpoint performance: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _analyze_protocol_impact(self, df: pd.DataFrame) -> Dict:
        """Analyze impact of different protocols on origin performance."""
        try:
            protocol_metrics = df.groupby('protocol').agg({
                'origin_time_avg': ['mean', 'std', 'count'],
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            })

            # Reset index to make protocol a column
            protocol_metrics = protocol_metrics.reset_index()

            # Flatten columns
            protocol_metrics.columns = ['protocol', 'origin_time_mean', 'origin_time_std',
                                     'origin_time_count', 'visits', 'bytes', 'error_rate']

            if protocol_metrics.empty:
                return {
                    'protocols': {},
                    'protocol_comparison': {
                        'fastest_protocol': 'unknown',
                        'most_reliable': 'unknown'
                    }
                }

            return {
                'protocols': {
                    str(row['protocol']): {
                        'performance': {
                            'avg_response_time': float(row['origin_time_mean']),
                            'std_dev': float(row['origin_time_std']),
                            'sample_size': int(row['origin_time_count'])
                        },
                        'usage': {
                            'request_count': int(row['visits']),
                            'bytes_transferred': float(row['bytes'])
                        },
                        'reliability': {
                            'error_rate': float(row['error_rate'])
                        }
                    }
                    for _, row in protocol_metrics.iterrows()
                },
                'protocol_comparison': {
                    'fastest_protocol': str(protocol_metrics.loc[
                        protocol_metrics['origin_time_mean'].idxmin(), 'protocol'
                    ]),
                    'most_reliable': str(protocol_metrics.loc[
                        protocol_metrics['error_rate'].idxmin(), 'protocol'
                    ])
                }
            }
        except Exception as e:
            logger.error(f"Error analyzing protocol impact: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _empty_overall_metrics(self) -> Dict:
        """Return empty overall metrics structure."""
        return {
            'response_time': {
                'avg': 0.0,
                'median': 0.0,
                'p95': 0.0,
                'p99': 0.0,
                'std_dev': 0.0
            },
            'request_volume': {
                'total_requests': 0,
                'requests_per_second': 0.0
            },
            'failure_rates': {
                'error_rate': 0.0,
                'timeout_rate': 0.0
            },
            'bandwidth': {
                'total_bytes': 0,
                'avg_response_size': 0.0
            }
        }

    def _empty_origin_metrics(self) -> Dict:
        """Return empty origin metrics structure."""
        return {
            'overall_metrics': self._empty_overall_metrics(),
            'temporal_analysis': {},
            'geographic_analysis': {},
            'error_analysis': {},
            'endpoint_analysis': {},
            'protocol_impact': {}
        }
