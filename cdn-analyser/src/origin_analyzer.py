import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging
from datetime import datetime, timezone
import traceback
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from typing import Dict, Optional
import json
from .types import (
    NetworkPathMetrics, PerformanceMetrics, ErrorMetrics, 
    ProcessedMetrics, SamplingMetrics
)

logger = logging.getLogger(__name__)

@dataclass
class OriginPerformanceMetrics:
    """Container for origin server performance metrics"""
    response_time: Dict[str, float]  # avg, p50, p95, p99, std
    request_volume: Dict[str, float]  # total, per_second
    failure_rates: Dict[str, float]  # error_rate, timeout_rate
    bandwidth: Dict[str, float]      # total_bytes, avg_response_size
    health_status: str               # healthy, degraded, critical

    def get(self, key: str, default: Optional[Dict] = None) -> Dict:
        """Add dict-like get method"""
        return getattr(self, key, default)

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return asdict(self)

class MetricsJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for metrics classes"""
    def default(self, obj):
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return super().default(obj)

@dataclass
class OriginPathMetrics:
    """Container for origin network path metrics"""
    origin_asn: str
    origin_asn_desc: str
    upper_tier: Optional[str]
    direct_requests: int
    tiered_requests: int
    path_latency: float

class OriginAnalyzer:
    """Enhanced analyzer for origin server performance metrics"""
    
    def __init__(self):
        self.ttfb_thresholds = {
            'good': 200,      # ms
            'warning': 500,   # ms
            'critical': 1000  # ms
        }
        self.error_rate_thresholds = {
            'good': 1,      # %
            'warning': 5,   # %
            'critical': 10  # %
        }
        self.timeout_thresholds = {
            'good': 0.1,    # %
            'warning': 1.0,  # %
            'critical': 5.0  # %
        }
        self.min_requests = 10  # Minimum requests for reliable analysis

    def analyze_origin_performance(self, df: pd.DataFrame) -> Optional[Dict]:
        """Analyze origin server performance with enhanced error handling"""
        try:
            if df is None or df.empty:
                logger.error("No data available for origin analysis")
                return self._empty_origin_metrics()

            # Validate and ensure consistent column structure
            df = self._validate_and_map_columns(df)

            # Filter for origin requests (non-cache hits and errors)
            origin_requests = df[
                (df['cache_status'].str.lower().isin(['miss', 'expired', 'bypass', 'dynamic'])) |
                (df['status_code'] >= 400)
            ].copy()

            if origin_requests.empty:
                logger.warning("No origin requests found in dataset")
                return self._empty_origin_metrics()

            # Calculate confidence score based on request volume
            confidence_score = min(1.0, len(origin_requests) / self.min_requests)
            
            logger.info(f"""
Origin Analysis Starting:
---------------------
Total Records: {len(df):,}
Origin Records: {len(origin_requests):,}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
Confidence Score: {confidence_score:.2f}

Basic Metrics:
- Average TTFB: {origin_requests['ttfb_avg'].mean():.2f}ms
- Average Origin Time: {origin_requests['origin_time_avg'].mean():.2f}ms
- Error Rate (4xx): {origin_requests['error_rate_4xx'].mean():.2f}%
- Error Rate (5xx): {origin_requests['error_rate_5xx'].mean():.2f}%
""")

            return {
                'overall_metrics': self._calculate_overall_metrics(origin_requests),
                'temporal_analysis': self._analyze_temporal_patterns(origin_requests),
                'geographic_analysis': self._analyze_geographic_patterns(origin_requests),
                'error_analysis': self._analyze_origin_errors(origin_requests),
                'endpoint_analysis': self._analyze_endpoint_performance(origin_requests),
                'network_analysis': self._analyze_network_paths(origin_requests),
                'metadata': {
                    'confidence_score': confidence_score,
                    'total_requests': len(origin_requests),
                    'unique_endpoints': origin_requests['endpoint'].nunique(),
                    'unique_paths': len(self._get_unique_paths(origin_requests)),
                    'time_range': {
                        'start': origin_requests['timestamp'].min().isoformat(),
                        'end': origin_requests['timestamp'].max().isoformat()
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error in origin performance analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_origin_metrics()

    def _calculate_overall_metrics(self, df: pd.DataFrame) -> OriginPerformanceMetrics:
        """Calculate overall origin performance metrics with enhanced error handling"""
        try:
            total_requests = df['requests_adjusted'].sum()
            error_requests = df[df['status'] >= 400]['requests_adjusted'].sum()
            timeout_requests = df[df['origin_time_avg'] > 30000]['requests_adjusted'].sum()

            # Calculate weighted averages using confidence scores
            weighted_times = {
                'avg': np.average(
                    df['origin_time_avg'],
                    weights=df['confidence_score'] * df['requests_adjusted']
                ),
                'median': df['origin_p50'].mean(),
                'p95': df['origin_p95'].mean(),
                'p99': df['origin_p99'].mean(),
                'std': df['origin_time_avg'].std()
            }

            time_range = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            requests_per_second = total_requests / max(time_range, 1)

            error_rate = (error_requests / total_requests * 100) if total_requests > 0 else 0
            timeout_rate = (timeout_requests / total_requests * 100) if total_requests > 0 else 0

            return OriginPerformanceMetrics(
                response_time={
                    'avg': float(weighted_times['avg']),
                    'median': float(weighted_times['median']),
                    'p95': float(weighted_times['p95']),
                    'p99': float(weighted_times['p99']),
                    'std': float(weighted_times['std'])
                },
                request_volume={
                    'total': int(total_requests),
                    'per_second': float(requests_per_second)
                },
                failure_rates={
                    'error_rate': float(error_rate),
                    'timeout_rate': float(timeout_rate)
                },
                bandwidth={
                    'total_bytes': float(df['bytes_adjusted'].sum()),
                    'avg_response_size': float(
                        df['bytes_adjusted'].sum() / total_requests if total_requests > 0 else 0
                    )
                },
                health_status=self._calculate_health_status(
                    weighted_times['avg'],
                    error_rate,
                    timeout_rate
                )
            )

        except Exception as e:
            logger.error(f"Error calculating overall metrics: {str(e)}")
            return self._empty_performance_metrics()

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns with enhanced statistical analysis"""
        try:
            # Check required columns
            required_cols = ['timestamp', 'origin_time_avg', 'requests_adjusted', 
                           'bytes_adjusted', 'status']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return self._empty_temporal_metrics()

            # Resample to 5-minute intervals
            df_time = df.set_index('timestamp')
            temporal_metrics = df_time.resample('5min').agg({
                'origin_time_avg': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100,
                'origin_p95': 'mean',
            }).reset_index()

            # Calculate moving averages
            temporal_metrics['response_time_ma'] = temporal_metrics['origin_time_avg'].rolling(
                window=3,
                min_periods=1
            ).mean()

            # Calculate trend
            temporal_metrics['response_time_trend'] = temporal_metrics['origin_time_avg'].diff()

            # Find peak periods
            peak_load = temporal_metrics.loc[temporal_metrics['requests_adjusted'].idxmax()] \
                if not temporal_metrics.empty else None
            peak_latency = temporal_metrics.loc[temporal_metrics['origin_time_avg'].idxmax()] \
                if not temporal_metrics.empty else None

            return {
                'time_series': {
                    str(row['timestamp']): {
                        'response_time': float(self._safe_numeric(row['origin_time_avg'])),
                        'response_time_ma': float(self._safe_numeric(row['response_time_ma'])),
                        'trend': float(self._safe_numeric(row['response_time_trend'])),
                        'requests': int(self._safe_numeric(row['requests_adjusted'])),
                        'error_rate': float(self._safe_numeric(row['status'])),
                        'bytes_transferred': float(self._safe_numeric(row['bytes_adjusted'])),
                        'p95_response_time': float(self._safe_numeric(row['origin_p95']))
                    }
                    for _, row in temporal_metrics.iterrows()
                },
                'peak_periods': {
                    'highest_load': {
                        'timestamp': str(peak_load['timestamp']) if peak_load is not None else None,
                        'requests': int(peak_load['requests_adjusted']) if peak_load is not None else 0,
                        'response_time': float(peak_load['origin_time_avg']) if peak_load is not None else 0
                    },
                    'worst_performance': {
                        'timestamp': str(peak_latency['timestamp']) if peak_latency is not None else None,
                        'response_time': float(peak_latency['origin_time_avg']) if peak_latency is not None else 0,
                        'requests': int(peak_latency['requests_adjusted']) if peak_latency is not None else 0
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_temporal_metrics()

    def _analyze_geographic_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic patterns with enhanced error detection"""
        try:
            # Check required columns
            required_cols = ['country', 'origin_time_avg', 'requests_adjusted', 
                            'error_rate_4xx', 'error_rate_5xx', 'confidence_score']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return {'countries': {}, 'summary': {'fastest_regions': [], 'slowest_regions': [], 'highest_error_regions': []}}

            # First aggregation
            base_metrics = df.groupby('country').agg({
                'origin_time_avg': ['mean', 'std', 'count'],
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'confidence_score': 'mean'
            })

            # Calculate performance scores - fixed to handle multi-dimensional data
            response_times = base_metrics[('origin_time_avg', 'mean')]
            error_rates = base_metrics['error_rate_5xx']['mean']  # Use mean for error rates
            confidence = base_metrics['confidence_score']['mean']  # Use mean for confidence
            
            # Now calculate performance scores with proper array dimensions
            perf_scores = (1 / response_times) * (1 - error_rates) * confidence
            perf_scores = pd.Series(perf_scores, index=base_metrics.index, name='perf_score').rank(pct=True)

            # Combine with base metrics
            geo_metrics = pd.concat([base_metrics, perf_scores], axis=1)

            return {
                'countries': {
                    str(country): {
                        'performance': {
                            'avg_response_time': float(metrics[('origin_time_avg', 'mean')]),
                            'std_dev': float(metrics[('origin_time_avg', 'std')]),
                            'sample_size': int(metrics[('origin_time_avg', 'count')]),
                        },
                        'traffic': {
                            'requests': int(metrics[('requests_adjusted', 'sum')]),
                            'bytes_gb': float(metrics[('bytes_adjusted', 'sum')] / (1024**3))
                        },
                        'reliability': {
                            'error_rate_4xx': float(metrics[('error_rate_4xx', 'mean')]),
                            'error_rate_5xx': float(metrics[('error_rate_5xx', 'mean')])
                        },
                        'perf_score': float(metrics['perf_score'])
                    }
                    for country, metrics in geo_metrics.iterrows()
                },
                'summary': {
                    'fastest_regions': list(
                        geo_metrics.nsmallest(5, ('origin_time_avg', 'mean')).index
                    ),
                    'slowest_regions': list(
                        geo_metrics.nlargest(5, ('origin_time_avg', 'mean')).index
                    ),
                    'highest_error_regions': list(
                        geo_metrics.nlargest(5, ('error_rate_5xx', 'mean')).index
                    )
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing geographic patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {'countries': {}, 'summary': {'fastest_regions': [], 'slowest_regions': [], 'highest_error_regions': []}}

    def _analyze_origin_errors(self, df: pd.DataFrame) -> Dict:
        """Analyze origin server errors with enhanced pattern detection"""
        try:
            error_df = df[df['status'] >= 500].copy()
            
            if error_df.empty:
                return self._empty_error_metrics()

            # Calculate error timing metrics
            error_timing = error_df.agg({
                'origin_time_avg': 'mean',
                'origin_p50': 'mean',
                'origin_p95': 'mean',
                'origin_p99': 'mean',
                'confidence_score': 'mean'
            })

            # Analyze error patterns over time
            hourly_errors = df.set_index('timestamp').resample('1h').agg({
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum',
                'confidence_score': 'mean'
            })

            return {
                'error_distribution': {
                    int(status): {
                        'count': int(count),
                        'percentage': float(count / len(error_df) * 100)
                    }
                    for status, count in error_df['status'].value_counts().items()
                },
                'timing': {
                    'avg_time_to_error': float(error_timing['origin_time_avg']),
                    'percentiles': {
                        'p50': float(error_timing['origin_p50']),
                        'p95': float(error_timing['origin_p95']),
                        'p99': float(error_timing['origin_p99'])
                    }
                },
                'patterns': {
                    'by_country': self._calculate_error_rates_by_dimension(df, 'country'),
                    'by_endpoint': self._calculate_error_rates_by_dimension(df, 'endpoint'),
                    'by_hour': {
                        str(timestamp): {
                            'error_rate': float(row['error_rate_5xx'] * 100),
                            'request_count': int(row['requests_adjusted']),
                            'confidence': float(row['confidence_score'])
                        }
                        for timestamp, row in hourly_errors.iterrows()
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing origin errors: {str(e)}")
            return self._empty_error_metrics()

    def _analyze_network_paths(self, df: pd.DataFrame) -> Dict:
        """Analyze network paths with enhanced path detection and default values."""
        try:
            # Map columns if they haven't been mapped yet
            df = self._validate_and_map_columns(df)

            # Use default values for missing columns
            df['client_asn'] = df.get('client_asn', 'unknown')
            df['client_asn_desc'] = df.get('client_asn_desc', 'Unknown ASN')
            df['origin_asn'] = df.get('origin_asn', 'unknown')
            df['origin_asn_desc'] = df.get('origin_asn_desc', 'Unknown ASN')
            df['upper_tier_colo'] = df.get('upper_tier_colo', None)
            
            # Group by network path components
            path_metrics = df.groupby(
                ['client_asn', 'client_asn_desc', 'upper_tier_colo'],
                observed=True
            ).agg({
                'requests_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'ttfb_avg': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            # Calculate path latency and tiered status
            path_metrics['path_latency'] = path_metrics['ttfb_avg'] - path_metrics['origin_time_avg']
            path_metrics['is_tiered'] = path_metrics['upper_tier_colo'].notna()

            # Create network paths dictionary
            network_paths = {}
            for _, path in path_metrics.iterrows():
                path_key = f"{path['client_asn']}_{path['upper_tier_colo'] or 'direct'}"
                
                network_paths[path_key] = {
                    'origin_asn': str(path['client_asn']),
                    'origin_asn_desc': str(path['client_asn_desc']),
                    'upper_tier': path['upper_tier_colo'],
                    'direct_requests': int(path['requests_adjusted']) if not path['is_tiered'] else 0,
                    'tiered_requests': int(path['requests_adjusted']) if path['is_tiered'] else 0,
                    'path_latency': float(path['path_latency'])
                }

            return {
                'paths': network_paths,
                'summary': {
                    'total_paths': len(network_paths),
                    'tiered_paths': sum(1 for m in network_paths.values() if m['upper_tier'] is not None),
                    'direct_paths': sum(1 for m in network_paths.values() if m['upper_tier'] is None),
                    'avg_path_latency': float(path_metrics['path_latency'].mean())
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing network paths: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_network_metrics()

    def _analyze_endpoint_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze endpoint performance with enhanced metrics"""
        try:
            # Check required columns
            required_cols = ['endpoint', 'origin_time_avg', 'requests_adjusted', 
                            'error_rate_4xx', 'error_rate_5xx', 'confidence_score']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return self._empty_endpoint_metrics()

            # First aggregation
            base_metrics = df.groupby('endpoint').agg({
                'origin_time_avg': ['mean', 'std', 'count'],
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'confidence_score': 'mean'
            })

            # Calculate load scores - fixed to handle multi-dimensional data
            requests = base_metrics[('requests_adjusted', 'sum')]
            response_times = base_metrics[('origin_time_avg', 'mean')]
            confidence = base_metrics[('confidence_score', 'mean')]
            
            # Calculate load scores properly
            load_scores = requests * response_times * confidence
            load_scores = pd.Series(load_scores, index=base_metrics.index, name='load_score').rank(pct=True)

            # Calculate reliability scores
            reliability_scores = 1 - (base_metrics[('error_rate_4xx', 'mean')] * 0.5 + 
                                    base_metrics[('error_rate_5xx', 'mean')])
            reliability_scores = pd.Series(reliability_scores, index=base_metrics.index, 
                                         name='reliability_score').rank(pct=True)

            # Combine metrics
            endpoint_metrics = pd.concat([base_metrics, load_scores, reliability_scores], axis=1)

            # Identify problematic endpoints
            problematic_mask = (
                (endpoint_metrics[('error_rate_5xx', 'mean')] > 0.05) | 
                (endpoint_metrics[('origin_time_avg', 'mean')] > 1000)
            ) & (endpoint_metrics[('confidence_score', 'mean')] > 0.8)
            
            problematic_endpoints = endpoint_metrics[problematic_mask].index.tolist()

            return {
                'endpoints': {
                    str(endpoint): {
                        'performance': {
                            'avg_response_time': float(metrics[('origin_time_avg', 'mean')]),
                            'std_dev': float(metrics[('origin_time_avg', 'std')]),
                            'sample_size': int(metrics[('origin_time_avg', 'count')])
                        },
                        'load': {
                            'requests': int(metrics[('requests_adjusted', 'sum')]),
                            'visits': int(metrics[('visits_adjusted', 'sum')]),
                            'bytes': float(metrics[('bytes_adjusted', 'sum')]),
                            'load_impact_score': float(metrics['load_score'])
                        },
                        'reliability': {
                            'error_rate_4xx': float(metrics[('error_rate_4xx', 'mean')]),
                            'error_rate_5xx': float(metrics[('error_rate_5xx', 'mean')]),
                            'reliability_score': float(metrics['reliability_score'])
                        },
                        'confidence': float(metrics[('confidence_score', 'mean')])
                    }
                    for endpoint, metrics in endpoint_metrics.iterrows()
                },
                'summary': {
                    'total_endpoints': len(endpoint_metrics),
                    'problematic_endpoints': problematic_endpoints,
                    'avg_response_time': float(endpoint_metrics[('origin_time_avg', 'mean')].mean()),
                    'high_impact_endpoints': list(
                        endpoint_metrics.nlargest(5, 'load_score').index
                    )
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing endpoint performance: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_endpoint_metrics()

    def _calculate_error_rates_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Calculate error rates for a specific dimension with confidence weighting"""
        try:
            if dimension not in df.columns:
                logger.warning(f"Dimension {dimension} not found in data")
                return {}

            metrics = df.groupby(dimension).agg({
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'confidence_score': 'mean'
            })
            
            # Calculate weighted error rates using confidence scores
            metrics['weighted_error_rate'] = metrics['error_rate_5xx'] * metrics['confidence_score']
            
            return {
                str(idx): {
                    'error_rate': float(row['error_rate_5xx'] * 100),
                    'weighted_error_rate': float(row['weighted_error_rate'] * 100),
                    'request_count': int(row['requests_adjusted']),
                    'avg_response_time': float(row['origin_time_avg']),
                    'confidence': float(row['confidence_score'])
                }
                for idx, row in metrics.iterrows()
            }
        except Exception as e:
            logger.error(f"Error calculating error rates for {dimension}: {str(e)}")
            return {}


    def _calculate_health_status(
        self,
        avg_response_time: float,
        error_rate: float,
        timeout_rate: float
    ) -> str:
        """Calculate overall health status with multiple indicators"""
        try:
            if (avg_response_time <= self.ttfb_thresholds['good'] and 
                error_rate <= self.error_rate_thresholds['good'] and
                timeout_rate <= self.timeout_thresholds['good']):
                return 'healthy'
            elif (avg_response_time >= self.ttfb_thresholds['critical'] or 
                  error_rate >= self.error_rate_thresholds['critical'] or
                  timeout_rate >= self.timeout_thresholds['critical']):
                return 'critical'
            elif (avg_response_time >= self.ttfb_thresholds['warning'] or 
                  error_rate >= self.error_rate_thresholds['warning'] or
                  timeout_rate >= self.timeout_thresholds['warning']):
                return 'degraded'
            return 'warning'
        except Exception as e:
            logger.error(f"Error calculating health status: {str(e)}")
            return 'unknown'

    def _get_unique_paths(self, df: pd.DataFrame) -> List[Dict]:
        """Get unique network paths with metrics"""
        try:
            required_columns = ['origin_asn', 'upperTierColoName', 'requests_adjusted', 
                              'origin_time_avg', 'confidence_score']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing columns for path analysis: {missing_columns}")
                return []

            paths = df.groupby(['origin_asn', 'upperTierColoName']).agg({
                'requests_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'confidence_score': 'mean'
            }).reset_index()
            
            return [
                {
                    'origin_asn': str(row['origin_asn']),
                    'upper_tier': row['upperTierColoName'],
                    'requests': int(row['requests_adjusted']),
                    'latency': float(row['origin_time_avg']),
                    'confidence': float(row['confidence_score'])
                }
                for _, row in paths.iterrows()
            ]
        except Exception as e:
            logger.error(f"Error getting unique paths: {str(e)}")
            return []


    def _empty_origin_metrics(self) -> Dict:
        """Return empty origin metrics structure"""
        current_time = datetime.now(timezone.utc).isoformat()
        return {
            'overall_metrics': self._empty_performance_metrics(),
            'temporal_analysis': {
                'time_series': {},
                'peak_periods': {
                    'highest_load': {'timestamp': None, 'requests': 0, 'response_time': 0},
                    'worst_performance': {'timestamp': None, 'response_time': 0, 'requests': 0}
                }
            },
            'geographic_analysis': {
                'countries': {},
                'summary': {
                    'fastest_regions': [],
                    'slowest_regions': [],
                    'highest_error_regions': []
                }
            },
            'error_analysis': self._empty_error_metrics(),
            'endpoint_analysis': self._empty_endpoint_metrics(),
            'network_analysis': self._empty_network_metrics(),
            'metadata': {
                'confidence_score': 0,
                'total_requests': 0,
                'unique_endpoints': 0,
                'unique_paths': 0,
                'time_range': {
                    'start': current_time,
                    'end': current_time
                }
            }
        }

    def _empty_performance_metrics(self) -> Dict:
        """Return empty performance metrics structure"""
        return {
            'response_time': {
                'avg': 0,
                'median': 0,
                'p95': 0,
                'p99': 0,
                'std': 0
            },
            'request_volume': {
                'total': 0,
                'per_second': 0
            },
            'failure_rates': {
                'error_rate': 0,
                'timeout_rate': 0
            },
            'bandwidth': {
                'total_bytes': 0,
                'avg_response_size': 0
            },
            'health_status': 'unknown'
        }

    def _empty_error_metrics(self) -> Dict:
        """Return empty error metrics structure"""
        return {
            'error_distribution': {},
            'timing': {
                'avg_time_to_error': 0,
                'percentiles': {'p50': 0, 'p95': 0, 'p99': 0}
            },
            'patterns': {
                'by_country': {},
                'by_endpoint': {},
                'by_hour': {}
            }
        }

    def _empty_network_metrics(self) -> Dict:
        """Return empty network metrics structure"""
        return {
            'paths': {},
            'summary': {
                'total_paths': 0,
                'tiered_paths': 0,
                'direct_paths': 0,
                'avg_path_latency': 0
            }
        }

    def _empty_endpoint_metrics(self) -> Dict:
        """Return empty endpoint metrics structure"""
        return {
            'endpoints': {},
            'summary': {
                'total_endpoints': 0,
                'problematic_endpoints': [],
                'avg_response_time': 0,
                'high_impact_endpoints': []
            }
        }

    def _safe_numeric(self, value: Any, default: Union[int, float] = 0) -> Union[int, float]:
        """Safely convert value to number with proper error handling."""
        try:
            if pd.isna(value):
                return default
            if isinstance(value, pd.Series):
                return float(value.iloc[0]) if len(value) > 0 else default
            return float(value)
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting value {value} to numeric: {str(e)}")
            return default

    def _validate_metrics(self, metrics: Dict) -> bool:
        """Validate metrics structure and values"""
        try:
            if not metrics:
                return False

            required_sections = [
                'overall_metrics',
                'temporal_analysis',
                'geographic_analysis',
                'error_analysis',
                'endpoint_analysis',
                'network_analysis',
                'metadata'
            ]

            missing_sections = [section for section in required_sections if section not in metrics]
            if missing_sections:
                logger.error(f"Missing required sections: {missing_sections}")
                return False

            # Validate overall metrics
            overall = metrics['overall_metrics']
            if not all(k in overall for k in ['response_time', 'request_volume', 'failure_rates']):
                logger.error("Invalid overall metrics structure")
                return False

            # Validate metadata
            metadata = metrics['metadata']
            if not all(k in metadata for k in ['confidence_score', 'total_requests', 'time_range']):
                logger.error("Invalid metadata structure")
                return False

            return True

        except Exception as e:
            logger.error(f"Error validating metrics: {str(e)}")
            return False

    def _format_metric_summary(self, metrics: Dict) -> str:
        """Format metrics summary for logging"""
        try:
            if not self._validate_metrics(metrics):
                return "Invalid metrics structure"

            overall = metrics['overall_metrics']
            meta = metrics['metadata']
            
            return f"""
Origin Performance Summary:
----------------------
Health Status: {overall.get('health_status', 'unknown').upper()}
Total Requests: {meta.get('total_requests', 0):,}
Confidence Score: {meta.get('confidence_score', 0):.2f}

Performance:
• Avg Response Time: {overall['response_time']['avg']:.2f}ms
• P95 Response Time: {overall['response_time']['p95']:.2f}ms
• Request Rate: {overall['request_volume']['per_second']:.2f}/s

Reliability:
• Error Rate: {overall['failure_rates']['error_rate']:.2f}%
• Timeout Rate: {overall['failure_rates']['timeout_rate']:.2f}%

Network:
• Unique Paths: {meta.get('unique_paths', 0)}
• Endpoints: {meta.get('unique_endpoints', 0)}

Time Range:
• Start: {meta['time_range']['start']}
• End: {meta['time_range']['end']}
"""
        except Exception as e:
            logger.error(f"Error formatting metric summary: {str(e)}")
            return "Error generating summary"

    def __str__(self) -> str:
        """String representation of the analyzer"""
        return f"OriginAnalyzer(ttfb_thresholds={self.ttfb_thresholds}, error_thresholds={self.error_rate_thresholds})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"OriginAnalyzer(min_requests={self.min_requests}, ttfb_thresholds={self.ttfb_thresholds})"

    def _validate_and_map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and map column names for consistency."""
        try:
            # Define expected columns and their default values
            expected_columns = {
                'client_asn': 'unknown',
                'client_asn_desc': 'Unknown ASN',
                'origin_asn': 'unknown',
                'origin_asn_desc': 'Unknown ASN',
                'colo_code': 'unknown',
                'country': 'unknown',
                'device_type': 'unknown',
                'protocol': 'unknown',
                'content_type': 'unknown',
                'status_code': 0,
                'endpoint': '/',
                'upper_tier_colo': None,
                'cache_status': 'unknown',
                'ttfb_avg': 0.0,
                'origin_time_avg': 0.0,
                'sampling_rate': 1.0,
                'visits': 0,
                'bytes': 0,
                'error_rate_4xx': 0.0,
                'error_rate_5xx': 0.0,
                'requests': 0,
                'requests_adjusted': 0.0,
                'visits_adjusted': 0.0,
                'bytes_adjusted': 0.0
            }

            # Create a copy of the DataFrame
            df_processed = df.copy()
            
            # Add any missing columns with default values
            for col, default in expected_columns.items():
                if col not in df_processed.columns:
                    df_processed[col] = default
                    logger.debug(f"Added missing column {col} with default value {default}")
            
            # Ensure numeric types for key metrics
            numeric_columns = {
                'status_code': 'int',
                'ttfb_avg': 'float',
                'origin_time_avg': 'float',
                'sampling_rate': 'float',
                'visits': 'int',
                'bytes': 'int',
                'error_rate_4xx': 'float',
                'error_rate_5xx': 'float',
                'requests': 'int',
                'requests_adjusted': 'float',
                'visits_adjusted': 'float',
                'bytes_adjusted': 'float'
            }
            
            for col, dtype in numeric_columns.items():
                if col in df_processed.columns:
                    df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
                    if dtype == 'int':
                        df_processed[col] = df_processed[col].fillna(0).astype(int)
                    else:
                        df_processed[col] = df_processed[col].fillna(0.0)

            # Ensure string types for categorical columns
            string_columns = [
                'client_asn', 'client_asn_desc', 'origin_asn', 'origin_asn_desc',
                'colo_code', 'country', 'device_type', 'protocol', 'content_type',
                'endpoint', 'upper_tier_colo', 'cache_status'
            ]
            
            for col in string_columns:
                if col in df_processed.columns:
                    df_processed[col] = df_processed[col].fillna('unknown').astype(str)

            # Log column validation results
            present_cols = set(df_processed.columns)
            expected_cols = set(expected_columns.keys())
            missing_cols = expected_cols - present_cols
            extra_cols = present_cols - expected_cols
            
            logger.debug(f"""
    Column validation complete:
    - Present columns: {len(present_cols)}
    - Expected columns: {len(expected_cols)}
    - Missing columns: {sorted(missing_cols) if missing_cols else 'None'}
    - Extra columns: {sorted(extra_cols) if extra_cols else 'None'}
    """)

            return df_processed

        except Exception as e:
            logger.error(f"Error validating columns: {str(e)}")
            logger.error(traceback.format_exc())
            return df

    def _parse_nested_json(self, raw_data: Dict) -> pd.DataFrame:
        """Parse nested JSON structure into flattened DataFrame."""
        try:
            # Extract request groups
            request_groups = self._extract_request_groups(raw_data)
            
            if not request_groups:
                logger.error("No request groups found in data")
                return pd.DataFrame()
                
            # Convert to DataFrame
            df = pd.DataFrame(request_groups)
            
            # Parse nested JSON columns
            nested_cols = ['dimensions', 'avg', 'sum', 'ratio', 'quantiles']
            for col in nested_cols:
                if col in df.columns:
                    # Convert string JSON to dict if needed
                    if df[col].dtype == 'object':
                        df[col] = df[col].apply(lambda x: x if isinstance(x, dict) else json.loads(x))
                        
            return df
            
        except Exception as e:
            logger.error(f"Error parsing nested JSON: {str(e)}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def _validate_required_columns(self, df: pd.DataFrame, required_cols: List[str]) -> bool:
        """Validate presence of required columns."""
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        return True
