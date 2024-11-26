import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging
from datetime import datetime, timezone
import traceback
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class OriginPerformanceMetrics:
    """Container for origin server performance metrics"""
    response_time: Dict[str, float]  # avg, median, p95, p99, std_dev
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

    def analyze_origin_performance(self, df: pd.DataFrame) -> Optional[Dict]:
        """Analyze origin server performance with enhanced error handling"""
        try:
            if df is None or df.empty:
                logger.error("No data available for origin analysis")
                return None

            # Filter for origin requests (non-cache hits and errors)
            origin_requests = df[
                (df['cache_status'].str.lower().isin(['miss', 'expired', 'bypass', 'dynamic'])) |
                (df['status'] >= 400)
            ].copy()

            if origin_requests.empty:
                logger.warning("No origin requests found in dataset")
                return self._empty_origin_metrics()

            # Calculate confidence score based on request volume
            confidence_score = min(1.0, len(origin_requests) / self.min_requests)
            
            logger.info(f"""
Origin Analysis Starting:
---------------------
Total Records: {len(df)}
Origin Records: {len(origin_requests)}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
Confidence Score: {confidence_score:.2f}

Basic Metrics:
- Average TTFB: {origin_requests['ttfb_avg'].mean():.2f}ms
- Average Origin Time: {origin_requests['origin_time_avg'].mean():.2f}ms
- Error Rate (4xx): {origin_requests['error_rate_4xx'].mean():.2f}%
- Error Rate (5xx): {origin_requests['error_rate_5xx'].mean():.2f}%
""")

            overall_metrics = self._calculate_overall_metrics(origin_requests)
            if not overall_metrics:
                return None

            return {
                'overall_metrics': overall_metrics,
                'temporal_analysis': self._analyze_temporal_patterns(origin_requests),
                'geographic_analysis': self._analyze_geographic_patterns(origin_requests),
                'error_analysis': self._analyze_origin_errors(origin_requests),
                'endpoint_analysis': self._analyze_endpoint_performance(origin_requests),
                'network_analysis': self._analyze_network_paths(origin_requests),
                'metadata': {
                    'confidence_score': confidence_score,
                    'total_records': len(origin_requests),
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
            return None

    def _calculate_overall_metrics(self, df: pd.DataFrame) -> OriginPerformanceMetrics:
        """Calculate overall origin performance metrics with enhanced error handling"""
        try:
            total_requests = df['requests_adjusted'].sum()
            error_requests = df[df['status'] >= 400]['requests_adjusted'].sum()
            timeout_requests = df[df['origin_time_avg'] > 30000]['requests_adjusted'].sum()

            # Calculate weighted averages using confidence scores
            origin_times = df.agg({
                'origin_time_avg': 'mean',
                'origin_p50': 'mean',
                'origin_p95': 'mean',
                'origin_p99': 'mean'
            })

            time_range = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            requests_per_second = total_requests / max(time_range, 1)

            error_rate = (error_requests / total_requests * 100) if total_requests > 0 else 0
            timeout_rate = (timeout_requests / total_requests * 100) if total_requests > 0 else 0

            return OriginPerformanceMetrics(
                response_time={
                    'avg': float(self._safe_series_value(origin_times['origin_time_avg'])),
                    'median': float(self._safe_series_value(origin_times['origin_p50'])),
                    'p95': float(self._safe_series_value(origin_times['origin_p95'])),
                    'p99': float(self._safe_series_value(origin_times['origin_p99'])),
                    'std_dev': float(df['origin_time_avg'].std())
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
                    self._safe_series_value(origin_times['origin_time_avg']),
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
            # Resample to 5-minute intervals
            df_time = df.set_index('timestamp')
            temporal_metrics = df_time.resample('5min').agg({
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            }).reset_index()

            # Calculate moving averages
            temporal_metrics['response_time_ma'] = temporal_metrics['origin_time_avg'].rolling(
                window=3,
                min_periods=1
            ).mean()

            # Find peak periods
            peak_load_idx = temporal_metrics['requests_adjusted'].idxmax()
            peak_latency_idx = temporal_metrics['origin_time_avg'].idxmax()

            peak_load = temporal_metrics.iloc[peak_load_idx] if not pd.isna(peak_load_idx) else None
            peak_latency = temporal_metrics.iloc[peak_latency_idx] if not pd.isna(peak_latency_idx) else None

            return {
                'time_series': {
                    str(row['timestamp']): {
                        'response_time': float(self._safe_series_value(row['origin_time_avg'])),
                        'response_time_ma': float(self._safe_series_value(row['response_time_ma'])),
                        'requests': int(self._safe_series_value(row['requests_adjusted'])),
                        'bytes': float(self._safe_series_value(row['bytes_adjusted'])),
                        'error_rate': float(self._safe_series_value(row['status']))
                    }
                    for _, row in temporal_metrics.iterrows()
                },
                'peak_periods': {
                    'highest_load': {
                        'timestamp': str(peak_load['timestamp']) if peak_load is not None else None,
                        'requests': int(self._safe_series_value(peak_load['requests_adjusted'])) if peak_load is not None else 0,
                        'response_time': float(self._safe_series_value(peak_load['origin_time_avg'])) if peak_load is not None else 0
                    },
                    'worst_performance': {
                        'timestamp': str(peak_latency['timestamp']) if peak_latency is not None else None,
                        'response_time': float(self._safe_series_value(peak_latency['origin_time_avg'])) if peak_latency is not None else 0,
                        'requests': int(self._safe_series_value(peak_latency['requests_adjusted'])) if peak_latency is not None else 0
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {str(e)}")
            return {'time_series': {}, 'peak_periods': {}}

    def _analyze_geographic_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic patterns with proper Series handling"""
        try:
            if 'country' not in df.columns:
                logger.error("Country column missing from data")
                return {'countries': {}, 'summary': {}}

            # Group by country with proper aggregation
            geo_metrics = df.groupby('country').agg({
                'origin_time_avg': ['mean', 'std'],
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            countries = {}
            for _, row in geo_metrics.iterrows():
                country = str(row['country'])
                request_count = self._safe_series_value(row['requests_adjusted'])
                error_4xx = self._safe_series_value(row['error_rate_4xx'])
                error_5xx = self._safe_series_value(row['error_rate_5xx'])

                countries[country] = {
                    'performance': {
                        'avg_response_time': float(self._safe_series_value(row['origin_time_avg']['mean'])),
                        'std_dev': float(self._safe_series_value(row['origin_time_avg']['std'])),
                        'sample_size': int(request_count)
                    },
                    'traffic': {
                        'requests': int(request_count)
                    },
                    'reliability': {
                        'error_rate_4xx': float(error_4xx * 100),
                        'error_rate_5xx': float(error_5xx * 100)
                    },
                    'confidence': float(self._safe_series_value(row['confidence_score']))
                }

            # Sort countries for summary
            sorted_by_response = sorted(
                countries.items(),
                key=lambda x: x[1]['performance']['avg_response_time']
            )
            sorted_by_errors = sorted(
                countries.items(),
                key=lambda x: (
                    x[1]['reliability']['error_rate_4xx'] + 
                    x[1]['reliability']['error_rate_5xx']
                ),
                reverse=True
            )

            return {
                'countries': countries,
                'summary': {
                    'fastest_regions': [country for country, _ in sorted_by_response[:5]],
                    'slowest_regions': [country for country, _ in sorted_by_response[-5:]],
                    'highest_error_regions': [country for country, _ in sorted_by_errors[:5]]
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing geographic patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {'countries': {}, 'summary': {}}

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
                'origin_p99': 'mean'
            })

            # Analyze error patterns over time
            hourly_errors = df.set_index('timestamp').resample('1h').agg({
                'status': lambda x: (x >= 500).mean() * 100,
                'requests_adjusted': 'sum',
                'confidence_score': 'mean'
            }).reset_index()

            # Calculate error distribution
            error_distribution = {}
            status_counts = error_df['status'].value_counts()
            total_errors = status_counts.sum()

            for status, count in status_counts.items():
                error_distribution[int(status)] = {
                    'count': int(count),
                    'percentage': float(count / total_errors * 100) if total_errors > 0 else 0
                }

            return {
                'error_distribution': error_distribution,
                'timing': {
                    'avg_time_to_error': float(self._safe_series_value(error_timing['origin_time_avg'])),
                    'percentiles': {
                        'p50': float(self._safe_series_value(error_timing['origin_p50'])),
                        'p95': float(self._safe_series_value(error_timing['origin_p95'])),
                        'p99': float(self._safe_series_value(error_timing['origin_p99']))
                    }
                },
                'temporal': {
                    str(row['timestamp']): {
                        'error_rate': float(self._safe_series_value(row['status'])),
                        'request_count': int(self._safe_series_value(row['requests_adjusted'])),
                        'confidence': float(self._safe_series_value(row['confidence_score']))
                    }
                    for _, row in hourly_errors.iterrows()
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing origin errors: {str(e)}")
            return self._empty_error_metrics()

    def _analyze_endpoint_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze endpoint performance with enhanced metric calculations"""
        try:
            endpoint_metrics = df.groupby('endpoint').agg({
                'origin_time_avg': ['mean', 'std'],
                'requests_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            endpoints = {}
            for _, row in endpoint_metrics.iterrows():
                endpoint = str(row['endpoint'])
                requests = self._safe_series_value(row['requests_adjusted'])
                error_4xx = self._safe_series_value(row['error_rate_4xx'])
                error_5xx = self._safe_series_value(row['error_rate_5xx'])

                endpoints[endpoint] = {
                    'performance': {
                        'avg_response_time': float(self._safe_series_value(row['origin_time_avg']['mean'])),
                        'std_dev': float(self._safe_series_value(row['origin_time_avg']['std']))
                    },
                    'load': {
                        'requests': int(requests)
                    },
                    'reliability': {
                        'error_rate_4xx': float(error_4xx * 100),
                        'error_rate_5xx': float(error_5xx * 100),
                        'reliability_score': float(1 - ((error_4xx + error_5xx) / 2))
                    }
                }

            # Identify problematic endpoints
            problematic_endpoints = [
                endpoint for endpoint, metrics in endpoints.items()
                if (metrics['reliability']['error_rate_5xx'] > 5 or 
                    metrics['performance']['avg_response_time'] > 1000)
            ]

            # Sort endpoints by impact
            sorted_by_load = sorted(
                endpoints.items(),
                key=lambda x: x[1]['load']['requests'],
                reverse=True
            )

            return {
                'endpoints': endpoints,
                'summary': {
                    'total_endpoints': len(endpoints),
                    'problematic_endpoints': problematic_endpoints,
                    'avg_response_time': float(df['origin_time_avg'].mean()),
                    'high_impact_endpoints': [endpoint for endpoint, _ in sorted_by_load[:5]]
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing endpoint performance: {str(e)}")
            return self._empty_endpoint_metrics()

    def _analyze_network_paths(self, df: pd.DataFrame) -> Dict:
        """Analyze network paths with proper column handling"""
        try:
            # Check required columns
            required_cols = ['requests_adjusted', 'origin_time_avg', 'ttfb_avg']
            if not all(col in df.columns for col in required_cols):
                logger.error("Missing required columns for network path analysis")
                return self._empty_network_metrics()

            # Select grouping columns that exist in the dataframe
            available_cols = []
            potential_cols = {
                'coloName': 'colo_name',
                'upperTierColoName': 'upper_tier_colo',
                'clientCountryName': 'country',
                'clientAsn': 'client_asn',
                'clientAsName': 'client_asn_desc'
            }

            for orig_col, new_col in potential_cols.items():
                if orig_col in df.columns:
                    df[new_col] = df[orig_col]
                    available_cols.append(new_col)
                elif new_col in df.columns:
                    available_cols.append(new_col)

            if not available_cols:
                logger.warning("No location columns available for network path analysis")
                return self._empty_network_metrics()

            # Group by available columns
            path_metrics = df.groupby(available_cols, observed=True).agg({
                'requests_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'ttfb_avg': 'mean'
            }).reset_index()

            paths = {}
            for _, row in path_metrics.iterrows():
                # Create a unique path ID using available columns
                path_components = [str(row[col]) for col in available_cols if pd.notna(row[col])]
                path_id = '_'.join(path_components) if path_components else 'default'
                
                # Get values
                requests = self._safe_series_value(row['requests_adjusted'])
                origin_time = self._safe_series_value(row['origin_time_avg'])
                ttfb = self._safe_series_value(row['ttfb_avg'])

                upper_tier = str(row['upper_tier_colo']) if 'upper_tier_colo' in row.index and pd.notna(row['upper_tier_colo']) else None
                client_asn = str(row['client_asn']) if 'client_asn' in row.index and pd.notna(row['client_asn']) else 'unknown'
                client_asn_desc = str(row['client_asn_desc']) if 'client_asn_desc' in row.index and pd.notna(row['client_asn_desc']) else 'Unknown ASN'

                paths[path_id] = {
                    'client_asn': client_asn,
                    'client_asn_desc': client_asn_desc,
                    'upper_tier': upper_tier,
                    'direct_requests': int(requests) if upper_tier is None else 0,
                    'tiered_requests': int(requests) if upper_tier is not None else 0,
                    'path_latency': float(ttfb - origin_time)
                }

            tiered_paths = sum(1 for p in paths.values() if p['upper_tier'] is not None)
            direct_paths = sum(1 for p in paths.values() if p['upper_tier'] is None)

            return {
                'paths': paths,
                'summary': {
                    'total_paths': len(paths),
                    'tiered_paths': tiered_paths,
                    'direct_paths': direct_paths,
                    'avg_path_latency': float(
                        np.mean([p['path_latency'] for p in paths.values()])
                    )
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing network paths: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_network_metrics()

    def _get_unique_paths(self, df: pd.DataFrame) -> List[Dict]:
        """Get unique network paths with proper column handling"""
        try:
            # Select available location columns
            location_cols = []
            potential_cols = {
                'coloName': 'colo_name',
                'upperTierColoName': 'upper_tier_colo',
                'clientCountryName': 'country',
                'clientAsn': 'client_asn',
                'clientAsName': 'client_asn_desc'
            }

            for orig_col, new_col in potential_cols.items():
                if orig_col in df.columns:
                    df[new_col] = df[orig_col]
                    location_cols.append(new_col)
                elif new_col in df.columns:
                    location_cols.append(new_col)

            if not location_cols:
                logger.warning("No location columns available for path analysis")
                return []

            # Group by available location columns
            path_metrics = df.groupby(location_cols, observed=True).agg({
                'requests_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            paths = []
            for _, row in path_metrics.iterrows():
                path = {
                    'location_id': '_'.join(str(row[col]) for col in location_cols if pd.notna(row[col])),
                    'requests': int(self._safe_series_value(row['requests_adjusted'])),
                    'latency': float(self._safe_series_value(row['origin_time_avg'])),
                    'confidence': float(self._safe_series_value(row['confidence_score']))
                }

                # Add any available location details
                for col in location_cols:
                    if pd.notna(row[col]):
                        path[col] = str(row[col])

                paths.append(path)

            return paths

        except Exception as e:
            logger.error(f"Error getting unique paths: {str(e)}")
            return []

    def _calculate_health_status(
        self,
        avg_response_time: float,
        error_rate: float,
        timeout_rate: float
    ) -> str:
        """Calculate overall health status with proper error handling"""
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

    def _empty_origin_metrics(self) -> Dict:
        """Return empty origin metrics structure"""
        return {
            'overall_metrics': self._empty_performance_metrics(),
            'temporal_analysis': {'time_series': {}, 'peak_periods': {}},
            'geographic_analysis': {'countries': {}, 'summary': {}},
            'error_analysis': self._empty_error_metrics(),
            'endpoint_analysis': self._empty_endpoint_metrics(),
            'network_analysis': self._empty_network_metrics(),
            'metadata': {
                'confidence_score': 0,
                'total_records': 0,
                'unique_endpoints': 0,
                'unique_paths': 0,
                'time_range': {
                    'start': datetime.now(timezone.utc).isoformat(),
                    'end': datetime.now(timezone.utc).isoformat()
                }
            }
        }

    def _empty_performance_metrics(self) -> OriginPerformanceMetrics:
        """Return empty performance metrics"""
        return OriginPerformanceMetrics(
            response_time={'avg': 0, 'median': 0, 'p95': 0, 'p99': 0, 'std_dev': 0},
            request_volume={'total': 0, 'per_second': 0},
            failure_rates={'error_rate': 0, 'timeout_rate': 0},
            bandwidth={'total_bytes': 0, 'avg_response_size': 0},
            health_status='unknown'
        )

    def _empty_error_metrics(self) -> Dict:
        """Return empty error metrics structure"""
        return {
            'error_distribution': {},
            'timing': {
                'avg_time_to_error': 0,
                'percentiles': {'p50': 0, 'p95': 0, 'p99': 0}
            },
            'temporal': {}
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

    def __str__(self) -> str:
        """String representation of the analyzer"""
        return (f"OriginAnalyzer(ttfb_thresholds={self.ttfb_thresholds}, "
                f"error_thresholds={self.error_rate_thresholds}, "
                f"timeout_thresholds={self.timeout_thresholds})")

    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"OriginAnalyzer(min_requests={self.min_requests})"
