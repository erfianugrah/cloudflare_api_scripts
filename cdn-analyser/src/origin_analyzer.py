import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from datetime import datetime, timezone
import traceback
from typing import Dict, List, Optional, Union, Any

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
            'MISS': ['miss', 'expired', 'updating', 'dynamic', 'bypass', 'revalidated'],
            'ERROR': ['error', 'disconnected', 'timeout']
        }
        self.min_requests = 10  # Minimum requests for reliable analysis
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

    def analyze_origin_performance(self, df: pd.DataFrame) -> Dict:
        """Main entry point for origin performance analysis."""
        try:
            if df is None or df.empty:
                logger.error("No data available for origin analysis")
                return self._empty_origin_metrics()

            # Filter for origin requests
            origin_requests = df[
                (df['cache_status'].isin([status for category in ['MISS', 'ERROR'] 
                                       for status in self.cache_categories[category]])) |
                (df['origin_time_avg'] > 0)  # Include any request that hit origin
            ].copy()

            if origin_requests.empty:
                logger.warning("No origin requests found in dataset")
                return self._empty_origin_metrics()

            # Calculate basic confidence score based on request volume
            confidence_score = min(1.0, len(origin_requests) / self.min_requests)
            
            # Log analysis scope
            logger.info(f"""
Origin Analysis Starting:
---------------------
Total Records: {len(df):,}
Origin Records: {len(origin_requests):,}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
Confidence Score: {confidence_score:.2f}
""")

            # Process metrics
            metrics = {
                'overall_metrics': self._calculate_overall_metrics(origin_requests),
                'temporal_analysis': self._analyze_temporal_patterns(origin_requests),
                'geographic_analysis': self._analyze_geographic_patterns(origin_requests),
                'error_analysis': self._analyze_origin_errors(origin_requests),
                'endpoint_analysis': self._analyze_endpoint_performance(origin_requests),
                'protocol_impact': self._analyze_protocol_impact(origin_requests),
                'metadata': {
                    'confidence_score': confidence_score,
                    'total_requests': len(origin_requests),
                    'unique_endpoints': origin_requests['endpoint'].nunique(),
                    'time_range': {
                        'start': origin_requests['timestamp'].min().isoformat(),
                        'end': origin_requests['timestamp'].max().isoformat()
                    }
                }
            }

            # Log completion summary
            logger.info(f"""
Origin Analysis Complete:
---------------------
Total Origin Requests: {metrics['metadata']['total_requests']:,}
Unique Endpoints: {metrics['metadata']['unique_endpoints']:,}
Avg Response Time: {metrics['overall_metrics']['response_time']['avg']:.2f}ms
Error Rate: {metrics['overall_metrics']['failure_rates']['error_rate']:.2f}%
Confidence Score: {metrics['metadata']['confidence_score']:.2f}
""")

            return metrics

        except Exception as e:
            logger.error(f"Error in origin performance analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_origin_metrics()

    def _calculate_overall_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate overall origin performance metrics with consistent error rate calculations."""
        try:
            # Use adjusted request counts for accurate error rates
            total_requests = df['requests_adjusted'].sum()
            error_requests_5xx = df[df['status'] >= 500]['requests_adjusted'].sum()
            error_requests_4xx = df[df['status'].between(400, 499)]['requests_adjusted'].sum()

            # Calculate error rates using adjusted values
            error_rate_5xx = (error_requests_5xx / total_requests * 100) if total_requests > 0 else 0
            error_rate_4xx = (error_requests_4xx / total_requests * 100) if total_requests > 0 else 0
            total_error_rate = error_rate_4xx + error_rate_5xx

            # Calculate weighted response times
            weighted_times = {
                'avg': df['origin_time_avg'].mean(),
                'median': df['origin_p50'].iloc[0] if 'origin_p50' in df.columns else df['origin_time_avg'].median(),
                'p95': df['origin_p95'].iloc[0] if 'origin_p95' in df.columns else df['origin_time_avg'].quantile(0.95),
                'p99': df['origin_p99'].iloc[0] if 'origin_p99' in df.columns else df['origin_time_avg'].quantile(0.99),
                'std': df['origin_time_avg'].std()
            }

            return {
                'response_time': {
                    'avg': float(weighted_times['avg']),
                    'median': float(weighted_times['median']),
                    'p95': float(weighted_times['p95']),
                    'p99': float(weighted_times['p99']),
                    'std_dev': float(weighted_times['std'])
                },
                'request_volume': {
                    'total_requests': int(total_requests),
                    'requests_per_second': float(total_requests / 
                        max((df['timestamp'].max() - df['timestamp'].min()).total_seconds(), 1))
                },
                'failure_rates': {
                    'error_rate': float(total_error_rate),
                    'error_rate_4xx': float(error_rate_4xx),
                    'error_rate_5xx': float(error_rate_5xx),
                    'timeout_rate': float(len(df[df['origin_time_avg'] > 30000]) / len(df) * 100)
                },
                'bandwidth': {
                    'total_bytes': float(df['bytes_adjusted'].sum()),
                    'avg_response_size': float(df['bytes_adjusted'].sum() / total_requests if total_requests > 0 else 0)
                },
                'health_status': self._calculate_health_status(
                    weighted_times['avg'], 
                    total_error_rate
                )
            }

        except Exception as e:
            logger.error(f"Error calculating overall metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_overall_metrics()

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns with improved statistical analysis."""
        try:
            # Resample to 5-minute intervals
            df_time = df.set_index('timestamp')
            temporal_metrics = df_time.resample('5min').agg({
                'origin_time_avg': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100,
                'origin_p95': 'mean',
                'cache_status': lambda x: x.isin(self.cache_categories['ERROR']).mean() * 100
            }).reset_index()

            # Calculate moving averages
            temporal_metrics['response_time_ma'] = temporal_metrics['origin_time_avg'].rolling(
                window=3, min_periods=1
            ).mean()

            # Calculate trend using simple linear regression
            temporal_metrics['response_time_trend'] = temporal_metrics['origin_time_avg'].diff()

            # Find peak periods
            peak_load_idx = temporal_metrics['requests_adjusted'].idxmax() if not temporal_metrics.empty else None
            peak_latency_idx = temporal_metrics['origin_time_avg'].idxmax() if not temporal_metrics.empty else None
            
            peak_load = temporal_metrics.iloc[peak_load_idx] if peak_load_idx is not None else None
            peak_latency = temporal_metrics.iloc[peak_latency_idx] if peak_latency_idx is not None else None

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
                        'response_time': float(peak_load['origin_time_avg']) if peak_load is not None else 0.0
                    },
                    'worst_performance': {
                        'timestamp': str(peak_latency['timestamp']) if peak_latency is not None else None,
                        'response_time': float(peak_latency['origin_time_avg']) if peak_latency is not None else 0.0,
                        'requests': int(peak_latency['requests_adjusted']) if peak_latency is not None else 0
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'time_series': {},
                'peak_periods': {
                    'highest_load': {'timestamp': None, 'requests': 0, 'response_time': 0.0},
                    'worst_performance': {'timestamp': None, 'response_time': 0.0, 'requests': 0}
                }
            }


    def _analyze_geographic_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic patterns with enhanced metrics."""
        try:
            # Fixed aggregation to handle multi-level columns
            geo_metrics = df.groupby('country').agg({
                'origin_time_avg': ['mean', 'std', 'count'],
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'origin_p95': 'max'
            })
            
            # Reset index to make country a column
            geo_metrics = geo_metrics.reset_index()
            
            # Flatten column names
            geo_metrics.columns = ['country', 'mean_time', 'time_std', 'time_count', 
                                 'requests', 'bytes', 'error_4xx', 'error_5xx', 'p95_time']

            # Calculate performance score
            if len(geo_metrics) > 0:
                geo_metrics['perf_score'] = (
                    (1 / geo_metrics['mean_time']) * 
                    (1 - geo_metrics['error_5xx']) * 
                    geo_metrics['time_count']
                ).rank(pct=True)
                
                sorted_metrics = geo_metrics.sort_values('mean_time')
                worst_performers = sorted_metrics.tail().sort_values('mean_time', ascending=False)

                return {
                    'countries': {
                        str(row['country']): {
                            'performance': {
                                'avg_response_time': float(row['mean_time']),
                                'std_dev': float(row['time_std']),
                                'sample_size': int(row['time_count']),
                                'p95_response_time': float(row['p95_time'])
                            },
                            'traffic': {
                                'requests': int(row['requests']),
                                'bytes_gb': float(row['bytes'] / (1024**3))
                            },
                            'reliability': {
                                'error_rate_4xx': float(row['error_4xx']),
                                'error_rate_5xx': float(row['error_5xx'])
                            },
                            'perf_score': float(row['perf_score'])
                        }
                        for _, row in geo_metrics.iterrows()
                    },
                    'regional_summary': {
                        'fastest_regions': list(sorted_metrics.head()['country']),
                        'slowest_regions': list(worst_performers['country']),
                        'highest_error_regions': list(
                            geo_metrics.nlargest(5, 'error_5xx')['country']
                        ),
                        'busiest_regions': list(
                            geo_metrics.nlargest(5, 'requests')['country']
                        )
                    }
                }
            else:
                return {
                    'countries': {},
                    'regional_summary': {
                        'fastest_regions': [],
                        'slowest_regions': [],
                        'highest_error_regions': [],
                        'busiest_regions': []
                    }
                }

        except Exception as e:
            logger.error(f"Error analyzing geographic patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'countries': {},
                'regional_summary': {
                    'fastest_regions': [],
                    'slowest_regions': [],
                    'highest_error_regions': [],
                    'busiest_regions': []
                }
            }

    def _analyze_origin_errors(self, df: pd.DataFrame) -> Dict:
        """Analyze origin server errors and failures with enhanced insights."""
        try:
            error_df = df[df['status'] >= 500].copy()
            
            if error_df.empty:
                return {
                    'error_distribution': {},
                    'error_timing': {
                        'avg_time_to_error': 0.0,
                        'error_percentiles': {'p50': 0.0, 'p95': 0.0, 'p99': 0.0}
                    },
                    'error_patterns': {'by_country': {}, 'by_endpoint': {}, 'by_hour': {}}
                }

            # Calculate error timing metrics
            error_timing = error_df.agg({
                'origin_time_avg': 'mean',
                'origin_p50': 'mean',  
                'origin_p95': 'mean',
                'origin_p99': 'mean'
            })

            # Analyze error patterns over time
            hourly_errors = df.set_index('timestamp').resample('1h').agg({
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum'
            })

            return {
                'error_distribution': {
                    int(status): {
                        'count': int(count),
                        'percentage': float(count / len(error_df) * 100)
                    }
                    for status, count in error_df['status'].value_counts().items()
                },
                'error_timing': {
                    'avg_time_to_error': float(error_timing['origin_time_avg']),
                    'error_percentiles': {
                        'p50': float(error_timing['origin_p50']),
                        'p95': float(error_timing['origin_p95']),
                        'p99': float(error_timing['origin_p99'])
                    }
                },
                'error_patterns': {
                    'by_country': self._calculate_error_rates_by_dimension(df, 'country'),
                    'by_endpoint': self._calculate_error_rates_by_dimension(df, 'endpoint'),
                    'by_hour': {
                        str(timestamp): {
                            'error_rate': float(row['error_rate_5xx'] * 100),
                            'request_count': int(row['requests_adjusted'])
                        }
                        for timestamp, row in hourly_errors.iterrows()
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing origin errors: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _calculate_error_rates_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Calculate error rates grouped by a specific dimension."""
        try:
            metrics = df.groupby(dimension).agg({
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum',
                'origin_time_avg': 'mean'
            })
            
            return {
                str(idx): {
                    'error_rate': float(row['error_rate_5xx'] * 100),
                    'request_count': int(row['requests_adjusted']),
                    'avg_response_time': float(row['origin_time_avg'])
                }
                for idx, row in metrics.iterrows()
            }
        except Exception as e:
            logger.error(f"Error calculating error rates for {dimension}: {str(e)}")
            return {}

    def _analyze_endpoint_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze endpoint performance with enhanced metrics."""
        try:
            # Fixed aggregation to handle multi-level columns
            endpoint_metrics = df.groupby('endpoint').agg({
                'origin_time_avg': ['mean', 'std', 'count'],
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'origin_p95': 'mean'
            })
            
            # Reset index and flatten columns
            endpoint_metrics = endpoint_metrics.reset_index()
            endpoint_metrics.columns = [
                'endpoint', 'mean_time', 'time_std', 'time_count', 
                'requests', 'visits', 'bytes', 'error_4xx', 'error_5xx', 'p95_time'
            ]

            # Calculate load impact score
            endpoint_metrics['load_score'] = (
                endpoint_metrics['requests'] * 
                endpoint_metrics['mean_time'] * 
                endpoint_metrics['bytes']
            ).rank(pct=True)

            # Calculate reliability score
            endpoint_metrics['reliability_score'] = (
                1 - (endpoint_metrics['error_4xx'] * 0.5 + endpoint_metrics['error_5xx'])
            ).rank(pct=True)

            # Fix problematic endpoints identification
            problematic_endpoints = endpoint_metrics[
                (endpoint_metrics['error_5xx'] > 0.05) | 
                (endpoint_metrics['mean_time'] > 1000)
            ]['endpoint'].tolist()

            return {
                'endpoints': {
                    str(row['endpoint']): {
                        'performance': {
                            'avg_response_time': float(row['mean_time']),
                            'std_dev': float(row['time_std']),
                            'sample_size': int(row['time_count']),
                            'p95_response_time': float(row['p95_time'])
                        },
                        'load': {
                            'requests': int(row['requests']),
                            'visits': int(row['visits']),
                            'bytes': float(row['bytes']),
                            'load_impact_score': float(row['load_score'])
                        },
                        'reliability': {
                            'error_rate_4xx': float(row['error_4xx']),
                            'error_rate_5xx': float(row['error_5xx']),
                            'reliability_score': float(row['reliability_score'])
                        }
                    }
                    for _, row in endpoint_metrics.iterrows()
                },
                'top_impacting_endpoints': list(
                    endpoint_metrics.nlargest(10, 'load_score')['endpoint']
                ),
                'summary': {
                    'total_endpoints': len(endpoint_metrics),
                    'avg_response_time': float(endpoint_metrics['mean_time'].mean()),
                    'problematic_endpoints': problematic_endpoints
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing endpoint performance: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'endpoints': {},
                'top_impacting_endpoints': [],
                'summary': {
                    'total_endpoints': 0,
                    'avg_response_time': 0.0,
                    'problematic_endpoints': []
                }
            }

    def _analyze_protocol_impact(self, df: pd.DataFrame) -> Dict:
        """Analyze protocol impact with accurate request share calculations."""
        try:
            if df.empty:
                return self._empty_protocol_metrics()

            # Clean protocol values and remove any nulls
            df['protocol'] = df['protocol'].fillna('unknown').str.strip().str.upper()
            
            # Group by protocol for main metrics
            protocol_metrics = df.groupby('protocol').agg({
                'origin_time_avg': ['mean', 'std', 'count'],
                'ttfb_avg': ['mean', 'std'],
                'ttfb_p95': 'mean',
                'ttfb_p99': 'mean',
                'origin_p95': 'mean',
                'origin_p99': 'mean',
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: x.str.lower().isin(['hit', 'stale', 'revalidated']).mean() * 100
            }).reset_index()

            total_requests = float(protocol_metrics['requests_adjusted'].sum().sum())
            
            # Build detailed protocol data
            protocols = {}
            for _, row in protocol_metrics.iterrows():
                protocol = str(row['protocol'])
                requests = float(row[('requests_adjusted', 'sum')])
                
                protocols[protocol] = {
                    'performance': {
                        'avg_response_time': float(self._safe_numeric(row[('origin_time_avg', 'mean')])),
                        'std_dev': float(self._safe_numeric(row[('origin_time_avg', 'std')])),
                        'sample_size': int(self._safe_numeric(row[('origin_time_avg', 'count')])),
                        'ttfb': {
                            'avg': float(self._safe_numeric(row[('ttfb_avg', 'mean')])),
                            'std': float(self._safe_numeric(row[('ttfb_avg', 'std')])),
                            'p95': float(self._safe_numeric(row['ttfb_p95'])),
                            'p99': float(self._safe_numeric(row['ttfb_p99']))
                        },
                        'origin': {
                            'p95': float(self._safe_numeric(row['origin_p95'])),
                            'p99': float(self._safe_numeric(row['origin_p99']))
                        }
                    },
                    'usage': {
                        'request_count': int(requests),
                        'visit_count': int(self._safe_numeric(row[('visits_adjusted', 'sum')])),
                        'bytes_transferred': float(self._safe_numeric(row[('bytes_adjusted', 'sum')])),
                        'bandwidth_gb': float(self._safe_numeric(row[('bytes_adjusted', 'sum')])) / (1024**3),
                        'request_share': float(requests / total_requests * 100) if total_requests > 0 else 0
                    },
                    'reliability': {
                        'error_rate_4xx': float(self._safe_numeric(row['error_rate_4xx']) * 100),
                        'error_rate_5xx': float(self._safe_numeric(row['error_rate_5xx']) * 100),
                        'total_error_rate': float(self._safe_numeric(row['error_rate_4xx'] + row['error_rate_5xx']) * 100),
                        'cache_hit_ratio': float(self._safe_numeric(row['cache_status']))
                    }
                }

            # Calculate time-based protocol metrics
            time_metrics = df.set_index('timestamp').groupby([
                pd.Grouper(freq='5min'), 'protocol'
            ]).agg({
                'requests_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            # Add temporal analysis
            temporal_analysis = {}
            for protocol in protocols.keys():
                protocol_data = time_metrics[time_metrics['protocol'] == protocol]
                if not protocol_data.empty:
                    temporal_analysis[protocol] = {
                        str(row['timestamp']): {
                            'requests': int(self._safe_numeric(row['requests_adjusted'])),
                            'avg_ttfb': float(self._safe_numeric(row['ttfb_avg'])),
                            'error_rate': float(self._safe_numeric(row['error_rate_4xx'] + row['error_rate_5xx']) * 100)
                        }
                        for _, row in protocol_data.iterrows()
                    }

            # Identify best performing protocols
            if not protocol_metrics.empty:
                best_performers = {
                    'response_time': str(protocol_metrics.loc[
                        protocol_metrics[('origin_time_avg', 'mean')].idxmin(), 'protocol'
                    ]),
                    'reliability': str(protocol_metrics.loc[
                        (protocol_metrics['error_rate_4xx'] + protocol_metrics['error_rate_5xx']).idxmin(), 'protocol'
                    ]),
                    'throughput': str(protocol_metrics.loc[
                        protocol_metrics[('requests_adjusted', 'sum')].idxmax(), 'protocol'
                    ]),
                    'cache_performance': str(protocol_metrics.loc[
                        protocol_metrics['cache_status'].idxmax(), 'protocol'
                    ])
                }
            else:
                best_performers = {
                    'response_time': 'unknown',
                    'reliability': 'unknown',
                    'throughput': 'unknown',
                    'cache_performance': 'unknown'
                }

            return {
                'protocols': protocols,
                'protocol_comparison': {
                    'fastest_protocol': best_performers['response_time'],
                    'most_reliable': best_performers['reliability'],
                    'most_used': best_performers['throughput'],
                    'best_cache_performance': best_performers['cache_performance']
                },
                'temporal_analysis': temporal_analysis,
                'summary': {
                    'total_protocols': len(protocols),
                    'protocol_distribution': {
                        protocol: float(metrics['usage']['request_share'])
                        for protocol, metrics in protocols.items()
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing protocol impact: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_protocol_metrics()

    def _empty_protocol_metrics(self) -> Dict:
        """Return empty protocol metrics structure."""
        return {
            'protocols': {},
            'protocol_comparison': {
                'fastest_protocol': 'unknown',
                'most_reliable': 'unknown',
                'most_used': 'unknown',
                'best_cache_performance': 'unknown'
            },
            'temporal_analysis': {},
            'summary': {
                'total_protocols': 0,
                'protocol_distribution': {}
            }
        }

    def _safe_numeric(self, value: Any, default: Union[int, float] = 0) -> Union[int, float]:
        """Safely convert value to number."""
        try:
            if pd.isna(value):
                return default
            elif isinstance(value, pd.Series):
                return float(value.iloc[0]) if len(value) > 0 else default
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """Safely convert value to integer."""
        return int(self._safe_numeric(value, default))

    def _calculate_health_status(self, avg_response_time: float, error_rate: float) -> str:
        """Calculate overall health status based on response time and error rate."""
        if (avg_response_time <= self.ttfb_thresholds['good'] and 
            error_rate <= self.error_rate_thresholds['good']):
            return 'healthy'
        elif (avg_response_time >= self.ttfb_thresholds['critical'] or 
              error_rate >= self.error_rate_thresholds['critical']):
            return 'critical'
        elif (avg_response_time >= self.ttfb_thresholds['warning'] or 
              error_rate >= self.error_rate_thresholds['warning']):
            return 'degraded'
        return 'warning'

    def _empty_origin_metrics(self) -> Dict:
        """Return empty origin metrics structure with health status."""
        return {
            'overall_metrics': self._empty_overall_metrics(),
            'temporal_analysis': {},
            'geographic_analysis': {},
            'error_analysis': {},
            'endpoint_analysis': {},
            'protocol_impact': {},
            'metadata': {
                'confidence_score': 0.0,
                'total_requests': 0,
                'unique_endpoints': 0,
                'time_range': {
                    'start': datetime.now(timezone.utc).isoformat(),
                    'end': datetime.now(timezone.utc).isoformat()
                }
            }
        }

    def _empty_overall_metrics(self) -> Dict:
        """Return empty overall metrics structure with health status."""
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
                'avg_response_size': 0.0,
                'bandwidth_mbps': 0.0
            },
            'health_status': 'unknown'
        }
