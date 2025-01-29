import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any, Tuple, Set
import logging
from datetime import datetime, timezone
import traceback
from dataclasses import dataclass, asdict
from prettytable import PrettyTable
from .types import OriginPerformanceMetrics, OriginPathMetrics
from .formatters import TableFormatter
logger = logging.getLogger(__name__)

class OriginAnalyzer:
    """Enhanced analyzer for origin server performance metrics"""

    def safe_agg(self, series: pd.Series, operation: str = 'mean') -> float:
        """Safely aggregate series values"""
        try:
            if isinstance(series, (int, float)):
                return float(series)
                
            if isinstance(series, pd.Series):
                if series.empty:
                    return 0.0
                    
                # Handle single value series
                if len(series) == 1:
                    return float(series.iloc[0])
                    
                # Calculate based on operation
                if operation == 'mean':
                    val = series.mean()
                elif operation == 'sum':
                    val = series.sum()
                elif operation == 'std':
                    val = series.std()
                elif operation == 'min':
                    val = series.min()
                elif operation == 'max':
                    val = series.max()
                else:
                    val = series.mean()
                    
                return float(val) if not pd.isna(val) else 0.0
                    
            return 0.0
                
        except Exception as e:
            logger.error(f"Error in safe aggregation: {str(e)}")
            return 0.0
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

        # Content type categories for better classification
        self.content_types = {
            'static': [
                'text/css', 'text/javascript', 'application/javascript',
                'image/jpeg', 'image/png', 'image/gif', 'image/svg+xml',
                'text/html', 'application/x-font-ttf', 'application/x-font-woff',
                'font/woff2', 'application/font-woff2', 'text/plain',
                'css', 'js', 'jpeg', 'jpg', 'png', 'gif', 'svg', 'html',
                'ttf', 'woff', 'woff2'
            ],
            'dynamic': [
                'application/json', 'application/xml', 'text/xml',
                'application/x-www-form-urlencoded',
                'json', 'xml'
            ]
        }
        
        self.min_requests = 10  # Minimum requests for reliable analysis
        self.table_formatter = TableFormatter()

    def analyze_origin_performance(self, df: pd.DataFrame) -> Optional[Dict]:
        """Analyze origin server performance with enhanced error handling"""
        try:
            if df is None or df.empty:
                logger.error("No data available for origin analysis")
                return None

            # Filter for origin requests (non-cache hits and errors)
            origin_requests = df[
                (~df['is_cache_hit']) | (df['is_error'])
            ].copy()

            if origin_requests.empty:
                logger.warning("No origin requests found in dataset")
                return self._empty_origin_metrics()

            # Calculate initial confidence score
            confidence_score = self._calculate_confidence_score(origin_requests)
            
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

            # Calculate overall metrics
            overall_metrics = self._calculate_overall_metrics(origin_requests)
            if not overall_metrics:
                return self._empty_origin_metrics()

            # Perform detailed analysis
            analysis_result = {
                'overall_metrics': overall_metrics,
                'temporal_analysis': self._analyze_temporal_patterns(origin_requests),
                'geographic_analysis': self._analyze_geographic_patterns(origin_requests),
                'error_analysis': self._analyze_origin_errors(origin_requests),
                'endpoint_analysis': self._analyze_endpoint_performance(origin_requests),
                'network_analysis': self._analyze_network_paths(origin_requests),
                'content_analysis': self._analyze_content_type_performance(origin_requests),
                'metadata': {
                    'confidence_score': confidence_score,
                    'total_records': len(origin_requests),
                    'unique_endpoints': origin_requests['path'].nunique(),
                    'unique_paths': len(self._get_unique_paths(origin_requests)),
                    'time_range': {
                        'start': origin_requests['timestamp'].min().isoformat(),
                        'end': origin_requests['timestamp'].max().isoformat()
                    }
                }
            }

            # Add correlation analysis if sufficient data
            if len(origin_requests) >= self.min_requests:
                analysis_result['correlations'] = self._analyze_metric_correlations(origin_requests)

            return analysis_result

        except Exception as e:
            logger.error(f"Error in origin performance analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _calculate_confidence_score(self, df: pd.DataFrame) -> float:
        """Calculate robust confidence score based on data quality"""
        try:
            if df.empty:
                return 0.0

            total_requests = float(df['requests_adjusted'].sum())
            if total_requests == 0:
                return 0.0
                
            # Calculate base confidence from request volume
            volume_score = min(1.0, np.log10(total_requests + 1) / np.log10(self.min_requests + 1))
            
            # Calculate metrics reliability
            error_rate = float(df['is_error'].mean())
            sampling_rate = float(df['sampling_rate'].mean())
            
            # Avoid division by zero
            if sampling_rate == 0:
                sampling_rate = 1.0
                
            # Calculate component scores
            error_penalty = min(0.5, error_rate)
            sampling_boost = min(0.5, sampling_rate)
            
            # Data completeness score
            completeness_score = sum([
                df['ttfb_avg'].notna().mean(),
                df['origin_time_avg'].notna().mean(),
                df['path_latency'].notna().mean(),
                df['status_code'].notna().mean()
            ]) / 4
            
            # Calculate final score
            final_score = max(0.0, min(1.0,
                volume_score * 0.4 +
                completeness_score * 0.3 +
                (1 - error_penalty) * 0.2 +
                sampling_boost * 0.1
            ))
            
            logger.debug(f"""
Confidence Score Calculation:
-------------------------
Volume Score: {volume_score:.3f}
Completeness Score: {completeness_score:.3f}
Error Penalty: {error_penalty:.3f}
Sampling Boost: {sampling_boost:.3f}
Final Score: {final_score:.3f}
""")
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating confidence score: {str(e)}")
            return 0.0

    def _calculate_overall_metrics(self, df: pd.DataFrame) -> Optional[Dict]:
        """Calculate overall metrics with proper Series handling"""
        try:
            if df.empty:
                return None

            # Calculate core metrics with safe aggregation
            response_time_metrics = {
                'avg': safe_agg(df['origin_time_avg']),
                'median': safe_agg(df['origin_p50']),
                'p95': safe_agg(df['origin_p95']),
                'p99': safe_agg(df['origin_p99']),
                'std_dev': safe_agg(df['origin_time_avg'], 'std'),
                'total': safe_agg(df['origin_sum'])
            }

            total_requests = safe_agg(df['requests_adjusted'], 'sum')
            time_range = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            
            request_metrics = {
                'total': int(total_requests),
                'per_second': safe_division(total_requests, max(time_range, 1)),
                'path_latency_avg': safe_agg(df['path_latency']),
                'path_latency_std': safe_agg(df['path_latency'], 'std')
            }

            # Calculate failure rates
            total_errors = len(df[df['is_error']])

            failure_metrics = {
                'error_rate': safe_division(total_errors, len(df)) * 100,
                'timeout_rate': safe_division(len(df[df['origin_time_avg'] > 30000]), len(df)) * 100,
                'error_rate_4xx': safe_agg(df['error_rate_4xx']) * 100,
                'error_rate_5xx': safe_agg(df['error_rate_5xx']) * 100
            }

            bandwidth_metrics = {
                'total_bytes': safe_agg(df['bytes_adjusted'], 'sum'),
                'avg_response_size': safe_agg(df['bytes_p50']),
                'p95_size': safe_agg(df['bytes_p95']),
                'p99_size': safe_agg(df['bytes_p99'])
            }

            return {
                'response_time': response_time_metrics,
                'request_volume': request_metrics,
                'failure_rates': failure_metrics,
                'bandwidth': bandwidth_metrics,
                'health_status': self._calculate_health_status(
                    response_time_metrics['avg'],
                    failure_metrics['error_rate'],
                    failure_metrics['timeout_rate']
                )
            }

        except Exception as e:
            logger.error(f"Error calculating overall metrics: {str(e)}")
            return None

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns with enhanced statistical analysis"""
        try:
            # Resample to 5-minute intervals with safe aggregation
            df_time = df.set_index('timestamp')
            
            temporal_metrics = df_time.resample('5min').agg({
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'bytes_p50': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'is_error': 'mean',
                'is_cache_hit': 'mean',
                'path_latency': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            # Calculate moving averages for trend analysis
            temporal_metrics['response_time_ma'] = temporal_metrics['origin_time_avg'].rolling(
                window=6,  # 30-minute window
                min_periods=1
            ).mean()

            temporal_metrics['load_ma'] = temporal_metrics['requests_adjusted'].rolling(
                window=6,
                min_periods=1
            ).mean()

            # Find peak periods
            peak_load_idx = temporal_metrics['requests_adjusted'].idxmax()
            peak_latency_idx = temporal_metrics['origin_time_avg'].idxmax()

            peak_load = temporal_metrics.iloc[peak_load_idx] if not pd.isna(peak_load_idx) else None
            peak_latency = temporal_metrics.iloc[peak_latency_idx] if not pd.isna(peak_latency_idx) else None

            # Build time series data
            time_series = {}
            for _, row in temporal_metrics.iterrows():
                time_series[str(row['timestamp'])] = {
                    'performance': {
                        'response_time': float(row['origin_time_avg']),
                        'response_time_ma': float(row['response_time_ma']),
                        'p95_response_time': float(row['origin_p95']),
                        'path_latency': float(row['path_latency']),
                        'avg_size': float(row['bytes_p50'])
                    },
                    'traffic': {
                        'requests': int(row['requests_adjusted']),
                        'requests_ma': float(row['load_ma']),
                        'bytes': float(row['bytes_adjusted'])
                    },
                    'reliability': {
                        'error_rate': float(row['is_error'] * 100)
                    },
                    'metadata': {
                        'confidence': float(row['confidence_score'])
                    }
                }

            # Calculate trend metrics
            first_metrics = temporal_metrics.iloc[0] if not temporal_metrics.empty else None
            last_metrics = temporal_metrics.iloc[-1] if not temporal_metrics.empty else None
            
            trend_metrics = {
                'direction': 'increasing' if (
                    first_metrics is not None and 
                    last_metrics is not None and
                    last_metrics['origin_time_avg'] > first_metrics['origin_time_avg']
                ) else 'decreasing',
                'change_percentage': (
                    ((last_metrics['origin_time_avg'] / first_metrics['origin_time_avg']) - 1) * 100
                    if first_metrics is not None and 
                       last_metrics is not None and 
                       first_metrics['origin_time_avg'] > 0 
                    else 0
                )
            }

            # Construct peak period metrics
            peak_periods = {
                'highest_load': {
                    'timestamp': str(peak_load['timestamp']) if peak_load is not None else None,
                    'requests': int(peak_load['requests_adjusted']) if peak_load is not None else 0,
                    'response_time': float(peak_load['origin_time_avg']) if peak_load is not None else 0,
                    'path_latency': float(peak_load['path_latency']) if peak_load is not None else 0
                },
                'worst_performance': {
                    'timestamp': str(peak_latency['timestamp']) if peak_latency is not None else None,
                    'response_time': float(peak_latency['origin_time_avg']) if peak_latency is not None else 0,
                    'requests': int(peak_latency['requests_adjusted']) if peak_latency is not None else 0,
                    'path_latency': float(peak_latency['path_latency']) if peak_latency is not None else 0
                }
            }

            return {
                'time_series': time_series,
                'peak_periods': peak_periods,
                'trends': trend_metrics,
                'summary': {
                    'total_intervals': len(temporal_metrics),
                    'avg_load': float(temporal_metrics['requests_adjusted'].mean()),
                    'load_std_dev': float(temporal_metrics['requests_adjusted'].std()),
                    'avg_response_time': float(temporal_metrics['origin_time_avg'].mean()),
                    'response_time_std_dev': float(temporal_metrics['origin_time_avg'].std())
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'time_series': {},
                'peak_periods': {
                    'highest_load': {'timestamp': None, 'requests': 0, 'response_time': 0, 'path_latency': 0},
                    'worst_performance': {'timestamp': None, 'response_time': 0, 'requests': 0, 'path_latency': 0}
                },
                'trends': {'direction': 'unknown', 'change_percentage': 0},
                'summary': {
                    'total_intervals': 0,
                    'avg_load': 0,
                    'load_std_dev': 0,
                    'avg_response_time': 0,
                    'response_time_std_dev': 0
                }
            }

    def _analyze_geographic_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic patterns with improved error handling"""
        try:
            if 'country' not in df.columns:
                logger.error("Country column missing from data")
                return {'countries': {}, 'summary': {}}

            # Group by country with comprehensive metrics
            geo_metrics = df.groupby('country', observed=True).agg({
                'origin_time_avg': ['mean', 'std'],
                'origin_p95': 'mean',
                'bytes_p50': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'path_latency': ['mean', 'std', 'min', 'max'],
                'confidence_score': 'mean'
            }).reset_index()

            countries = {}
            total_requests = float(df['requests_adjusted'].sum())

            for _, row in geo_metrics.iterrows():
                country = self._clean_string_value(row['country'])
                
                # Calculate key metrics
                total_error_rate = float(row['error_rate_4xx'] + row['error_rate_5xx']) * 100
                requests = float(row['requests_adjusted'])
                
                countries[country] = {
                    'performance': {
                        'avg_response_time': float(row['origin_time_avg']['mean']),
                        'std_dev': float(row['origin_time_avg']['std']),
                        'p95_response_time': float(row['origin_p95']),
                        'path_latency': {
                            'avg': float(row['path_latency']['mean']),
                            'std': float(row['path_latency']['std']),
                            'min': float(row['path_latency']['min']),
                            'max': float(row['path_latency']['max'])
                        },
                        'avg_size': float(row['bytes_p50'])
                    },
                    'traffic': {
                        'requests': int(requests),
                        'bytes': float(row['bytes_adjusted']),
                        'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0
                    },
                    'reliability': {
                        'error_rate_4xx': float(row['error_rate_4xx'] * 100),
                        'error_rate_5xx': float(row['error_rate_5xx'] * 100),
                        'total_error_rate': total_error_rate
                    },
                    'confidence': float(row['confidence_score'])
                }

            # Sort countries for various rankings
            sorted_by_response = sorted(
                countries.items(),
                key=lambda x: x[1]['performance']['avg_response_time']
            )

            sorted_by_errors = sorted(
                countries.items(),
                key=lambda x: x[1]['reliability']['total_error_rate'],
                reverse=True
            )

            sorted_by_latency = sorted(
                countries.items(),
                key=lambda x: x[1]['performance']['path_latency']['avg'],
                reverse=True
            )

            sorted_by_traffic = sorted(
                countries.items(),
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )

            # Calculate global metrics
            global_metrics = {
                'total_countries': len(countries),
                'avg_response_time': float(df['origin_time_avg'].mean()),
                'avg_path_latency': float(df['path_latency'].mean()),
                'avg_error_rate': float(df['is_error'].mean() * 100),
                'response_time_std': float(df['origin_time_avg'].std()),
                'path_latency_std': float(df['path_latency'].std())
            }

            return {
                'countries': countries,
                'summary': {
                    'global_metrics': global_metrics,
                    'top_countries': {
                        'by_traffic': [k for k, _ in sorted_by_traffic[:5]],
                        'fastest': [k for k, _ in sorted_by_response[:5]],
                        'slowest': [k for k, _ in sorted_by_response[-5:]],
                        'highest_errors': [
                            k for k, v in sorted_by_errors[:5]
                            if v['reliability']['total_error_rate'] > 0
                        ],
                        'highest_latency': [k for k, _ in sorted_by_latency[:5]]
                    },
                    'regional_distribution': self._calculate_regional_distribution(countries)
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing geographic patterns: {str(e)}")
            return {'countries': {}, 'summary': {}}

    def _calculate_regional_distribution(self, countries: Dict) -> Dict:
        """Calculate regional traffic distribution"""
        try:
            regions = {
                'North America': ['US', 'CA', 'MX'],
                'Europe': ['GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'PL'],
                'Asia Pacific': ['JP', 'CN', 'KR', 'SG', 'AU', 'IN', 'HK', 'TW'],
                'Latin America': ['BR', 'AR', 'CL', 'CO', 'PE'],
                'Middle East': ['AE', 'SA', 'IL', 'TR'],
                'Africa': ['ZA', 'NG', 'KE', 'EG']
            }

            regional_stats = {region: {'requests': 0, 'bytes': 0} for region in regions}
            total_requests = sum(c['traffic']['requests'] for c in countries.values())

            for country, metrics in countries.items():
                region_found = False
                for region, country_list in regions.items():
                    if country in country_list:
                        regional_stats[region]['requests'] += metrics['traffic']['requests']
                        regional_stats[region]['bytes'] += metrics['traffic']['bytes']
                        region_found = True
                        break
                
                if not region_found:
                    if 'Other' not in regional_stats:
                        regional_stats['Other'] = {'requests': 0, 'bytes': 0}
                    regional_stats['Other']['requests'] += metrics['traffic']['requests']
                    regional_stats['Other']['bytes'] += metrics['traffic']['bytes']

            # Calculate percentages
            for region in regional_stats:
                regional_stats[region]['percentage'] = (
                    regional_stats[region]['requests'] / total_requests * 100
                    if total_requests > 0 else 0
                )

            return regional_stats

        except Exception as e:
            logger.error(f"Error calculating regional distribution: {str(e)}")
            return {}

    def _calculate_health_status(self, response_time: float, error_rate: float, timeout_rate: float) -> str:
        """Calculate origin server health status based on performance metrics"""
        try:
            # Log incoming metrics
            logger.debug(f"""
    Health Status Calculation:
    ----------------------
    Response Time: {response_time:.2f}ms
    Error Rate: {error_rate:.2f}%
    Timeout Rate: {timeout_rate:.2f}%

    Thresholds:
    Response Time: good={self.ttfb_thresholds['good']}, warning={self.ttfb_thresholds['warning']}, critical={self.ttfb_thresholds['critical']}
    Error Rate: good={self.error_rate_thresholds['good']}, warning={self.error_rate_thresholds['warning']}, critical={self.error_rate_thresholds['critical']}
    Timeout Rate: good={self.timeout_thresholds['good']}, warning={self.timeout_thresholds['warning']}, critical={self.timeout_thresholds['critical']}
    """)

            # Check critical conditions first
            if (response_time >= self.ttfb_thresholds['critical'] or
                error_rate >= self.error_rate_thresholds['critical'] or
                timeout_rate >= self.timeout_thresholds['critical']):
                status = 'critical'
                reason = []
                if response_time >= self.ttfb_thresholds['critical']:
                    reason.append(f"response time ({response_time:.2f}ms) exceeds critical threshold ({self.ttfb_thresholds['critical']}ms)")
                if error_rate >= self.error_rate_thresholds['critical']:
                    reason.append(f"error rate ({error_rate:.2f}%) exceeds critical threshold ({self.error_rate_thresholds['critical']}%)")
                if timeout_rate >= self.timeout_thresholds['critical']:
                    reason.append(f"timeout rate ({timeout_rate:.2f}%) exceeds critical threshold ({self.timeout_thresholds['critical']}%)")
                
                logger.warning(f"Origin health status CRITICAL: {', '.join(reason)}")
                return 'critical'

            # Check warning conditions
            if (response_time >= self.ttfb_thresholds['warning'] or
                error_rate >= self.error_rate_thresholds['warning'] or
                timeout_rate >= self.timeout_thresholds['warning']):
                status = 'degraded'
                reason = []
                if response_time >= self.ttfb_thresholds['warning']:
                    reason.append(f"response time ({response_time:.2f}ms) exceeds warning threshold ({self.ttfb_thresholds['warning']}ms)")
                if error_rate >= self.error_rate_thresholds['warning']:
                    reason.append(f"error rate ({error_rate:.2f}%) exceeds warning threshold ({self.error_rate_thresholds['warning']}%)")
                if timeout_rate >= self.timeout_thresholds['warning']:
                    reason.append(f"timeout rate ({timeout_rate:.2f}%) exceeds warning threshold ({self.timeout_thresholds['warning']}%)")
                
                logger.warning(f"Origin health status DEGRADED: {', '.join(reason)}")
                return 'degraded'

            # Check if metrics are within good thresholds
            if (response_time <= self.ttfb_thresholds['good'] and
                error_rate <= self.error_rate_thresholds['good'] and
                timeout_rate <= self.timeout_thresholds['good']):
                logger.info(f"""
    Origin health status HEALTHY:
    - Response time: {response_time:.2f}ms (threshold: {self.ttfb_thresholds['good']}ms)
    - Error rate: {error_rate:.2f}% (threshold: {self.error_rate_thresholds['good']}%)
    - Timeout rate: {timeout_rate:.2f}% (threshold: {self.timeout_thresholds['good']}%)
    """)
                return 'healthy'

            # If not critical, degraded, or healthy, return warning status
            logger.info(f"""
    Origin health status WARNING:
    - Response time: {response_time:.2f}ms
    - Error rate: {error_rate:.2f}%
    - Timeout rate: {timeout_rate:.2f}%
    """)
            return 'warning'

        except Exception as e:
            logger.error(f"Error calculating health status: {str(e)}")
            logger.error(traceback.format_exc())
            return 'unknown'

    def _analyze_origin_errors(self, df: pd.DataFrame) -> Dict:
        """Analyze origin server errors with comprehensive metrics"""
        try:
            error_df = df[df['is_error']].copy()
            
            if error_df.empty:
                return self._empty_error_metrics()

            # Calculate error timing metrics with robust aggregation
            error_timing = {
                'avg_response_time': float(error_df['origin_time_avg'].mean()),
                'percentiles': {
                    'p50': float(error_df['origin_p50'].mean()),
                    'p95': float(error_df['origin_p95'].mean()),
                    'p99': float(error_df['origin_p99'].mean())
                },
                'path_latency': float(error_df['path_latency'].mean()),
                'avg_size': float(error_df['bytes_p50'].mean())
            }

            # Calculate error distribution by status code
            status_counts = error_df['status_code'].value_counts()
            total_errors = float(status_counts.sum())
            error_distribution = {}

            for status, count in status_counts.items():
                status_df = error_df[error_df['status_code'] == status]
                count_val = float(count)

                error_distribution[int(status)] = {
                    'count': int(count_val),
                    'percentage': float(count_val / total_errors * 100) if total_errors > 0 else 0,
                    'avg_response_time': float(status_df['origin_time_avg'].mean()),
                    'p95_response_time': float(status_df['origin_p95'].mean()),
                    'avg_size': float(status_df['bytes_p50'].mean()),
                    'path_latency': float(status_df['path_latency'].mean()),
                    'error_context': {
                        'request_method': status_df['method'].mode().iloc[0] if not status_df.empty else 'unknown',
                        'most_common_path': status_df['path'].mode().iloc[0] if not status_df.empty else 'unknown',
                        'is_dynamic_content': float(status_df['is_dynamic_content'].mean() * 100)
                    }
                }

            # Analyze network and content impact
            network_impact = self._analyze_error_network_impact(error_df)
            content_impact = self._analyze_error_content_impact(error_df)
            error_correlations = self._analyze_error_correlations(df)

            # Calculate temporal error patterns
            temporal_metrics = error_df.set_index('timestamp').resample('5min').agg({
                'requests_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'path_latency': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            temporal_patterns = {
                str(row['timestamp']): {
                    'requests': int(row['requests_adjusted']),
                    'error_breakdown': {
                        '4xx': float(row['error_rate_4xx'] * 100),
                        '5xx': float(row['error_rate_5xx'] * 100)
                    },
                    'path_latency': float(row['path_latency']),
                    'confidence': float(row['confidence_score'])
                }
                for _, row in temporal_metrics.iterrows()
            }

            return {
                'error_distribution': error_distribution,
                'timing': error_timing,
                'temporal': temporal_patterns,
                'network_impact': network_impact,
                'content_impact': content_impact,
                'correlations': error_correlations,
                'summary': {
                    'total_errors': int(total_errors),
                    'unique_status_codes': len(error_distribution),
                    'avg_error_rate': float(df['is_error'].mean() * 100),
                    'peak_error_rate': float(temporal_metrics['error_rate_4xx'].max() + 
                                          temporal_metrics['error_rate_5xx'].max()) * 100 
                                    if not temporal_metrics.empty else 0
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing origin errors: {str(e)}")
            return self._empty_error_metrics()

    def _analyze_network_paths(self, df: pd.DataFrame) -> Dict:
        """Analyze network paths with proper Series handling"""
        try:
            # Group by network path components
            network_metrics = df.groupby(['colo_code', 'upper_tier_colo'], observed=True).agg({
                'requests_adjusted': 'sum',
                'origin_time_avg': ['mean', 'std'],
                'path_latency': ['mean', 'std', 'min', 'max'],
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'bytes_adjusted': 'sum',
                'confidence_score': 'mean'
            }).reset_index()

            paths = {}
            total_requests = safe_agg(df['requests_adjusted'], 'sum')

            for idx, row in network_metrics.iterrows():
                colo = clean_series_value(row['colo_code'])
                upper_tier = row['upper_tier_colo'].iloc[0] if isinstance(row['upper_tier_colo'], pd.Series) else row['upper_tier_colo']

                path_key = colo if pd.isna(upper_tier) else f"{colo}->{upper_tier}"
                requests = safe_agg(pd.Series([row['requests_adjusted']]))

                paths[path_key] = {
                    'traffic': {
                        'requests': int(requests),
                        'percentage': safe_division(requests, total_requests) * 100,
                        'bytes': safe_agg(pd.Series([row['bytes_adjusted']]))
                    },
                    'performance': {
                        'origin_time': {
                            'avg': safe_agg(pd.Series([row['origin_time_avg']['mean']])),
                            'std': safe_agg(pd.Series([row['origin_time_avg']['std']]))
                        },
                        'path_latency': {
                            'avg': safe_agg(pd.Series([row['path_latency']['mean']])),
                            'std': safe_agg(pd.Series([row['path_latency']['std']])),
                            'min': safe_agg(pd.Series([row['path_latency']['min']])),
                            'max': safe_agg(pd.Series([row['path_latency']['max']]))
                        }
                    },
                    'reliability': {
                        'error_rates': {
                            '4xx': safe_agg(pd.Series([row['error_rate_4xx']])) * 100,
                            '5xx': safe_agg(pd.Series([row['error_rate_5xx']])) * 100
                        }
                    },
                    'confidence': safe_agg(pd.Series([row['confidence_score']]))
                }

            # Calculate path distribution metrics
            tiered_df = df[df['upper_tier_colo'].notna()]
            direct_df = df[df['upper_tier_colo'].isna()]

            tiered_requests = safe_agg(tiered_df['requests_adjusted'], 'sum')
            direct_requests = safe_agg(direct_df['requests_adjusted'], 'sum')

            # Calculate performance comparison
            performance_comparison = {
                'tiered': {
                    'avg_response_time': safe_agg(tiered_df['origin_time_avg']),
                    'avg_latency': safe_agg(tiered_df['path_latency']),
                    'error_rate': safe_agg(tiered_df['is_error']) * 100,
                    'requests': int(tiered_requests)
                },
                'direct': {
                    'avg_response_time': safe_agg(direct_df['origin_time_avg']),
                    'avg_latency': safe_agg(direct_df['path_latency']),
                    'error_rate': safe_agg(direct_df['is_error']) * 100,
                    'requests': int(direct_requests)
                }
            }

            return {
                'paths': paths,
                'summary': {
                    'total_paths': len(paths),
                    'direct_paths': len([p for p in paths.keys() if '->' not in p]),
                    'tiered_paths': len([p for p in paths.keys() if '->' in p]),
                    'path_distribution': {
                        'direct_percentage': safe_division(direct_requests, total_requests) * 100,
                        'tiered_percentage': safe_division(tiered_requests, total_requests) * 100
                    },
                    'performance_comparison': performance_comparison,
                    'reliability': {
                        'error_rate': safe_division(
                            safe_agg(df[df['is_error']]['requests_adjusted'], 'sum'),
                            total_requests
                        ) * 100,
                        'path_errors': {
                            'direct': len(direct_df[direct_df['is_error']]),
                            'tiered': len(tiered_df[tiered_df['is_error']])
                        }
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing network paths: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'paths': {},
                'summary': {
                    'total_paths': 0,
                    'direct_paths': 0,
                    'tiered_paths': 0,
                    'path_distribution': {'direct_percentage': 0, 'tiered_percentage': 0},
                    'performance_comparison': {
                        'tiered': {'avg_response_time': 0, 'avg_latency': 0, 'error_rate': 0, 'requests': 0},
                        'direct': {'avg_response_time': 0, 'avg_latency': 0, 'error_rate': 0, 'requests': 0}
                    },
                    'reliability': {'error_rate': 0, 'path_errors': {'direct': 0, 'tiered': 0}}
                }
            }

    def _create_path_performance_table(self, paths: Dict) -> PrettyTable:
        """Create properly formatted path performance table"""
        try:
            table_data = []
            columns = [
                'Path', 'Requests', 'Response Time', 'Latency', 'Error Rate'
            ]
            column_types = {
                'Path': 'text',
                'Requests': 'numeric',
                'Response Time': 'numeric',
                'Latency': 'numeric',
                'Error Rate': 'percentage'
            }

            for path, metrics in paths.items():
                error_rate = sum(metrics['reliability']['error_rates'].values())
                
                table_data.append({
                    'Path': path,
                    'Requests': metrics['traffic']['requests'],
                    'Response Time': metrics['performance']['origin_time']['avg'],
                    'Latency': metrics['performance']['path_latency']['avg'],
                    'Error Rate': error_rate
                })

            return self.table_formatter.format_table(table_data, columns, column_types)

        except Exception as e:
            logger.error(f"Error creating path performance table: {str(e)}")
            return PrettyTable()

    def _analyze_error_network_impact(self, df: pd.DataFrame) -> Dict:
        """Analyze how network paths affect error rates"""
        try:
            network_metrics = df.groupby(['colo_code', 'upper_tier_colo'], observed=True).agg({
                'requests_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'path_latency': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            path_impact = {}
            total_requests = float(df['requests_adjusted'].sum())

            for _, row in network_metrics.iterrows():
                path_key = str(row['colo_code'])
                if pd.notna(row['upper_tier_colo']):
                    path_key += f"->{row['upper_tier_colo']}"

                requests = float(row['requests_adjusted'])
                path_impact[path_key] = {
                    'requests': int(requests),
                    'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                    'avg_response_time': float(row['origin_time_avg']),
                    'path_latency': float(row['path_latency']),
                    'error_rates': {
                        '4xx': float(row['error_rate_4xx'] * 100),
                        '5xx': float(row['error_rate_5xx'] * 100)
                    }
                }

            # Calculate tiered vs direct path stats
            tiered_requests = df[df['upper_tier_colo'].notna()]['requests_adjusted'].sum()
            direct_requests = df[df['upper_tier_colo'].isna()]['requests_adjusted'].sum()
            total_path_requests = tiered_requests + direct_requests

            return {
                'paths': path_impact,
                'summary': {
                    'tiered_percentage': float(tiered_requests / total_path_requests * 100) 
                                      if total_path_requests > 0 else 0,
                    'direct_percentage': float(direct_requests / total_path_requests * 100)
                                      if total_path_requests > 0 else 0,
                    'path_distribution': {
                        'tiered': int(tiered_requests),
                        'direct': int(direct_requests)
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing error network impact: {str(e)}")
            return {'paths': {}, 'summary': {}}

    def _analyze_error_content_impact(self, df: pd.DataFrame) -> Dict:
        """Analyze how content types affect error rates"""
        try:
            content_metrics = df.groupby('content_type', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'path_latency': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean'
            }).reset_index()

            content_impact = {}
            total_requests = float(df['requests_adjusted'].sum())

            for _, row in content_metrics.iterrows():
                content_type = str(row['content_type'])
                requests = float(row['requests_adjusted'])

                if pd.isna(content_type) or content_type == '':
                    continue

                content_impact[content_type] = {
                    'requests': int(requests),
                    'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                    'avg_response_time': float(row['origin_time_avg']),
                    'path_latency': float(row['path_latency']),
                    'error_rates': {
                        '4xx': float(row['error_rate_4xx'] * 100),
                        '5xx': float(row['error_rate_5xx'] * 100)
                    },
                    'content_type': 'static' if row['is_static_content'] > 0.5 else 'dynamic' 
                                  if row['is_dynamic_content'] > 0.5 else 'unknown',
                    'avg_size': float(row['bytes_adjusted'] / requests) if requests > 0 else 0
                }

            # Calculate category summaries
            static_df = df[df['is_static_content']]
            dynamic_df = df[df['is_dynamic_content']]

            return {
                'content_types': content_impact,
                'summary': {
                    'static_error_rate': float(static_df['is_error'].mean() * 100) 
                                      if not static_df.empty else 0,
                    'dynamic_error_rate': float(dynamic_df['is_error'].mean() * 100) 
                                       if not dynamic_df.empty else 0,
                    'content_correlation': float(df['is_dynamic_content'].corr(df['is_error']))
                                       if not df.empty else 0
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing error content impact: {str(e)}")
            return {'content_types': {}, 'summary': {}}

    def _analyze_error_correlations(self, df: pd.DataFrame) -> Dict:
        """Analyze correlations between errors and various metrics"""
        try:
            error_rate = df['is_error']
            
            correlations = {
                'performance': {},
                'traffic': {},
                'network': {}
            }

            # Calculate performance correlations if we have valid data
            if not df.empty and error_rate.std() != 0:
                perf_metrics = {
                    'response_time': df['origin_time_avg'],
                    'path_latency': df['path_latency'],
                    'size': df['bytes_p50']
                }
                
                correlations['performance'] = {
                    key: float(error_rate.corr(val))
                    for key, val in perf_metrics.items()
                    if val.std() != 0
                }

                # Calculate traffic correlations
                traffic_metrics = {
                    'requests': df['requests_adjusted'],
                    'bytes': df['bytes_adjusted']
                }
                
                correlations['traffic'] = {
                    key: float(error_rate.corr(val))
                    for key, val in traffic_metrics.items()
                    if val.std() != 0
                }

                # Calculate network correlations
                is_tiered = df['upper_tier_colo'].notna().astype(float)
                if is_tiered.std() != 0:
                    correlations['network']['is_tiered'] = float(error_rate.corr(is_tiered))

            # Filter out weak correlations and NaN values
            for category in correlations:
                correlations[category] = {
                    metric: val 
                    for metric, val in correlations[category].items() 
                    if not pd.isna(val) and abs(val) > 0.1
                }

            return correlations

        except Exception as e:
            logger.error(f"Error analyzing error correlations: {str(e)}")
            return {
                'performance': {},
                'traffic': {},
                'network': {}
            }

    def _analyze_metric_correlations(self, df: pd.DataFrame) -> Dict:
        """Analyze correlations between key metrics"""
        try:
            # Define metrics to correlate
            metrics = {
                'performance': ['origin_time_avg', 'ttfb_avg', 'path_latency'],
                'traffic': ['requests_adjusted', 'bytes_adjusted'],
                'reliability': ['error_rate_4xx', 'error_rate_5xx'],
                'cache': ['is_cache_hit']
            }

            correlations = {}
            
            # Calculate correlations between metric groups
            for category1, metrics1 in metrics.items():
                correlations[category1] = {}
                for metric1 in metrics1:
                    correlations[category1][metric1] = {}
                    
                    for category2, metrics2 in metrics.items():
                        if category2 != category1:  # Avoid self-correlation
                            for metric2 in metrics2:
                                if metric1 in df and metric2 in df:
                                    series1 = df[metric1]
                                    series2 = df[metric2]
                                    
                                    # Only calculate if both series have variance
                                    if series1.std() != 0 and series2.std() != 0:
                                        corr = float(series1.corr(series2))
                                        if not pd.isna(corr) and abs(corr) > 0.3:  # Only keep significant correlations
                                            correlations[category1][metric1][metric2] = {
                                                'coefficient': corr,
                                                'strength': 'strong' if abs(corr) > 0.7 else 'moderate',
                                                'direction': 'positive' if corr > 0 else 'negative'
                                            }

            # Add significant patterns
            patterns = []
            for category, category_metrics in correlations.items():
                for metric, correlations_dict in category_metrics.items():
                    for other_metric, corr_data in correlations_dict.items():
                        if abs(corr_data['coefficient']) > 0.7:
                            patterns.append({
                                'metrics': [metric, other_metric],
                                'correlation': corr_data['coefficient'],
                                'description': (
                                    f"Strong {corr_data['direction']} correlation between "
                                    f"{metric} and {other_metric}"
                                )
                            })

            return {
                'correlations': correlations,
                'patterns': patterns,
                'summary': {
                    'total_correlations': sum(
                        len(metric_corrs) 
                        for cat_corrs in correlations.values() 
                        for metric_corrs in cat_corrs.values()
                    ),
                    'strong_correlations': len([
                        p for p in patterns 
                        if abs(p['correlation']) > 0.7
                    ])
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing metric correlations: {str(e)}")
            return {
                'correlations': {},
                'patterns': [],
                'summary': {
                    'total_correlations': 0,
                    'strong_correlations': 0
                }
            }

    def _analyze_endpoint_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze endpoint performance with enhanced metrics"""
        try:
            # Group by normalized path for consistent endpoint analysis
            endpoint_metrics = df.groupby('path').agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'bytes_p50': 'mean',
                'is_error': 'mean',
                'path_latency': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            endpoints = {}
            total_requests = float(df['requests_adjusted'].sum())
            avg_response_time = float(df['origin_time_avg'].mean())
            
            for _, row in endpoint_metrics.iterrows():
                endpoint = str(row['path'])
                requests = float(row['requests_adjusted'])
                response_time = float(row['origin_time_avg'])
                error_rate = float(row['is_error'] * 100)
                
                endpoints[endpoint] = {
                    'performance': {
                        'avg_response_time': response_time,
                        'p95_response_time': float(row['origin_p95']),
                        'path_latency': float(row['path_latency']),
                        'avg_size': float(row['bytes_p50'])
                    },
                    'traffic': {
                        'requests': int(requests),
                        'bytes': float(row['bytes_adjusted']),
                        'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0
                    },
                    'reliability': {
                        'error_rate': error_rate
                    },
                    'content_type': 'static' if row['is_static_content'] > 0.5 else 'dynamic' 
                                  if row['is_dynamic_content'] > 0.5 else 'unknown',
                    'confidence': float(row['confidence_score'])
                }

            # Identify problematic endpoints
            problem_threshold = avg_response_time + (2 * df['origin_time_avg'].std())
            problematic_endpoints = [
                endpoint for endpoint, metrics in endpoints.items()
                if (
                    metrics['performance']['avg_response_time'] > problem_threshold or
                    metrics['reliability']['error_rate'] > self.error_rate_thresholds['warning']
                )
            ]

            # Sort endpoints by impact
            sorted_by_time = sorted(
                endpoints.items(),
                key=lambda x: x[1]['performance']['avg_response_time'],
                reverse=True
            )

            sorted_by_errors = sorted(
                endpoints.items(),
                key=lambda x: x[1]['reliability']['error_rate'],
                reverse=True
            )

            return {
                'endpoints': endpoints,
                'summary': {
                    'total_endpoints': len(endpoints),
                    'problematic_endpoints': problematic_endpoints,
                    'slowest_endpoints': [e[0] for e in sorted_by_time[:5]],
                    'highest_error_endpoints': [e[0] for e in sorted_by_errors[:5]],
                    'static_endpoints': sum(1 for e in endpoints.values() if e['content_type'] == 'static'),
                    'dynamic_endpoints': sum(1 for e in endpoints.values() if e['content_type'] == 'dynamic')
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing endpoint performance: {str(e)}")
            return self._empty_endpoint_metrics()

    def _analyze_content_type_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance by content type with enhanced categorization"""
        try:
            content_metrics = df.groupby('content_type', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'bytes_p50': 'mean',
                'path_latency': 'mean',
                'is_error': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean'
            }).reset_index()

            content_types = {}
            total_requests = float(df['requests_adjusted'].sum())

            for _, row in content_metrics.iterrows():
                content_type = str(row['content_type'])
                requests = float(row['requests_adjusted'])
                
                if pd.isna(content_type) or content_type == '':
                    continue

                content_types[content_type] = {
                    'traffic': {
                        'requests': int(requests),
                        'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                        'bytes': float(row['bytes_adjusted'])
                    },
                    'performance': {
                        'avg_response_time': float(row['origin_time_avg']),
                        'p95_response_time': float(row['origin_p95']),
                        'path_latency': float(row['path_latency']),
                        'avg_size': float(row['bytes_p50'])
                    },
                    'reliability': {
                        'error_rate': float(row['is_error'] * 100)
                    },
                    'type': 'static' if row['is_static_content'] > 0.5 else 'dynamic' 
                           if row['is_dynamic_content'] > 0.5 else 'unknown'
                }

            # Calculate performance by category
            static_df = df[df['is_static_content']]
            dynamic_df = df[df['is_dynamic_content']]

            category_performance = {
                'static': {
                    'avg_response_time': float(static_df['origin_time_avg'].mean()) 
                                      if not static_df.empty else 0,
                    'error_rate': float(static_df['is_error'].mean() * 100) 
                                if not static_df.empty else 0
                },
                'dynamic': {
                    'avg_response_time': float(dynamic_df['origin_time_avg'].mean()) 
                                      if not dynamic_df.empty else 0,
                    'error_rate': float(dynamic_df['is_error'].mean() * 100) 
                                if not dynamic_df.empty else 0
                }
            }

            return {
                'content_types': content_types,
                'summary': {
                    'total_types': len(content_types),
                    'static_types': sum(1 for ct in content_types.values() if ct['type'] == 'static'),
                    'dynamic_types': sum(1 for ct in content_types.values() if ct['type'] == 'dynamic'),
                    'performance_by_category': category_performance
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing content type performance: {str(e)}")
            return {'content_types': {}, 'summary': {}}

    def _get_unique_paths(self, df: pd.DataFrame) -> Set[str]:
        """Get unique network paths with proper Series handling"""
        try:
            paths = set()
            
            # Group by colo and upper tier
            path_groups = df.groupby(['colo_code', 'upper_tier_colo'], observed=True)
            
            for (colo, upper_tier), _ in path_groups:
                colo = clean_series_value(colo)
                if pd.isna(upper_tier):
                    paths.add(colo)
                else:
                    upper_tier = clean_series_value(upper_tier)
                    paths.add(f"{colo}->{upper_tier}")
                    
            return paths
            
        except Exception as e:
            logger.error(f"Error getting unique paths: {str(e)}")
            return set()

    def _empty_origin_metrics(self) -> Dict:
        """Return empty origin metrics structure"""
        return {
            'overall_metrics': self._empty_performance_metrics(),
            'temporal_analysis': {'time_series': {}, 'peak_periods': {}},
            'geographic_analysis': {'countries': {}, 'summary': {}},
            'error_analysis': self._empty_error_metrics(),
            'endpoint_analysis': self._empty_endpoint_metrics(),
            'network_analysis': {'paths': {}, 'summary': {}},
            'content_analysis': {'content_types': {}, 'summary': {}},
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

    def _empty_performance_metrics(self) -> Dict:
        """Return empty performance metrics structure"""
        return {
            'response_time': {
                'avg': 0, 'median': 0, 'p95': 0, 'p99': 0, 
                'std_dev': 0, 'total': 0
            },
            'request_volume': {
                'total': 0, 'per_second': 0,
                'path_latency_avg': 0, 'path_latency_std': 0
            },
            'failure_rates': {
                'error_rate': 0, 'timeout_rate': 0,
                'error_rate_4xx': 0, 'error_rate_5xx': 0
            },
            'bandwidth': {
                'total_bytes': 0, 'avg_response_size': 0,
                'p95_size': 0, 'p99_size': 0
            },
            'health_status': 'unknown'
        }

    def _empty_error_metrics(self) -> Dict:
        """Return empty error metrics structure"""
        return {
            'error_distribution': {},
            'timing': {
                'avg_response_time': 0,
                'percentiles': {'p50': 0, 'p95': 0, 'p99': 0},
                'path_latency': 0,
                'avg_size': 0
            },
            'temporal': {},
            'network_impact': {'paths': {}, 'summary': {}},
            'content_impact': {'content_types': {}, 'summary': {}},
            'correlations': {
                'performance': {},
                'traffic': {},
                'network': {}
            }
        }

    def _empty_endpoint_metrics(self) -> Dict:
        """Return empty endpoint metrics structure"""
        return {
            'endpoints': {},
            'summary': {
                'total_endpoints': 0,
                'problematic_endpoints': [],
                'slowest_endpoints': [],
                'highest_error_endpoints': [],
                'static_endpoints': 0,
                'dynamic_endpoints': 0
            }
        }

    def _clean_string_value(self, value: str) -> str:
        """Clean string value by removing DataFrame metadata"""
        try:
            if not isinstance(value, str):
                return str(value)
            
            value = str(value).split('\n')[0].strip()
            value = value.split('Name:')[0].strip()
            value = value.split('dtype:')[0].strip()
            
            return value
            
        except Exception as e:
            logger.error(f"Error cleaning string value: {str(e)}")
            return str(value)

    def __str__(self) -> str:
        """String representation of the analyzer"""
        return f"OriginAnalyzer(thresholds={self.ttfb_thresholds})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"OriginAnalyzer(min_requests={self.min_requests})"
