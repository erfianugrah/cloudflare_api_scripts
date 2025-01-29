import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging
from datetime import datetime, timezone
import traceback
from dataclasses import dataclass, asdict
from prettytable import PrettyTable
from .formatters import TableFormatter
from .data_processor import DataProcessor 
logger = logging.getLogger(__name__)

class EdgeAnalyzer:
    """Analyzer for edge/CDN performance metrics"""
    
    def __init__(self):
        self.ttfb_thresholds = {
            'good': 100,      # ms
            'warning': 200,   # ms
            'critical': 500   # ms
        }
        self.error_rate_thresholds = {
            'good': 1,      # %
            'warning': 5,   # %
            'critical': 10  # %
        }
        self.cache_hit_thresholds = {
            'static': {
                'good': 80,     # % - for static content
                'warning': 60,  # %
                'critical': 40  # %
            },
            'dynamic': {
                'good': 5,      # % - for dynamic content
                'warning': 2,   # %
                'critical': 1   # %
            }
        }
        self.min_requests = 10  # Minimum requests for reliable analysis
        self.table_formatter = TableFormatter()
        self.save_agg = DataProcessor()

        # Content type categories
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

    def _analyze_content_type_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance by content type with enhanced metrics"""
        try:
            # Group by content type with comprehensive metrics
            content_metrics = df.groupby('content_type', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'ttfb_p95': 'mean',
                'bytes_p50': 'mean',
                'path_latency': 'mean',
                'is_error': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            content_types = {}
            total_requests = df['requests_adjusted'].sum()

            for _, row in content_metrics.iterrows():
                content_type = str(row['content_type'])
                if pd.isna(content_type) or not content_type:
                    continue

                requests = row['requests_adjusted']
                content_types[content_type] = {
                    'traffic': {
                        'requests': int(requests),
                        'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                        'bytes': float(row['bytes_adjusted'])
                    },
                    'performance': {
                        'avg_ttfb': float(row['ttfb_avg']),
                        'p95_ttfb': float(row['ttfb_p95']),
                        'path_latency': float(row['path_latency']),
                        'avg_size': float(row['bytes_p50'])
                    },
                    'reliability': {
                        'error_rate': float(row['is_error'] * 100)
                    },
                    'type': 'static' if row['is_static_content'] > 0.5 else 'dynamic' 
                           if row['is_dynamic_content'] > 0.5 else 'unknown',
                    'confidence': float(row['confidence_score'])
                }

            # Calculate performance by category
            static_df = df[df['is_static_content']]
            dynamic_df = df[df['is_dynamic_content']]

            category_performance = {
                'static': {
                    'avg_response_time': float(static_df['ttfb_avg'].mean()) if not static_df.empty else 0,
                    'error_rate': float(static_df['is_error'].mean() * 100) if not static_df.empty else 0,
                    'avg_size': float(static_df['bytes_p50'].mean()) if not static_df.empty else 0
                },
                'dynamic': {
                    'avg_response_time': float(dynamic_df['ttfb_avg'].mean()) if not dynamic_df.empty else 0,
                    'error_rate': float(dynamic_df['is_error'].mean() * 100) if not dynamic_df.empty else 0,
                    'avg_size': float(dynamic_df['bytes_p50'].mean()) if not dynamic_df.empty else 0
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

    def _analyze_protocol_summary(self, df: pd.DataFrame) -> Dict:
        """Analyze protocol summary with improved error handling"""
        try:
            def safe_agg(series: pd.Series, operation: str = 'sum') -> float:
                try:
                    if series.empty:
                        return 0.0
                    if operation == 'mean':
                        val = series.mean()
                    elif operation == 'sum':
                        val = series.sum()
                    else:
                        val = series.mean()
                    return float(val) if not pd.isna(val) else 0.0
                except:
                    return 0.0

            df['protocol_normalized'] = df['protocol'].str.upper()
            valid_df = df[
                df['protocol_normalized'].notna() &
                (df['requests_adjusted'] > 0)
            ].copy()

            if valid_df.empty:
                return {
                    'http2_percentage': 0,
                    'http3_percentage': 0,
                    'http1_percentage': 0,
                    'fastest_protocol': 'Unknown',
                    'most_reliable_protocol': 'Unknown',
                    'best_cache_protocol': 'Unknown'
                }

            total_requests = safe_agg(valid_df['requests_adjusted'])
            
            # Calculate protocol distribution
            protocol_counts = {
                'HTTP/2': safe_agg(valid_df[valid_df['protocol_normalized'].str.contains('HTTP/2', na=False)]['requests_adjusted']),
                'HTTP/3': safe_agg(valid_df[valid_df['protocol_normalized'].str.contains('HTTP/3|QUIC', na=False)]['requests_adjusted']),
                'HTTP/1.1': safe_agg(valid_df[valid_df['protocol_normalized'].str.contains('HTTP/1.1', na=False)]['requests_adjusted']),
                'HTTP/1.0': safe_agg(valid_df[valid_df['protocol_normalized'].str.contains('HTTP/1.0', na=False)]['requests_adjusted'])
            }

            # Calculate percentages
            http2_pct = (protocol_counts['HTTP/2'] / total_requests * 100) if total_requests > 0 else 0
            http3_pct = (protocol_counts['HTTP/3'] / total_requests * 100) if total_requests > 0 else 0
            http1_pct = ((protocol_counts['HTTP/1.1'] + protocol_counts['HTTP/1.0']) / total_requests * 100) if total_requests > 0 else 0

            # Calculate detailed performance metrics by protocol
            protocol_perf = valid_df.groupby('protocol_normalized', observed=True).agg({
                'ttfb_avg': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'is_cache_hit': 'mean',
                'bytes_p50': 'mean',
                'path_latency': 'mean'
            }).reset_index()

            # Find best performing protocols
            fastest_protocol = 'Unknown'
            most_reliable = 'Unknown'
            best_cache = 'Unknown'

            if not protocol_perf.empty:
                try:
                    min_ttfb_idx = protocol_perf['ttfb_avg'].idxmin()
                    min_error_idx = (protocol_perf['error_rate_4xx'] + protocol_perf['error_rate_5xx']).idxmin()
                    max_cache_idx = protocol_perf['is_cache_hit'].idxmax()

                    fastest_protocol = str(protocol_perf.iloc[min_ttfb_idx]['protocol_normalized'])
                    most_reliable = str(protocol_perf.iloc[min_error_idx]['protocol_normalized'])
                    best_cache = str(protocol_perf.iloc[max_cache_idx]['protocol_normalized'])
                except Exception as e:
                    logger.error(f"Error finding best protocols: {str(e)}")

            # Calculate protocol comparisons
            comparisons = {}
            try:
                for i, p1 in enumerate(protocol_perf['protocol_normalized']):
                    for j, p2 in enumerate(protocol_perf['protocol_normalized']):
                        if i < j:
                            p1_data = protocol_perf.iloc[i]
                            p2_data = protocol_perf.iloc[j]
                            
                            comparison_key = f"{p1}_vs_{p2}"
                            p1_ttfb = safe_agg(pd.Series([p1_data['ttfb_avg']]))
                            p2_ttfb = safe_agg(pd.Series([p2_data['ttfb_avg']]))
                            p1_error = safe_agg(pd.Series([p1_data['error_rate_4xx'] + p1_data['error_rate_5xx']]))
                            p2_error = safe_agg(pd.Series([p2_data['error_rate_4xx'] + p2_data['error_rate_5xx']]))
                            
                            comparisons[comparison_key] = {
                                'ttfb_difference': p2_ttfb - p1_ttfb,
                                'error_rate_difference': p2_error - p1_error,
                                'performance_winner': p1 if p1_ttfb < p2_ttfb else p2,
                                'reliability_winner': p1 if p1_error < p2_error else p2
                            }
            except Exception as e:
                logger.error(f"Error calculating protocol comparisons: {str(e)}")

            return {
                'http2_percentage': float(http2_pct),
                'http3_percentage': float(http3_pct),
                'http1_percentage': float(http1_pct),
                'fastest_protocol': fastest_protocol,
                'most_reliable_protocol': most_reliable,
                'best_cache_protocol': best_cache,
                'distribution': {
                    'HTTP/2': int(protocol_counts['HTTP/2']),
                    'HTTP/3': int(protocol_counts['HTTP/3']),
                    'HTTP/1.1': int(protocol_counts['HTTP/1.1']),
                    'HTTP/1.0': int(protocol_counts['HTTP/1.0'])
                },
                'comparisons': comparisons
            }

        except Exception as e:
            logger.error(f"Error analyzing protocol summary: {str(e)}")
            return {
                'http2_percentage': 0,
                'http3_percentage': 0,
                'http1_percentage': 0,
                'fastest_protocol': 'Unknown',
                'most_reliable_protocol': 'Unknown',
                'best_cache_protocol': 'Unknown'
            }

    def analyze_edge_performance(self, df: pd.DataFrame) -> Optional[Dict]:
        """Analyze edge/CDN performance with enhanced error handling"""
        try:
            if df is None or df.empty:
                logger.error("No data available for edge analysis")
                return None

            # Calculate initial confidence score
            confidence_score = min(1.0, len(df) / self.min_requests)

            logger.info(f"""
Edge Analysis Starting:
-------------------
Total Records: {len(df)}
Total Requests: {df['requests_adjusted'].sum():,}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
""")

            # Calculate edge metrics
            edge_metrics = self._calculate_edge_metrics(df)
            if not edge_metrics:
                return None

            # Analyze cache performance
            cache_metrics = self._analyze_cache_performance(df)
            if not cache_metrics:
                return None

            # Analyze protocol performance
            protocol_metrics = {
                'protocols': self._analyze_protocol_performance(df),
                'summary': self._analyze_protocol_summary(df)
            }

            # Calculate comprehensive metrics
            analysis_result = {
                'edge_metrics': edge_metrics,
                'cache_metrics': cache_metrics,
                'protocol_metrics': protocol_metrics,
                'geographic_metrics': self._analyze_geographic_performance(df),
                'temporal_analysis': self._analyze_temporal_patterns(df),
                'error_analysis': self._analyze_edge_errors(df),
                'content_type_analysis': self._analyze_content_type_performance(df),
                'metadata': {
                    'confidence_score': confidence_score,
                    'total_requests': int(df['requests_adjusted'].sum()),
                    'time_range': {
                        'start': df['timestamp'].min().isoformat(),
                        'end': df['timestamp'].max().isoformat()
                    }
                }
            }

            # Add correlation analysis if sufficient data
            if len(df) >= self.min_requests:
                analysis_result['correlations'] = self._analyze_metric_correlations(df)

            return analysis_result

        except Exception as e:
            logger.error(f"Error in edge performance analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _calculate_edge_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate edge metrics with improved error handling and validation"""
        try:
            # Calculate total requests and error counts
            total_requests = df['requests_adjusted'].sum()
            error_requests_4xx = df[df['is_client_error']]['requests_adjusted'].sum()
            error_requests_5xx = df[df['is_server_error']]['requests_adjusted'].sum()

            # Safe aggregation helper
            def safe_agg(series: pd.Series, operation: str = 'mean') -> float:
                try:
                    if operation == 'mean':
                        return float(series.iloc[0]) if len(series) == 1 else float(series.mean())
                    elif operation == 'std':
                        return float(series.iloc[0]) if len(series) == 1 else float(series.std())
                    elif operation == 'sum':
                        return float(series.sum())
                    return 0.0
                except:
                    return 0.0

            # Calculate weighted performance metrics
            performance_metrics = {
                'ttfb_avg': safe_agg(df['ttfb_avg']),
                'ttfb_p50': safe_agg(df['ttfb_p50']),
                'ttfb_p95': safe_agg(df['ttfb_p95']),
                'ttfb_p99': safe_agg(df['ttfb_p99']),
                'ttfb_std': safe_agg(df['ttfb_avg'], 'std'),
                'ttfb_total': safe_agg(df['ttfb_sum'], 'sum')
            }

            # Calculate bytes metrics
            bytes_metrics = {
                'total': safe_agg(df['bytes_adjusted'], 'sum'),
                'avg_per_request': safe_agg(df['bytes_adjusted']) / total_requests if total_requests > 0 else 0,
                'p50': safe_agg(df['bytes_p50']),
                'p95': safe_agg(df['bytes_p95']),
                'p99': safe_agg(df['bytes_p99'])
            }

            # Calculate request metrics
            time_range = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            requests_per_second = total_requests / max(time_range, 1)

            # Calculate path latency metrics with proper handling
            path_latency_mean = safe_agg(df['path_latency'])
            path_latency_std = safe_agg(df['path_latency'], 'std')
            path_latency_min = float(df['path_latency'].min())
            path_latency_max = float(df['path_latency'].max())

            return {
                'edge_response_time': {
                    'avg': performance_metrics['ttfb_avg'],
                    'median': performance_metrics['ttfb_p50'],
                    'p95': performance_metrics['ttfb_p95'],
                    'p99': performance_metrics['ttfb_p99'],
                    'std': performance_metrics['ttfb_std'],
                    'total': performance_metrics['ttfb_total']
                },
                'request_metrics': {
                    'total': int(total_requests),
                    'per_second': float(requests_per_second),
                    'sampling_rate': float(df['sampling_rate'].mean() * 100)
                },
                'error_rates': {
                    'error_rate_4xx': float(error_requests_4xx / total_requests * 100) if total_requests > 0 else 0,
                    'error_rate_5xx': float(error_requests_5xx / total_requests * 100) if total_requests > 0 else 0,
                    'total_error_rate': float((error_requests_4xx + error_requests_5xx) / total_requests * 100) 
                                      if total_requests > 0 else 0
                },
                'bandwidth': bytes_metrics,
                'path_latency': {
                    'avg': path_latency_mean,
                    'std': path_latency_std,
                    'min': path_latency_min,
                    'max': path_latency_max
                },
                'health_status': self._calculate_edge_health_status(
                    performance_metrics['ttfb_avg'],
                    (error_requests_4xx + error_requests_5xx) / total_requests * 100 if total_requests > 0 else 0
                )
            }

        except Exception as e:
            logger.error(f"Error calculating edge metrics: {str(e)}")
            return self._empty_edge_performance_metrics()

    def _calculate_edge_health_status(self, ttfb_avg: float, error_rate: float) -> str:
        """Calculate edge health status with improved classification"""
        try:
            if (ttfb_avg <= self.ttfb_thresholds['good'] and 
                error_rate <= self.error_rate_thresholds['good']):
                return 'healthy'
            elif (ttfb_avg >= self.ttfb_thresholds['critical'] or 
                  error_rate >= self.error_rate_thresholds['critical']):
                return 'critical'
            elif (ttfb_avg >= self.ttfb_thresholds['warning'] or 
                  error_rate >= self.error_rate_thresholds['warning']):
                return 'degraded'
            return 'warning'

        except Exception as e:
            logger.error(f"Error calculating health status: {str(e)}")
            return 'unknown'

    def _analyze_cache_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze cache performance with content-type awareness"""
        try:
            # Get appropriate thresholds based on content mix
            thresholds = self._calculate_cache_thresholds(df)
            
            # Calculate total metrics
            total_requests = float(df['requests_adjusted'].sum())
            total_bytes = float(df['bytes_adjusted'].sum())
            
            if total_requests == 0:
                return self._empty_cache_metrics()

            # Calculate cache hits
            cache_hits = df[df['is_cache_hit']]['requests_adjusted'].sum()
            cache_bytes = df[df['is_cache_hit']]['bytes_adjusted'].sum()
            
            # Separate analysis for static and dynamic content
            static_df = df[df['is_static_content']]
            dynamic_df = df[df['is_dynamic_content']]
            
            static_hit_ratio = (
                static_df[static_df['is_cache_hit']]['requests_adjusted'].sum() / 
                static_df['requests_adjusted'].sum() * 100
            ) if not static_df.empty else 0
            
            dynamic_hit_ratio = (
                dynamic_df[dynamic_df['is_cache_hit']]['requests_adjusted'].sum() / 
                dynamic_df['requests_adjusted'].sum() * 100
            ) if not dynamic_df.empty else 0

            # Calculate content mix percentages
            static_pct = static_df['requests_adjusted'].sum() / total_requests * 100
            dynamic_pct = dynamic_df['requests_adjusted'].sum() / total_requests * 100

            # Calculate cache performance impact
            cache_perf = self._calculate_cache_performance_impact(df)

            return {
                'overall': {
                    'hit_ratio': float(cache_hits / total_requests * 100),
                    'static_hit_ratio': float(static_hit_ratio),
                    'dynamic_hit_ratio': float(dynamic_hit_ratio),
                    'content_mix': {
                        'static_percentage': float(static_pct),
                        'dynamic_percentage': float(dynamic_pct),
                        'unknown_percentage': float(100 - static_pct - dynamic_pct)
                    },
                    'total_requests': int(total_requests),
                    'total_bytes': float(total_bytes),
                    'bandwidth_saving': float(cache_bytes / total_bytes * 100) if total_bytes > 0 else 0,
                    'thresholds': thresholds
                },
                'status_distribution': self._analyze_cache_status(df),
                'content_type_analysis': self._analyze_cache_by_content_type(df),
                'edge_performance_impact': cache_perf,
                'path_analysis': self._analyze_cache_by_path(df)
            }

        except Exception as e:
            logger.error(f"Error analyzing cache performance: {str(e)}")
            return self._empty_cache_metrics()

    def _calculate_cache_thresholds(self, df: pd.DataFrame) -> Dict:
        """Calculate content-aware cache thresholds"""
        try:
            # Get total request volume with safe calculation
            total_requests = max(1.0, float(df['requests_adjusted'].sum()))
            
            # Calculate content type distribution
            static_requests = float(df[df['is_static_content']]['requests_adjusted'].sum())
            dynamic_requests = float(df[df['is_dynamic_content']]['requests_adjusted'].sum())
            
            # Calculate percentages
            static_pct = (static_requests / total_requests * 100)
            dynamic_pct = (dynamic_requests / total_requests * 100)
            
            logger.info(f"Using {'static' if static_pct > dynamic_pct else 'dynamic'} content thresholds "
                       f"({static_pct:.1f}% static content)")

            # Determine appropriate thresholds
            if dynamic_pct > 80:
                return self.cache_hit_thresholds['dynamic']
            else:
                return self.cache_hit_thresholds['static']

        except Exception as e:
            logger.error(f"Error calculating cache thresholds: {str(e)}")
            return self.cache_hit_thresholds['static']

    def _calculate_cache_performance_impact(self, df: pd.DataFrame) -> Dict:
        """Calculate cache performance impact with improved error handling"""
        try:
            # Separate cache hits and misses
            cache_hits = df[df['is_cache_hit']]
            cache_misses = df[~df['is_cache_hit']]
            
            def safe_mean(series: pd.Series) -> float:
                """Calculate mean safely"""
                try:
                    if series.empty:
                        return 0.0
                    val = series.mean()
                    if pd.isna(val):
                        return 0.0
                    return float(val)
                except:
                    return 0.0

            def safe_sum(series: pd.Series) -> float:
                """Calculate sum safely"""
                try:
                    if series.empty:
                        return 0.0
                    val = series.sum()
                    if pd.isna(val):
                        return 0.0
                    return float(val)
                except:
                    return 0.0

            # Calculate hit and miss metrics
            hit_metrics = {
                'ttfb': safe_mean(cache_hits['ttfb_avg']),
                'ttfb_p95': safe_mean(cache_hits['ttfb_p95']),
                'bytes': safe_mean(cache_hits['bytes_p50']),
                'bytes_total': safe_sum(cache_hits['bytes_adjusted']),
                'request_count': int(safe_sum(cache_hits['requests_adjusted']))
            }

            miss_metrics = {
                'ttfb': safe_mean(cache_misses['ttfb_avg']),
                'ttfb_p95': safe_mean(cache_misses['ttfb_p95']),
                'bytes': safe_mean(cache_misses['bytes_p50']),
                'request_count': int(safe_sum(cache_misses['requests_adjusted']))
            }

            # Calculate performance improvement
            ttfb_reduction = max(0, miss_metrics['ttfb'] - hit_metrics['ttfb'])
            ttfb_reduction_pct = (
                ((miss_metrics['ttfb'] - hit_metrics['ttfb']) / miss_metrics['ttfb'] * 100)
                if miss_metrics['ttfb'] > 0 else 0
            )

            return {
                'cache_hit_ttfb': hit_metrics['ttfb'],
                'cache_miss_ttfb': miss_metrics['ttfb'],
                'cache_hit_ttfb_p95': hit_metrics['ttfb_p95'],
                'cache_miss_ttfb_p95': miss_metrics['ttfb_p95'],
                'cache_hit_bytes': hit_metrics['bytes'],
                'cache_miss_bytes': miss_metrics['bytes'],
                'performance_improvement': {
                    'ttfb_reduction': ttfb_reduction,
                    'ttfb_reduction_percentage': ttfb_reduction_pct
                },
                'bandwidth_impact': {
                    'bytes_saved': hit_metrics['bytes_total'],
                    'requests_served': hit_metrics['request_count']
                }
            }

        except Exception as e:
            logger.error(f"Error calculating cache performance impact: {str(e)}")
            return {
                'cache_hit_ttfb': 0,
                'cache_miss_ttfb': 0,
                'cache_hit_ttfb_p95': 0,
                'cache_miss_ttfb_p95': 0,
                'cache_hit_bytes': 0,
                'cache_miss_bytes': 0,
                'performance_improvement': {'ttfb_reduction': 0, 'ttfb_reduction_percentage': 0},
                'bandwidth_impact': {'bytes_saved': 0, 'requests_served': 0}
            }

    def _analyze_cache_status(self, df: pd.DataFrame) -> Dict:
        """Analyze cache status distribution with improved error handling"""
        try:
            def safe_value(val, default: float = 0.0) -> float:
                """Safely extract float value"""
                try:
                    if pd.isna(val):
                        return default
                    if isinstance(val, pd.Series):
                        return float(val.iloc[0]) if len(val) == 1 else float(val)
                    return float(val)
                except:
                    return default

            total_requests = safe_value(df['requests_adjusted'].sum())
            total_bytes = safe_value(df['bytes_adjusted'].sum())
            
            status_metrics = df.groupby('cache_status', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'ttfb_p95': 'mean',
                'bytes_p50': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean'
            }).reset_index()

            status_dist = {}
            
            for _, row in status_metrics.iterrows():
                status = str(row['cache_status'])
                requests = safe_value(row['requests_adjusted'])
                bytes_val = safe_value(row['bytes_adjusted'])

                status_dist[status] = {
                    'requests': int(requests),
                    'requests_percentage': (requests / total_requests * 100) if total_requests > 0 else 0,
                    'bytes': bytes_val,
                    'bytes_percentage': (bytes_val / total_bytes * 100) if total_bytes > 0 else 0,
                    'avg_ttfb': safe_value(row['ttfb_avg']),
                    'p95_ttfb': safe_value(row['ttfb_p95']),
                    'avg_bytes': safe_value(row['bytes_p50']),
                    'content_type': {
                        'static_percentage': safe_value(row['is_static_content']) * 100,
                        'dynamic_percentage': safe_value(row['is_dynamic_content']) * 100
                    }
                }

            return status_dist

        except Exception as e:
            logger.error(f"Error analyzing cache status: {str(e)}")
            return {}

    def _analyze_cache_by_content_type(self, df: pd.DataFrame) -> Dict:
        """Analyze cache performance by content type with enhanced metrics"""
        try:
            content_metrics = df.groupby('content_type', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_p50': 'mean',
                'is_cache_hit': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            total_requests = float(df['requests_adjusted'].sum())
            content_types = {}

            for _, row in content_metrics.iterrows():
                content_type = str(row['content_type'])
                requests = float(row['requests_adjusted'])
                
                # Skip invalid content types
                if pd.isna(content_type) or not content_type:
                    continue

                content_types[content_type] = {
                    'traffic': {
                        'requests': int(requests),
                        'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                        'bytes': float(row['bytes_adjusted'])
                    },
                    'performance': {
                        'avg_ttfb': float(row['ttfb_avg']),
                        'avg_size': float(row['bytes_p50'])
                    },
                    'cache_hit_ratio': float(row['is_cache_hit'] * 100),
                    'type': 'static' if row['is_static_content'] > 0.5 else 'dynamic' 
                           if row['is_dynamic_content'] > 0.5 else 'unknown',
                    'confidence': float(row['confidence_score'])
                }

            # Calculate category summaries
            static_df = df[df['is_static_content']]
            dynamic_df = df[df['is_dynamic_content']]

            return {
                'content_types': content_types,
                'summary': {
                    'total_types': len(content_types),
                    'static_types': sum(1 for ct in content_types.values() if ct['type'] == 'static'),
                    'dynamic_types': sum(1 for ct in content_types.values() if ct['type'] == 'dynamic'),
                    'performance_by_category': {
                        'static': {
                            'avg_response_time': float(static_df['ttfb_avg'].mean()) if not static_df.empty else 0,
                            'error_rate': float(static_df['is_error'].mean() * 100) if not static_df.empty else 0,
                            'cache_hit_ratio': float(static_df['is_cache_hit'].mean() * 100) if not static_df.empty else 0
                        },
                        'dynamic': {
                            'avg_response_time': float(dynamic_df['ttfb_avg'].mean()) if not dynamic_df.empty else 0,
                            'error_rate': float(dynamic_df['is_error'].mean() * 100) if not dynamic_df.empty else 0,
                            'cache_hit_ratio': float(dynamic_df['is_cache_hit'].mean() * 100) if not dynamic_df.empty else 0
                        }
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing cache by content type: {str(e)}")
            return {'content_types': {}, 'summary': {}}

    def _analyze_cache_by_path(self, df: pd.DataFrame) -> Dict:
        """Analyze cache performance by path with improved error handling"""
        try:
            def safe_agg(series: pd.Series, operation: str = 'mean') -> float:
                """Safely aggregate values"""
                try:
                    if series.empty:
                        return 0.0
                    if operation == 'mean':
                        val = series.mean()
                    elif operation == 'sum':
                        val = series.sum()
                    else:
                        val = series.mean()
                    return float(val) if not pd.isna(val) else 0.0
                except:
                    return 0.0

            path_metrics = df.groupby('path', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'is_cache_hit': 'mean',
                'path_latency': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            paths = {}
            total_requests = safe_agg(df['requests_adjusted'], 'sum')

            for _, row in path_metrics.iterrows():
                path = str(row['path'])
                requests = safe_agg(pd.Series([row['requests_adjusted']]))
                
                paths[path] = {
                    'requests': int(requests),
                    'bytes': safe_agg(pd.Series([row['bytes_adjusted']])),
                    'hit_ratio': safe_agg(pd.Series([row['is_cache_hit']])) * 100,
                    'avg_ttfb': safe_agg(pd.Series([row['ttfb_avg']])),
                    'path_latency': safe_agg(pd.Series([row['path_latency']])),
                    'content_type': (
                        'static' if safe_agg(pd.Series([row['is_static_content']])) > 0.5 
                        else 'dynamic' if safe_agg(pd.Series([row['is_dynamic_content']])) > 0.5 
                        else 'unknown'
                    ),
                    'error_rates': {
                        '4xx': safe_agg(pd.Series([row['error_rate_4xx']])) * 100,
                        '5xx': safe_agg(pd.Series([row['error_rate_5xx']])) * 100
                    }
                }

            # Calculate summary statistics
            return {
                'paths': paths,
                'summary': {
                    'total_paths': len(paths),
                    'static_paths': sum(1 for p in paths.values() if p['content_type'] == 'static'),
                    'dynamic_paths': sum(1 for p in paths.values() if p['content_type'] == 'dynamic'),
                    'cache_performance': {
                        'high_hit_paths': sum(1 for p in paths.values() if p['hit_ratio'] >= 80),
                        'low_hit_paths': sum(1 for p in paths.values() if p['hit_ratio'] < 20),
                        'problematic_paths': sum(
                            1 for p in paths.values() 
                            if p['hit_ratio'] < 20 and sum(p['error_rates'].values()) > 5
                        )
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing cache by path: {str(e)}")
            return {
                'paths': {},
                'summary': {
                    'total_paths': 0,
                    'static_paths': 0,
                    'dynamic_paths': 0,
                    'cache_performance': {
                        'high_hit_paths': 0,
                        'low_hit_paths': 0,
                        'problematic_paths': 0
                    }
                }
            }

    def _analyze_protocol_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance by protocol with fixed Series handling"""
        try:
            protocol_df = df[
                (df['protocol'].notna()) & 
                (df['requests_adjusted'] > 0) & 
                (df['ttfb_avg'] > 0)
            ].copy()

            if protocol_df.empty:
                logger.warning("No valid protocol data available for analysis")
                return {}

            # Group by protocol with proper aggregation
            protocol_metrics = protocol_df.groupby('protocol', observed=True).agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': ['mean', 'std'],
                'ttfb_p50': 'mean',
                'ttfb_p95': 'mean',
                'ttfb_p99': 'mean',
                'bytes_p50': 'mean',
                'bytes_p95': 'mean',
                'bytes_p99': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'is_cache_hit': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            total_requests = DataProcessor.safe_agg(protocol_df['requests_adjusted'], 'sum')
            protocols = {}

            for _, row in protocol_metrics.iterrows():
                protocol = DataProcessor.clean_series_value(row['protocol'])
                requests = DataProcessor.safe_agg(row['requests_adjusted'], 'sum')
                
                protocols[protocol] = {
                    'requests': int(requests),
                    'requests_percentage': DataProcessor.safe_division(requests, total_requests) * 100,
                    'performance': {
                        'ttfb': {
                            'avg': DataProcessor.safe_agg(row['ttfb_avg']['mean']),
                            'std': DataProcessor.safe_agg(row['ttfb_avg']['std']),
                            'p50': DataProcessor.safe_agg(row['ttfb_p50']),
                            'p95': DataProcessor.safe_agg(row['ttfb_p95']),
                            'p99': DataProcessor.safe_agg(row['ttfb_p99'])
                        },
                        'bytes': {
                            'avg': DataProcessor.safe_agg(row['bytes_p50']),
                            'p95': DataProcessor.safe_agg(row['bytes_p95']),
                            'p99': DataProcessor.safe_agg(row['bytes_p99'])
                        }
                    },
                    'reliability': {
                        'error_rates': {
                            '4xx': DataProcessor.safe_agg(row['error_rate_4xx']) * 100,
                            '5xx': DataProcessor.safe_agg(row['error_rate_5xx']) * 100
                        },
                        'total_error_rate': DataProcessor.safe_agg(row['error_rate_4xx'] + row['error_rate_5xx']) * 100
                    },
                    'cache_hit_ratio': DataProcessor.safe_agg(row['is_cache_hit']) * 100,
                    'confidence': DataProcessor.safe_agg(row['confidence_score'])
                }

            return protocols

        except Exception as e:
            logger.error(f"Error analyzing protocol performance: {str(e)}")
            return {}

    def _create_protocol_table(self, protocols: Dict) -> PrettyTable:
        """Create properly formatted protocol performance table"""
        try:
            table_data = []
            columns = [
                'Protocol', 'Requests %', 'Avg TTFB', 'P95 TTFB',
                'Error Rate', 'Cache Hit'
            ]
            column_types = {
                'Protocol': 'text',
                'Requests %': 'percentage',
                'Avg TTFB': 'numeric',
                'P95 TTFB': 'numeric',
                'Error Rate': 'percentage',
                'Cache Hit': 'percentage'
            }

            for protocol, metrics in protocols.items():
                table_data.append({
                    'Protocol': protocol,
                    'Requests %': metrics['requests_percentage'],
                    'Avg TTFB': metrics['performance']['ttfb']['avg'],
                    'P95 TTFB': metrics['performance']['ttfb']['p95'],
                    'Error Rate': metrics['reliability']['total_error_rate'],
                    'Cache Hit': metrics['cache_hit_ratio']
                })

            return self.table_formatter.format_table(table_data, columns, column_types)

        except Exception as e:
            logger.error(f"Error creating protocol table: {str(e)}")
            return PrettyTable()

    def _analyze_geographic_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic performance with fixed Series handling"""
        try:
            if 'country' not in df.columns:
                logger.error("Country column missing from data")
                return {'countries': {}, 'summary': {}}

            # Group by country with proper aggregation
            geo_metrics = df.groupby('country', observed=True).agg({
                'ttfb_avg': ['mean', 'std', 'min', 'max'],
                'ttfb_p95': 'mean',
                'bytes_p50': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'path_latency': ['mean', 'std', 'min', 'max'],
                'confidence_score': 'mean'
            }).reset_index()

            countries = {}
            total_requests = DataProcessor.safe_agg(df['requests_adjusted'], 'sum')

            for _, row in geo_metrics.iterrows():
                country = DataProcessor.clean_series_value(row['country'])
                requests = DataProcessor.safe_agg(row['requests_adjusted'], 'sum')
                
                countries[country] = {
                    'traffic': {
                        'requests': int(requests),
                        'requests_percentage': DataProcessor.safe_division(requests, total_requests) * 100,
                        'bytes': DataProcessor.safe_agg(row['bytes_adjusted'], 'sum')
                    },
                    'performance': {
                        'ttfb': {
                            'avg': DataProcessor.safe_agg(row['ttfb_avg']['mean']),
                            'std': DataProcessor.safe_agg(row['ttfb_avg']['std']),
                            'min': DataProcessor.safe_agg(row['ttfb_avg']['min']),
                            'max': DataProcessor.safe_agg(row['ttfb_avg']['max']),
                            'p95': DataProcessor.safe_agg(row['ttfb_p95'])
                        },
                        'path_latency': {
                            'avg': DataProcessor.safe_agg(row['path_latency']['mean']),
                            'std': DataProcessor.safe_agg(row['path_latency']['std'])
                        },
                        'avg_response_size': DataProcessor.safe_agg(row['bytes_p50'])
                    },
                    'reliability': {
                        'error_rate_4xx': DataProcessor.safe_agg(row['error_rate_4xx']) * 100,
                        'error_rate_5xx': DataProcessor.safe_agg(row['error_rate_5xx']) * 100,
                        'total_error_rate': DataProcessor.safe_agg(row['error_rate_4xx'] + row['error_rate_5xx']) * 100
                    },
                    'confidence': DataProcessor.safe_agg(row['confidence_score'])
                }

            # Calculate global metrics
            global_metrics = {
                'avg_ttfb': DataProcessor.safe_agg(df['ttfb_avg']),
                'ttfb_std': DataProcessor.safe_agg(df['ttfb_avg'], 'std'),
                'avg_path_latency': DataProcessor.safe_agg(df['path_latency']),
                'path_latency_std': DataProcessor.safe_agg(df['path_latency'], 'std'),
                'total_countries': len(countries),
                'total_requests': int(total_requests),
                'avg_error_rate': DataProcessor.safe_agg(df['is_error']) * 100
            }

            sorted_by_ttfb = sorted(
                countries.items(),
                key=lambda x: x[1]['performance']['ttfb']['avg']
            )

            sorted_by_errors = sorted(
                countries.items(),
                key=lambda x: x[1]['reliability']['total_error_rate'],
                reverse=True
            )

            sorted_by_traffic = sorted(
                countries.items(),
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )

            return {
                'countries': countries,
                'summary': {
                    'global_metrics': global_metrics,
                    'regional_distribution': self._calculate_regional_distribution(countries),
                    'top_countries': {
                        'by_traffic': [DataProcessor.clean_series_value(k) for k, _ in sorted_by_traffic[:5]],
                        'fastest': [DataProcessor.clean_series_value(k) for k, _ in sorted_by_ttfb[:5]],
                        'slowest': [DataProcessor.clean_series_value(k) for k, _ in sorted_by_ttfb[-5:]],
                        'highest_errors': [
                            DataProcessor.clean_series_value(k) for k, v in sorted_by_errors[:5]
                            if v['reliability']['total_error_rate'] > 0
                        ]
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing geographic performance: {str(e)}")
            return {'countries': {}, 'summary': {}}

    def _calculate_regional_distribution(self, countries: Dict) -> Dict:
        """Calculate regional traffic distribution with improved error handling"""
        try:
            regions = {
                'North America': ['US', 'CA', 'MX'],
                'Europe': ['GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'PL'],
                'Asia Pacific': ['JP', 'CN', 'KR', 'SG', 'AU', 'IN', 'HK', 'TW'],
                'Latin America': ['BR', 'AR', 'CL', 'CO', 'PE'],
                'Middle East': ['AE', 'SA', 'IL', 'TR'],
                'Africa': ['ZA', 'NG', 'KE', 'EG']
            }

            def safe_calc(val: Any, default: float = 0.0) -> float:
                try:
                    if pd.isna(val):
                        return default
                    return float(val)
                except:
                    return default

            # Initialize regional statistics
            regional_stats = {region: {
                'requests': 0,
                'bytes': 0,
                'ttfb_sum': 0,
                'error_count': 0,
                'path_latency_sum': 0,
                'country_count': 0,
                'request_count': 0
            } for region in regions}
            
            # Add 'Other' region for unclassified countries
            regional_stats['Other'] = {
                'requests': 0,
                'bytes': 0,
                'ttfb_sum': 0,
                'error_count': 0,
                'path_latency_sum': 0,
                'country_count': 0,
                'request_count': 0
            }

            # Aggregate metrics by region
            for country, metrics in countries.items():
                region_found = False
                traffic = metrics.get('traffic', {})
                performance = metrics.get('performance', {})
                reliability = metrics.get('reliability', {})
                
                requests = safe_calc(traffic.get('requests', 0))
                bytes_val = safe_calc(traffic.get('bytes', 0))
                ttfb = safe_calc(performance.get('ttfb', {}).get('avg', 0))
                path_latency = safe_calc(performance.get('path_latency', {}).get('avg', 0))
                error_rate = safe_calc(reliability.get('total_error_rate', 0))

                for region, country_list in regions.items():
                    if country in country_list:
                        regional_stats[region]['requests'] += requests
                        regional_stats[region]['bytes'] += bytes_val
                        regional_stats[region]['ttfb_sum'] += ttfb * requests
                        regional_stats[region]['path_latency_sum'] += path_latency * requests
                        regional_stats[region]['error_count'] += error_rate * requests / 100
                        regional_stats[region]['country_count'] += 1
                        regional_stats[region]['request_count'] += requests
                        region_found = True
                        break

                if not region_found:
                    regional_stats['Other']['requests'] += requests
                    regional_stats['Other']['bytes'] += bytes_val
                    regional_stats['Other']['ttfb_sum'] += ttfb * requests
                    regional_stats['Other']['path_latency_sum'] += path_latency * requests
                    regional_stats['Other']['error_count'] += error_rate * requests / 100
                    regional_stats['Other']['country_count'] += 1
                    regional_stats['Other']['request_count'] += requests

            # Calculate regional summaries
            total_requests = sum(stats['requests'] for stats in regional_stats.values())

            for region in regional_stats:
                stats = regional_stats[region]
                requests = stats['requests']
                request_count = stats['request_count'] if stats['request_count'] > 0 else 1

                regional_stats[region].update({
                    'percentage': safe_calc(requests / total_requests * 100) if total_requests > 0 else 0,
                    'avg_ttfb': safe_calc(stats['ttfb_sum'] / request_count),
                    'avg_path_latency': safe_calc(stats['path_latency_sum'] / request_count),
                    'error_rate': safe_calc(stats['error_count'] / request_count),
                    'avg_bytes': safe_calc(stats['bytes'] / request_count),
                    'countries': stats['country_count']
                })

            # Clean up intermediate calculation fields
            for region in regional_stats:
                for field in ['ttfb_sum', 'path_latency_sum', 'error_count', 'request_count']:
                    regional_stats[region].pop(field, None)

            return regional_stats

        except Exception as e:
            logger.error(f"Error calculating regional distribution: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns with enhanced error handling"""
        try:
            # Safe aggregation helper
            def safe_agg(series: pd.Series) -> float:
                """Safely aggregate series values"""
                try:
                    if series.empty:
                        return 0.0
                    val = series.mean()
                    if pd.isna(val):
                        return 0.0
                    if isinstance(val, pd.Series):
                        return float(val.iloc[0])
                    return float(val)
                except:
                    return 0.0

            # Resample to 5-minute intervals
            df_time = df.set_index('timestamp')
            temporal_metrics = df_time.resample('5min').agg({
                'ttfb_avg': 'mean',
                'ttfb_p95': 'mean',
                'bytes_p50': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'is_error': 'mean',
                'path_latency': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            if temporal_metrics.empty:
                return self._empty_temporal_metrics()

            # Calculate moving averages
            temporal_metrics['ttfb_ma'] = temporal_metrics['ttfb_avg'].rolling(
                window=6,  # 30-minute window
                min_periods=1
            ).mean()

            temporal_metrics['requests_ma'] = temporal_metrics['requests_adjusted'].rolling(
                window=6,
                min_periods=1
            ).mean()

            # Find peak periods
            peak_load_idx = temporal_metrics['requests_adjusted'].idxmax()
            peak_latency_idx = temporal_metrics['ttfb_avg'].idxmax()

            # Build time series data
            time_series = {}
            for _, row in temporal_metrics.iterrows():
                time_series[str(row['timestamp'])] = {
                    'performance': {
                        'ttfb': safe_agg(row['ttfb_avg']),
                        'ttfb_ma': safe_agg(row['ttfb_ma']),
                        'ttfb_p95': safe_agg(row['ttfb_p95']),
                        'path_latency': safe_agg(row['path_latency'])
                    },
                    'traffic': {
                        'requests': int(row['requests_adjusted']),
                        'requests_ma': safe_agg(row['requests_ma']),
                        'bytes': safe_agg(row['bytes_adjusted'])
                    },
                    'reliability': {
                        'error_rate': safe_agg(row['is_error']) * 100
                    }
                }

            # Format peak periods safely
            peak_load_data = temporal_metrics.iloc[peak_load_idx] if not pd.isna(peak_load_idx) else None
            peak_latency_data = temporal_metrics.iloc[peak_latency_idx] if not pd.isna(peak_latency_idx) else None

            peak_periods = {
                'highest_load': {
                    'timestamp': str(peak_load_data['timestamp']) if peak_load_data is not None else None,
                    'requests': int(peak_load_data['requests_adjusted']) if peak_load_data is not None else 0,
                    'ttfb': safe_agg(peak_load_data['ttfb_avg']) if peak_load_data is not None else 0,
                    'path_latency': safe_agg(peak_load_data['path_latency']) if peak_load_data is not None else 0
                },
                'highest_latency': {
                    'timestamp': str(peak_latency_data['timestamp']) if peak_latency_data is not None else None,
                    'ttfb': safe_agg(peak_latency_data['ttfb_avg']) if peak_latency_data is not None else 0,
                    'requests': int(peak_latency_data['requests_adjusted']) if peak_latency_data is not None else 0,
                    'path_latency': safe_agg(peak_latency_data['path_latency']) if peak_latency_data is not None else 0
                }
            }

            return {
                'time_series': time_series,
                'peak_periods': peak_periods,
                'summary': {
                    'total_intervals': len(temporal_metrics),
                    'avg_ttfb': safe_agg(temporal_metrics['ttfb_avg']),
                    'ttfb_std': safe_agg(temporal_metrics['ttfb_avg'].std()),
                    'avg_requests': safe_agg(temporal_metrics['requests_adjusted']),
                    'requests_std': safe_agg(temporal_metrics['requests_adjusted'].std())
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_temporal_metrics()

    def _empty_temporal_metrics(self) -> Dict:
        """Return empty temporal metrics structure"""
        return {
            'time_series': {},
            'peak_periods': {
                'highest_load': {'timestamp': None, 'requests': 0, 'ttfb': 0, 'path_latency': 0},
                'highest_latency': {'timestamp': None, 'ttfb': 0, 'requests': 0, 'path_latency': 0}
            },
            'summary': {
                'total_intervals': 0,
                'avg_ttfb': 0,
                'ttfb_std': 0,
                'avg_requests': 0,
                'requests_std': 0
            }
        }

    def _analyze_edge_errors(self, df: pd.DataFrame) -> Dict:
        """Analyze edge errors with comprehensive metrics"""
        try:
            error_df = df[df['is_error']].copy()
            
            if error_df.empty:
                return self._empty_error_metrics()

            def safe_agg(series: pd.Series, operation: str = 'mean') -> float:
                """Calculate aggregates safely"""
                try:
                    if series.empty:
                        return 0.0
                    if operation == 'mean':
                        val = series.mean()
                    elif operation == 'sum':
                        val = series.sum()
                    elif operation == 'std':
                        val = series.std()
                    else:
                        val = series.mean()
                    return float(val) if not pd.isna(val) else 0.0
                except:
                    return 0.0

            # Calculate error timing metrics
            error_timing = {
                'avg_response_time': safe_agg(error_df['ttfb_avg']),
                'percentiles': {
                    'p50': safe_agg(error_df['ttfb_p50']),
                    'p95': safe_agg(error_df['ttfb_p95']),
                    'p99': safe_agg(error_df['ttfb_p99'])
                },
                'path_latency': safe_agg(error_df['path_latency']),
                'avg_size': safe_agg(error_df['bytes_p50'])
            }

            # Calculate error distribution by status code
            status_counts = error_df['status_code'].value_counts()
            total_errors = safe_agg(status_counts, 'sum')
            error_distribution = {}

            for status, count in status_counts.items():
                status_df = error_df[error_df['status_code'] == status]
                count_val = safe_agg(pd.Series([count]))

                error_distribution[int(status)] = {
                    'count': int(count_val),
                    'percentage': safe_agg(pd.Series([count_val / total_errors * 100])) if total_errors > 0 else 0,
                    'avg_response_time': safe_agg(status_df['ttfb_avg']),
                    'p95_response_time': safe_agg(status_df['ttfb_p95']),
                    'avg_size': safe_agg(status_df['bytes_p50']),
                    'path_latency': safe_agg(status_df['path_latency']),
                    'error_context': {
                        'request_method': str(status_df['method'].mode().iloc[0]) if not status_df.empty else 'unknown',
                        'most_common_path': str(status_df['path'].mode().iloc[0]) if not status_df.empty else 'unknown',
                        'content_type': str(status_df['content_type'].mode().iloc[0]) if not status_df.empty else 'unknown'
                    }
                }

            # Calculate temporal error patterns
            temporal_metrics = error_df.set_index('timestamp').resample('5min').agg({
                'requests_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'path_latency': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            # Analyze network and content impact
            network_impact = self._analyze_error_network_impact(error_df)
            content_impact = self._analyze_error_content_impact(error_df)
            error_correlations = self._analyze_error_correlations(error_df)

            # Build response with all metrics
            return {
                'error_distribution': error_distribution,
                'timing': error_timing,
                'temporal': {
                    str(row['timestamp']): {
                        'error_rates': {
                            '4xx': safe_agg(pd.Series([row['error_rate_4xx']])) * 100,
                            '5xx': safe_agg(pd.Series([row['error_rate_5xx']])) * 100
                        },
                        'requests': int(safe_agg(pd.Series([row['requests_adjusted']]))),
                        'path_latency': safe_agg(pd.Series([row['path_latency']])),
                        'confidence': safe_agg(pd.Series([row['confidence_score']]))
                    }
                    for _, row in temporal_metrics.iterrows()
                },
                'network_impact': network_impact,
                'content_impact': content_impact,
                'correlations': error_correlations,
                'summary': {
                    'total_errors': int(total_errors),
                    'unique_status_codes': len(error_distribution),
                    'peak_error_rate': safe_agg(
                        temporal_metrics['error_rate_4xx'] + temporal_metrics['error_rate_5xx']
                    ) * 100
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing edge errors: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_error_metrics()

    def _analyze_error_network_impact(self, df: pd.DataFrame) -> Dict:
        """Analyze how network paths affect error rates"""
        try:
            def safe_agg(series: pd.Series, operation: str = 'mean') -> float:
                try:
                    if series.empty:
                        return 0.0
                    if operation == 'mean':
                        val = series.mean()
                    elif operation == 'sum':
                        val = series.sum()
                    else:
                        val = series.mean()
                    return float(val) if not pd.isna(val) else 0.0
                except:
                    return 0.0

            network_metrics = df.groupby(['colo_code', 'upper_tier_colo'], observed=True).agg({
                'requests_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'path_latency': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            path_impact = {}
            total_requests = safe_agg(df['requests_adjusted'], 'sum')

            for _, row in network_metrics.iterrows():
                path_key = str(row['colo_code'])
                if pd.notna(row['upper_tier_colo']):
                    path_key += f"->{row['upper_tier_colo']}"

                requests = safe_agg(pd.Series([row['requests_adjusted']]))
                path_impact[path_key] = {
                    'requests': int(requests),
                    'percentage': safe_agg(pd.Series([requests / total_requests * 100])) if total_requests > 0 else 0,
                    'avg_response_time': safe_agg(pd.Series([row['ttfb_avg']])),
                    'path_latency': safe_agg(pd.Series([row['path_latency']])),
                    'error_rates': {
                        '4xx': safe_agg(pd.Series([row['error_rate_4xx']])) * 100,
                        '5xx': safe_agg(pd.Series([row['error_rate_5xx']])) * 100
                    }
                }

            # Calculate tiered vs direct path stats
            tiered_requests = safe_agg(df[df['upper_tier_colo'].notna()]['requests_adjusted'])
            direct_requests = safe_agg(df[df['upper_tier_colo'].isna()]['requests_adjusted'])
            total_path_requests = tiered_requests + direct_requests

            return {
                'paths': path_impact,
                'summary': {
                    'tiered_percentage': safe_agg(pd.Series([tiered_requests / total_path_requests * 100])) 
                                      if total_path_requests > 0 else 0,
                    'direct_percentage': safe_agg(pd.Series([direct_requests / total_path_requests * 100]))
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
            def safe_agg(series: pd.Series, operation: str = 'mean') -> float:
                try:
                    if series.empty:
                        return 0.0
                    if operation == 'mean':
                        val = series.mean()
                    elif operation == 'sum':
                        val = series.sum()
                    else:
                        val = series.mean()
                    return float(val) if not pd.isna(val) else 0.0
                except:
                    return 0.0

            content_metrics = df.groupby('content_type', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'path_latency': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'is_static_content': 'mean',
                'is_dynamic_content': 'mean'
            }).reset_index()

            content_impact = {}
            total_requests = safe_agg(df['requests_adjusted'], 'sum')

            for _, row in content_metrics.iterrows():
                content_type = str(row['content_type'])
                requests = safe_agg(pd.Series([row['requests_adjusted']]))

                if pd.isna(content_type) or not content_type:
                    continue

                content_impact[content_type] = {
                    'requests': int(requests),
                    'percentage': safe_agg(pd.Series([requests / total_requests * 100])) if total_requests > 0 else 0,
                    'avg_response_time': safe_agg(pd.Series([row['ttfb_avg']])),
                    'path_latency': safe_agg(pd.Series([row['path_latency']])),
                    'error_rates': {
                        '4xx': safe_agg(pd.Series([row['error_rate_4xx']])) * 100,
                        '5xx': safe_agg(pd.Series([row['error_rate_5xx']])) * 100
                    },
                    'content_type': (
                        'static' if safe_agg(pd.Series([row['is_static_content']])) > 0.5 
                        else 'dynamic' if safe_agg(pd.Series([row['is_dynamic_content']])) > 0.5 
                        else 'unknown'
                    ),
                    'avg_size': safe_agg(pd.Series([row['bytes_adjusted']])) / max(requests, 1)
                }

            # Calculate category summaries
            static_df = df[df['is_static_content']]
            dynamic_df = df[df['is_dynamic_content']]

            return {
                'content_types': content_impact,
                'summary': {
                    'static_error_rate': safe_agg(static_df['is_error'], 'mean') * 100 if not static_df.empty else 0,
                    'dynamic_error_rate': safe_agg(dynamic_df['is_error'], 'mean') * 100 if not dynamic_df.empty else 0,
                    'content_correlation': safe_agg(pd.Series([df['is_dynamic_content'].corr(df['is_error'])]))
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing error content impact: {str(e)}")
            return {'content_types': {}, 'summary': {}}

    def _analyze_error_correlations(self, df: pd.DataFrame) -> Dict:
        """Analyze correlations between errors and various metrics"""
        try:
            def safe_corr(s1: pd.Series, s2: pd.Series) -> float:
                try:
                    if s1.empty or s2.empty:
                        return 0.0
                    if s1.std() == 0 or s2.std() == 0:
                        return 0.0
                    corr = s1.corr(s2)
                    return float(corr) if not pd.isna(corr) else 0.0
                except:
                    return 0.0

            error_rate = df['is_error']
            correlations = {
                'performance': {},
                'traffic': {},
                'network': {}
            }

            # Calculate performance correlations if we have valid data
            if not df.empty and error_rate.std() != 0:
                perf_metrics = {
                    'response_time': df['ttfb_avg'],
                    'path_latency': df['path_latency'],
                    'size': df['bytes_p50']
                }
                
                correlations['performance'] = {
                    key: safe_corr(error_rate, val)
                    for key, val in perf_metrics.items()
                    if not val.empty and val.std() != 0
                }

                # Calculate traffic correlations
                traffic_metrics = {
                    'requests': df['requests_adjusted'],
                    'bytes': df['bytes_adjusted']
                }
                
                correlations['traffic'] = {
                    key: safe_corr(error_rate, val)
                    for key, val in traffic_metrics.items()
                    if not val.empty and val.std() != 0
                }

                # Calculate network correlations
                is_tiered = df['upper_tier_colo'].notna().astype(float)
                if is_tiered.std() != 0:
                    correlations['network']['is_tiered'] = safe_corr(error_rate, is_tiered)

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

    def _analyze_geographic_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic patterns with improved error handling"""
        try:
            if 'country' not in df.columns:
                logger.error("Country column missing from data")
                return {'countries': {}, 'summary': {}}

            def safe_agg(series: pd.Series, operation: str = 'mean') -> float:
                """Safely aggregate values"""
                try:
                    if series.empty:
                        return 0.0
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
                except:
                    return 0.0

            # Calculate geographic metrics with proper aggregation
            geo_metrics = df.groupby('country', observed=True).agg({
                'ttfb_avg': ['mean', 'std', 'min', 'max'],
                'ttfb_p95': 'mean',
                'bytes_p50': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'path_latency': ['mean', 'std', 'min', 'max'],
                'confidence_score': 'mean'
            }).reset_index()

            countries = {}
            total_requests = safe_agg(df['requests_adjusted'], 'sum')

            for _, row in geo_metrics.iterrows():
                country = self._clean_string_value(row['country'])
                requests = safe_agg(pd.Series([row['requests_adjusted']]), 'sum')
                
                # Calculate error rates
                error_4xx = safe_agg(pd.Series([row['error_rate_4xx']])) * 100
                error_5xx = safe_agg(pd.Series([row['error_rate_5xx']])) * 100
                total_error_rate = error_4xx + error_5xx
                
                # Calculate performance metrics
                ttfb_mean = safe_agg(pd.Series([row['ttfb_avg']['mean']]))
                ttfb_std = safe_agg(pd.Series([row['ttfb_avg']['std']]))
                ttfb_min = safe_agg(pd.Series([row['ttfb_avg']['min']]))
                ttfb_max = safe_agg(pd.Series([row['ttfb_avg']['max']]))
                
                # Calculate path latency metrics
                path_latency_mean = safe_agg(pd.Series([row['path_latency']['mean']]))
                path_latency_std = safe_agg(pd.Series([row['path_latency']['std']]))
                path_latency_min = safe_agg(pd.Series([row['path_latency']['min']]))
                path_latency_max = safe_agg(pd.Series([row['path_latency']['max']]))
                
                countries[country] = {
                    'performance': {
                        'ttfb': {
                            'avg': ttfb_mean,
                            'std': ttfb_std,
                            'min': ttfb_min,
                            'max': ttfb_max,
                            'p95': safe_agg(pd.Series([row['ttfb_p95']]))
                        },
                        'path_latency': {
                            'avg': path_latency_mean,
                            'std': path_latency_std,
                            'min': path_latency_min,
                            'max': path_latency_max
                        },
                        'avg_size': safe_agg(pd.Series([row['bytes_p50']]))
                    },
                    'traffic': {
                        'requests': int(requests),
                        'bytes': safe_agg(pd.Series([row['bytes_adjusted']]), 'sum'),
                        'percentage': (requests / total_requests * 100) if total_requests > 0 else 0
                    },
                    'reliability': {
                        'error_rate_4xx': error_4xx,
                        'error_rate_5xx': error_5xx,
                        'total_error_rate': total_error_rate
                    }
                }

            # Calculate global metrics safely
            global_metrics = {
                'avg_ttfb': safe_agg(df['ttfb_avg'], 'mean'),
                'ttfb_std': safe_agg(df['ttfb_avg'], 'std'),
                'avg_path_latency': safe_agg(df['path_latency'], 'mean'),
                'path_latency_std': safe_agg(df['path_latency'], 'std'),
                'total_countries': len(countries),
                'total_requests': int(total_requests),
                'avg_error_rate': safe_agg(df['is_error'], 'mean') * 100
            }

            # Sort countries by various metrics
            sorted_by_ttfb = sorted(
                countries.items(),
                key=lambda x: x[1]['performance']['ttfb']['avg']
            )

            sorted_by_errors = sorted(
                countries.items(),
                key=lambda x: x[1]['reliability']['total_error_rate'],
                reverse=True
            )

            sorted_by_traffic = sorted(
                countries.items(),
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )

            return {
                'countries': countries,
                'summary': {
                    'global_metrics': global_metrics,
                    'regional_distribution': self._calculate_regional_distribution(countries),
                    'top_countries': {
                        'by_traffic': [k for k, _ in sorted_by_traffic[:5]],
                        'fastest': [k for k, _ in sorted_by_ttfb[:5]],
                        'slowest': [k for k, _ in sorted_by_ttfb[-5:]],
                        'highest_errors': [
                            k for k, v in sorted_by_errors[:5]
                            if v['reliability']['total_error_rate'] > 0
                        ]
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing geographic patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {'countries': {}, 'summary': {}}

    def _analyze_error_patterns(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze error patterns for a specific dimension"""
        try:
            pattern_metrics = df.groupby(dimension, observed=True).agg({
                'requests_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'is_cache_hit': 'mean',
                'path_latency': 'mean'
            }).reset_index()

            patterns = {}
            total_errors = float(df['requests_adjusted'].sum())

            for _, row in pattern_metrics.iterrows():
                dim_value = str(row[dimension])
                requests = float(row['requests_adjusted'])

                patterns[dim_value] = {
                    'error_count': int(requests),
                    'error_percentage': float(requests / total_errors * 100) if total_errors > 0 else 0,
                    'avg_ttfb': float(row['ttfb_avg']),
                    'error_rates': {
                        '4xx': float(row['error_rate_4xx'] * 100),
                        '5xx': float(row['error_rate_5xx'] * 100)
                    },
                    'cache_hit_ratio': float(row['is_cache_hit'] * 100),
                    'path_latency': float(row['path_latency'])
                }

            return patterns

        except Exception as e:
            logger.error(f"Error analyzing error patterns for {dimension}: {str(e)}")
            return {}

    def _calculate_protocol_error_impact(self, error_df: pd.DataFrame, full_df: pd.DataFrame) -> Dict:
        """Calculate error impact by protocol"""
        try:
            protocol_impact = {}
            
            for protocol in full_df['protocol'].unique():
                if pd.isna(protocol):
                    continue
                    
                protocol_requests = float(full_df[full_df['protocol'] == protocol]['requests_adjusted'].sum())
                protocol_errors = float(error_df[error_df['protocol'] == protocol]['requests_adjusted'].sum())
                
                if protocol_requests > 0:
                    protocol_error_rate = (protocol_errors / protocol_requests) * 100
                    protocol_df = error_df[error_df['protocol'] == protocol]
                    
                    protocol_impact[str(protocol)] = {
                        'error_rate': float(protocol_error_rate),
                        'error_count': int(protocol_errors),
                        'total_requests': int(protocol_requests),
                        'performance': {
                            'avg_ttfb': float(protocol_df['ttfb_avg'].mean()),
                            'path_latency': float(protocol_df['path_latency'].mean())
                        },
                        'error_types': {
                            '4xx': float(protocol_df['error_rate_4xx'].mean() * 100),
                            '5xx': float(protocol_df['error_rate_5xx'].mean() * 100)
                        }
                    }

            return protocol_impact

        except Exception as e:
            logger.error(f"Error calculating protocol error impact: {str(e)}")
            return {}

    def _calculate_geographic_error_impact(self, error_df: pd.DataFrame, full_df: pd.DataFrame) -> Dict:
        """Calculate error impact by geographic region"""
        try:
            geo_impact = {}
            
            for country in full_df['country'].unique():
                if pd.isna(country):
                    continue
                    
                country_requests = float(full_df[full_df['country'] == country]['requests_adjusted'].sum())
                country_errors = float(error_df[error_df['country'] == country]['requests_adjusted'].sum())
                
                if country_requests > 0:
                    country_error_rate = (country_errors / country_requests) * 100
                    country_df = error_df[error_df['country'] == country]
                    
                    geo_impact[str(country)] = {
                        'error_rate': float(country_error_rate),
                        'error_count': int(country_errors),
                        'total_requests': int(country_requests),
                        'performance': {
                            'avg_ttfb': float(country_df['ttfb_avg'].mean()),
                            'path_latency': float(country_df['path_latency'].mean())
                        },
                        'error_types': {
                            '4xx': float(country_df['error_rate_4xx'].mean() * 100),
                            '5xx': float(country_df['error_rate_5xx'].mean() * 100)
                        },
                        'cache_hit_ratio': float(country_df['is_cache_hit'].mean() * 100)
                    }

            # Add regional aggregation
            regions = self._calculate_regional_error_distribution(geo_impact)

            return {
                'countries': geo_impact,
                'regions': regions
            }

        except Exception as e:
            logger.error(f"Error calculating geographic error impact: {str(e)}")
            return {'countries': {}, 'regions': {}}

    def _calculate_regional_error_distribution(self, geo_impact: Dict) -> Dict:
        """Calculate error distribution by region"""
        try:
            regions = {
                'North America': ['US', 'CA', 'MX'],
                'Europe': ['GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'PL'],
                'Asia Pacific': ['JP', 'CN', 'KR', 'SG', 'AU', 'IN', 'HK', 'TW'],
                'Latin America': ['BR', 'AR', 'CL', 'CO', 'PE'],
                'Middle East': ['AE', 'SA', 'IL', 'TR'],
                'Africa': ['ZA', 'NG', 'KE', 'EG']
            }

            regional_stats = {}
            
            for region, countries in regions.items():
                region_metrics = {
                    'total_errors': 0,
                    'total_requests': 0,
                    'ttfb_sum': 0,
                    'error_4xx_sum': 0,
                    'error_5xx_sum': 0
                }

                for country, metrics in geo_impact.items():
                    if country in countries:
                        region_metrics['total_errors'] += metrics['error_count']
                        region_metrics['total_requests'] += metrics['total_requests']
                        region_metrics['ttfb_sum'] += metrics['performance']['avg_ttfb'] * metrics['error_count']
                        region_metrics['error_4xx_sum'] += metrics['error_types']['4xx'] * metrics['error_count']
                        region_metrics['error_5xx_sum'] += metrics['error_types']['5xx'] * metrics['error_count']

                if region_metrics['total_errors'] > 0:
                    regional_stats[region] = {
                        'error_rate': float(region_metrics['total_errors'] / max(region_metrics['total_requests'], 1) * 100),
                        'error_count': int(region_metrics['total_errors']),
                        'avg_ttfb': float(region_metrics['ttfb_sum'] / region_metrics['total_errors']),
                        'error_types': {
                            '4xx': float(region_metrics['error_4xx_sum'] / region_metrics['total_errors']),
                            '5xx': float(region_metrics['error_5xx_sum'] / region_metrics['total_errors'])
                        }
                    }

            return regional_stats

        except Exception as e:
            logger.error(f"Error calculating regional error distribution: {str(e)}")
            return {}

    def _analyze_metric_correlations(self, df: pd.DataFrame) -> Dict:
        """Analyze correlations between key metrics with enhanced statistical analysis"""
        try:
            # Define metrics to correlate
            metrics = {
                'performance': ['ttfb_avg', 'path_latency', 'bytes_p50'],
                'traffic': ['requests_adjusted', 'bytes_adjusted'],
                'reliability': ['error_rate_4xx', 'error_rate_5xx'],
                'cache': ['is_cache_hit']
            }

            correlations = {}
            patterns = []
            
            # Calculate correlations between metric groups
            for category1, metrics1 in metrics.items():
                correlations[category1] = {}
                for metric1 in metrics1:
                    correlations[category1][metric1] = {}
                    
                    for category2, metrics2 in metrics.items():
                        if category2 != category1:  # Avoid self-correlation
                            for metric2 in metrics2:
                                if metric1 in df.columns and metric2 in df.columns:
                                    series1 = df[metric1]
                                    series2 = df[metric2]
                                    
                                    # Only calculate if both series have variance
                                    if series1.std() != 0 and series2.std() != 0:
                                        corr = float(series1.corr(series2))
                                        if not pd.isna(corr) and abs(corr) > 0.3:  # Only keep significant correlations
                                            correlations[category1][metric1][metric2] = {
                                                'coefficient': corr,
                                                'strength': 'strong' if abs(corr) > 0.7 else 'moderate',
                                                'direction': 'positive' if corr > 0 else 'negative',
                                                'significance': self._calculate_correlation_significance(corr, len(df))
                                            }

                                            # Add to patterns if correlation is strong
                                            if abs(corr) > 0.7:
                                                patterns.append({
                                                    'metrics': [metric1, metric2],
                                                    'correlation': corr,
                                                    'description': f"Strong {correlations[category1][metric1][metric2]['direction']} "
                                                                 f"correlation between {metric1} and {metric2}"
                                                })

            # Calculate time-based correlations
            time_correlations = self._analyze_time_correlations(df)

            return {
                'metric_correlations': correlations,
                'time_correlations': time_correlations,
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
                'metric_correlations': {},
                'time_correlations': {},
                'patterns': [],
                'summary': {
                    'total_correlations': 0,
                    'strong_correlations': 0
                }
            }

    def _calculate_correlation_significance(self, correlation: float, sample_size: int) -> str:
        """Calculate statistical significance of correlation"""
        try:
            # Calculate t-statistic
            t = correlation * np.sqrt((sample_size - 2) / (1 - correlation**2))
            
            # Define significance thresholds
            if abs(t) > 3.291:  # p < 0.001
                return 'highly significant'
            elif abs(t) > 2.576:  # p < 0.01
                return 'very significant'
            elif abs(t) > 1.96:   # p < 0.05
                return 'significant'
            else:
                return 'not significant'

        except Exception as e:
            logger.error(f"Error calculating correlation significance: {str(e)}")
            return 'unknown'

    def _analyze_time_correlations(self, df: pd.DataFrame) -> Dict:
        """Analyze time-based correlations and patterns"""
        try:
            # Create time-based features
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            
            # Calculate hourly patterns
            hourly_metrics = df.groupby('hour').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum',
                'is_cache_hit': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            # Calculate daily patterns
            daily_metrics = df.groupby('day_of_week').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum',
                'is_cache_hit': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            # Identify peak hours
            peak_traffic_hour = int(hourly_metrics.loc[hourly_metrics['requests_adjusted'].idxmax()]['hour'])
            peak_latency_hour = int(hourly_metrics.loc[hourly_metrics['ttfb_avg'].idxmax()]['hour'])
            peak_error_hour = int(hourly_metrics.loc[
                (hourly_metrics['error_rate_4xx'] + hourly_metrics['error_rate_5xx']).idxmax()
            ]['hour'])

            return {
                'hourly_patterns': {
                    str(int(row['hour'])): {
                        'ttfb': float(row['ttfb_avg']),
                        'requests': int(row['requests_adjusted']),
                        'cache_hit_ratio': float(row['is_cache_hit'] * 100),
                        'error_rate': float((row['error_rate_4xx'] + row['error_rate_5xx']) * 100)
                    }
                    for _, row in hourly_metrics.iterrows()
                },
                'daily_patterns': {
                    str(int(row['day_of_week'])): {
                        'ttfb': float(row['ttfb_avg']),
                        'requests': int(row['requests_adjusted']),
                        'cache_hit_ratio': float(row['is_cache_hit'] * 100),
                        'error_rate': float((row['error_rate_4xx'] + row['error_rate_5xx']) * 100)
                    }
                    for _, row in daily_metrics.iterrows()
                },
                'peak_hours': {
                    'traffic': peak_traffic_hour,
                    'latency': peak_latency_hour,
                    'errors': peak_error_hour
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing time correlations: {str(e)}")
            return {
                'hourly_patterns': {},
                'daily_patterns': {},
                'peak_hours': {
                    'traffic': 0,
                    'latency': 0,
                    'errors': 0
                }
            }

    def _empty_edge_performance_metrics(self) -> Dict:
        """Return empty edge performance metrics structure"""
        return {
            'edge_response_time': {
                'avg': 0, 'median': 0, 'p95': 0, 'p99': 0, 
                'std': 0, 'total': 0
            },
            'request_metrics': {
                'total': 0, 'per_second': 0, 'sampling_rate': 0
            },
            'error_rates': {
                'error_rate_4xx': 0, 'error_rate_5xx': 0, 'total_error_rate': 0
            },
            'bandwidth': {
                'total': 0, 'avg_per_request': 0, 'p50': 0, 'p95': 0, 'p99': 0
            },
            'path_latency': {
                'avg': 0, 'std': 0, 'min': 0, 'max': 0
            },
            'health_status': 'unknown'
        }

    def _empty_cache_metrics(self) -> Dict:
        """Return empty cache metrics structure"""
        return {
            'overall': {
                'hit_ratio': 0,
                'static_hit_ratio': 0,
                'dynamic_hit_ratio': 0,
                'content_mix': {
                    'static_percentage': 0,
                    'dynamic_percentage': 0,
                    'unknown_percentage': 100
                },
                'total_requests': 0,
                'total_bytes': 0,
                'bandwidth_saving': 0,
                'thresholds': self.cache_hit_thresholds['static']
            },
            'status_distribution': {},
            'content_type_analysis': {},
            'edge_performance_impact': {
                'cache_hit_ttfb': 0,
                'cache_miss_ttfb': 0,
                'cache_hit_ttfb_p95': 0,
                'cache_miss_ttfb_p95': 0,
                'cache_hit_bytes': 0,
                'cache_miss_bytes': 0
            },
            'path_analysis': {
                'paths': {},
                'summary': {'total_paths': 0, 'static_paths': 0, 'dynamic_paths': 0}
            }
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
            },
            'summary': {
                'total_errors': 0,
                'unique_status_codes': 0,
                'peak_error_rate': 0
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
        return f"EdgeAnalyzer(thresholds={self.ttfb_thresholds})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (
            f"EdgeAnalyzer(min_requests={self.min_requests}, "
            f"ttfb_thresholds={self.ttfb_thresholds}, "
            f"error_rate_thresholds={self.error_rate_thresholds}, "
            f"cache_hit_thresholds={self.cache_hit_thresholds})"
        )
