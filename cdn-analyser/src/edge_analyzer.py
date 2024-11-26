import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import logging
from datetime import datetime, timezone
import traceback
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class EdgePerformanceMetrics:
    """Container for edge performance metrics"""
    response_time: Dict[str, float]  # avg, p50, p95, p99, std
    request_volume: Dict[str, float]  # total, per_second
    failure_rates: Dict[str, float]  # error_rate_4xx, error_rate_5xx
    bandwidth: Dict[str, float]      # total_bytes, avg_response_size
    health_status: str               # healthy, degraded, critical

    def get(self, key: str, default: Optional[Dict] = None) -> Dict:
        """Add dict-like get method"""
        return getattr(self, key, default)

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return asdict(self)

class EdgeAnalyzer:
    """Analyzer for edge/CDN performance metrics"""
    
    def __init__(self):
        self.ttfb_thresholds = {
            'good': 100,      # ms
            'warning': 200,   # ms
            'critical': 500  # ms
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

        # Content type categories
        self.content_types = {
            'static': [
                'text/css', 'text/javascript', 'application/javascript',
                'image/jpeg', 'image/png', 'image/gif', 'image/svg+xml',
                'text/html', 'application/x-font-ttf', 'application/x-font-woff',
                'font/woff2', 'application/font-woff2', 'text/plain',
                # Add common file extensions
                'css', 'js', 'jpeg', 'jpg', 'png', 'gif', 'svg', 'html',
                'ttf', 'woff', 'woff2'
            ],
            'dynamic': [
                'application/json', 'application/xml', 'text/xml',
                'application/x-www-form-urlencoded',
                # Add common file extensions
                'json', 'xml'
            ]
        }

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

    def analyze_edge_performance(self, df: pd.DataFrame) -> Optional[Dict]:
        """Analyze edge/CDN performance with enhanced error handling"""
        try:
            if df is None or df.empty:
                logger.error("No data available for edge analysis")
                return self._empty_edge_metrics()

            logger.info(f"""
Edge Analysis Starting:
-------------------
Total Records: {len(df)}
Total Requests: {df['requests_adjusted'].sum():,}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
""")

            # Calculate confidence score based on request volume
            confidence_score = min(1.0, len(df) / self.min_requests)

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

            # Analyze geographic performance
            geographic_metrics = self._analyze_geographic_performance(df)

            # Analyze temporal patterns
            temporal_analysis = self._analyze_temporal_patterns(df)

            # Analyze error patterns
            error_analysis = self._analyze_edge_errors(df)

            return {
                'edge_metrics': edge_metrics,
                'cache_metrics': cache_metrics,
                'protocol_metrics': protocol_metrics,
                'geographic_metrics': geographic_metrics,
                'temporal_analysis': temporal_analysis,
                'error_analysis': error_analysis,
                'metadata': {
                    'confidence_score': confidence_score,
                    'total_requests': int(df['requests_adjusted'].sum()),
                    'time_range': {
                        'start': df['timestamp'].min().isoformat(),
                        'end': df['timestamp'].max().isoformat()
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error in edge performance analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_edge_metrics()

    def _calculate_edge_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate edge metrics with proper handling of missing columns"""
        try:
            total_requests = df['requests_adjusted'].sum()
            error_requests_4xx = df[df['status'].between(400, 499)]['requests_adjusted'].sum()
            error_requests_5xx = df[df['status'].between(500, 599)]['requests_adjusted'].sum()

            # Calculate weighted edge performance metrics
            weighted_metrics = df.agg({
                'ttfb_avg': 'mean',
                'ttfb_p50': 'mean',
                'ttfb_p95': 'mean',
                'ttfb_p99': 'mean'
            })

            # Calculate bytes metrics
            bytes_metrics = {
                'total': float(df['bytes_adjusted'].sum()),
                'avg_per_request': float(df['bytes_adjusted'].sum() / total_requests) if total_requests > 0 else 0
            }

            # Add percentile metrics if available
            if all(col in df.columns for col in ['bytes_p50', 'bytes_p95', 'bytes_p99']):
                bytes_metrics.update({
                    'p50': float(df['bytes_p50'].mean()),
                    'p95': float(df['bytes_p95'].mean()),
                    'p99': float(df['bytes_p99'].mean())
                })

            time_range = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            requests_per_second = total_requests / max(time_range, 1)

            error_rate_4xx = (error_requests_4xx / total_requests * 100) if total_requests > 0 else 0
            error_rate_5xx = (error_requests_5xx / total_requests * 100) if total_requests > 0 else 0

            metrics = {
                'edge_response_time': {
                    'avg': float(self._safe_series_value(weighted_metrics['ttfb_avg'])),
                    'median': float(self._safe_series_value(weighted_metrics['ttfb_p50'])),
                    'p95': float(self._safe_series_value(weighted_metrics['ttfb_p95'])),
                    'p99': float(self._safe_series_value(weighted_metrics['ttfb_p99'])),
                    'std': float(df['ttfb_avg'].std())
                },
                'request_metrics': {
                    'total': int(total_requests),
                    'per_second': float(requests_per_second),
                    'sampling_rate': float(df['sampling_rate'].mean() * 100)
                },
                'error_rates': {
                    'error_rate_4xx': float(error_rate_4xx),
                    'error_rate_5xx': float(error_rate_5xx),
                    'total_error_rate': float(error_rate_4xx + error_rate_5xx)
                },
                'bandwidth': bytes_metrics,
                'health_status': self._calculate_edge_health_status(
                    self._safe_series_value(weighted_metrics['ttfb_avg']),
                    error_rate_4xx + error_rate_5xx
                )
            }

            return metrics

        except Exception as e:
            logger.error(f"Error calculating edge metrics: {str(e)}")
            return self._empty_edge_performance_metrics()

    def _calculate_cache_thresholds(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate appropriate cache thresholds based on content mix"""
        try:
            # Calculate percentage of static vs dynamic content
            total_requests = df['requests_adjusted'].sum()
            if total_requests == 0:
                return self.cache_hit_thresholds['static']
                
            static_mask = df['content_type'].isin(self.content_types['static'])
            dynamic_mask = df['content_type'].isin(self.content_types['dynamic'])
            
            static_pct = df[static_mask]['requests_adjusted'].sum() / total_requests * 100
            dynamic_pct = df[dynamic_mask]['requests_adjusted'].sum() / total_requests * 100
            
            # If mostly dynamic content, use dynamic thresholds
            if dynamic_pct > 80:
                thresholds = self.cache_hit_thresholds['dynamic']
                logger.info(f"Using dynamic content thresholds ({dynamic_pct:.1f}% dynamic content)")
            else:
                thresholds = self.cache_hit_thresholds['static']
                logger.info(f"Using static content thresholds ({static_pct:.1f}% static content)")
                
            return thresholds
            
        except Exception as e:
            logger.error(f"Error calculating cache thresholds: {str(e)}")
            return self.cache_hit_thresholds['static']  # Default to static thresholds

    def _analyze_cache_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze cache performance with content-type awareness"""
        try:
            # Get appropriate thresholds
            thresholds = self._calculate_cache_thresholds(df)
            
            # Calculate total metrics
            total_requests = df['requests_adjusted'].sum()
            total_bytes = df['bytes_adjusted'].sum()
            
            # Calculate cache hits
            cache_hits = df[df['cache_status'].str.lower() == 'hit']['requests_adjusted'].sum()
            cache_bytes = df[df['cache_status'].str.lower() == 'hit']['bytes_adjusted'].sum()
            
            # Separate analysis for static and dynamic content
            static_df = df[df['content_type'].isin(self.content_types['static'])]
            dynamic_df = df[df['content_type'].isin(self.content_types['dynamic'])]
            
            static_hit_ratio = (
                static_df[static_df['cache_status'].str.lower() == 'hit']['requests_adjusted'].sum() / 
                static_df['requests_adjusted'].sum() * 100
            ) if not static_df.empty else 0
            
            dynamic_hit_ratio = (
                dynamic_df[dynamic_df['cache_status'].str.lower() == 'hit']['requests_adjusted'].sum() / 
                dynamic_df['requests_adjusted'].sum() * 100
            ) if not dynamic_df.empty else 0

            # Calculate cache performance impact
            cache_perf = self._calculate_cache_performance_impact(df)
            
            return {
                'overall': {
                    'hit_ratio': float(cache_hits / total_requests * 100) if total_requests > 0 else 0,
                    'static_hit_ratio': float(static_hit_ratio),
                    'dynamic_hit_ratio': float(dynamic_hit_ratio),
                    'content_mix': {
                        'static_percentage': float(static_df['requests_adjusted'].sum() / total_requests * 100) if total_requests > 0 else 0,
                        'dynamic_percentage': float(dynamic_df['requests_adjusted'].sum() / total_requests * 100) if total_requests > 0 else 0
                    },
                    'total_requests': int(total_requests),
                    'total_bytes': float(total_bytes),
                    'bandwidth_saving': float(cache_bytes / total_bytes * 100) if total_bytes > 0 else 0,
                    'thresholds': thresholds
                },
                'status_distribution': self._analyze_cache_status(df),
                'content_type_analysis': self._analyze_cache_by_content_type(df),
                'edge_performance_impact': cache_perf
            }

        except Exception as e:
            logger.error(f"Error analyzing cache performance: {str(e)}")
            return self._empty_cache_metrics()

    def _calculate_cache_performance_impact(self, df: pd.DataFrame) -> Dict:
        """Calculate cache performance impact on edge metrics"""
        try:
            cache_hits = df[df['cache_status'].str.lower() == 'hit']
            cache_misses = df[df['cache_status'].str.lower() == 'miss']
            
            return {
                'cache_hit_ttfb': float(cache_hits['ttfb_avg'].mean()) if not cache_hits.empty else 0,
                'cache_miss_ttfb': float(cache_misses['ttfb_avg'].mean()) if not cache_misses.empty else 0,
                'cache_hit_ttfb_p95': float(cache_hits['ttfb_p95'].mean()) if not cache_hits.empty else 0,
                'cache_miss_ttfb_p95': float(cache_misses['ttfb_p95'].mean()) if not cache_misses.empty else 0
            }

        except Exception as e:
            logger.error(f"Error calculating cache performance impact: {str(e)}")
            return {
                'cache_hit_ttfb': 0,
                'cache_miss_ttfb': 0,
                'cache_hit_ttfb_p95': 0,
                'cache_miss_ttfb_p95': 0
            }

    def _analyze_cache_status(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Analyze cache status distribution with detailed metrics"""
        try:
            total_requests = df['requests_adjusted'].sum()
            total_bytes = df['bytes_adjusted'].sum()
            status_dist = {}

            for status in df['cache_status'].unique():
                if pd.isna(status):
                    continue
                    
                status_df = df[df['cache_status'] == status]
                requests = status_df['requests_adjusted'].sum()
                bytes_val = status_df['bytes_adjusted'].sum()
                
                status_dist[str(status)] = {
                    'requests': int(requests),
                    'requests_percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                    'bytes': float(bytes_val),
                    'bytes_percentage': float(bytes_val / total_bytes * 100) if total_bytes > 0 else 0,
                    'avg_ttfb': float(status_df['ttfb_avg'].mean()),
                    'p95_ttfb': float(status_df['ttfb_p95'].mean())
                }

            return status_dist

        except Exception as e:
            logger.error(f"Error analyzing cache status: {str(e)}")
            return {}

    def _get_content_type_category(self, content_type: str) -> str:
        """Determine content type category with improved matching"""
        try:
            if not content_type or pd.isna(content_type):
                return 'unknown'
                
            content_type = content_type.lower()
            
            # Check full content type
            if any(static in content_type for static in self.content_types['static']):
                return 'static'
            if any(dynamic in content_type for dynamic in self.content_types['dynamic']):
                return 'dynamic'
                
            # Check file extensions
            ext = content_type.split('/')[-1].split(';')[0].strip()
            if ext in self.content_types['static']:
                return 'static'
            if ext in self.content_types['dynamic']:
                return 'dynamic'
                
            return 'unknown'
                
        except Exception as e:
            logger.error(f"Error determining content type category: {str(e)}")
            return 'unknown'

    def _analyze_cache_by_content_type(self, df: pd.DataFrame) -> Dict:
        """Analyze cache performance by content type with improved categorization"""
        try:
            content_types = {}
            total_requests = df['requests_adjusted'].sum()
            
            for content_type in df['content_type'].unique():
                if pd.isna(content_type):
                    continue
                    
                ct_df = df[df['content_type'] == content_type]
                requests = ct_df['requests_adjusted'].sum()
                hits = ct_df[ct_df['cache_status'].str.lower() == 'hit']['requests_adjusted'].sum()
                
                content_types[content_type] = {
                    'requests': int(requests),
                    'percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                    'hit_ratio': float(hits / requests * 100) if requests > 0 else 0,
                    'type': self._get_content_type_category(content_type)
                }
                
            return content_types
            
        except Exception as e:
            logger.error(f"Error analyzing cache by content type: {str(e)}")
            return {}

    def _analyze_protocol_summary(self, df: pd.DataFrame) -> Dict:
        """Analyze protocol performance summary (renamed from _summarize_protocol_metrics)"""
        try:
            # Normalize protocol values
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

            total_requests = valid_df['requests_adjusted'].sum()
            
            # Calculate protocol distribution
            protocol_counts = {
                'HTTP/2': valid_df[valid_df['protocol_normalized'].str.contains('HTTP/2', na=False)]['requests_adjusted'].sum(),
                'HTTP/3': valid_df[valid_df['protocol_normalized'].str.contains('HTTP/3|QUIC', na=False)]['requests_adjusted'].sum(),
                'HTTP/1.1': valid_df[valid_df['protocol_normalized'].str.contains('HTTP/1.1', na=False)]['requests_adjusted'].sum(),
                'HTTP/1.0': valid_df[valid_df['protocol_normalized'].str.contains('HTTP/1.0', na=False)]['requests_adjusted'].sum()
            }

            # Calculate percentages
            http2_pct = (protocol_counts['HTTP/2'] / total_requests * 100) if total_requests > 0 else 0
            http3_pct = (protocol_counts['HTTP/3'] / total_requests * 100) if total_requests > 0 else 0
            http1_pct = ((protocol_counts['HTTP/1.1'] + protocol_counts['HTTP/1.0']) / total_requests * 100) if total_requests > 0 else 0

            # Calculate performance metrics by protocol
            protocol_perf = valid_df.groupby('protocol_normalized', observed=True).agg({
                'ttfb_avg': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: (x.str.lower() == 'hit').mean() * 100
            }).reset_index()

            # Determine best performing protocols
            if len(protocol_perf) > 0:
                fastest_protocol = str(protocol_perf.loc[protocol_perf['ttfb_avg'].idxmin(), 'protocol_normalized'])
                most_reliable = str(protocol_perf.loc[
                    (protocol_perf['error_rate_4xx'] + protocol_perf['error_rate_5xx']).idxmin(),
                    'protocol_normalized'
                ])
                best_cache = str(protocol_perf.loc[protocol_perf['cache_status'].idxmax(), 'protocol_normalized'])
            else:
                fastest_protocol = most_reliable = best_cache = 'Unknown'

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
                }
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

    def _analyze_protocol_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance by HTTP protocol with improved error handling"""
        try:
            # Normalize protocol values and remove invalid entries
            df['protocol_normalized'] = df['protocol'].str.upper()
            protocol_df = df[
                (df['protocol_normalized'].notna()) & 
                (df['requests_adjusted'] > 0) & 
                (df['ttfb_avg'] > 0)
            ].copy()

            if protocol_df.empty:
                logger.warning("No valid protocol data available for analysis")
                return {}

            # Calculate protocol-specific metrics with proper weights
            protocol_metrics = protocol_df.groupby('protocol_normalized', observed=True).agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': lambda x: np.average(x, weights=protocol_df.loc[x.index, 'requests_adjusted']),
                'ttfb_p50': 'mean',
                'ttfb_p95': 'mean',
                'ttfb_p99': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: (x.str.lower().isin(['hit', 'stale', 'revalidated'])).mean() * 100,
                'confidence_score': 'mean'
            }).reset_index()

            total_requests = protocol_metrics['requests_adjusted'].sum()
            protocols = {}

            for _, row in protocol_metrics.iterrows():
                protocol = str(row['protocol_normalized'])
                requests = self._safe_series_value(row['requests_adjusted'])
                
                protocols[protocol] = {
                    'requests': int(requests),
                    'requests_percentage': float(requests / total_requests * 100) if total_requests > 0 else 0,
                    'performance': {
                        'avg_ttfb': float(self._safe_series_value(row['ttfb_avg'])),
                        'p50_ttfb': float(self._safe_series_value(row['ttfb_p50'])),
                        'p95_ttfb': float(self._safe_series_value(row['ttfb_p95'])),
                        'p99_ttfb': float(self._safe_series_value(row['ttfb_p99']))
                    },
                    'error_rates': {
                        'error_4xx': float(self._safe_series_value(row['error_rate_4xx']) * 100),
                        'error_5xx': float(self._safe_series_value(row['error_rate_5xx']) * 100)
                    },
                    'cache_hit_ratio': float(self._safe_series_value(row['cache_status'])),
                    'confidence': float(self._safe_series_value(row['confidence_score']))
                }

            return protocols

        except Exception as e:
            logger.error(f"Error analyzing protocol performance: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _calculate_protocol_comparisons(self, protocols: Dict) -> Dict:
        """Calculate performance comparisons between protocols"""
        try:
            comparisons = {}
            protocol_list = list(protocols.keys())
            
            for i in range(len(protocol_list)):
                for j in range(i + 1, len(protocol_list)):
                    proto1, proto2 = protocol_list[i], protocol_list[j]
                    
                    # Calculate TTFB difference
                    ttfb_diff = (
                        protocols[proto1]['performance']['avg_ttfb'] -
                        protocols[proto2]['performance']['avg_ttfb']
                    )
                    
                    # Calculate error rate difference
                    error_diff = (
                        sum(protocols[proto1]['error_rates'].values()) -
                        sum(protocols[proto2]['error_rates'].values())
                    )
                    
                    # Calculate cache performance difference
                    cache_diff = (
                        protocols[proto1]['cache_hit_ratio'] -
                        protocols[proto2]['cache_hit_ratio']
                    )
                    
                    comparisons[f"{proto1}_vs_{proto2}"] = {
                        'ttfb_difference': float(ttfb_diff),
                        'error_rate_difference': float(error_diff),
                        'cache_difference': float(cache_diff),
                        'performance_winner': proto2 if ttfb_diff > 0 else proto1,
                        'reliability_winner': proto2 if error_diff > 0 else proto1,
                        'cache_winner': proto2 if cache_diff < 0 else proto1
                    }
            
            return comparisons

        except Exception as e:
            logger.error(f"Error calculating protocol comparisons: {str(e)}")
            return {}

    def _analyze_geographic_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance by geographic location with proper Series handling"""
        try:
            if 'country' not in df.columns:
                logger.error("Country column missing from data")
                return {'countries': {}, 'summary': {}}

            # Calculate total requests once
            total_requests = float(df['requests_adjusted'].sum())  # Convert to float immediately
            if total_requests == 0:
                logger.warning("No requests found in data")
                return {'countries': {}, 'summary': {}}

            # Group by country with proper aggregation
            geo_metrics = df.groupby('country', observed=True).agg({
                'ttfb_avg': ['mean', 'std'],
                'ttfb_p95': 'mean',
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: (x.str.lower() == 'hit').mean() * 100,
                'confidence_score': 'mean'
            }).reset_index()

            countries = {}
            for _, row in geo_metrics.iterrows():
                country = self._clean_string_value(row['country'])
                requests = float(self._safe_series_value(row['requests_adjusted']))  # Convert to float
                
                # Calculate error rates
                error_4xx = float(self._safe_series_value(row['error_rate_4xx']))
                error_5xx = float(self._safe_series_value(row['error_rate_5xx']))

                # Handle ttfb values
                ttfb_avg = float(self._safe_series_value(row['ttfb_avg']['mean']))
                ttfb_std = float(self._safe_series_value(row['ttfb_avg']['std']))
                if pd.isna(ttfb_std):  # Handle NaN standard deviation
                    ttfb_std = 0.0

                countries[country] = {
                    'traffic': {
                        'requests': int(requests),  # Store as integer for display
                        'requests_percentage': float((requests / total_requests) * 100),
                        'bytes': float(self._safe_series_value(row['bytes_adjusted']))
                    },
                    'performance': {
                        'ttfb_avg': max(0.0, ttfb_avg),  # Ensure non-negative
                        'ttfb_p95': float(self._safe_series_value(row['ttfb_p95'])),
                        'ttfb_std': ttfb_std,
                        'cache_hit_ratio': float(self._safe_series_value(row['cache_status']))
                    },
                    'reliability': {
                        'error_rate_4xx': error_4xx * 100,
                        'error_rate_5xx': error_5xx * 100
                    },
                    'confidence': float(self._safe_series_value(row['confidence_score']))
                }

            # Sort countries for summary
            sorted_by_ttfb = sorted(
                countries.items(),
                key=lambda x: x[1]['performance']['ttfb_avg']
            )
            sorted_by_errors = sorted(
                countries.items(),
                key=lambda x: (
                    x[1]['reliability']['error_rate_4xx'] +
                    x[1]['reliability']['error_rate_5xx']
                ),
                reverse=True
            )
            sorted_by_requests = sorted(
                countries.items(),
                key=lambda x: x[1]['traffic']['requests'],
                reverse=True
            )

            # Get unique lists for summary
            summary = {
                'fastest_edge': [k for k, _ in sorted_by_ttfb[:5]],
                'slowest_edge': [k for k, _ in sorted_by_ttfb[-5:]],
                'highest_errors': [
                    k for k, v in sorted_by_errors[:5] 
                    if v['reliability']['error_rate_4xx'] + v['reliability']['error_rate_5xx'] > 0
                ],
                'top_traffic': [k for k, _ in sorted_by_requests[:5]],
                'global_metrics': {
                    'avg_ttfb': float(df['ttfb_avg'].mean()),
                    'ttfb_std': float(df['ttfb_avg'].std()),
                    'total_countries': len(countries),
                    'total_requests': int(total_requests)
                }
            }

            return {
                'countries': countries,
                'summary': summary
            }

        except Exception as e:
            logger.error(f"Error analyzing geographic performance: {str(e)}")
            logger.error(traceback.format_exc())
            return {'countries': {}, 'summary': {}}

    def _clean_string_value(self, value: str) -> str:
        """Clean string value by removing DataFrame metadata"""
        try:
            if not isinstance(value, str):
                return str(value)
            
            # Remove DataFrame metadata
            value = str(value).split('\n')[0].strip()
            value = value.split('Name:')[0].strip()
            value = value.split('dtype:')[0].strip()
            
            return value
            
        except Exception as e:
            logger.error(f"Error cleaning string value: {str(e)}")
            return str(value)

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns in edge performance"""
        try:
            df_time = df.set_index('timestamp')
            temporal_metrics = df_time.resample('5min').agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'ttfb_p95': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: (x.str.lower() == 'hit').mean() * 100,
                'confidence_score': 'mean'
            }).reset_index()

            # Calculate moving averages
            temporal_metrics['ttfb_ma'] = temporal_metrics['ttfb_avg'].rolling(
                window=6,  # 30-minute window
                min_periods=1
            ).mean()

            # Find peak periods
            peak_load_idx = temporal_metrics['requests_adjusted'].idxmax()
            peak_latency_idx = temporal_metrics['ttfb_avg'].idxmax()
            
            peak_load = temporal_metrics.iloc[peak_load_idx] if not pd.isna(peak_load_idx) else None
            peak_latency = temporal_metrics.iloc[peak_latency_idx] if not pd.isna(peak_latency_idx) else None

            return {
                'time_series': {
                    str(row['timestamp']): {
                        'traffic': {
                            'requests': int(self._safe_series_value(row['requests_adjusted'])),
                            'bytes': float(self._safe_series_value(row['bytes_adjusted']))
                        },
                        'performance': {
                            'ttfb': float(self._safe_series_value(row['ttfb_avg'])),
                            'ttfb_ma': float(self._safe_series_value(row['ttfb_ma'])),
                            'ttfb_p95': float(self._safe_series_value(row['ttfb_p95'])),
                            'cache_hit_ratio': float(self._safe_series_value(row['cache_status']))
                        },
                        'reliability': {
                            'error_rate_4xx': float(self._safe_series_value(row['error_rate_4xx']) * 100),
                            'error_rate_5xx': float(self._safe_series_value(row['error_rate_5xx']) * 100)
                        }
                    }
                    for _, row in temporal_metrics.iterrows()
                },
                'peak_periods': {
                    'highest_load': {
                        'timestamp': str(peak_load['timestamp']) if peak_load is not None else None,
                        'requests': int(self._safe_series_value(peak_load['requests_adjusted'])) if peak_load is not None else 0,
                        'ttfb': float(self._safe_series_value(peak_load['ttfb_avg'])) if peak_load is not None else 0
                    },
                    'highest_latency': {
                        'timestamp': str(peak_latency['timestamp']) if peak_latency is not None else None,
                        'ttfb': float(self._safe_series_value(peak_latency['ttfb_avg'])) if peak_latency is not None else 0,
                        'requests': int(self._safe_series_value(peak_latency['requests_adjusted'])) if peak_latency is not None else 0
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {str(e)}")
            logger.error(traceback.format_exc())
            return {'time_series': {}, 'peak_periods': {}}

    def _analyze_edge_errors(self, df: pd.DataFrame) -> Dict:
        """Analyze edge errors with comprehensive metrics"""
        try:
            # Filter for error responses
            error_df = df[
                (df['error_rate_4xx'] > 0) | 
                (df['error_rate_5xx'] > 0) |
                (df['status'] >= 400)
            ].copy()

            if error_df.empty:
                return self._empty_error_metrics()

            # Calculate error timing metrics
            error_timing = error_df.agg({
                'ttfb_avg': 'mean',
                'ttfb_p50': 'mean',
                'ttfb_p95': 'mean',
                'ttfb_p99': 'mean'
            })

            # Calculate error distribution
            status_counts = error_df['status'].value_counts()
            total_errors = status_counts.sum()
            error_distribution = {}

            for status, count in status_counts.items():
                if status >= 400:  # Ensure we only include error statuses
                    error_distribution[int(status)] = {
                        'count': int(count),
                        'percentage': float(count / total_errors * 100) if total_errors > 0 else 0,
                        'avg_ttfb': float(
                            error_df[error_df['status'] == status]['ttfb_avg'].mean()
                        )
                    }

            # Analyze error patterns over time
            hourly_errors = df.set_index('timestamp').resample('1h').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum',
                'confidence_score': 'mean'
            }).reset_index()

            # Find peak error periods
            total_error_rate = hourly_errors['error_rate_4xx'] + hourly_errors['error_rate_5xx']
            peak_error_idx = total_error_rate.idxmax()
            peak_errors = hourly_errors.iloc[peak_error_idx] if not pd.isna(peak_error_idx) else None

            # Analyze error patterns by dimension
            dimension_patterns = {
                'by_country': self._calculate_error_rates_by_dimension(df, 'country'),
                'by_protocol': self._calculate_error_rates_by_dimension(df, 'protocol'),
                'by_cache_status': self._calculate_error_rates_by_dimension(df, 'cache_status')
            }

            # Analyze error correlation with performance
            error_correlations = self._analyze_error_correlations(df)

            return {
                'error_distribution': error_distribution,
                'timing': {
                    'avg_ttfb': float(self._safe_series_value(error_timing['ttfb_avg'])),
                    'percentiles': {
                        'p50': float(self._safe_series_value(error_timing['ttfb_p50'])),
                        'p95': float(self._safe_series_value(error_timing['ttfb_p95'])),
                        'p99': float(self._safe_series_value(error_timing['ttfb_p99']))
                    }
                },
                'patterns': dimension_patterns,
                'correlations': error_correlations,
                'temporal': {
                    str(row['timestamp']): {
                        'error_rate_4xx': float(self._safe_series_value(row['error_rate_4xx']) * 100),
                        'error_rate_5xx': float(self._safe_series_value(row['error_rate_5xx']) * 100),
                        'total_error_rate': float(
                            self._safe_series_value(row['error_rate_4xx']) * 100 +
                            self._safe_series_value(row['error_rate_5xx']) * 100
                        ),
                        'requests': int(self._safe_series_value(row['requests_adjusted'])),
                        'confidence': float(self._safe_series_value(row['confidence_score']))
                    }
                    for _, row in hourly_errors.iterrows()
                },
                'peak_error_period': {
                    'timestamp': str(peak_errors['timestamp']) if peak_errors is not None else None,
                    'error_rate': float(
                        (self._safe_series_value(peak_errors['error_rate_4xx']) +
                         self._safe_series_value(peak_errors['error_rate_5xx'])) * 100
                    ) if peak_errors is not None else 0,
                    'requests': int(self._safe_series_value(peak_errors['requests_adjusted']))
                    if peak_errors is not None else 0
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing edge errors: {str(e)}")
            logger.error(traceback.format_exc())
            return self._empty_error_metrics()

    def _calculate_error_rates_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Calculate error rates for a specific dimension with improved aggregation"""
        try:
            # Process metrics using proper groupby handling
            metrics = df.groupby(dimension, observed=True).agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'confidence_score': 'mean'
            }).reset_index()

            dimension_data = {}
            for _, row in metrics.iterrows():
                dim_value = str(row[dimension])
                requests = self._safe_series_value(row['requests_adjusted'])
                
                dimension_data[dim_value] = {
                    'error_rate_4xx': float(self._safe_series_value(row['error_rate_4xx']) * 100),
                    'error_rate_5xx': float(self._safe_series_value(row['error_rate_5xx']) * 100),
                    'total_error_rate': float(
                        self._safe_series_value(row['error_rate_4xx']) * 100 +
                        self._safe_series_value(row['error_rate_5xx']) * 100
                    ),
                    'requests': int(requests),
                    'avg_ttfb': float(self._safe_series_value(row['ttfb_avg'])),
                    'confidence': float(self._safe_series_value(row['confidence_score']))
                }

            return dimension_data

        except Exception as e:
            logger.error(f"Error calculating error rates for {dimension}: {str(e)}")
            return {}

    def _analyze_error_correlations(self, df: pd.DataFrame) -> Dict:
        """Analyze correlations between errors and other metrics"""
        try:
            correlations = {}
            error_rate = df['error_rate_4xx'] + df['error_rate_5xx']
            
            # Correlation with performance metrics
            correlations['performance'] = {
                'ttfb': float(error_rate.corr(df['ttfb_avg'])),
                'ttfb_p95': float(error_rate.corr(df['ttfb_p95']))
            }
            
            # Correlation with traffic
            correlations['traffic'] = {
                'requests': float(error_rate.corr(df['requests_adjusted'])),
                'bytes': float(error_rate.corr(df['bytes_adjusted']))
            }
            
            return correlations

        except Exception as e:
            logger.error(f"Error analyzing error correlations: {str(e)}")
            return {}

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

    def _empty_edge_metrics(self) -> Dict:
        """Return empty edge metrics structure"""
        return {
            'edge_metrics': self._empty_edge_performance_metrics(),
            'cache_metrics': self._empty_cache_metrics(),
            'protocol_metrics': {'protocols': {}, 'summary': {}},
            'geographic_metrics': {'countries': {}, 'summary': {}},
            'temporal_analysis': {'time_series': {}, 'peak_periods': {}},
            'error_analysis': self._empty_error_metrics(),
            'metadata': {
                'confidence_score': 0,
                'total_requests': 0,
                'time_range': {
                    'start': datetime.now(timezone.utc).isoformat(),
                    'end': datetime.now(timezone.utc).isoformat()
                }
            }
        }

    def _empty_edge_performance_metrics(self) -> Dict:
        """Return empty edge performance metrics"""
        return {
            'edge_response_time': {
                'avg': 0, 'median': 0, 'p95': 0, 'p99': 0, 'std': 0
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
            'health_status': 'unknown'
        }

    def _empty_cache_metrics(self) -> Dict:
        """Return empty cache metrics"""
        return {
            'overall': {
                'hit_ratio': 0,
                'static_hit_ratio': 0,
                'dynamic_hit_ratio': 0,
                'content_mix': {
                    'static_percentage': 0,
                    'dynamic_percentage': 0
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
                'cache_miss_ttfb_p95': 0
            }
        }

    def _empty_error_metrics(self) -> Dict:
        """Return empty error metrics"""
        return {
            'error_distribution': {},
            'timing': {
                'avg_ttfb': 0,
                'percentiles': {'p50': 0, 'p95': 0, 'p99': 0}
            },
            'patterns': {
                'by_country': {},
                'by_protocol': {},
                'by_cache_status': {}
            },
            'correlations': {
                'performance': {'ttfb': 0, 'ttfb_p95': 0},
                'traffic': {'requests': 0, 'bytes': 0}
            },
            'temporal': {}
        }

    def _empty_protocol_metrics(self) -> Dict:
        """Return empty protocol metrics"""
        return {
            'protocols': {},
            'summary': {
                'http2_percentage': 0,
                'http3_percentage': 0,
                'http1_percentage': 0,
                'fastest_protocol': 'Unknown',
                'most_reliable_protocol': 'Unknown',
                'best_cache_protocol': 'Unknown',
                'comparisons': {}
            }
        }
