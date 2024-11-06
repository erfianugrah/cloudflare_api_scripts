# analyzer.py
import pandas as pd 
import numpy as np
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import traceback

logger = logging.getLogger(__name__)
from .origin_analyzer import OriginAnalyzer

class Analyzer:
    def __init__(self, config):
        self.config = config
        self.cache_categories = {
            'HIT': ['hit', 'stale', 'revalidated'],
            'MISS': ['miss', 'expired', 'updating'],
            'BYPASS': ['bypass', 'ignored'],
            'ERROR': ['error'],
            'UNKNOWN': ['unknown']
        }
        # Initialize origin analyzer
        self.origin_analyzer = OriginAnalyzer()

    def analyze_metrics(self, df: pd.DataFrame, zone_name: str) -> Dict:
        """Main analysis entry point with enhanced error handling."""
        try:
            if df is None or df.empty:
                logger.error(f"No data available for analysis of zone {zone_name}")
                return None

            # Log DataFrame information for debugging
            logger.debug(f"DataFrame columns: {df.columns.tolist()}")
            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(f"DataFrame sample:\n{df.head()}")

            # Ensure required columns exist
            required_columns = [
                'cache_status', 'visits_adjusted', 'bytes_adjusted', 'ttfb_avg',
                'status', 'error_rate_4xx', 'error_rate_5xx', 'timestamp', 'endpoint'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return None

            # Perform analysis components
            analysis_result = {
                'zone_name': zone_name,
                'cache_analysis': self._analyze_cache_metrics(df),
                'latency_analysis': self._analyze_latency_metrics(df),
                'error_analysis': self._analyze_error_metrics(df),
                'time_trends': self._analyze_time_trends(df),
                'geographic_metrics': self._analyze_geographic_metrics(df),
                'endpoint_metrics': self._analyze_endpoint_metrics(df),
                'sampling_metrics': self._analyze_sampling_distribution(df)
            }

            # Add origin analysis
            origin_analysis = self.origin_analyzer.analyze_origin_performance(df)
            if origin_analysis:
                analysis_result['origin_analysis'] = origin_analysis

            # Validate results
            valid_result = all(
                v is not None for k, v in analysis_result.items()
                if k != 'zone_name'
            )

            if not valid_result:
                logger.error("One or more analysis components failed")
                return None

            # Save analysis results
            self._save_analysis(analysis_result, f"{zone_name}_analysis.json")
            return analysis_result

        except Exception as e:
            logger.error(f"Error in metrics analysis for {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _analyze_cache_metrics(self, df: pd.DataFrame) -> Dict:
        """Analyze cache performance metrics."""
        try:
            # Calculate overall cache hit ratio
            hit_ratio = df['cache_status'].isin(self.cache_categories['HIT']).mean() * 100
            
            # Calculate cache status distribution
            status_counts = df.groupby('cache_status').agg({
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum'
            }).reset_index()
            
            total_requests = status_counts['visits_adjusted'].sum()
            total_bytes = status_counts['bytes_adjusted'].sum()
            
            status_distribution = {
                row['cache_status']: {
                    'requests': int(row['visits_adjusted']),
                    'percentage': float(row['visits_adjusted'] / total_requests * 100),
                    'bytes': float(row['bytes_adjusted']),
                    'bytes_percentage': float(row['bytes_adjusted'] / total_bytes * 100)
                }
                for _, row in status_counts.iterrows()
            }
            
            # Content type analysis
            content_metrics = df.groupby('content_type').agg({
                'cache_status': lambda x: x.isin(self.cache_categories['HIT']).mean() * 100,
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean'
            }).reset_index()
            
            content_type_dict = {
                str(row['content_type']): {
                    'hit_ratio': float(row['cache_status'].iloc[0] if isinstance(row['cache_status'], pd.Series) else row['cache_status']),
                    'requests': int(row['visits_adjusted']),
                    'bytes_gb': float(row['bytes_adjusted'] / (1024 ** 3)),
                    'avg_ttfb': float(row['ttfb_avg'])
                }
                for _, row in content_metrics.iterrows()
            }
            
            return {
                'overall': {
                    'hit_ratio': float(hit_ratio),
                    'total_requests': int(df['visits_adjusted'].sum()),
                    'total_bytes': float(df['bytes_adjusted'].sum() / (1024 ** 3)),  # GB
                    'bandwidth_saving': float(
                        (df[df['cache_status'].isin(self.cache_categories['HIT'])]['bytes_adjusted'].sum() / 
                         df['bytes_adjusted'].sum()) * 100
                    )
                },
                'status_distribution': status_distribution,
                'content_type_metrics': content_type_dict
            }
        
        except Exception as e:
            logger.error(f"Error analyzing cache metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _analyze_latency_metrics(self, df: pd.DataFrame) -> Dict:
        """Analyze latency and performance metrics."""
        try:
            # Basic latency statistics
            basic_metrics = {
                'ttfb': {
                    'avg': float(df['ttfb_avg'].mean()),
                    'p50': float(df['ttfb_p50'].mean()),
                    'p95': float(df['ttfb_p95'].mean()),
                    'p99': float(df['ttfb_p99'].mean()),
                    'std': float(df['ttfb_avg'].std())
                },
                'origin_time': {
                    'avg': float(df['origin_time_avg'].mean()),
                    'p50': float(df['origin_p50'].mean()),
                    'p95': float(df['origin_p95'].mean()),
                    'p99': float(df['origin_p99'].mean()),
                    'std': float(df['origin_time_avg'].std())
                }
            }

            # Cache status impact
            cache_impact = df.groupby('cache_status').agg({
                'ttfb_avg': ['mean', 'std', 'count'],
                'origin_time_avg': ['mean', 'std', 'count']
            }).round(2)

            # Protocol impact
            protocol_impact = df.groupby('protocol').agg({
                'ttfb_avg': ['mean', 'std', 'count'],
                'origin_time_avg': ['mean', 'std', 'count']
            }).round(2)

            return {
                'basic_metrics': basic_metrics,
                'cache_impact': {
                    status: {
                        'ttfb': {
                            'avg': float(metrics['ttfb_avg']['mean']),
                            'std': float(metrics['ttfb_avg']['std']),
                            'sample_size': int(metrics['ttfb_avg']['count'])
                        },
                        'origin_time': {
                            'avg': float(metrics['origin_time_avg']['mean']),
                            'std': float(metrics['origin_time_avg']['std']),
                            'sample_size': int(metrics['origin_time_avg']['count'])
                        }
                    }
                    for status, metrics in cache_impact.iterrows()
                },
                'protocol_impact': {
                    protocol: {
                        'ttfb': {
                            'avg': float(metrics['ttfb_avg']['mean']),
                            'std': float(metrics['ttfb_avg']['std']),
                            'sample_size': int(metrics['ttfb_avg']['count'])
                        },
                        'origin_time': {
                            'avg': float(metrics['origin_time_avg']['mean']),
                            'std': float(metrics['origin_time_avg']['std']),
                            'sample_size': int(metrics['origin_time_avg']['count'])
                        }
                    }
                    for protocol, metrics in protocol_impact.iterrows()
                }
            }
        
        except Exception as e:
            logger.error(f"Error analyzing latency metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _analyze_error_metrics(self, df: pd.DataFrame) -> Dict:
        """Analyze error metrics."""
        try:
            total_requests = df['visits_adjusted'].sum()
            
            # Status code distribution
            status_dist = df.groupby('status').agg({
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'cache_status': lambda x: x.isin(self.cache_categories['HIT']).mean() * 100
            }).reset_index()
            
            # Error patterns over time
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            error_trends = df.set_index('timestamp').resample('5min').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            return {
                'overall': {
                    'error_rate_4xx': float(df['error_rate_4xx'].mean() * 100),
                    'error_rate_5xx': float(df['error_rate_5xx'].mean() * 100),
                    'total_errors': int(
                        df[df['status'] >= 400]['visits_adjusted'].sum()
                    ),
                    'error_percentage': float(
                        df[df['status'] >= 400]['visits_adjusted'].sum() / total_requests * 100
                    )
                },
                'status_distribution': {
                    str(int(row['status'])): {
                        'requests': int(row['visits_adjusted']),
                        'percentage': float(row['visits_adjusted'] / total_requests * 100),
                        'avg_ttfb': float(row['ttfb_avg']),
                        'cache_hit_ratio': float(row['cache_status'])
                    }
                    for _, row in status_dist.iterrows()
                },
                'trends': {
                    str(timestamp): {
                        'error_rate_4xx': float(row['error_rate_4xx']),
                        'error_rate_5xx': float(row['error_rate_5xx']),
                        'requests': int(row['visits_adjusted'])
                    }
                    for timestamp, row in error_trends.iterrows()
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing error metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _analyze_geographic_metrics(self, df: pd.DataFrame) -> Dict:
        """Analyze metrics by geographic location."""
        try:
            geo_metrics = df.groupby('country').agg({
                'ttfb_avg': ['mean', 'std'],
                'origin_time_avg': ['mean', 'std'],
                'cache_status': lambda x: x.isin(self.cache_categories['HIT']).mean() * 100,
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'sampling_rate': 'mean'
            })
            
            total_requests = geo_metrics['visits_adjusted']['sum'].sum()
            
            return {
                'countries': {
                    country: {
                        'performance': {
                            'ttfb': {
                                'avg': float(metrics['ttfb_avg']['mean']),
                                'std': float(metrics['ttfb_avg']['std'])
                            },
                            'origin_time': {
                                'avg': float(metrics['origin_time_avg']['mean']),
                                'std': float(metrics['origin_time_avg']['std'])
                            }
                        },
                        'cache': {
                            'hit_ratio': float(metrics['cache_status'])
                        },
                        'traffic': {
                            'requests': int(metrics['visits_adjusted']['sum']),
                            'bytes_gb': float(metrics['bytes_adjusted']['sum'] / (1024 ** 3)),
                            'percentage': float(metrics['visits_adjusted']['sum'] / total_requests * 100)
                        },
                        'errors': {
                            'error_rate_4xx': float(metrics['error_rate_4xx']['mean'] * 100),
                            'error_rate_5xx': float(metrics['error_rate_5xx']['mean'] * 100)
                        },
                        'sampling': {
                            'rate': float(metrics['sampling_rate']['mean'] * 100)
                        }
                    }
                    for country, metrics in geo_metrics.iterrows()
                },
                'summary': {
                    'total_countries': len(geo_metrics),
                    'top_traffic_countries': list(
                        geo_metrics.nlargest(5, ('visits_adjusted', 'sum')).index
                    ),
                    'best_performing_countries': list(
                        geo_metrics.nsmallest(5, ('ttfb_avg', 'mean')).index
                    )
                }
            }
        
        except Exception as e:
            logger.error(f"Error analyzing geographic metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _analyze_sampling_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze sampling-specific metrics."""
        try:
            sampling_stats = df.agg({
                'sampling_rate': ['min', 'max', 'mean', 'median'],
                'visits': 'sum',
                'visits_adjusted': 'sum'
            })
            
            # Calculate confidence scores
            confidence_scores = df.apply(
                lambda row: self._calculate_confidence_score(
                    row['sampling_rate'], 
                    row['visits']
                ),
                axis=1
            )
            
            return {
                'sampling_rates': {
                    'min': float(sampling_stats['sampling_rate']['min'] * 100),
                    'max': float(sampling_stats['sampling_rate']['max'] * 100),
                    'mean': float(sampling_stats['sampling_rate']['mean'] * 100),
                    'median': float(sampling_stats['sampling_rate']['median'] * 100)
                },
                'confidence_scores': {
                    'min': float(confidence_scores.min()),
                    'max': float(confidence_scores.max()),
                    'mean': float(confidence_scores.mean()),
                    'median': float(confidence_scores.median())
                },
                'sample_counts': {
                    'total_samples': int(sampling_stats['visits']['sum']),
                    'estimated_total': int(sampling_stats['visits_adjusted']['sum'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing sampling distribution: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _analyze_time_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze metrics over time."""
        try:
            # Ensure timestamp is datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Calculate time ranges for different granularities
            time_range = df['timestamp'].max() - df['timestamp'].min()
            
            if time_range.total_seconds() <= 3600:  # 1 hour or less
                freq = '1min'
            elif time_range.total_seconds() <= 86400:  # 24 hours or less
                freq = '5min'
            else:
                freq = '1H'
            
            # Resample and aggregate metrics
            time_series = df.set_index('timestamp').resample(freq).agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'cache_status': lambda x: x.isin(self.cache_categories['HIT']).mean() * 100,
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'sampling_rate': 'mean'
            })
            
            # Calculate rolling averages for trend analysis
            rolling_window = 5
            rolling_metrics = time_series.rolling(window=rolling_window, min_periods=1).mean()
            
            return {
                'intervals': {
                    str(timestamp): {
                        'ttfb': float(row['ttfb_avg']),
                        'origin_time': float(row['origin_time_avg']),
                        'cache_hit_ratio': float(row['cache_status']),
                        'requests': int(row['visits_adjusted']),
                        'bytes': float(row['bytes_adjusted']),
                        'error_rate': float(row['error_rate_4xx'] + row['error_rate_5xx']),
                        'sampling_rate': float(row['sampling_rate'])
                    }
                    for timestamp, row in time_series.iterrows()
                },
                'trends': {
                    str(timestamp): {
                        'ttfb_trend': float(row['ttfb_avg']),
                        'cache_hit_ratio_trend': float(row['cache_status']),
                        'error_rate_trend': float(row['error_rate_4xx'] + row['error_rate_5xx'])
                    }
                    for timestamp, row in rolling_metrics.iterrows()
                },
                'metadata': {
                    'granularity': freq,
                    'start_time': str(df['timestamp'].min()),
                    'end_time': str(df['timestamp'].max()),
                    'rolling_window': rolling_window
                }
            }
        
        except Exception as e:
            logger.error(f"Error analyzing time trends: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _analyze_endpoint_metrics(self, df: pd.DataFrame) -> Dict:
        """Analyze metrics by endpoint."""
        try:
            endpoint_metrics = df.groupby('endpoint').agg({
                'ttfb_avg': ['mean', 'std', 'count'],
                'cache_status': lambda x: x.isin(self.cache_categories['HIT']).mean() * 100,
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            })

            total_requests = endpoint_metrics['visits_adjusted']['sum'].sum()
            
            return {
                'endpoints': {
                    str(endpoint): {
                        'performance': {
                            'ttfb_avg': float(metrics['ttfb_avg']['mean']),
                            'ttfb_std': float(metrics['ttfb_avg']['std']),
                            'sample_size': int(metrics['ttfb_avg']['count'])
                        },
                        'cache': {
                            'hit_ratio': float(metrics['cache_status'])
                        },
                        'traffic': {
                            'requests': int(metrics['visits_adjusted']['sum']),
                            'bytes_gb': float(metrics['bytes_adjusted']['sum'] / (1024 ** 3)),
                            'percentage': float(metrics['visits_adjusted']['sum'] / total_requests * 100)
                        },
                        'errors': {
                            'error_rate_4xx': float(metrics['error_rate_4xx']['mean'] * 100),
                            'error_rate_5xx': float(metrics['error_rate_5xx']['mean'] * 100)
                        }
                    }
                    for endpoint, metrics in endpoint_metrics.iterrows()
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing endpoint metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _calculate_confidence_score(self, sampling_rate: float, sample_size: int) -> float:
        """Calculate confidence score based on sampling rate and sample size."""
        try:
            # Base confidence from sampling rate
            if sampling_rate >= 0.5:  # 50% or more sampling
                base_confidence = 0.99
            elif sampling_rate >= 0.1:  # 10% or more sampling
                base_confidence = 0.95 if sample_size >= 1000 else 0.90
            elif sampling_rate >= 0.01:  # 1% or more sampling
                base_confidence = 0.90 if sample_size >= 10000 else 0.85
            else:
                base_confidence = 0.80 if sample_size >= 100000 else 0.75
            
            # Adjust for sample size
            size_factor = min(1.0, np.log10(sample_size + 1) / 6)
            return base_confidence * size_factor
            
        except Exception as e:
            logger.error(f"Error calculating confidence score: {str(e)}")
            return 0.0

    def _save_analysis(self, analysis: Dict, filename: str) -> None:
        """Save analysis results to JSON file."""
        try:
            output_path = self.config.json_dir / filename
            
            def convert_to_native(obj):
                if isinstance(obj, (np.integer, np.int64)):
                    return int(obj)
                elif isinstance(obj, (np.floating, np.float64)):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, dict):
                    return {key: convert_to_native(value) for key, value in obj.items()}
                elif isinstance(obj, (list, tuple)):
                    return [convert_to_native(item) for item in obj]
                elif pd.isna(obj):
                    return None
                elif isinstance(obj, pd.Timestamp):
                    return obj.isoformat()
                return obj

            converted_analysis = convert_to_native(analysis)
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(converted_analysis, f, indent=2)
                
            logger.info(f"Analysis results saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")
            logger.error(traceback.format_exc())

    def _get_cache_category(self, status: str) -> str:
        """Get cache category for a given cache status."""
        for category, statuses in self.cache_categories.items():
            if status.lower() in statuses:
                return category
        return 'UNKNOWN'
