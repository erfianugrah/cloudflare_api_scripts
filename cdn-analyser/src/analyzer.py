# analyzer.py
import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Union
import logging
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path

logger = logging.getLogger(__name__)

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

    def _safe_weighted_average(self, values: pd.Series, weights: pd.Series) -> float:
        """Calculate weighted average with proper handling of zero weights."""
        try:
            valid_mask = ~(pd.isna(values) | pd.isna(weights) | (weights == 0))
            if not valid_mask.any():
                return values.mean() if not pd.isna(values).all() else 0.0
            return np.average(values[valid_mask], weights=weights[valid_mask])
        except Exception as e:
            logger.warning(f"Error calculating weighted average: {str(e)}")
            return values.mean() if not pd.isna(values).all() else 0.0

    def _determine_time_freq(self, df: pd.DataFrame) -> str:
        """Determine appropriate time frequency based on data range."""
        try:
            time_range = df['timestamp'].max() - df['timestamp'].min()
            total_minutes = time_range.total_seconds() / 60
            
            if total_minutes <= 60:  # 1 hour
                return '5min'
            elif total_minutes <= 24 * 60:  # 24 hours
                return '1h'
            elif total_minutes <= 7 * 24 * 60:  # 7 days
                return '6h'
            elif total_minutes <= 30 * 24 * 60:  # 30 days
                return '1D'
            elif total_minutes <= 90 * 24 * 60:  # 90 days
                return '1W'
            else:
                return '1M'
        except Exception as e:
            logger.error(f"Error determining time frequency: {str(e)}")
            return '1h'

    def analyze_cache(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze cache performance metrics including path analysis."""
        try:
            if df is None or df.empty:
                logger.error(f"No data available for cache analysis of zone {zone_name}")
                return None

            df_copy = df.copy()
            
            # Calculate overall metrics
            total_requests = df_copy['visits_adjusted'].sum()
            total_bytes = df_copy['bytes_adjusted'].sum()
            
            # Calculate hit ratio
            cache_hits = df_copy[df_copy['cache_status'].isin(self.cache_categories['HIT'])]
            hit_requests = cache_hits['visits_adjusted'].sum()
            hit_ratio = (hit_requests / total_requests * 100) if total_requests > 0 else 0

            # Calculate averages safely
            avg_sampling_rate = df_copy['sampling_rate'].mean()
            avg_confidence = self._safe_weighted_average(
                df_copy['confidence_score'],
                df_copy['visits']
            )

            cache_analysis = {
                'zone_name': zone_name,
                'overall': {
                    'total_requests': int(total_requests),
                    'total_bytes_gb': round(total_bytes / (1024 ** 3), 2),
                    'hit_ratio': round(hit_ratio, 2),
                    'avg_sampling_rate': round(avg_sampling_rate * 100, 2),
                    'confidence_score': round(avg_confidence, 3),
                    'sampled_requests': int(df_copy['visits'].sum())
                },
                'by_cache_status': self._analyze_cache_distribution(df_copy),
                'by_content_type': self._analyze_by_dimension(df_copy, 'content_type'),
                'by_country': self._analyze_by_dimension(df_copy, 'country'),
                'by_device': self._analyze_by_dimension(df_copy, 'device_type'),
                'by_path': self._analyze_paths(df_copy),
                'temporal': self._analyze_temporal_patterns(df_copy),
                'sampling_metrics': self._analyze_sampling_distribution(df_copy)
            }

            self._save_analysis(cache_analysis, f"{zone_name}_cache_analysis.json")
            return cache_analysis

        except Exception as e:
            logger.error(f"Error in cache analysis for {zone_name}: {str(e)}")
            return None

    def analyze_performance(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze performance metrics including path analysis."""
        try:
            if df is None or df.empty:
                logger.error(f"No data available for performance analysis of zone {zone_name}")
                return None

            df_copy = df.copy()

            # Calculate weighted performance metrics safely
            weighted_ttfb = self._safe_weighted_average(df_copy['ttfb_avg'], df_copy['visits'])
            weighted_origin = self._safe_weighted_average(df_copy['origin_time_avg'], df_copy['visits'])
            
            # Calculate confidence metrics
            avg_sampling_rate = df_copy['sampling_rate'].mean()
            avg_confidence = self._safe_weighted_average(df_copy['confidence_score'], df_copy['visits'])

            # Get percentiles
            percentiles = {
                'ttfb': {
                    'p50': float(df_copy['ttfb_p50'].mean()),
                    'p95': float(df_copy['ttfb_p95'].mean()),
                    'p99': float(df_copy['ttfb_p99'].mean())
                },
                'origin': {
                    'p50': float(df_copy['origin_p50'].mean()),
                    'p95': float(df_copy['origin_p95'].mean()),
                    'p99': float(df_copy['origin_p99'].mean())
                }
            }

            # Prepare analysis dictionary
            perf_analysis = {
                'zone_name': zone_name,
                'overall': {
                    'avg_ttfb_ms': round(weighted_ttfb, 2),
                    'avg_origin_time_ms': round(weighted_origin, 2),
                    'total_requests': int(df_copy['visits_adjusted'].sum()),
                    'sampled_requests': int(df_copy['visits'].sum()),
                    'avg_sampling_rate': round(avg_sampling_rate * 100, 2),
                    'confidence_score': round(avg_confidence, 3)
                },
                'percentiles': percentiles,
                'by_content_type': self._analyze_performance_by_dimension(df_copy, 'content_type'),
                'by_country': self._analyze_performance_by_dimension(df_copy, 'country'),
                'by_device': self._analyze_performance_by_dimension(df_copy, 'device_type'),
                'by_path': self._analyze_path_performance(df_copy),
                'trends': self._analyze_performance_trends(df_copy),
                'sampling_metrics': self._analyze_sampling_distribution(df_copy)
            }

            self._save_analysis(perf_analysis, f"{zone_name}_performance_analysis.json")
            return perf_analysis

        except Exception as e:
            logger.error(f"Error in performance analysis for {zone_name}: {str(e)}")
            return None

    def _analyze_cache_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze cache status distribution with safe calculations."""
        try:
            df_copy = df.copy()
            cache_metrics = df_copy.groupby('cache_status').agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            })

            total_requests = cache_metrics['visits_adjusted'].sum()
            if total_requests == 0:
                logger.warning("No requests found in cache distribution analysis")
                return {}

            return {
                status: {
                    'percentage': round((row['visits_adjusted'] / total_requests * 100), 2),
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                    'avg_sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for status, row in cache_metrics.iterrows()
                if row['visits'] > 0
            }

        except Exception as e:
            logger.error(f"Error in cache distribution analysis: {str(e)}")
            return {}

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns with path information."""
        try:
            df_copy = df.copy()
            time_freq = self._determine_time_freq(df_copy)

            # Define aggregation functions
            agg_funcs = {
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'sampling_rate': 'mean',
                'confidence_score': 'mean',
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean'
            }

            # Get top paths
            top_paths = (df_copy.groupby('path_group')['visits_adjusted']
                        .sum()
                        .sort_values(ascending=False)
                        .head(5)
                        .index)

            # Create result structure
            temporal_metrics = {'overall': {}, 'by_path': {}}

            # Process overall metrics
            df_copy.set_index('timestamp', inplace=True)
            overall_metrics = df_copy.resample(time_freq).agg(agg_funcs)

            temporal_metrics['overall'] = {
                str(dt): {
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                    'ttfb_avg': round(row['ttfb_avg'], 2),
                    'origin_time_avg': round(row['origin_time_avg'], 2),
                    'sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for dt, row in overall_metrics.iterrows()
            }

            # Process path-specific metrics
            for path in top_paths:
                path_df = df[df['path_group'] == path].copy()
                if not path_df.empty:
                    path_df.set_index('timestamp', inplace=True)
                    path_metrics = path_df.resample(time_freq).agg(agg_funcs)

                    temporal_metrics['by_path'][str(path)] = {
                        str(dt): {
                            'requests_sampled': int(row['visits']),
                            'requests_estimated': int(row['visits_adjusted']),
                            'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                            'ttfb_avg': round(row['ttfb_avg'], 2),
                            'origin_time_avg': round(row['origin_time_avg'], 2),
                            'sampling_rate': round(row['sampling_rate'] * 100, 2),
                            'confidence_score': round(row['confidence_score'], 3)
                        }
                        for dt, row in path_metrics.iterrows()
                    }

            return temporal_metrics

        except Exception as e:
            logger.error(f"Error in temporal analysis: {str(e)}")
            return {'overall': {}, 'by_path': {}}

    def _analyze_performance_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze performance trends."""
        try:
            df_copy = df.copy()
            time_freq = self._determine_time_freq(df_copy)

            # Set up aggregation
            df_copy.set_index('timestamp', inplace=True)
            resampled = df_copy.resample(time_freq).agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            })

            return {
                str(dt): {
                    'ttfb': round(row['ttfb_avg'], 2),
                    'origin_time': round(row['origin_time_avg'], 2),
                    'error_rate': round((row['error_rate_4xx'] + row['error_rate_5xx']) * 100, 2),
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for dt, row in resampled.iterrows()
            }

        except Exception as e:
            logger.error(f"Error in performance trends analysis: {str(e)}")
            return {}

    def _analyze_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze metrics by dimension with safe calculations."""
        try:
            df_copy = df.copy()
            metrics = df_copy.groupby(dimension).agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            })

            total_requests = metrics['visits_adjusted'].sum()
            if total_requests == 0:
                return {}

            return {
                str(dim): {
                    'percentage': round((row['visits_adjusted'] / total_requests * 100), 2),
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                    'avg_ttfb': round(row['ttfb_avg'], 2),
                    'sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for dim, row in metrics.iterrows()
                if row['visits'] > 0
            }

        except Exception as e:
            logger.error(f"Error in dimension analysis: {str(e)}")
            return {}

    def _analyze_sampling_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze sampling-specific metrics."""
        try:
            df_copy = df.copy()
            return {
                'sampling_rates': {
                    'min': float(df_copy['sampling_rate'].min() * 100),
                    'max': float(df_copy['sampling_rate'].max() * 100),
                    'mean': float(df_copy['sampling_rate'].mean() * 100),
                    'median': float(df_copy['sampling_rate'].median() * 100)
                },
                'confidence_scores': {
                    'min': float(df_copy['confidence_score'].min()),
                    'max': float(df_copy['confidence_score'].max()),
                    'mean': float(df_copy['confidence_score'].mean()),
                    'median': float(df_copy['confidence_score'].median())
                },
                'total_samples': int(df_copy['visits'].sum()),
                'estimated_total': int(df_copy['visits_adjusted'].sum()),
                'avg_sampling_rate': float(df_copy['sampling_rate'].mean() * 100),
                'avg_confidence_score': float(df_copy['confidence_score'].mean())
            }
        except Exception as e:
            logger.error(f"Error in sampling distribution analysis: {str(e)}")
            return {
                'sampling_rates': {'min': 0, 'max': 0, 'mean': 0, 'median': 0},
                'confidence_scores': {'min': 0, 'max': 0, 'mean': 0, 'median': 0},
                'total_samples': 0,
                'estimated_total': 0,
                'avg_sampling_rate': 0,
                'avg_confidence_score': 0
            }

    def _analyze_path_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance metrics by path."""
        try:
            df_copy = df.copy()
            # Calculate path-level performance metrics
            path_perf = df_copy.groupby('path_group').agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'ttfb_p95': 'mean',
                'ttfb_p99': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            }).sort_values('visits_adjusted', ascending=False)

            # Calculate weighted averages for each path separately
            for path, group in df_copy.groupby('path_group'):
                if path in path_perf.index:
                    # Calculate weighted TTFB
                    path_perf.loc[path, 'ttfb_avg'] = self._safe_weighted_average(
                        group['ttfb_avg'],
                        group['visits']
                    )
                    # Calculate weighted origin time
                    path_perf.loc[path, 'origin_time_avg'] = self._safe_weighted_average(
                        group['origin_time_avg'],
                        group['visits']
                    )

            return {
                str(path): {
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'ttfb_avg': round(row['ttfb_avg'], 2),
                    'ttfb_p95': round(row['ttfb_p95'], 2),
                    'ttfb_p99': round(row['ttfb_p99'], 2),
                    'origin_time_avg': round(row['origin_time_avg'], 2),
                    'error_rate': round((row['error_rate_4xx'] + row['error_rate_5xx']) * 100, 2),
                    'sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for path, row in path_perf.iterrows()
            }

        except Exception as e:
            logger.error(f"Error in path performance analysis: {str(e)}")
            return {}

    def _analyze_paths(self, df: pd.DataFrame) -> Dict:
        """Analyze metrics by path."""
        try:
            df_copy = df.copy()
            path_metrics = df_copy.groupby('path_group').agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'sampling_rate': 'mean',
                'confidence_score': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).sort_values('visits_adjusted', ascending=False)

            total_requests = path_metrics['visits_adjusted'].sum()

            return {
                str(path): {
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                    'percentage_traffic': round((row['visits_adjusted'] / total_requests * 100), 2),
                    'avg_ttfb': round(row['ttfb_avg'], 2),
                    'avg_origin_time': round(row['origin_time_avg'], 2),
                    'error_rate': round((row['error_rate_4xx'] + row['error_rate_5xx']) * 100, 2),
                    'sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for path, row in path_metrics.iterrows()
            }

        except Exception as e:
            logger.error(f"Error in path analysis: {str(e)}")
            return {}

    def _analyze_performance_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze performance metrics by dimension."""
        try:
            df_copy = df.copy()
            metrics = df_copy.groupby(dimension).agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'ttfb_p95': 'mean',
                'ttfb_p99': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            }).sort_values('visits_adjusted', ascending=False)

            # Calculate weighted averages for performance metrics
            for dim, group in df_copy.groupby(dimension):
                if dim in metrics.index:
                    metrics.loc[dim, 'ttfb_avg'] = self._safe_weighted_average(
                        group['ttfb_avg'],
                        group['visits']
                    )
                    metrics.loc[dim, 'origin_time_avg'] = self._safe_weighted_average(
                        group['origin_time_avg'],
                        group['visits']
                    )

            return {
                str(dim): {
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'ttfb_avg': round(row['ttfb_avg'], 2),
                    'ttfb_p95': round(row['ttfb_p95'], 2),
                    'ttfb_p99': round(row['ttfb_p99'], 2),
                    'origin_time_avg': round(row['origin_time_avg'], 2),
                    'error_rate': round((row['error_rate_4xx'] + row['error_rate_5xx']) * 100, 2),
                    'sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for dim, row in metrics.iterrows()
                if row['visits'] > 0
            }
        except Exception as e:
            logger.error(f"Error in performance dimension analysis: {str(e)}")
            return {}

    def _save_analysis(self, analysis: Dict, filename: str) -> None:
        """Save analysis results to JSON file with proper type conversion."""
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
