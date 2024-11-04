# analyzer.py
import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Union
import logging
from datetime import datetime, timezone
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

    def analyze_cache(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze cache performance metrics with additional safety checks."""
        try:
            if df is None or df.empty:
                logger.error(f"No data available for cache analysis of zone {zone_name}")
                return None

            # Ensure required columns exist
            required_columns = ['visits', 'visits_adjusted', 'bytes_adjusted', 'sampling_rate', 
                              'confidence_score', 'cache_status', 'timestamp']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns for analysis: {missing_columns}")
                return None

            # Calculate overall metrics safely
            total_requests = df['visits_adjusted'].sum()
            total_bytes = df['bytes_adjusted'].sum()
            
            # Calculate hit ratio safely
            cache_hits = df[df['cache_status'].isin(self.cache_categories['HIT'])]
            hit_requests = cache_hits['visits_adjusted'].sum()
            hit_ratio = (hit_requests / total_requests * 100) if total_requests > 0 else 0

            # Calculate averages safely
            avg_sampling_rate = df['sampling_rate'].mean() if not df['sampling_rate'].empty else 0
            avg_confidence = np.average(df['confidence_score'], 
                                      weights=df['visits']) if df['visits'].sum() > 0 else df['confidence_score'].mean()

            cache_analysis = {
                'zone_name': zone_name,
                'overall': {
                    'total_requests': int(total_requests),
                    'total_bytes_gb': round(total_bytes / (1024 ** 3), 2),
                    'hit_ratio': round(hit_ratio, 2),
                    'avg_sampling_rate': round(avg_sampling_rate * 100, 2),
                    'confidence_score': round(avg_confidence, 3),
                    'sampled_requests': int(df['visits'].sum())
                },
                'by_cache_status': self._analyze_cache_distribution(df),
                'by_content_type': self._analyze_by_dimension(df, 'content_type'),
                'by_country': self._analyze_by_dimension(df, 'country'),
                'by_device': self._analyze_by_dimension(df, 'device_type'),
                'temporal': self._analyze_temporal_patterns(df),
                'sampling_metrics': self._analyze_sampling_distribution(df)
            }

            self._save_analysis(cache_analysis, f"{zone_name}_cache_analysis.json")
            return cache_analysis

        except Exception as e:
            logger.error(f"Error in cache analysis for {zone_name}: {str(e)}")
            return None

    def analyze_performance(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze performance metrics with sampling considerations."""
        try:
            if df is None or df.empty:
                logger.error(f"No data available for performance analysis of zone {zone_name}")
                return None

            # Calculate weighted performance metrics
            weighted_ttfb = np.average(df['ttfb_avg'], weights=df['visits'])
            weighted_origin = np.average(df['origin_time_avg'], weights=df['visits'])
            
            # Calculate confidence metrics
            avg_sampling_rate = df['sampling_rate'].mean()
            avg_confidence = np.average(df['confidence_score'], weights=df['visits'])

            perf_analysis = {
                'zone_name': zone_name,
                'overall': {
                    'avg_ttfb_ms': round(weighted_ttfb, 2),
                    'avg_origin_time_ms': round(weighted_origin, 2),
                    'total_requests': int(df['visits_adjusted'].sum()),
                    'sampled_requests': int(df['visits'].sum()),
                    'avg_sampling_rate': round(avg_sampling_rate * 100, 2),
                    'confidence_score': round(avg_confidence, 3)
                },
                'percentiles': self._analyze_performance_percentiles(df),
                'by_content_type': self._analyze_performance_by_dimension(df, 'content_type'),
                'by_country': self._analyze_performance_by_dimension(df, 'country'),
                'by_device': self._analyze_performance_by_dimension(df, 'device_type'),
                'trends': self._analyze_performance_trends(df),
                'sampling_metrics': self._analyze_sampling_distribution(df)
            }

            # Save analysis results
            self._save_analysis(perf_analysis, f"{zone_name}_performance_analysis.json")
            return perf_analysis

        except Exception as e:
            logger.error(f"Error in performance analysis for {zone_name}: {str(e)}")
            return None

    def _analyze_cache_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze cache status distribution with safe calculations."""
        try:
            cache_metrics = df.groupby('cache_status').agg({
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
                    'percentage': round((row['visits_adjusted'] / total_requests * 100), 2) if total_requests > 0 else 0,
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                    'avg_sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for status, row in cache_metrics.iterrows()
                if row['visits'] > 0  # Only include statuses with actual visits
            }
        except Exception as e:
            logger.error(f"Error in cache distribution analysis: {str(e)}")
            return {}

    def _analyze_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze metrics by a specific dimension with safe weight handling."""
        try:
            metrics = df.groupby(dimension).agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'sampling_rate': 'mean',
                'confidence_score': 'mean',
                'ttfb_avg': 'mean'  # Changed from weighted average to simple mean for safety
            })

            total_requests = metrics['visits_adjusted'].sum()
            if total_requests == 0:
                logger.warning(f"No requests found for dimension {dimension}")
                return {}

            # Safe weighted average calculation
            def safe_weighted_avg(group):
                weights = df.loc[group.index, 'visits']
                if weights.sum() > 0:
                    return np.average(group, weights=weights)
                return group.mean()  # Fallback to simple mean if no weights

            # Calculate TTFB with safe weighting
            ttfb_by_dim = df.groupby(dimension)['ttfb_avg'].apply(safe_weighted_avg)

            return {
                str(dim): {
                    'percentage': round((row['visits_adjusted'] / total_requests * 100), 2) if total_requests > 0 else 0,
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                    'avg_ttfb': round(ttfb_by_dim.get(dim, 0), 2),
                    'avg_sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for dim, row in metrics.iterrows()
                if row['visits'] > 0  # Only include dimensions with actual visits
            }
        except Exception as e:
            logger.error(f"Error in dimension analysis: {str(e)}")
            return {}

    def _analyze_temporal_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns with sampling considerations."""
        try:
            df['hour'] = df['timestamp'].dt.hour
            
            hourly_metrics = df.groupby('hour').agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'sampling_rate': 'mean',
                'confidence_score': 'mean',
                'ttfb_avg': lambda x: np.average(x, weights=df.loc[x.index, 'visits'])
            })

            return {
                'hourly': {
                    str(hour): {
                        'requests_sampled': int(row['visits']),
                        'requests_estimated': int(row['visits_adjusted']),
                        'bytes_gb': round(row['bytes_adjusted'] / (1024 ** 3), 2),
                        'avg_ttfb': round(row['ttfb_avg'], 2),
                        'sampling_rate': round(row['sampling_rate'] * 100, 2),
                        'confidence_score': round(row['confidence_score'], 3)
                    }
                    for hour, row in hourly_metrics.iterrows()
                }
            }
        except Exception as e:
            logger.error(f"Error in temporal analysis: {str(e)}")
            return {'hourly': {}}

    def _analyze_performance_percentiles(self, df: pd.DataFrame) -> Dict:
        """Analyze performance percentile metrics with sampling considerations."""
        try:
            # Calculate weighted percentiles
            weighted_metrics = {
                'ttfb': {
                    'p50': np.average(df['ttfb_p50'], weights=df['visits']),
                    'p95': np.average(df['ttfb_p95'], weights=df['visits']),
                    'p99': np.average(df['ttfb_p99'], weights=df['visits'])
                },
                'origin_time': {
                    'p50': np.average(df['origin_p50'], weights=df['visits']),
                    'p95': np.average(df['origin_p95'], weights=df['visits']),
                    'p99': np.average(df['origin_p99'], weights=df['visits'])
                }
            }

            return {
                'ttfb': {
                    'p50': round(weighted_metrics['ttfb']['p50'], 2),
                    'p95': round(weighted_metrics['ttfb']['p95'], 2),
                    'p99': round(weighted_metrics['ttfb']['p99'], 2)
                },
                'origin_time': {
                    'p50': round(weighted_metrics['origin_time']['p50'], 2),
                    'p95': round(weighted_metrics['origin_time']['p95'], 2),
                    'p99': round(weighted_metrics['origin_time']['p99'], 2)
                }
            }
        except Exception as e:
            logger.error(f"Error analyzing performance percentiles: {str(e)}")
            return {
                'ttfb': {'p50': 0, 'p95': 0, 'p99': 0},
                'origin_time': {'p50': 0, 'p95': 0, 'p99': 0}
            }

    def _analyze_performance_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze performance metrics by dimension with safe weight handling."""
        try:
            # First aggregate basic metrics
            perf_metrics = df.groupby(dimension).agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            })

            # Safe weighted average calculation
            def safe_weighted_avg(group):
                weights = df.loc[group.index, 'visits']
                if weights.sum() > 0:
                    return np.average(group, weights=weights)
                return group.mean()

            # Calculate weighted averages safely
            ttfb_weighted = df.groupby(dimension)['ttfb_avg'].apply(safe_weighted_avg)
            origin_weighted = df.groupby(dimension)['origin_time_avg'].apply(safe_weighted_avg)

            return {
                str(dim): {
                    'ttfb_avg': round(ttfb_weighted.get(dim, 0), 2),
                    'origin_time_avg': round(origin_weighted.get(dim, 0), 2),
                    'requests_sampled': int(row['visits']),
                    'requests_estimated': int(row['visits_adjusted']),
                    'sampling_rate': round(row['sampling_rate'] * 100, 2),
                    'confidence_score': round(row['confidence_score'], 3)
                }
                for dim, row in perf_metrics.iterrows()
                if row['visits'] > 0  # Only include dimensions with actual visits
            }
        except Exception as e:
            logger.error(f"Error in performance dimension analysis: {str(e)}")
            return {}

    def _analyze_performance_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze performance trends over time."""
        try:
            df['hour'] = df['timestamp'].dt.hour
            
            hourly_perf = df.groupby('hour').agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': lambda x: np.average(x, weights=df.loc[x.index, 'visits']),
                'origin_time_avg': lambda x: np.average(x, weights=df.loc[x.index, 'visits']),
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            })

            return {
                'hourly': {
                    str(hour): {
                        'ttfb': round(row['ttfb_avg'], 2),
                        'origin_time': round(row['origin_time_avg'], 2),
                        'error_rate': round((row['error_rate_4xx'] + row['error_rate_5xx']) * 100, 2),
                        'requests_sampled': int(row['visits']),
                        'requests_estimated': int(row['visits_adjusted']),
                        'sampling_rate': round(row['sampling_rate'] * 100, 2),
                        'confidence_score': round(row['confidence_score'], 3)
                    }
                    for hour, row in hourly_perf.iterrows()
                }
            }
        except Exception as e:
            logger.error(f"Error in performance trends analysis: {str(e)}")
            return {'hourly': {}}

    def _analyze_sampling_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze sampling-specific metrics."""
        try:
            return {
                'sampling_rates': {
                    'min': float(df['sampling_rate'].min() * 100),
                    'max': float(df['sampling_rate'].max() * 100),
                    'mean': float(df['sampling_rate'].mean() * 100),
                    'median': float(df['sampling_rate'].median() * 100)
                },
                'confidence_scores': {
                    'min': float(df['confidence_score'].min()),
                    'max': float(df['confidence_score'].max()),
                    'mean': float(df['confidence_score'].mean()),
                    'median': float(df['confidence_score'].median())
                },
                'total_samples': int(df['visits'].sum()),
                'estimated_total': int(df['visits_adjusted'].sum()),
                'avg_sampling_rate': float(df['sampling_rate'].mean() * 100),
                'avg_confidence_score': float(df['confidence_score'].mean())
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

    def _save_analysis(self, analysis: Dict, filename: str) -> None:
        """Save analysis results to JSON file."""
        try:
            # Convert numpy types to Python native types
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
                return obj

            converted_analysis = convert_to_native(analysis)
            
            output_path = self.config.json_dir / filename
            with open(output_path, 'w') as f:
                json.dump(converted_analysis, f, indent=2)
                
            logger.info(f"Analysis results saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")

    def get_summary_metrics(self, df: pd.DataFrame) -> Dict:
        """Get summary metrics for quick analysis."""
        try:
            return {
                'total_requests': int(df['visits_adjusted'].sum()),
                'total_bytes_gb': round(df['bytes_adjusted'].sum() / (1024 ** 3), 2),
                'avg_ttfb_ms': round(np.average(df['ttfb_avg'], weights=df['visits']), 2),
                'avg_origin_time_ms': round(np.average(df['origin_time_avg'], weights=df['visits']), 2),
                'error_rate': round((df['error_rate_4xx'].mean() + df['error_rate_5xx'].mean()) * 100, 2),
                'sampling_metrics': self._analyze_sampling_distribution(df)
            }
        except Exception as e:
            logger.error(f"Error calculating summary metrics: {str(e)}")
            return {
                'total_requests': 0,
                'total_bytes_gb': 0,
                'avg_ttfb_ms': 0,
                'avg_origin_time_ms': 0,
                'error_rate': 0,
                'sampling_metrics': {
                    'sampling_rates': {'min': 0, 'max': 0, 'mean': 0, 'median': 0},
                    'confidence_scores': {'min': 0, 'max': 0, 'mean': 0, 'median': 0},
                    'total_samples': 0,
                    'estimated_total': 0,
                    'avg_sampling_rate': 0,
                    'avg_confidence_score': 0
                }
            }

    def analyze_errors(self, df: pd.DataFrame) -> Dict:
        """Analyze error rates and patterns."""
        try:
            error_metrics = {
                'overall': {
                    '4xx_rate': round(df['error_rate_4xx'].mean() * 100, 2),
                    '5xx_rate': round(df['error_rate_5xx'].mean() * 100, 2),
                    'total_error_rate': round((df['error_rate_4xx'].mean() + df['error_rate_5xx'].mean()) * 100, 2)
                }
            }

            # Error rates by content type
            content_errors = df.groupby('content_type').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits': 'sum'
            })

            error_metrics['by_content_type'] = {
                str(content_type): {
                    '4xx_rate': round(row['error_rate_4xx'] * 100, 2),
                    '5xx_rate': round(row['error_rate_5xx'] * 100, 2),
                    'total_requests': int(row['visits'])
                }
                for content_type, row in content_errors.iterrows()
            }

            # Error rates by country
            country_errors = df.groupby('country').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits': 'sum'
            })

            error_metrics['by_country'] = {
                str(country): {
                    '4xx_rate': round(row['error_rate_4xx'] * 100, 2),
                    '5xx_rate': round(row['error_rate_5xx'] * 100, 2),
                    'total_requests': int(row['visits'])
                }
                for country, row in country_errors.iterrows()
            }

            # Temporal error patterns
            df['hour'] = df['timestamp'].dt.hour
            hourly_errors = df.groupby('hour').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits': 'sum'
            })

            error_metrics['temporal'] = {
                str(hour): {
                    '4xx_rate': round(row['error_rate_4xx'] * 100, 2),
                    '5xx_rate': round(row['error_rate_5xx'] * 100, 2),
                    'total_requests': int(row['visits'])
                }
                for hour, row in hourly_errors.iterrows()
            }

            return error_metrics

        except Exception as e:
            logger.error(f"Error analyzing error patterns: {str(e)}")
            return {
                'overall': {'4xx_rate': 0, '5xx_rate': 0, 'total_error_rate': 0},
                'by_content_type': {},
                'by_country': {},
                'temporal': {}
            }
