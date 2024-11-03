# src/analyzer.py
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
import json
from datetime import datetime, timezone
from .utils import NumpyJSONEncoder

logger = logging.getLogger(__name__)

class Analyzer:
    def __init__(self, config):
        self.config = config
    
    def analyze_cache(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze cache performance metrics with sampling considerations."""
        try:
            cache_analysis = {
                'zone_name': zone_name,
                'overall': self._analyze_overall_cache(df),
                'by_content_type': self._analyze_by_dimension(df, 'content_type'),
                'by_path': self._analyze_by_dimension(df, 'path'),
                'by_country': self._analyze_by_dimension(df, 'country'),
                'by_status': self._analyze_by_dimension(df, 'status'),
                'by_time': self._analyze_time_based(df),
                'cache_status_distribution': self._analyze_cache_distribution(df),
                'sampling_metrics': self._analyze_sampling_metrics(df)
            }
            
            # Save analysis
            self._save_analysis(cache_analysis, f"{zone_name}_cache_analysis.json")
            return cache_analysis
            
        except Exception as e:
            logger.error(f"Error in cache analysis for {zone_name}: {str(e)}")
            return None

    def analyze_performance(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze performance metrics."""
        try:
            perf_analysis = {
                'zone_name': zone_name,
                'overall': self._analyze_overall_performance(df),
                'by_path': self._analyze_performance_by_dimension(df, 'path'),
                'by_content_type': self._analyze_performance_by_dimension(df, 'content_type'),
                'by_country': self._analyze_performance_by_dimension(df, 'country'),
                'by_colo': self._analyze_performance_by_dimension(df, 'colo'),
                'percentiles': self._analyze_percentiles(df),
                'trends': self._analyze_performance_trends(df),
                'sampling_metrics': self._analyze_sampling_metrics(df)
            }
            
            self._save_analysis(perf_analysis, f"{zone_name}_performance_analysis.json")
            return perf_analysis
            
        except Exception as e:
            logger.error(f"Error in performance analysis for {zone_name}: {str(e)}")
            return None

    def _analyze_overall_cache(self, df: pd.DataFrame) -> Dict:
        """Analyze overall cache metrics with sampling rate considerations."""
        try:
            # Use estimated total requests for calculations
            total_requests = df['estimated_total_requests'].sum()
            sampled_requests = df['sampled_requests'].sum()
            
            # Calculate weighted hit ratio based on sampling rates
            hit_counts = df[df['cache_status'].isin(['hit', 'stale', 'revalidated'])]['estimated_total_requests'].sum()
            hit_ratio = (hit_counts / total_requests * 100) if total_requests > 0 else 0
            
            # Calculate average sampling rate with safe handling of zero weights
            avg_sampling_rate = (
                (df['sampling_rate'] * df['sampled_requests']).sum() / sampled_requests 
                if sampled_requests > 0 else 0
            )
            
            return {
                'total_requests': int(total_requests),
                'sampled_requests': int(sampled_requests),
                'average_sampling_rate': float(avg_sampling_rate * 100),  # As percentage
                'hit_ratio': float(hit_ratio),
                'total_bytes': float(df['bytes'].sum()),
                'sampling_confidence': self._calculate_sampling_confidence(df)
            }
        except Exception as e:
            logger.error(f"Error in overall cache analysis: {str(e)}")
            return {
                'total_requests': 0,
                'sampled_requests': 0,
                'average_sampling_rate': 0,
                'hit_ratio': 0,
                'total_bytes': 0,
                'sampling_confidence': 0
            }

    def _analyze_overall_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze overall performance metrics with sampling considerations."""
        try:
            return {
                'avg_ttfb': float(np.average(df['ttfb_avg'], weights=df['sampled_requests'])),
                'avg_origin_time': float(np.average(df['origin_time_avg'], weights=df['sampled_requests'])),
                'error_rate': float(
                    np.average(df['error_4xx_ratio'] + df['error_5xx_ratio'], 
                             weights=df['sampled_requests']) * 100
                ),
                'total_requests': int(df['estimated_total_requests'].sum()),
                'sampled_requests': int(df['sampled_requests'].sum())
            }
        except Exception as e:
            logger.error(f"Error in overall performance analysis: {str(e)}")
            return {
                'avg_ttfb': 0,
                'avg_origin_time': 0,
                'error_rate': 0,
                'total_requests': 0,
                'sampled_requests': 0
            }

    def _analyze_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze metrics by dimension with sampling rate considerations."""
        try:
            grouped = df.groupby(dimension).agg({
                'bytes': 'sum',
                'estimated_total_requests': 'sum',
                'sampled_requests': 'sum',
            }).round(2)
            
            # Calculate sampling rate with safe handling of zero weights
            grouped['sampling_rate'] = df.groupby(dimension).apply(
                lambda x: np.average(x['sampling_rate'], 
                                   weights=x['sampled_requests']) if x['sampled_requests'].sum() > 0 else 0
            )
            
            # Calculate hit ratio for each dimension
            for dim in grouped.index:
                dim_data = df[df[dimension] == dim]
                hit_counts = dim_data[dim_data['cache_status'].isin(['hit', 'stale', 'revalidated'])]['estimated_total_requests'].sum()
                total_requests = dim_data['estimated_total_requests'].sum()
                grouped.loc[dim, 'hit_ratio'] = (hit_counts / total_requests * 100) if total_requests > 0 else 0
            
            return {
                str(idx): {
                    'bytes': float(row['bytes']),
                    'hit_ratio': float(row['hit_ratio']),
                    'total_requests': int(row['estimated_total_requests']),
                    'sampled_requests': int(row['sampled_requests']),
                    'sampling_rate': float(row['sampling_rate'] * 100)  # As percentage
                } for idx, row in grouped.iterrows()
            }
        except Exception as e:
            logger.error(f"Error in dimension analysis: {str(e)}")
            return {}

    def _analyze_cache_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze cache status distribution."""
        try:
            status_counts = df.groupby('cache_status').agg({
                'sampled_requests': 'sum',
                'estimated_total_requests': 'sum'
            })
            
            total_estimated = status_counts['estimated_total_requests'].sum()
            if total_estimated == 0:
                return {}
                
            return {
                str(status): {
                    'sampled_requests': int(row['sampled_requests']),
                    'estimated_requests': int(row['estimated_total_requests']),
                    'percentage': float((row['estimated_total_requests'] / total_estimated * 100).round(2))
                } for status, row in status_counts.iterrows()
            }
        except Exception as e:
            logger.error(f"Error in cache distribution analysis: {str(e)}")
            return {}

    def _analyze_performance_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze performance metrics by dimension."""
        try:
            grouped = df.groupby(dimension).agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'error_4xx_ratio': 'mean',
                'error_5xx_ratio': 'mean',
                'sampled_requests': 'sum',
                'estimated_total_requests': 'sum'
            }).round(2)
            
            return {
                str(idx): {
                    'ttfb': float(row['ttfb_avg']),
                    'origin_time': float(row['origin_time_avg']),
                    'error_rate': float((row['error_4xx_ratio'] + row['error_5xx_ratio']) * 100),
                    'sampled_requests': int(row['sampled_requests']),
                    'total_requests': int(row['estimated_total_requests'])
                } for idx, row in grouped.iterrows()
            }
        except Exception as e:
            logger.error(f"Error in performance dimension analysis: {str(e)}")
            return {}

    def _analyze_time_based(self, df: pd.DataFrame) -> Dict:
        """Analyze metrics over time with sampling rate considerations."""
        try:
            df['hour'] = pd.to_datetime(df['datetime']).dt.hour
            hourly = df.groupby('hour').agg({
                'bytes': 'sum',
                'estimated_total_requests': 'sum',
                'sampled_requests': 'sum'
            }).round(2)
            
            # Calculate sampling rate for each hour
            hourly['sampling_rate'] = df.groupby('hour').apply(
                lambda x: np.average(x['sampling_rate'], 
                                   weights=x['sampled_requests']) if x['sampled_requests'].sum() > 0 else 0
            )
            
            # Calculate hourly hit ratios
            for hour in hourly.index:
                hour_data = df[df['hour'] == hour]
                hit_counts = hour_data[hour_data['cache_status'].isin(['hit', 'stale', 'revalidated'])]['estimated_total_requests'].sum()
                total_requests = hour_data['estimated_total_requests'].sum()
                hourly.loc[hour, 'hit_ratio'] = (hit_counts / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'hourly': {
                    str(hour): {
                        'hit_ratio': float(row['hit_ratio']),
                        'bytes': float(row['bytes']),
                        'total_requests': int(row['estimated_total_requests']),
                        'sampled_requests': int(row['sampled_requests']),
                        'sampling_rate': float(row['sampling_rate'] * 100)  # As percentage
                    } for hour, row in hourly.iterrows()
                }
            }
        except Exception as e:
            logger.error(f"Error in time-based analysis: {str(e)}")
            return {'hourly': {}}

    def _analyze_sampling_metrics(self, df: pd.DataFrame) -> Dict:
        """Analyze sampling-specific metrics."""
        try:
            sampling_rates = df['sampling_rate']
            request_counts = df['sampled_requests']
            
            # Safe calculation of weighted average
            if request_counts.sum() > 0:
                mean_rate = np.average(sampling_rates, weights=request_counts)
            else:
                mean_rate = 0
            
            return {
                'sampling_rate_distribution': {
                    'min': float(sampling_rates.min() * 100),
                    'max': float(sampling_rates.max() * 100),
                    'mean': float(mean_rate * 100),
                    'median': float(sampling_rates.median() * 100)
                },
                'sampling_confidence': float(self._calculate_sampling_confidence(df)),
                'total_samples': int(df['sampled_requests'].sum()),
                'estimated_total': int(df['estimated_total_requests'].sum())
            }
        except Exception as e:
            logger.error(f"Error in sampling metrics analysis: {str(e)}")
            return {
                'sampling_rate_distribution': {'min': 0, 'max': 0, 'mean': 0, 'median': 0},
                'sampling_confidence': 0,
                'total_samples': 0,
                'estimated_total': 0
            }

    def _analyze_percentiles(self, df: pd.DataFrame) -> Dict:
        """Analyze performance percentiles."""
        try:
            return {
                'ttfb': {
                    'p50': float(df['ttfb_p50'].mean()),
                    'p95': float(df['ttfb_p95'].mean()),
                    'p99': float(df['ttfb_p99'].mean())
                },
                'origin_time': {
                    'p50': float(df['origin_time_p50'].mean()),
                    'p95': float(df['origin_time_p95'].mean()),
                    'p99': float(df['origin_time_p99'].mean())
                }
            }
        except Exception as e:
            logger.error(f"Error in percentiles analysis: {str(e)}")
            return {
                'ttfb': {'p50': 0, 'p95': 0, 'p99': 0},
                'origin_time': {'p50': 0, 'p95': 0, 'p99': 0}
            }

    def _analyze_performance_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze performance trends over time."""
        try:
            df['hour'] = pd.to_datetime(df['datetime']).dt.hour
            hourly = df.groupby('hour').agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'error_4xx_ratio': 'mean',
                'error_5xx_ratio': 'mean',
                'sampled_requests': 'sum',
                'estimated_total_requests': 'sum'
            }).round(2)
            
            return {
                'hourly': {
                    str(hour): {
                        'ttfb': float(row['ttfb_avg']),
                        'origin_time': float(row['origin_time_avg']),
                        'error_rate': float((row['error_4xx_ratio'] + row['error_5xx_ratio']) * 100),
                        'sampled_requests': int(row['sampled_requests']),
                        'total_requests': int(row['estimated_total_requests'])
                    } for hour, row in hourly.iterrows()
                }
            }
        except Exception as e:
            logger.error(f"Error in performance trends analysis: {str(e)}")
            return {'hourly': {}}

    def _calculate_sampling_confidence(self, df: pd.DataFrame) -> float:
        """Calculate confidence level based on sampling rates and sample sizes."""
        try:
            # Weight confidence by number of requests in each sample
            if df['sampled_requests'].sum() == 0:
                return 0.0
                
            weighted_confidence = (
                df.apply(lambda row: self._get_confidence_for_sample(
                    row['sampling_rate'], 
                    row['sampled_requests']
                ) * row['sampled_requests'],
                axis=1).sum() / df['sampled_requests'].sum()
            )
            return float(            weighted_confidence)
        except Exception as e:
            logger.error(f"Error calculating sampling confidence: {str(e)}")
            return 0.0

    def _get_confidence_for_sample(self, sampling_rate: float, sample_size: int) -> float:
        """Calculate confidence level for a single sample based on sampling rate and size."""
        try:
            if sampling_rate >= 0.5:  # 50% or more sampling
                return 0.99
            elif sampling_rate >= 0.1:  # 10% or more sampling
                return 0.95 if sample_size >= 1000 else 0.90
            elif sampling_rate >= 0.01:  # 1% or more sampling
                return 0.90 if sample_size >= 10000 else 0.85
            else:  # Less than 1% sampling
                return 0.80 if sample_size >= 100000 else 0.75
        except Exception as e:
            logger.error(f"Error calculating sample confidence: {str(e)}")
            return 0.0

    def _save_analysis(self, analysis: Dict, filename: str) -> None:
        """Save analysis results to JSON file."""
        try:
            with open(self.config.json_dir / filename, 'w') as f:
                json.dump(analysis, f, indent=2, cls=NumpyJSONEncoder)
        except Exception as e:
            logger.error(f"Error saving analysis: {str(e)}")
