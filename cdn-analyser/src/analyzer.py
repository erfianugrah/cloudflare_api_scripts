import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
import json
from .utils import NumpyJSONEncoder

logger = logging.getLogger(__name__)

class Analyzer:
    def __init__(self, config):
        self.config = config
    
    def analyze_cache(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze cache performance metrics."""
        try:
            cache_analysis = {
                'zone_name': zone_name,
                'overall': self._analyze_overall_cache(df),
                'by_content_type': self._analyze_by_dimension(df, 'content_type'),
                'by_path': self._analyze_by_dimension(df, 'path'),
                'by_country': self._analyze_by_dimension(df, 'country'),
                'by_status': self._analyze_by_dimension(df, 'status'),
                'by_time': self._analyze_time_based(df),
                'cache_status_distribution': self._analyze_cache_distribution(df)
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
                'trends': self._analyze_performance_trends(df)
            }
            
            self._save_analysis(perf_analysis, f"{zone_name}_performance_analysis.json")
            return perf_analysis
            
        except Exception as e:
            logger.error(f"Error in performance analysis for {zone_name}: {str(e)}")
            return None
    
    def _analyze_overall_cache(self, df: pd.DataFrame) -> Dict:
        """Analyze overall cache metrics."""
        total_samples = len(df)
        cache_hits = df['cache_status'].isin(['hit', 'stale', 'revalidated']).sum()
        total_bytes = float(df['bytes'].sum())
        
        return {
            'total_samples': int(total_samples),
            'total_bytes': float(total_bytes),
            'cache_hits': int(cache_hits),
            'hit_ratio': float((cache_hits / total_samples * 100) if total_samples > 0 else 0),
            'bandwidth_saved_gb': float(total_bytes / (1024 ** 3))
        }
    
    def _analyze_overall_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze overall performance metrics."""
        return {
            'total_samples': int(len(df)),
            'avg_ttfb': float(df['ttfb_avg'].mean()),
            'avg_origin_time': float(df['origin_time_avg'].mean()),
            'avg_dns_time': float(df['dns_time_avg'].mean()),
            'error_rate': float((df['error_4xx_ratio'].mean() + df['error_5xx_ratio'].mean()) * 100)
        }
    
    def _analyze_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze metrics by dimension."""
        grouped = df.groupby(dimension).agg({
            'bytes': 'sum',
            'cache_hit_ratio': 'mean',
            'visits': 'sum'
        }).round(2)
        
        return {
            str(idx): {
                'bytes': float(row['bytes']),
                'hit_ratio': float(row['cache_hit_ratio']),
                'visits': int(row['visits'])
            } for idx, row in grouped.iterrows()
        }
    
    def _analyze_performance_by_dimension(self, df: pd.DataFrame, dimension: str) -> Dict:
        """Analyze performance metrics by dimension."""
        grouped = df.groupby(dimension).agg({
            'ttfb_avg': 'mean',
            'origin_time_avg': 'mean',
            'error_4xx_ratio': 'mean',
            'error_5xx_ratio': 'mean',
            'visits': 'sum'
        }).round(2)
        
        return {
            str(idx): {
                'ttfb': float(row['ttfb_avg']),
                'origin_time': float(row['origin_time_avg']),
                'error_rate': float((row['error_4xx_ratio'] + row['error_5xx_ratio']) * 100),
                'visits': int(row['visits'])
            } for idx, row in grouped.iterrows()
        }
    
    def _analyze_time_based(self, df: pd.DataFrame) -> Dict:
        """Analyze metrics over time."""
        df['hour'] = pd.to_datetime(df['datetime']).dt.hour
        hourly = df.groupby('hour').agg({
            'cache_hit_ratio': 'mean',
            'bytes': 'sum',
            'visits': 'sum'
        }).round(2)
        
        return {
            'hourly': {
                str(hour): {
                    'hit_ratio': float(row['cache_hit_ratio']),
                    'bytes': float(row['bytes']),
                    'visits': int(row['visits'])
                } for hour, row in hourly.iterrows()
            }
        }
    
    def _analyze_cache_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze cache status distribution."""
        status_counts = df['cache_status'].value_counts()
        total = status_counts.sum()
        
        return {
            str(status): {
                'count': int(count),
                'percentage': float((count / total * 100).round(2))
            } for status, count in status_counts.items()
        }
    
    def _analyze_percentiles(self, df: pd.DataFrame) -> Dict:
        """Analyze performance percentiles."""
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
    
    def _analyze_performance_trends(self, df: pd.DataFrame) -> Dict:
        """Analyze performance trends over time."""
        df['hour'] = pd.to_datetime(df['datetime']).dt.hour
        hourly = df.groupby('hour').agg({
            'ttfb_avg': 'mean',
            'origin_time_avg': 'mean',
            'error_4xx_ratio': 'mean',
            'error_5xx_ratio': 'mean',
            'visits': 'sum'
        }).round(2)
        
        return {
            'hourly': {
                str(hour): {
                    'ttfb': float(row['ttfb_avg']),
                    'origin_time': float(row['origin_time_avg']),
                    'error_rate': float((row['error_4xx_ratio'] + row['error_5xx_ratio']) * 100),
                    'visits': int(row['visits'])
                } for hour, row in hourly.iterrows()
            }
        }
    
    def _save_analysis(self, analysis: Dict, filename: str) -> None:
        """Save analysis results to JSON file."""
        try:
            with open(self.config.json_dir / filename, 'w') as f:
                json.dump(analysis, f, indent=2, cls=NumpyJSONEncoder)
        except Exception as e:
            logger.error(f"Error saving analysis: {str(e)}")
