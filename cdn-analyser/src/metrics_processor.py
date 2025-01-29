from typing import Dict, List, Optional, Union, Any
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from pathlib import Path
import json
from .types import (
    NetworkPathMetrics, RequestDetails, PerformanceMetrics,
    CacheMetrics, ErrorMetrics, ProcessedMetrics, SamplingMetrics,
    MetricGroup, MetricDimensions
)

logger = logging.getLogger(__name__)

class MetricsProcessor:
    def __init__(self):
        self.cache_statuses = {
            'HIT': ['hit'],
            'MISS': ['miss'],
            'EXPIRED': ['expired'],
            'STALE': ['stale'],
            'BYPASS': ['bypass'],
            'REVALIDATED': ['revalidated'],
            'UPDATING': ['updating'],
            'DYNAMIC': ['dynamic'],
            'NONE': ['none', 'unknown']
        }
        
        self.column_mappings = {
            # Dimensions mappings
            'datetimeMinute': 'timestamp',
            'clientAsn': 'client_asn',
            'clientASNDescription': 'client_asn_desc',
            'originASN': 'origin_asn',
            'originASNDescription': 'origin_asn_desc',
            'upperTierColoName': 'upper_tier_colo',
            'coloCode': 'colo_code',
            'clientCountryName': 'country',
            'clientDeviceType': 'device_type',
            'clientRequestHTTPProtocol': 'protocol',
            'clientRequestHTTPMethodName': 'method',
            'clientRequestHTTPHost': 'host',
            'clientRequestPath': 'path',
            'clientIP': 'client_ip',
            'clientRefererHost': 'referer',
            'edgeResponseContentTypeName': 'content_type',
            'edgeResponseStatus': 'status_code',
            'cacheStatus': 'cache_status',
            
            # Average metrics mappings
            'edgeTimeToFirstByteMs': 'ttfb_avg',
            'originResponseDurationMs': 'origin_time_avg',
            'sampleInterval': 'sample_interval',
            
            # Sum metrics mappings
            'visits': 'visits',
            'edgeResponseBytes': 'bytes',
            'edgeTimeToFirstByteMs': 'ttfb_sum',
            'originResponseDurationMs': 'origin_sum',
            
            # Quantile metrics mappings
            'edgeResponseBytesP50': 'bytes_p50',
            'edgeResponseBytesP95': 'bytes_p95',
            'edgeResponseBytesP99': 'bytes_p99',
            'edgeTimeToFirstByteMsP50': 'ttfb_p50',
            'edgeTimeToFirstByteMsP95': 'ttfb_p95',
            'edgeTimeToFirstByteMsP99': 'ttfb_p99',
            'originResponseDurationMsP50': 'origin_p50',
            'originResponseDurationMsP95': 'origin_p95',
            'originResponseDurationMsP99': 'origin_p99',
            
            # Ratio metrics mappings
            'status4xx': 'error_rate_4xx',
            'status5xx': 'error_rate_5xx'
        }

    def process_metrics(self, raw_data: Dict) -> Optional[pd.DataFrame]:
        """Process raw metrics with enhanced error handling and metric calculation"""
        try:
            if not self._validate_raw_data(raw_data):
                return None

            # Parse nested JSON structure
            df = self._parse_nested_json(raw_data)
            if df.empty:
                return None
                
            # Map and validate columns
            df = self._validate_and_map_columns(df)
            
            # Process timestamps
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Add derived metrics
            df = self._add_derived_metrics(df)
            
            # Add confidence scores
            df = self._add_confidence_metrics(df)
            
            # Add endpoint metrics
            df = self._add_endpoint_metrics(df)

            self._log_processing_summary(df)

            return df

        except Exception as e:
            logger.error(f"Error processing metrics: {str(e)}")
            return None

    def _validate_raw_data(self, raw_data: Dict) -> bool:
        """Validate raw data structure"""
        if not raw_data or 'data' not in raw_data:
            logger.error("Invalid raw data format")
            return False
            
        viewer_data = raw_data.get('data', {}).get('viewer', {})
        zones_data = viewer_data.get('zones', [])
        
        if not zones_data or not zones_data[0].get('httpRequestsAdaptiveGroups'):
            logger.error("No metrics data found in response")
            return False
            
        return True

    def _parse_nested_json(self, raw_data: Dict) -> pd.DataFrame:
        """Parse nested JSON structure into DataFrame"""
        try:
            request_groups = self._extract_request_groups(raw_data)
            metrics = []
            
            for group in request_groups:
                metric = self._process_metric_group(group)
                if metric is not None:
                    metrics.append(metric)
            
            if not metrics:
                logger.warning("No valid metrics processed")
                return pd.DataFrame()
                
            return pd.DataFrame(metrics)
            
        except Exception as e:
            logger.error(f"Error parsing nested JSON: {str(e)}")
            return pd.DataFrame()

    def _process_metric_group(self, group: Dict) -> Optional[Dict]:
        """Process individual metric group"""
        try:
            metric_group = MetricGroup(group)
            
            # Process dimensions
            dimensions = metric_group.dimensions
            
            # Calculate request metrics
            requests = metric_group.count
            visits = metric_group.sum.get('visits', 0)
            
            # Calculate sampling rate
            sample_interval = metric_group.avg.get('sampleInterval', 1)
            sampling_rate = 1 / sample_interval if sample_interval > 0 else 1
            
            # Get performance metrics
            ttfb_avg = metric_group.avg.get('edgeTimeToFirstByteMs', 0)
            origin_time = metric_group.avg.get('originResponseDurationMs', 0)
            
            # Get sum metrics
            ttfb_sum = metric_group.sum.get('edgeTimeToFirstByteMs', 0)
            origin_sum = metric_group.sum.get('originResponseDurationMs', 0)
            
            # Get quantile metrics
            quantiles = metric_group.quantiles
            bytes_p50 = float(quantiles.get('edgeResponseBytesP50', 0))
            bytes_p95 = float(quantiles.get('edgeResponseBytesP95', 0))
            bytes_p99 = float(quantiles.get('edgeResponseBytesP99', 0))
            
            return {
                'timestamp': dimensions.datetime,
                'client_asn': dimensions.client_asn,
                'client_asn_desc': dimensions.client_asn_desc,
                'origin_asn': dimensions.origin_asn,
                'origin_asn_desc': dimensions.origin_asn_desc,
                'upper_tier_colo': dimensions.upper_tier,
                'colo_code': dimensions.colo,
                'country': dimensions.country,
                'device_type': dimensions.device_type,
                'protocol': dimensions.protocol,
                'method': dimensions.method,
                'host': dimensions.host,
                'path': dimensions.path,
                'client_ip': dimensions.client_ip,
                'referer': dimensions.referer,
                'content_type': dimensions.content_type,
                'status_code': dimensions.status,
                'cache_status': dimensions.cache_status,
                
                # Performance metrics
                'ttfb_avg': ttfb_avg,
                'origin_time_avg': origin_time,
                'ttfb_sum': ttfb_sum,
                'origin_sum': origin_sum,
                'bytes_p50': bytes_p50,
                'bytes_p95': bytes_p95,
                'bytes_p99': bytes_p99,
                'ttfb_p50': float(quantiles.get('edgeTimeToFirstByteMsP50', ttfb_avg)),
                'ttfb_p95': float(quantiles.get('edgeTimeToFirstByteMsP95', ttfb_avg)),
                'ttfb_p99': float(quantiles.get('edgeTimeToFirstByteMsP99', ttfb_avg)),
                'origin_p50': float(quantiles.get('originResponseDurationMsP50', origin_time)),
                'origin_p95': float(quantiles.get('originResponseDurationMsP95', origin_time)),
                'origin_p99': float(quantiles.get('originResponseDurationMsP99', origin_time)),
                
                # Request metrics
                'requests': requests,
                'visits': visits,
                'bytes': metric_group.sum.get('edgeResponseBytes', 0),
                'requests_adjusted': requests / sampling_rate,
                'visits_adjusted': visits / sampling_rate,
                'bytes_adjusted': metric_group.sum.get('edgeResponseBytes', 0) / sampling_rate,
                
                # Error rates
                'error_rate_4xx': metric_group.ratio.get('status4xx', 0),
                'error_rate_5xx': metric_group.ratio.get('status5xx', 0),
                
                # Sampling metadata
                'sampling_rate': sampling_rate,
                'sample_interval': sample_interval,
                
                # Cache metrics
                'cache_hit': dimensions.cache_status.lower() in self.cache_statuses['HIT'],
                'is_dynamic': dimensions.cache_status.lower() in self.cache_statuses['DYNAMIC']
            }

        except Exception as e:
            logger.error(f"Error processing metric group: {str(e)}")
            return None

    def _validate_and_map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and map columns using column mappings"""
        try:
            # Rename columns according to mapping
            df = df.rename(columns=self.column_mappings)
            
            # Verify required columns exist
            required_cols = [
                'timestamp', 'client_asn', 'origin_asn', 'country',
                'ttfb_avg', 'origin_time_avg', 'requests', 'bytes'
            ]
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                return pd.DataFrame()
            
            return df
            
        except Exception as e:
            logger.error(f"Error validating columns: {str(e)}")
            return pd.DataFrame()

    def _add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived metrics to DataFrame"""
        try:
            # Calculate cache metrics
            df['cache_category'] = df['cache_status'].apply(
                lambda x: next(
                    (cat for cat, statuses in self.cache_statuses.items() 
                     if str(x).lower() in statuses),
                    'UNKNOWN'
                )
            )

            # Calculate error rates
            df['is_error'] = df['status_code'] >= 400
            df['is_client_error'] = (df['status_code'] >= 400) & (df['status_code'] < 500)
            df['is_server_error'] = df['status_code'] >= 500

            # Calculate network metrics
            df['is_tiered'] = df['upper_tier_colo'].notna()
            df['path_latency'] = df['ttfb_avg'] - df['origin_time_avg']
            
            # Calculate content type metrics
            df['is_static'] = df['content_type'].str.lower().isin([
                'text/css', 'text/javascript', 'application/javascript',
                'image/jpeg', 'image/png', 'image/gif', 'image/svg+xml',
                'text/html', 'application/x-font-ttf', 'application/x-font-woff'
            ])
            
            # Group metrics by endpoint
            endpoint_metrics = df.groupby('path', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'cache_hit': 'mean',
                'is_error': 'mean'
            })
            
            # Add endpoint metrics back to main DataFrame
            df = df.merge(
                endpoint_metrics.add_prefix('endpoint_'),
                left_on='path',
                right_index=True,
                how='left'
            )

            return df

        except Exception as e:
            logger.error(f"Error adding derived metrics: {str(e)}")
            return df

    def _add_confidence_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add confidence scores and reliability metrics"""
        try:
            # Calculate base confidence from sampling rate
            df['confidence_score'] = df.apply(
                lambda row: self._calculate_confidence_score(
                    row['sampling_rate'],
                    row['requests']
                ),
                axis=1
            )

            # Add reliability indicators
            df['high_confidence'] = df['confidence_score'] >= 0.95
            df['medium_confidence'] = df['confidence_score'].between(0.85, 0.95)
            df['low_confidence'] = df['confidence_score'] < 0.85

            return df

        except Exception as e:
            logger.error(f"Error adding confidence metrics: {str(e)}")
            return df

    def _calculate_confidence_score(self, sampling_rate: float, requests: int) -> float:
        """Calculate confidence score based on sampling rate and sample size"""
        try:
            # Base confidence from sampling rate
            if sampling_rate >= 0.5:
                base_confidence = 0.99
            elif sampling_rate >= 0.1:
                base_confidence = 0.95 if requests >= 1000 else 0.90
            elif sampling_rate >= 0.01:
                base_confidence = 0.90 if requests >= 10000 else 0.85
            else:
                base_confidence = 0.80 if requests >= 100000 else 0.75
            
            # Adjust for sample size
            size_factor = min(1.0, np.log10(requests + 1) / 6)
            return base_confidence * size_factor

        except Exception as e:
            logger.warning(f"Error calculating confidence score: {str(e)}")
            return 0.0

    def _log_processing_summary(self, df: pd.DataFrame) -> None:
        """Log comprehensive processing summary"""
        try:
            logger.info(f"""
Metrics Summary:
-------------
Total Records: {len(df)}
Total Raw Requests: {df['requests'].sum():,}
Total Adjusted Requests: {df['requests_adjusted'].sum():,}
Total Raw Visits: {df['visits'].sum():,}
Total Adjusted Visits: {df['visits_adjusted'].sum():,}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
Error Rates: 4xx={df['is_client_error'].mean()*100:.2f}%, 5xx={df['is_server_error'].mean()*100:.2f}%

Cache Performance:
- Hit Ratio: {df['cache_hit'].mean()*100:.2f}%
- Dynamic Requests: {df['is_dynamic'].mean()*100:.2f}%
- Static Content: {df['is_static'].mean()*100:.2f}%

Performance Metrics:
- Average TTFB: {df['ttfb_avg'].mean():.2f}ms
- P95 TTFB: {df['ttfb_p95'].mean():.2f}ms
- Average Origin Time: {df['origin_time_avg'].mean():.2f}ms
- Average Path Latency: {df['path_latency'].mean():.2f}ms
""")

        except Exception as e:
            logger.error(f"Error logging processing summary: {str(e)}")

    def __str__(self) -> str:
        """String representation of the processor"""
        return f"MetricsProcessor(cache_statuses={len(self.cache_statuses)})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"MetricsProcessor(mappings={len(self.column_mappings)})"
