from typing import Dict, List, Optional, Union, Any
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import json
from .types import (
    NetworkPathMetrics, RequestDetails, PerformanceMetrics,
    CacheMetrics, ErrorMetrics, ProcessedMetrics, SamplingMetrics
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
        
        # Column mappings for standardization
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
            'clientRequestPath': 'endpoint',
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
            
            # Quantile metrics mappings
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
            logger.error(traceback.format_exc())
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

    def _extract_request_groups(self, raw_data: Dict) -> List[Dict]:
        """Extract request groups from raw data"""
        return raw_data['data']['viewer']['zones'][0]['httpRequestsAdaptiveGroups']

    def _process_metric_group(self, group: Dict) -> Optional[Dict]:
        """Process individual metric group"""
        try:
            metric = {}
            
            # Process dimensions directly from the dimensions object
            dimensions = group.get('dimensions', {})
            if dimensions:
                # Map dimension fields directly
                metric['timestamp'] = dimensions.get('datetimeMinute')
                metric['client_asn'] = dimensions.get('clientAsn', 'unknown')
                metric['client_asn_desc'] = dimensions.get('clientASNDescription', 'Unknown ASN')
                metric['origin_asn'] = dimensions.get('originASN', 'unknown')
                metric['origin_asn_desc'] = dimensions.get('originASNDescription', 'Unknown ASN')
                metric['upper_tier_colo'] = dimensions.get('upperTierColoName')
                metric['colo_code'] = dimensions.get('coloCode', 'unknown')
                metric['country'] = dimensions.get('clientCountryName', 'unknown')
                metric['device_type'] = dimensions.get('clientDeviceType', 'unknown')
                metric['protocol'] = dimensions.get('clientRequestHTTPProtocol', 'unknown')
                metric['host'] = dimensions.get('clientRequestHTTPHost', 'unknown')
                metric['endpoint'] = dimensions.get('clientRequestPath', '/')
                metric['content_type'] = dimensions.get('edgeResponseContentTypeName', 'unknown')
                metric['status_code'] = dimensions.get('edgeResponseStatus', 0)
                metric['cache_status'] = dimensions.get('cacheStatus', 'unknown')
                metric['method'] = dimensions.get('clientRequestHTTPMethodName', 'unknown')
            
            # Process averages
            averages = group.get('avg', {})
            if averages:
                metric['ttfb_avg'] = averages.get('edgeTimeToFirstByteMs', 0)
                metric['origin_time_avg'] = averages.get('originResponseDurationMs', 0)
                metric['sample_interval'] = averages.get('sampleInterval', 1)

            # Process sums
            sums = group.get('sum', {})
            if sums:
                metric['bytes'] = sums.get('edgeResponseBytes', 0)
                metric['visits'] = sums.get('visits', 0)
            
            # Process quantiles
            quantiles = group.get('quantiles', {})
            if quantiles:
                metric['ttfb_p50'] = quantiles.get('edgeTimeToFirstByteMsP50', 0)
                metric['ttfb_p95'] = quantiles.get('edgeTimeToFirstByteMsP95', 0)
                metric['ttfb_p99'] = quantiles.get('edgeTimeToFirstByteMsP99', 0)
                metric['origin_p50'] = quantiles.get('originResponseDurationMsP50', 0)
                metric['origin_p95'] = quantiles.get('originResponseDurationMsP95', 0)
                metric['origin_p99'] = quantiles.get('originResponseDurationMsP99', 0)

            # Process ratios
            ratios = group.get('ratio', {})
            if ratios:
                metric['error_rate_4xx'] = ratios.get('status4xx', 0)
                metric['error_rate_5xx'] = ratios.get('status5xx', 0)

            # Add count
            metric['requests'] = group.get('count', 0)

            # Calculate sampling-adjusted metrics
            sample_interval = metric.get('sample_interval', 1)
            if sample_interval > 0:
                metric['sampling_rate'] = 1 / sample_interval
                metric['requests_adjusted'] = metric['requests'] / sample_interval
                metric['visits_adjusted'] = metric.get('visits', 0) / sample_interval
                metric['bytes_adjusted'] = metric.get('bytes', 0) / sample_interval
            else:
                metric['sampling_rate'] = 1
                metric['requests_adjusted'] = metric['requests']
                metric['visits_adjusted'] = metric.get('visits', 0)
                metric['bytes_adjusted'] = metric.get('bytes', 0)

            # Add cache hit indicator
            metric['cache_hit'] = (
                metric.get('cache_status', '').lower() in self.cache_statuses['HIT']
            )

            return metric

        except Exception as e:
            logger.error(f"Error processing metric group: {str(e)}")
            logger.error(traceback.format_exc())
            return None

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
            
            # Group metrics by endpoint
            endpoint_metrics = df.groupby('endpoint', observed=True).agg({
                'requests_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'cache_hit': 'mean',
                'is_error': 'mean'
            })
            
            # Add endpoint metrics back to main DataFrame
            df = df.merge(
                endpoint_metrics.add_prefix('endpoint_'),
                left_on='endpoint',
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
Total Requests (Adjusted): {df['requests_adjusted'].sum():,}
Total Raw Visits: {df['visits'].sum():,}
Total Visits (Adjusted): {df['visits_adjusted'].sum():,}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
Error Rates: 4xx={df['is_client_error'].mean()*100:.2f}%, 5xx={df['is_server_error'].mean()*100:.2f}%
""")

            # Log endpoint metrics
            top_endpoints = (
                df.groupby('endpoint')
                .agg({
                    'requests_adjusted': 'sum',
                    'visits_adjusted': 'sum'
                })
                .sort_values('requests_adjusted', ascending=False)
                .head()
            )
            
            logger.info(f"""
Endpoint Metrics Summary:
---------------------
Total Endpoints: {df['endpoint'].nunique()}
Top Endpoints by Requests:
{top_endpoints.to_string()}
""")

        except Exception as e:
            logger.error(f"Error logging processing summary: {str(e)}")
