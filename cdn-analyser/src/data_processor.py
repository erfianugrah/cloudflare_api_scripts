import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Any, Set
import logging
from datetime import datetime, timezone
import hashlib
import traceback
import json
import re
from .types import MetricGroup, MetricDimensions
from prettytable import PrettyTable
from .formatters import TableFormatter

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.cache_statuses = {
            'HIT': ['hit'],  # Cached resource served
            'MISS': ['miss'],  # Resource not in cache, served from origin
            'EXPIRED': ['expired'],  # Cached but expired, served from origin
            'STALE': ['stale'],  # Cached but expired, served from cache
            'BYPASS': ['bypass'],  # Explicitly instructed to bypass cache
            'REVALIDATED': ['revalidated'],  # Cached but revalidated with origin
            'UPDATING': ['updating'],  # Cached, but origin is updating resource
            'DYNAMIC': ['dynamic'],  # Not cached, served from origin
            'NONE': ['none', 'unknown'],  # Asset not eligible for caching
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
    
        self.status_categories = {
            'success': range(200, 300),
            'redirect': range(300, 400),
            'client_error': range(400, 500),
            'server_error': range(500, 600)
        }

        # Content type categories for classification
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

    def safe_agg(self, series: pd.Series, operation: str = 'mean') -> float:
        """Safely aggregate pandas series with proper null handling"""
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
                    
                return float(val) if pd.notnull(val) else 0.0
                
            return 0.0
            
        except Exception as e:
            logger.error(f"Error in safe aggregation: {str(e)}")
            return 0.0

    def safe_division(self, numerator: float, denominator: float) -> float:
        """Safely perform division with zero handling"""
        try:
            if pd.isnull(numerator) or pd.isnull(denominator):
                return 0.0
            if denominator == 0:
                return 0.0
            return float(numerator) / float(denominator)
        except Exception as e:
            logger.error(f"Error in safe division: {str(e)}")
            return 0.0

    def clean_series_value(self, value: Any) -> str:
        """Clean pandas series metadata from string values"""
        try:
            if pd.isnull(value):
                return ''
                
            if isinstance(value, pd.Series):
                if value.empty:
                    return ''
                value = value.iloc[0]
                
            # Convert to string and clean
            value_str = str(value)
            value_str = re.sub(r'Name:\s+\d+,\s+dtype:\s+\w+', '', value_str)
            value_str = re.sub(r'\s+Name:\s+\d+\s+dtype:\s+object', '', value_str)
            
            return value_str.strip()
            
        except Exception as e:
            logger.error(f"Error cleaning series value: {str(e)}")
            return str(value)

    def validate_numeric(self, value: Any, default: float = 0.0) -> float:
        """Validate and convert numeric values with proper handling"""
        try:
            if pd.isnull(value):
                return default
                
            if isinstance(value, pd.Series):
                if value.empty:
                    return default
                value = value.iloc[0]
                
            val = float(value)
            return val if val >= 0 else default
            
        except Exception as e:
            logger.error(f"Error validating numeric value: {str(e)}")
            return default

    def process_zone_metrics(self, raw_data: Dict) -> Optional[pd.DataFrame]:
        """Process raw zone metrics with enhanced error handling"""
        try:
            if raw_data is None:
                logger.error("Raw data is None")
                return None
                
            viewer_data = raw_data.get('data', {}).get('viewer', {})
            zones_data = viewer_data.get('zones', [])
            
            if not zones_data:
                logger.error("No zones data found in response")
                return None
                
            http_requests = zones_data[0].get('httpRequestsAdaptiveGroups', [])
            
            if not http_requests:
                logger.error("No HTTP requests data found in response")
                return None

            # Process each metric group
            metrics = []
            for group in http_requests:
                if group is not None:
                    metric_group = MetricGroup(group)
                    metric = self._process_metric_group(metric_group)
                    if metric is not None:
                        metrics.append(metric)

            if not metrics:
                logger.warning("No valid metrics processed")
                return None

            # Create DataFrame
            df = pd.DataFrame(metrics)
            
            # Convert timestamp column
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Calculate adjusted metrics
            self._calculate_adjusted_metrics(df)
            
            # Add derived metrics
            df = self._add_derived_metrics(df)

            # Calculate endpoint metrics
            df = self._add_endpoint_metrics(df)

            # Calculate confidence scores
            df = self._add_confidence_metrics(df)

            # Log processing summary
            self._log_processing_summary(df)

            return df

        except Exception as e:
            logger.error(f"Error processing zone metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _process_metric_group(self, metric_group: MetricGroup) -> Optional[Dict]:
        """Process individual metric group with proper status code handling"""
        try:
            dimensions = metric_group.dimensions
            if not dimensions or not dimensions.datetime:
                logger.warning("Missing or invalid dimensions in metric group")
                return None

            # Validate and parse datetime
            try:
                metric_datetime = datetime.strptime(
                    dimensions.datetime, 
                    "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
            except ValueError as e:
                logger.warning(f"Invalid datetime format in metric: {dimensions.datetime} - {str(e)}")
                return None

            # Calculate request and sampling metrics
            requests = metric_group.count
            visits = metric_group.sum.get('visits', 0)
            sample_interval = metric_group.avg.get('sampleInterval', 1)
            sampling_rate = 1 / sample_interval if sample_interval > 0 else 1

            # Process cache status
            cache_status = dimensions.cache_status.lower() if dimensions.cache_status else 'unknown'
            cache_category = next(
                (cat for cat, statuses in self.cache_statuses.items() 
                 if cache_status in statuses),
                'NONE'
            )

            # Calculate cache effectiveness
            is_cache_hit = cache_category == 'HIT'
            is_cache_miss = cache_category == 'MISS'
            is_dynamic = cache_category == 'DYNAMIC'

            # Get performance metrics
            ttfb_avg = metric_group.avg.get('edgeTimeToFirstByteMs', 0)
            origin_time = metric_group.avg.get('originResponseDurationMs', 0)
            bytes_sent = metric_group.sum.get('edgeResponseBytes', 0)

            # Get status code and calculate error rates
            status_code = dimensions.status
            error_rate_4xx = float(metric_group.ratio.get('status4xx', 0))
            error_rate_5xx = float(metric_group.ratio.get('status5xx', 0))
            is_error = status_code >= 400 if status_code else False
            is_client_error = 400 <= status_code < 500 if status_code else False
            is_server_error = status_code >= 500 if status_code else False

            # Calculate performance percentiles
            quantiles = metric_group.quantiles
            performance_metrics = {
                'bytes_p50': float(quantiles.get('edgeResponseBytesP50', 0)),
                'bytes_p95': float(quantiles.get('edgeResponseBytesP95', 0)),
                'bytes_p99': float(quantiles.get('edgeResponseBytesP99', 0)),
                'ttfb_p50': float(quantiles.get('edgeTimeToFirstByteMsP50', ttfb_avg)),
                'ttfb_p95': float(quantiles.get('edgeTimeToFirstByteMsP95', ttfb_avg)),
                'ttfb_p99': float(quantiles.get('edgeTimeToFirstByteMsP99', ttfb_avg)),
                'origin_p50': float(quantiles.get('originResponseDurationMsP50', origin_time)),
                'origin_p95': float(quantiles.get('originResponseDurationMsP95', origin_time)),
                'origin_p99': float(quantiles.get('originResponseDurationMsP99', origin_time))
            }

            # Get sum metrics
            sum_metrics = {
                'ttfb_sum': float(metric_group.sum.get('edgeTimeToFirstByteMs', 0)),
                'origin_sum': float(metric_group.sum.get('originResponseDurationMs', 0))
            }

            # Determine content type category
            content_type = dimensions.content_type.lower() if dimensions.content_type else 'unknown'
            is_static = any(static in content_type for static in self.content_types['static'])
            is_dynamic = any(dynamic in content_type for dynamic in self.content_types['dynamic'])

            return {
                # Temporal dimensions
                'timestamp': metric_datetime.isoformat(),
                
                # Request metadata
                'country': dimensions.country,
                'client_asn': dimensions.client_asn,
                'client_asn_desc': dimensions.client_asn_desc,
                'device_type': dimensions.device_type,
                'protocol': dimensions.protocol,
                'content_type': dimensions.content_type,
                'colo_code': dimensions.colo,
                'method': dimensions.method,
                'host': dimensions.host,
                'path': dimensions.path,
                'client_ip': dimensions.client_ip,
                'referer': dimensions.referer,
                'status_code': status_code,
                
                # Cache metrics
                'cache_status': cache_status,
                'cache_category': cache_category,
                'is_cache_hit': is_cache_hit,
                'is_cache_miss': is_cache_miss,
                'is_dynamic': is_dynamic,
                
                # Network path information
                'upper_tier_colo': dimensions.upper_tier,
                'origin_asn': dimensions.origin_asn,
                'origin_asn_desc': dimensions.origin_asn_desc,
                
                # Performance metrics
                'ttfb_avg': ttfb_avg,
                'origin_time_avg': origin_time,
                **performance_metrics,
                **sum_metrics,
                
                # Request and response metrics
                'requests': requests,
                'visits': visits,
                'bytes': bytes_sent,
                'requests_adjusted': requests / sampling_rate if sampling_rate > 0 else requests,
                'visits_adjusted': visits / sampling_rate if sampling_rate > 0 else visits,
                'bytes_adjusted': bytes_sent / sampling_rate if sampling_rate > 0 else bytes_sent,
                
                # Error rates and flags
                'error_rate_4xx': error_rate_4xx,
                'error_rate_5xx': error_rate_5xx,
                'is_error': is_error,
                'is_client_error': is_client_error,
                'is_server_error': is_server_error,
                
                # Content type metrics
                'is_static_content': is_static,
                'is_dynamic_content': is_dynamic,
                
                # Sampling metadata
                'sampling_rate': sampling_rate,
                'sample_interval': sample_interval
            }

        except Exception as e:
            logger.error(f"Error processing metric group: {str(e)}")
            logger.error(f"Group data: {json.dumps(metric_group.to_dict(), indent=2)}")
            return None

    def _calculate_adjusted_metrics(self, df: pd.DataFrame) -> None:
        """Calculate sampling-adjusted metrics"""
        try:
            df['requests_adjusted'] = df.apply(
                lambda row: row['requests'] / row['sampling_rate'] 
                if row['sampling_rate'] > 0 else row['requests'],
                axis=1
            )
            
            df['visits_adjusted'] = df.apply(
                lambda row: row['visits'] / row['sampling_rate'] 
                if row['sampling_rate'] > 0 else row['visits'],
                axis=1
            )
            
            df['bytes_adjusted'] = df.apply(
                lambda row: row['bytes'] / row['sampling_rate']
                if row['sampling_rate'] > 0 else row['bytes'],
                axis=1
            )
        except Exception as e:
            logger.error(f"Error calculating adjusted metrics: {str(e)}")

    def _add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived metrics to the dataframe"""
        try:
            # Calculate error rates
            df['is_error'] = df['status_code'] >= 400
            df['is_client_error'] = df['status_code'].between(400, 499)
            df['is_server_error'] = df['status_code'] >= 500

            # Calculate cache metrics
            df['cache_hit_ratio'] = df.groupby('path')['is_cache_hit'].transform('mean') * 100
            df['dynamic_ratio'] = df.groupby('path')['is_dynamic'].transform('mean') * 100

            # Calculate path metrics
            df['path_latency'] = df['ttfb_avg'] - df['origin_time_avg']
            df['is_tiered'] = df['upper_tier_colo'].notna()

            # Calculate content type metrics
            df['content_size_avg'] = df['bytes'] / df['requests'].clip(lower=1)

            return df

        except Exception as e:
            logger.error(f"Error adding derived metrics: {str(e)}")
            return df

    def _add_endpoint_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add endpoint-specific metrics to the dataframe"""
        try:
            # Normalize paths
            df['normalized_path'] = df['path'].apply(self._normalize_path)
            
            # Calculate endpoint metrics
            endpoint_metrics = df.groupby('normalized_path').agg({
                'requests': 'sum',
                'requests_adjusted': 'sum',
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'bytes': 'sum',
                'bytes_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'is_cache_hit': 'mean',
                'is_error': 'mean',
                'sampling_rate': 'mean'
            }).reset_index()
            
            # Add confidence scores
            endpoint_metrics['endpoint_confidence'] = endpoint_metrics.apply(
                lambda row: self._calculate_confidence_score(
                    row['sampling_rate'],
                    row['requests']
                ),
                axis=1
            )
            
            # Merge back to original data
            df = df.merge(
                endpoint_metrics.add_prefix('endpoint_'),
                left_on='normalized_path',
                right_on='endpoint_normalized_path',
                how='left'
            )

            return df

        except Exception as e:
            logger.error(f"Error adding endpoint metrics: {str(e)}")
            return df

    def _normalize_path(self, path: str) -> str:
        """Normalize path with improved pattern recognition"""
        try:
            if not path or path == '/':
                return '/'
                
            # Remove query parameters but preserve the path
            base_path = path.split('?')[0].rstrip('/')
            
            # Split path into components
            parts = base_path.split('/')
            normalized_parts = []
            
            for part in parts:
                if not part:
                    continue
                    
                # Check for various ID patterns
                if any(pattern.match(part) for pattern in [
                    # UUID pattern
                    re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
                    # Numeric ID
                    re.compile(r'^\d+$'),
                    # Hash-like string
                    re.compile(r'^[0-9a-f]{32}$'),
                    # Base64-like string
                    re.compile(r'^[A-Za-z0-9+/=]{22,}$'),
                    # API version numbers
                    re.compile(r'^v\d+$'),
                    # Date patterns
                    re.compile(r'^\d{4}-\d{2}-\d{2}$')
                ]):
                    normalized_parts.append('{id}')
                else:
                    normalized_parts.append(part)
            
            normalized_path = '/' + '/'.join(normalized_parts)
            
            # Log path normalization for debugging
            if normalized_path != path:
                logger.debug(f"Normalized path: {path} -> {normalized_path}")
                
            return normalized_path
                
        except Exception as e:
            logger.error(f"Error normalizing path {path}: {str(e)}")
            return '/'

    def _add_confidence_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate confidence scores based on sampling rate and request count"""
        try:
            df['confidence_score'] = df.apply(
                lambda row: self._calculate_confidence_score(
                    row['sampling_rate'],
                    row['requests']
                ),
                axis=1
            )

            # Add confidence categories
            df['high_confidence'] = df['confidence_score'] >= 0.95
            df['medium_confidence'] = df['confidence_score'].between(0.85, 0.95)
            df['low_confidence'] = df['confidence_score'] < 0.85

            return df

        except Exception as e:
            logger.error(f"Error calculating confidence scores: {str(e)}")
            return df

    def _calculate_confidence_score(self, sampling_rate: float, request_count: int) -> float:
        """Calculate confidence score based on sampling rate and request volume"""
        try:
            # Base confidence from sampling rate
            if sampling_rate >= 0.5:
                base_confidence = 0.99
            elif sampling_rate >= 0.1:
                base_confidence = 0.95 if request_count >= 1000 else 0.90
            elif sampling_rate >= 0.01:
                base_confidence = 0.90 if request_count >= 10000 else 0.85
            else:
                base_confidence = 0.80 if request_count >= 100000 else 0.75
            
            # Adjust for sample size
            size_factor = min(1.0, np.log10(float(request_count) + 1) / 6)
            return base_confidence * size_factor
            
        except Exception as e:
            logger.warning(f"Error calculating confidence score: {str(e)}")
            return 0.0

    def _log_processing_summary(self, df: pd.DataFrame) -> None:
        """Log comprehensive processing summary"""
        try:
            logger.info(f"""
Processing Summary:
----------------
Total Records: {len(df)}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}

Request Metrics:
- Total Raw Requests: {df['requests'].sum():,}
- Total Adjusted Requests: {df['requests_adjusted'].sum():,}
- Total Raw Visits: {df['visits'].sum():,}
- Total Adjusted Visits: {df['visits_adjusted'].sum():,}
- Total Bandwidth: {df['bytes_adjusted'].sum() / (1024**3):.2f} GB

Cache Performance:
- Hit Ratio: {(df['is_cache_hit'].mean() * 100):.2f}%
- Dynamic Requests: {(df['is_dynamic'].mean() * 100):.2f}%
- Static Content: {(df['is_static_content'].mean() * 100):.2f}%

Error Rates:
- 4xx Errors: {(df['error_rate_4xx'].mean() * 100):.2f}%
- 5xx Errors: {(df['error_rate_5xx'].mean() * 100):.2f}%

Performance:
- Average TTFB: {df['ttfb_avg'].mean():.2f}ms
- P95 TTFB: {df['ttfb_p95'].mean():.2f}ms
- Average Origin Time: {df['origin_time_avg'].mean():.2f}ms
- Average Path Latency: {df['path_latency'].mean():.2f}ms

Sampling:
- Average Rate: {(df['sampling_rate'].mean() * 100):.2f}%
- High Confidence Records: {(df['high_confidence'].mean() * 100):.2f}%
""")

        except Exception as e:
            logger.error(f"Error logging processing summary: {str(e)}")

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

    def __str__(self) -> str:
        """String representation of the processor"""
        return f"DataProcessor(cache_statuses={len(self.cache_statuses)})"

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"DataProcessor(mappings={len(self.column_mappings)}, "
                f"content_types={len(self.content_types)})")
