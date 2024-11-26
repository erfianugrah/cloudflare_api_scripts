import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
from datetime import datetime, timezone
import hashlib
import traceback
import json
import re

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
    
        self.status_categories = {
            'success': range(200, 300),
            'redirect': range(300, 400),
            'client_error': range(400, 500),
            'server_error': range(500, 600)
        }

    def process_zone_metrics(self, raw_data: Dict) -> Optional[pd.DataFrame]:
        """Process raw zone metrics with enhanced error handling."""
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

            # Process metrics and filter out None values
            metrics = []
            for group in http_requests:
                if group is not None:
                    metric = self._process_metric_group(group)
                    if metric is not None:
                        metrics.append(metric)

            if not metrics:
                logger.warning("No valid metrics processed")
                return None

            # Create DataFrame
            df = pd.DataFrame(metrics)
            
            # Convert timestamp column
            df['timestamp'] = pd.to_datetime(df['datetime'])
            
            # Ensure status column exists and is properly formatted
            df['status'] = pd.to_numeric(df['edgeResponseStatus'], errors='coerce').fillna(0).astype(int)
            
            # Calculate adjusted metrics
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

            # Calculate error rates
            df['error_rate_4xx'] = df['status'].between(400, 499).astype(float)
            df['error_rate_5xx'] = df['status'].between(500, 599).astype(float)

            # Log metrics summary
            logger.info(f"""
Metrics Summary:
-------------
Total Records: {len(df)}
Total Requests (Raw): {df['requests'].sum():,}
Total Requests (Adjusted): {df['requests_adjusted'].sum():,}
Total Visits (Raw): {df['visits'].sum():,}
Total Visits (Adjusted): {df['visits_adjusted'].sum():,}
Time Range: {df['timestamp'].min()} to {df['timestamp'].max()}
Error Rates: 4xx={df['error_rate_4xx'].mean()*100:.2f}%, 5xx={df['error_rate_5xx'].mean()*100:.2f}%
""")

            # Create endpoint identifier and add endpoint metrics
            df['endpoint'] = df.apply(
                lambda row: self._create_endpoint_identifier(
                    row['clientRequestPath'],
                    row['clientRequestHTTPHost']
                ),
                axis=1
            )
            
            # Add endpoint metrics
            df = self._add_endpoint_metrics(df)

            # Calculate confidence scores
            df['confidence_score'] = self._calculate_confidence_scores(df)

            # Log final DataFrame info
            logger.debug(f"Processed DataFrame shape: {df.shape}")
            logger.debug(f"Processed DataFrame columns: {df.columns.tolist()}")

            return df

        except Exception as e:
            logger.error(f"Error processing zone metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _process_metric_group(self, group: Dict) -> Optional[Dict]:
        """Process individual metric group with improved cache and performance handling"""
        try:
            dimensions = group.get('dimensions', {})
            if not dimensions:
                logger.warning("Missing dimensions in metric group")
                return None

            # Get and validate datetime
            datetime_str = dimensions.get('datetimeMinute')
            if not datetime_str:
                logger.warning("Missing datetimeMinute in dimensions")
                return None

            try:
                metric_datetime = datetime.strptime(
                    datetime_str, 
                    "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
            except ValueError as e:
                logger.warning(f"Invalid datetime format in metric: {datetime_str} - {str(e)}")
                return None

            avg_metrics = group.get('avg', {})
            quantiles = group.get('quantiles', {})
            sums = group.get('sum', {})
            ratios = group.get('ratio', {})

            # Calculate request and visit metrics
            requests = group.get('count', 0)  # Total request count
            visits = sums.get('visits', 0)    # Unique visitor count

            # Calculate sampling rate and interval
            sample_interval = avg_metrics.get('sampleInterval', 1)
            sampling_rate = 1 / sample_interval if sample_interval > 0 else 1

            # Process cache status with improved handling
            cache_status = dimensions.get('cacheStatus', 'unknown').strip().lower()
            cache_category = next(
                (cat for cat, statuses in self.cache_statuses.items() 
                 if cache_status in statuses),
                'NONE'
            )

            # Calculate cache effectiveness
            is_cache_hit = cache_category == 'HIT'
            is_cache_miss = cache_category == 'MISS'
            is_dynamic = cache_category == 'DYNAMIC'

            # Handle response status
            status_code = dimensions.get('edgeResponseStatus', 0)
            status_category = next(
                (cat for cat, range_obj in self.status_categories.items() 
                 if status_code in range_obj),
                'unknown'
            )

            # Get performance metrics with proper defaults
            ttfb_avg = avg_metrics.get('edgeTimeToFirstByteMs', 0)
            origin_time = avg_metrics.get('originResponseDurationMs', 0)
            bytes_sent = sums.get('edgeResponseBytes', 0)

            # Calculate performance percentiles
            ttfb_percentiles = {
                'p50': quantiles.get('edgeTimeToFirstByteMsP50', ttfb_avg),
                'p95': quantiles.get('edgeTimeToFirstByteMsP95', ttfb_avg),
                'p99': quantiles.get('edgeTimeToFirstByteMsP99', ttfb_avg)
            }

            origin_percentiles = {
                'p50': quantiles.get('originResponseDurationMsP50', origin_time),
                'p95': quantiles.get('originResponseDurationMsP95', origin_time),
                'p99': quantiles.get('originResponseDurationMsP99', origin_time)
            }

            return {
                # Temporal dimensions
                'datetime': metric_datetime.isoformat(),
                'timestamp': metric_datetime,
                
                # Request metadata
                'country': dimensions.get('clientCountryName', 'Unknown'),
                'clientAsn': dimensions.get('clientAsn', 'Unknown'),
                'clientASNDescription': dimensions.get('clientASNDescription', 'Unknown'),
                'device_type': dimensions.get('clientDeviceType', 'Unknown'),
                'protocol': dimensions.get('clientRequestHTTPProtocol', 'Unknown'),
                'content_type': dimensions.get('edgeResponseContentTypeName', 'Unknown'),
                'colo': dimensions.get('coloCode', 'Unknown'),
                'clientRequestPath': dimensions.get('clientRequestPath', '/'),
                'clientRequestMethod': dimensions.get('clientRequestHTTPMethodName', 'GET'),
                'clientRequestHTTPHost': dimensions.get('clientRequestHTTPHost', 'unknown'),
                'edgeResponseStatus': status_code,
                'status_category': status_category,
                'clientIP': dimensions.get('clientIP', 'Unknown'),
                'clientRefererHost': dimensions.get('clientRefererHost', 'unknown'),
                
                # Cache metrics
                'cache_status': cache_status,
                'cache_category': cache_category,
                'is_cache_hit': is_cache_hit,
                'is_cache_miss': is_cache_miss,
                'is_dynamic': is_dynamic,
                
                # Network path information
                'upperTierColoName': dimensions.get('upperTierColoName'),
                'originASN': dimensions.get('originASN', 'Unknown'),
                'originASNDescription': dimensions.get('originASNDescription', 'Unknown'),
                
                # Performance metrics
                'ttfb_avg': ttfb_avg,
                'ttfb_p50': ttfb_percentiles['p50'],
                'ttfb_p95': ttfb_percentiles['p95'],
                'ttfb_p99': ttfb_percentiles['p99'],
                'origin_time_avg': origin_time,
                'origin_p50': origin_percentiles['p50'],
                'origin_p95': origin_percentiles['p95'],
                'origin_p99': origin_percentiles['p99'],
                'dns_time_avg': avg_metrics.get('edgeDnsResponseTimeMs', 0),
                
                # Request and response metrics
                'requests': requests,
                'visits': visits,
                'bytes': bytes_sent,
                'requests_adjusted': requests / sampling_rate,
                'visits_adjusted': visits / sampling_rate,
                'bytes_adjusted': bytes_sent / sampling_rate,
                
                # Error rates
                'error_rate_4xx': ratios.get('status4xx', 0),
                'error_rate_5xx': ratios.get('status5xx', 0),
                
                # Sampling metadata
                'sampling_rate': sampling_rate,
                'sample_interval': sample_interval
            }

        except Exception as e:
            logger.error(f"Error processing metric group: {str(e)}")
            logger.error(f"Group data: {json.dumps(group, indent=2)}")
            return None

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
                    re.compile(r'^[A-Za-z0-9+/=]{22,}$')
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

    def _create_endpoint_identifier(self, path: str, host: str) -> str:
        """Create a unique endpoint identifier without truncation"""
        try:
            if not path or not host:
                return "unknown"
                
            normalized_path = self._normalize_path(path)
            full_endpoint = f"{host}{normalized_path}"
            
            # Create a hash for very long endpoints but preserve readability
            if len(full_endpoint) > 100:
                endpoint_hash = hashlib.md5(full_endpoint.encode()).hexdigest()[:8]
                truncated_endpoint = f"{full_endpoint[:92]}_{endpoint_hash}"
                logger.debug(f"Truncated long endpoint: {full_endpoint} -> {truncated_endpoint}")
                return truncated_endpoint
                
            return full_endpoint
                
        except Exception as e:
            logger.error(f"Error creating endpoint identifier: {str(e)}")
            return "unknown"

    def _add_endpoint_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add endpoint-specific metrics to the dataframe."""
        try:
            # Calculate endpoint-level metrics
            endpoint_metrics = df.groupby('endpoint').agg({
                'requests': 'sum',
                'requests_adjusted': 'sum',
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'cache_status': lambda x: (x.isin(['hit', 'stale', 'revalidated']).mean() * 100),
                'sampling_rate': 'mean'
            }).reset_index()
            
            endpoint_metrics.columns = [
                'endpoint', 'endpoint_requests', 'endpoint_requests_adjusted',
                'endpoint_visits', 'endpoint_visits_adjusted',
                'endpoint_ttfb_avg', 'endpoint_cache_hit_ratio', 
                'endpoint_sampling_rate'
            ]
            
            # Calculate endpoint confidence scores
            endpoint_metrics['endpoint_confidence_score'] = endpoint_metrics.apply(
                lambda row: self._calculate_confidence_score(
                    row['endpoint_sampling_rate'],
                    row['endpoint_requests']
                ),
                axis=1
            )
            
            # Log endpoint metrics
            logger.info(f"""
Endpoint Metrics Summary:
---------------------
Total Endpoints: {len(endpoint_metrics)}
Top Endpoints by Requests:
{endpoint_metrics.nlargest(5, 'endpoint_requests_adjusted')[['endpoint', 'endpoint_requests_adjusted', 'endpoint_visits_adjusted']].to_string()}
""")
            
            # Merge back to original dataframe
            return df.merge(endpoint_metrics, on='endpoint', how='left')
                
        except Exception as e:
            logger.error(f"Error adding endpoint metrics: {str(e)}")
            return df

    def _calculate_confidence_scores(self, df: pd.DataFrame) -> pd.Series:
        """Calculate confidence scores based on sampling rate and request count."""
        def get_confidence(row):
            try:
                rate = row['sampling_rate']
                request_count = row['requests']
                
                if rate >= 0.5:  # 50% or more sampling
                    base_confidence = 0.99
                elif rate >= 0.1:  # 10% or more sampling
                    base_confidence = 0.95 if request_count >= 1000 else 0.90
                elif rate >= 0.01:  # 1% or more sampling
                    base_confidence = 0.90 if request_count >= 10000 else 0.85
                else:
                    base_confidence = 0.80 if request_count >= 100000 else 0.75
                
                # Adjust for sample size
                size_factor = min(1.0, np.log10(request_count + 1) / 6)
                return base_confidence * size_factor
            except Exception as e:
                logger.warning(f"Error calculating confidence score: {str(e)}")
                return 0.0

        return df.apply(get_confidence, axis=1)

    def _calculate_confidence_score(self, sampling_rate: float, request_count: int) -> float:
        """Calculate single confidence score."""
        try:
            if sampling_rate >= 0.5:
                base_confidence = 0.99
            elif sampling_rate >= 0.1:
                base_confidence = 0.95 if request_count >= 1000 else 0.90
            elif sampling_rate >= 0.01:
                base_confidence = 0.90 if request_count >= 10000 else 0.85
            else:
                base_confidence = 0.80 if request_count >= 100000 else 0.75
            
            size_factor = min(1.0, np.log10(request_count + 1) / 6)
            return base_confidence * size_factor
            
        except Exception as e:
            logger.warning(f"Error calculating confidence score: {str(e)}")
            return 0.0
