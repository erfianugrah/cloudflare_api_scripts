import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
from datetime import datetime, timezone
import hashlib
import traceback
import json

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.cache_statuses = {
            'HIT': ['hit', 'stale', 'revalidated'],
            'MISS': ['miss', 'expired', 'updating'],
            'BYPASS': ['bypass', 'ignored'],
            'ERROR': ['error'],
            'UNKNOWN': ['unknown']
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
        """Process individual metric group with endpoint information."""
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

            # Count both requests and visits
            requests = group.get('count', 0)  # Total request count
            visits = sums.get('visits', 0)    # Unique visitor count

            # Calculate sampling rate
            sample_interval = avg_metrics.get('sampleInterval', 1)
            sampling_rate = 1 / sample_interval if sample_interval > 0 else 1

            # Log raw metrics for debugging
            logger.debug(f"""
    Raw Metric Data:
    -------------
    Timestamp: {datetime_str}
    Requests: {requests}
    Visits: {visits}
    Sample Interval: {sample_interval}
    Sampling Rate: {sampling_rate}
    Status: {dimensions.get('edgeResponseStatus')}
    Cache: {dimensions.get('cacheStatus')}
    Path: {dimensions.get('clientRequestPath')}
    Upper Tier: {dimensions.get('upperTierColoName', 'None')}
    ASN: {dimensions.get('clientAsn', 'None')}
    Protocol: {dimensions.get('clientRequestHTTPProtocol', 'None')}
    Country: {dimensions.get('clientCountryName', 'None')}
    """)

            # Categorize cache status
            cache_status = dimensions.get('cacheStatus', 'unknown').lower()
            cache_category = next(
                (cat for cat, statuses in self.cache_statuses.items() 
                 if cache_status in statuses),
                'UNKNOWN'
            )

            return {
                # Temporal dimensions
                'datetime': metric_datetime.isoformat(),
                
                # Request metadata
                'country': dimensions.get('clientCountryName', 'Unknown'),
                'clientAsn': dimensions.get('clientAsn', 'Unknown'),
                'device_type': dimensions.get('clientDeviceType', 'Unknown'),
                'protocol': dimensions.get('clientRequestHTTPProtocol', 'Unknown'),
                'content_type': dimensions.get('edgeResponseContentTypeName', 'Unknown'),
                'colo': dimensions.get('coloCode', 'Unknown'),
                'clientRequestPath': dimensions.get('clientRequestPath', '/'),
                'clientRequestMethod': dimensions.get('clientRequestHTTPMethodName', 'GET'),
                'clientRequestHTTPHost': dimensions.get('clientRequestHTTPHost', 'unknown'),
                'edgeResponseStatus': dimensions.get('edgeResponseStatus', 0),
                'clientIP': dimensions.get('clientIP', 'Unknown'),
                'clientRefererHost': dimensions.get('clientRefererHost', 'unknown'),
                
                # Cache and tier information
                'cache_status': cache_status,
                'cache_category': cache_category,
                'upperTierColoName': dimensions.get('upperTierColoName'),
                
                # Performance metrics
                'ttfb_avg': avg_metrics.get('edgeTimeToFirstByteMs', 0),
                'origin_time_avg': avg_metrics.get('originResponseDurationMs', 0),
                'dns_time_avg': avg_metrics.get('edgeDnsResponseTimeMs', 0),
                
                # Request and visit counts
                'requests': requests,
                'visits': visits,
                'bytes': sums.get('edgeResponseBytes', 0),
                
                # Percentile metrics
                'ttfb_p50': quantiles.get('edgeTimeToFirstByteMsP50', 0),
                'ttfb_p95': quantiles.get('edgeTimeToFirstByteMsP95', 0),
                'ttfb_p99': quantiles.get('edgeTimeToFirstByteMsP99', 0),
                'origin_p50': quantiles.get('originResponseDurationMsP50', 0),
                'origin_p95': quantiles.get('originResponseDurationMsP95', 0),
                'origin_p99': quantiles.get('originResponseDurationMsP99', 0),
                
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
        """Normalize path for grouping similar paths."""
        try:
            if not path or path == '/':
                return '/'
                
            # Remove query parameters
            path = path.split('?')[0]
            
            # Remove trailing slash
            path = path.rstrip('/')
            
            # Remove numeric IDs
            parts = path.split('/')
            normalized_parts = []
            for part in parts:
                # If part is purely numeric, replace with {id}
                if part.isdigit():
                    normalized_parts.append('{id}')
                # If part contains UUID pattern, replace with {id}
                elif len(part) == 36 and '-' in part:
                    normalized_parts.append('{id}')
                else:
                    normalized_parts.append(part)
                    
            normalized_path = '/'.join(normalized_parts)
            return normalized_path or '/'
            
        except Exception as e:
            logger.warning(f"Error normalizing path {path}: {str(e)}")
            return '/'

    def _create_endpoint_identifier(self, path: str, host: str) -> str:
        """Create a unique endpoint identifier from host and path."""
        try:
            normalized_path = self._normalize_path(path)
            return f"{host}{normalized_path}"
        except Exception as e:
            logger.warning(f"Error creating endpoint identifier: {str(e)}")
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
