# data_processor.py
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
from datetime import datetime
import hashlib
import traceback

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
        """Process raw zone metrics with endpoint information."""
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

            metrics = [
                self._process_metric_group(group)
                for group in http_requests
                if group is not None
            ]

            metrics = [m for m in metrics if m is not None]
            if not metrics:
                logger.warning("No valid metrics processed")
                return None

            df = pd.DataFrame(metrics)
            
            # Process timestamps
            df['timestamp'] = pd.to_datetime(df['datetime'])
            
            # Ensure status column exists and is properly formatted
            df['status'] = df['edgeResponseStatus'].astype(int)
            
            # Calculate adjusted metrics
            df['visits_adjusted'] = df.apply(
                lambda row: row['visits'] / row['sampling_rate'] 
                if row['sampling_rate'] > 0 else 0,
                axis=1
            )
            df['bytes_adjusted'] = df.apply(
                lambda row: row['bytes'] / row['sampling_rate']
                if row['sampling_rate'] > 0 else 0,
                axis=1
            )

            # Calculate error rates
            df['error_rate_4xx'] = df['status'].between(400, 499).astype(float)
            df['error_rate_5xx'] = df['status'].between(500, 599).astype(float)

            # Calculate confidence scores
            df['confidence_score'] = self._calculate_confidence_scores(df)
            
            # Create endpoint identifier combining host and path
            df['endpoint'] = df.apply(
                lambda row: self._create_endpoint_identifier(
                    row['clientRequestPath'],
                    row['clientRequestHTTPHost']
                ),
                axis=1
            )
            
            # Add endpoint-specific metrics
            df = self._add_endpoint_metrics(df)

            # Log DataFrame info for debugging
            logger.debug(f"Processed DataFrame columns: {df.columns.tolist()}")
            logger.debug(f"Processed DataFrame shape: {df.shape}")

            return df

        except Exception as e:
            logger.error(f"Error processing zone metrics: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None

    def _process_metric_group(self, group: Dict) -> Optional[Dict]:
        """Process individual metric group with endpoint information."""
        try:
            dimensions = group['dimensions']
            avg_metrics = group['avg']
            quantiles = group.get('quantiles', {})
            sums = group['sum']
            ratios = group.get('ratio', {})

            # Calculate sampling rate from interval
            sample_interval = avg_metrics.get('sampleInterval', 1)
            sampling_rate = 1 / sample_interval if sample_interval > 0 else 1

            # Categorize cache status
            cache_status = dimensions.get('cacheStatus', 'unknown').lower()
            cache_category = next(
                (cat for cat, statuses in self.cache_statuses.items() 
                 if cache_status in statuses),
                'UNKNOWN'
            )

            return {
                # Temporal dimensions
                'datetime': dimensions['datetime'],
                
                # Request metadata
                'country': dimensions.get('clientCountryName', 'Unknown'),
                'device_type': dimensions.get('clientDeviceType', 'Unknown'),
                'protocol': dimensions.get('clientRequestHTTPProtocol', 'Unknown'),
                'content_type': dimensions.get('edgeResponseContentTypeName', 'Unknown'),
                'colo': dimensions.get('coloCode', 'Unknown'),
                'clientRequestPath': dimensions.get('clientRequestPath', '/'),
                'clientRequestMethod': dimensions.get('clientRequestHTTPMethodName', 'GET'),
                'clientRequestHTTPHost': dimensions.get('clientRequestHTTPHost', 'unknown'),
                'edgeResponseStatus': dimensions.get('edgeResponseStatus', 0),
                
                # Cache information
                'cache_status': cache_status,
                'cache_category': cache_category,
                
                # Performance metrics
                'ttfb_avg': avg_metrics.get('edgeTimeToFirstByteMs', 0),
                'origin_time_avg': avg_metrics.get('originResponseDurationMs', 0),
                'dns_time_avg': avg_metrics.get('edgeDnsResponseTimeMs', 0),
                
                # Error rates
                'error_rate_4xx': ratios.get('status4xx', 0),
                'error_rate_5xx': ratios.get('status5xx', 0),
                
                # Percentile metrics
                'ttfb_p50': quantiles.get('edgeTimeToFirstByteMsP50', 0),
                'ttfb_p95': quantiles.get('edgeTimeToFirstByteMsP95', 0),
                'ttfb_p99': quantiles.get('edgeTimeToFirstByteMsP99', 0),
                'origin_p50': quantiles.get('originResponseDurationMsP50', 0),
                'origin_p95': quantiles.get('originResponseDurationMsP95', 0),
                'origin_p99': quantiles.get('originResponseDurationMsP99', 0),
                
                # Volume metrics
                'visits': sums.get('visits', 0),
                'bytes': sums.get('edgeResponseBytes', 0),
                
                # Sampling metadata
                'sampling_rate': sampling_rate,
                'sample_interval': sample_interval,
                'sample_count': group.get('count', 0)
            }

        except Exception as e:
            logger.warning(f"Error processing individual metric: {str(e)}")
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
        normalized_path = self._normalize_path(path)
        return f"{host}{normalized_path}"

    def _add_endpoint_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add endpoint-specific metrics to the dataframe."""
        try:
            # Calculate endpoint-level metrics
            endpoint_metrics = df.groupby('endpoint').agg({
                'visits': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'cache_status': lambda x: (x.isin(['hit', 'stale', 'revalidated']).mean() * 100),
                'sampling_rate': 'mean',
                'confidence_score': 'mean'
            }).reset_index()
            
            endpoint_metrics.columns = [
                'endpoint', 'endpoint_visits', 'endpoint_visits_adjusted',
                'endpoint_ttfb_avg', 'endpoint_cache_hit_ratio', 'endpoint_sampling_rate',
                'endpoint_confidence_score'
            ]
            
            # Merge back to original dataframe
            df = df.merge(endpoint_metrics, on='endpoint', how='left')
            
            return df
            
        except Exception as e:
            logger.error(f"Error adding endpoint metrics: {str(e)}")
            return df

    def _calculate_confidence_scores(self, df: pd.DataFrame) -> pd.Series:
        """Calculate confidence scores based on sampling rate and sample size."""
        def get_confidence(row):
            rate = row['sampling_rate']
            sample_size = row['visits']
            
            if rate >= 0.5:  # 50% or more sampling
                base_confidence = 0.99
            elif rate >= 0.1:  # 10% or more sampling
                base_confidence = 0.95 if sample_size >= 1000 else 0.90
            elif rate >= 0.01:  # 1% or more sampling
                base_confidence = 0.90 if sample_size >= 10000 else 0.85
            else:
                base_confidence = 0.80 if sample_size >= 100000 else 0.75
            
            # Adjust for sample size
            size_factor = min(1.0, np.log10(sample_size + 1) / 6)
            return base_confidence * size_factor

        return df.apply(get_confidence, axis=1)
