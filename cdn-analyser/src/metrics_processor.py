# metrics_processor.py
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class MetricsProcessor:
    def __init__(self):
        pass
    
    async def process_metrics(self, raw_data: Dict) -> pd.DataFrame:
        """Process raw metrics with sampling interval considerations."""
        try:
            if not raw_data or 'data' not in raw_data:
                raise ValueError("Invalid raw data format")

            viewer_data = raw_data['data'].get('viewer', {})
            zones_data = viewer_data.get('zones', [])
            
            if not zones_data or not zones_data[0].get('httpRequestsAdaptiveGroups'):
                raise ValueError("No metrics data found in response")
            
            metrics = []
            
            for group in zones_data[0]['httpRequestsAdaptiveGroups']:
                # Calculate sampling rate from interval
                sample_interval = group['avg'].get('sampleInterval', 1)
                sampling_rate = 1 / sample_interval if sample_interval > 0 else 1
                
                metric = {
                    'datetime': group['dimensions']['datetime'],
                    'cache_status': group['dimensions'].get('cacheStatus', 'unknown'),
                    'country': group['dimensions'].get('clientCountryName', 'unknown'),
                    'content_type': group['dimensions'].get('edgeResponseContentTypeName', 'unknown'),
                    'protocol': group['dimensions'].get('clientRequestHTTPProtocol', 'unknown'),
                    'device_type': group['dimensions'].get('clientDeviceType', 'unknown'),
                    'colo': group['dimensions'].get('coloCode', 'unknown'),
                    'status': group['dimensions'].get('edgeResponseStatus', 0),
                    
                    # Performance metrics
                    'ttfb_avg': group['avg'].get('edgeTimeToFirstByteMs', 0),
                    'origin_time_avg': group['avg'].get('originResponseDurationMs', 0),
                    'dns_time_avg': group['avg'].get('edgeDnsResponseTimeMs', 0),
                    'sampling_rate': sampling_rate,
                    
                    # Request counts and bandwidth
                    'visits': group['sum'].get('visits', 0),
                    'bytes': group['sum'].get('edgeResponseBytes', 0),
                    'ttfb_sum': group['sum'].get('edgeTimeToFirstByteMs', 0),
                    'origin_time_sum': group['sum'].get('originResponseDurationMs', 0),
                    
                    # Error rates
                    'error_rate_4xx': group['ratio'].get('status4xx', 0),
                    'error_rate_5xx': group['ratio'].get('status5xx', 0),
                }
                
                # Add percentile metrics if available
                if 'quantiles' in group:
                    percentiles = group['quantiles']
                    metric.update({
                        'ttfb_p50': percentiles.get('edgeTimeToFirstByteMsP50', 0),
                        'ttfb_p95': percentiles.get('edgeTimeToFirstByteMsP95', 0),
                        'ttfb_p99': percentiles.get('edgeTimeToFirstByteMsP99', 0),
                        'origin_p50': percentiles.get('originResponseDurationMsP50', 0),
                        'origin_p95': percentiles.get('originResponseDurationMsP95', 0),
                        'origin_p99': percentiles.get('originResponseDurationMsP99', 0),
                    })
                
                metrics.append(metric)
            
            df = pd.DataFrame(metrics)
            df['timestamp'] = pd.to_datetime(df['datetime'])
            
            # Calculate adjusted metrics
            df['visits_adjusted'] = df.apply(
                lambda row: row['visits'] / row['sampling_rate'] if row['sampling_rate'] > 0 else 0,
                axis=1
            )
            df['bytes_adjusted'] = df.apply(
                lambda row: row['bytes'] / row['sampling_rate'] if row['sampling_rate'] > 0 else 0,
                axis=1
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing metrics: {str(e)}")
            raise
