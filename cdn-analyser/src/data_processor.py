import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
from .types import MetricGroup

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.cache_categories = {
            'HIT': ['hit', 'stream_hit'],
            'MISS': ['miss', 'expired', 'updating', 'stale'],
            'BYPASS': ['bypass', 'ignored'],
            'REVALIDATED': ['revalidated'],
            'DYNAMIC': ['dynamic'],
            'DEFERRED': ['deferred'],
            'UNKNOWN': ['unknown']
        }
    
    def process_zone_metrics(self, raw_data: Dict) -> Optional[pd.DataFrame]:
        """Process raw zone metrics with correct sampling interval interpretation."""
        try:
            viewer_data = raw_data.get('data', {}).get('viewer', {})
            zones_data = viewer_data.get('zones', [])
            
            if not zones_data:
                return None
                
            requests_data = zones_data[0].get('httpRequestsAdaptiveGroups', [])
            
            if not requests_data:
                return None
            
            metrics = []
            for group in requests_data:
                try:
                    metric = self._process_metric_group(group)
                    
                    # Get sampling interval and calculate sampling rate
                    sample_interval = group['avg'].get('sampleInterval', 1)
                    sampling_rate = 1 / sample_interval if sample_interval > 0 else 1
                    
                    # Store sampling metadata
                    metric['sample_interval'] = sample_interval
                    metric['sampling_rate'] = sampling_rate
                    
                    # Adjust count-based metrics for sampling rate
                    self._adjust_for_sampling_rate(metric)
                    metrics.append(metric)
                    
                except Exception as e:
                    logger.warning(f"Error processing metric group: {str(e)}")
                    continue
            
            if not metrics:
                return None
            
            df = pd.DataFrame(metrics)
            
            # Add timestamp column for time-series analysis
            df['timestamp'] = pd.to_datetime(df['datetime'])
            
            # Calculate sampling statistics
            df['sampled_requests'] = df['visits_sampled']
            df['estimated_total_requests'] = df['visits']
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing zone metrics: {str(e)}")
            return None
    
    def _process_metric_group(self, group: MetricGroup) -> Dict:
        """Process a single metric group."""
        cache_status = group['dimensions'].get('cacheStatus', 'unknown').lower()
        cache_category = 'UNKNOWN'
        for category, statuses in self.cache_categories.items():
            if cache_status in statuses:
                cache_category = category
                break
        
        metric = {
            'datetime': group['dimensions']['datetime'],
            'country': group['dimensions'].get('clientCountryName', 'Unknown'),
            'host': group['dimensions'].get('clientRequestHTTPHost', 'Unknown'),
            'path': group['dimensions'].get('clientRequestPath', '/'),
            'method': group['dimensions'].get('clientRequestHTTPMethodName', 'Unknown'),
            'protocol': group['dimensions'].get('clientRequestHTTPProtocol', 'Unknown'),
            'content_type': group['dimensions'].get('edgeResponseContentTypeName', 'Unknown'),
            'status': group['dimensions'].get('edgeResponseStatus', 0),
            'colo': group['dimensions'].get('coloCode', 'Unknown'),
            'cache_status': cache_status,
            'cache_category': cache_category,
        }
        
        # Add performance metrics
        metric.update(self._extract_performance_metrics(group))
        
        return metric
    
    def _adjust_for_sampling_rate(self, metric: Dict) -> None:
        """
        Adjust count-based metrics using sampling rate.
        SI=1 means 100% data, SI=10 means 10%, SI=100 means 1%, etc.
        """
        sampling_rate = metric['sampling_rate']
        
        # Metrics that need to be scaled up to represent 100% of traffic
        count_metrics = ['visits', 'bytes']
        for field in count_metrics:
            if field in metric:
                # Scale up by dividing by sampling rate
                # Example: If SI=10 (10% sampling), multiply by 10
                metric[f'estimated_total_{field}'] = int(metric[field] / sampling_rate)
                metric[f'{field}_sampled'] = metric[field]  # Keep original sampled values
                metric[field] = metric[f'estimated_total_{field}']  # Update main metric
    
    def _extract_performance_metrics(self, group: MetricGroup) -> Dict:
        """Extract performance metrics from a group."""
        avg_data = group.get('avg', {})
        quantiles = group.get('quantiles', {})
        sum_data = group.get('sum', {})
        ratio_data = group.get('ratio', {})
        
        return {
            'ttfb_avg': avg_data.get('edgeTimeToFirstByteMs', 0),
            'origin_time_avg': avg_data.get('originResponseDurationMs', 0),
            'dns_time_avg': avg_data.get('edgeDnsResponseTimeMs', 0),
            'sample_interval': avg_data.get('sampleInterval', 1),
            'dns_time_p50': quantiles.get('edgeDnsResponseTimeMsP50', 0),
            'dns_time_p95': quantiles.get('edgeDnsResponseTimeMsP95', 0),
            'dns_time_p99': quantiles.get('edgeDnsResponseTimeMsP99', 0),
            'ttfb_p50': quantiles.get('edgeTimeToFirstByteMsP50', 0),
            'ttfb_p95': quantiles.get('edgeTimeToFirstByteMsP95', 0),
            'ttfb_p99': quantiles.get('edgeTimeToFirstByteMsP99', 0),
            'origin_time_p50': quantiles.get('originResponseDurationMsP50', 0),
            'origin_time_p95': quantiles.get('originResponseDurationMsP95', 0),
            'origin_time_p99': quantiles.get('originResponseDurationMsP99', 0),
            'bytes': sum_data.get('edgeResponseBytes', 0),
            'visits': sum_data.get('visits', 0),
            'error_4xx_ratio': ratio_data.get('status4xx', 0),
            'error_5xx_ratio': ratio_data.get('status5xx', 0),
        }
