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
        """Process raw zone metrics into a DataFrame."""
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
                    metrics.append(metric)
                except Exception as e:
                    logger.warning(f"Error processing metric group: {str(e)}")
                    continue
            
            if not metrics:
                return None
            
            df = pd.DataFrame(metrics)
            df['cache_hit_ratio'] = (
                df['cache_status'].isin(['hit', 'stale', 'revalidated'])
            ).astype(float) * 100
            
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
