# api_client.py
import requests
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import traceback
from pathlib import Path
import concurrent.futures
from threading import Lock
import time
from queue import Queue

from .graphql_queries import (
    ZONE_METRICS_BASIC_QUERY,
    ZONE_METRICS_DETAILED_QUERY,
    generate_time_slices,
    validate_time_range,
    reduce_slice_size,
    TimeSlice,
    MAX_QUERIES_PER_5_MIN,
    MIN_SLICE_MINUTES,
    INITIAL_SLICE_MINUTES,
    RATE_WINDOW_MINUTES,
)

logger = logging.getLogger(__name__)

class CloudflareAPIClient:
    def __init__(self, config):
        self.config = config
        self.base_url = "https://api.cloudflare.com/client/v4"
        self.headers = {
            'X-Auth-Email': config.email,
            'X-Auth-Key': config.api_key,
            'Content-Type': 'application/json'
        }
        self.rate_limit_lock = Lock()
        self.query_count = 0
        self.last_query_time = datetime.now(timezone.utc)
        self.metrics_queue = Queue()
        self.max_retries = 3
        self.min_retry_wait = 1
        self.request_timeout = 30
        self.debug_mode = True
        
        # Concurrent processing settings
        self.max_workers = 5
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        self.query_timestamps = []

    def fetch_zone_metrics(
        self,
        zone_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        sample_interval: Optional[int] = None
    ) -> Optional[Dict]:
        """Main entry point for fetching zone metrics with concurrent processing."""
        try:
            # Validate and normalize time range
            end = end_time or datetime.now(timezone.utc)
            start = start_time or (end - timedelta(hours=24))

            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)

            logger.info(f"""
Fetching metrics with parameters:
-------------------------------
Zone ID: {zone_id}
Time Range: {start.isoformat()} to {end.isoformat()}
Duration: {end - start}
Sample Interval: {sample_interval}
""")

            if not validate_time_range(start, end):
                return None

            # Generate time slices for concurrent processing
            time_slices = generate_time_slices(start, end, INITIAL_SLICE_MINUTES)
            if not time_slices:
                return None

            all_metrics = []
            failed_slices = []

            # Process slices concurrently
            futures = {}
            for time_slice in time_slices:
                future = self.executor.submit(
                    self._fetch_slice_metrics,
                    zone_id,
                    time_slice.start,
                    time_slice.end,
                    sample_interval
                )
                futures[future] = time_slice

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                time_slice = futures[future]
                try:
                    metrics = future.result()
                    if metrics:
                        all_metrics.extend(metrics)
                    else:
                        failed_slices.append(time_slice)
                except Exception as e:
                    logger.error(f"Error processing slice {time_slice}: {str(e)}")
                    failed_slices.append(time_slice)

            # Retry failed slices with reduced time windows
            if failed_slices:
                logger.info(f"Retrying {len(failed_slices)} failed slices")
                for failed_slice in failed_slices:
                    reduced_slices = generate_time_slices(
                        failed_slice.start,
                        failed_slice.end,
                        reduce_slice_size(failed_slice.size_minutes)
                    )
                    
                    for reduced_slice in reduced_slices:
                        retry_metrics = self._fetch_slice_metrics(
                            zone_id,
                            reduced_slice.start,
                            reduced_slice.end,
                            sample_interval
                        )
                        if retry_metrics:
                            all_metrics.extend(retry_metrics)

            if not all_metrics:
                logger.error("No metrics collected from any time slice")
                return None

            # Deduplicate and create response structure
            unique_metrics = self._deduplicate_metrics(all_metrics)
            
            response_data = {
                'data': {
                    'viewer': {
                        'zones': [{
                            'httpRequestsAdaptiveGroups': unique_metrics
                        }]
                    }
                }
            }

            self._save_raw_response(response_data, zone_id, sample_interval, start, end)
            return response_data

        except Exception as e:
            logger.error(f"Error fetching metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _fetch_slice_metrics(
        self,
        zone_id: str,
        start_time: datetime,
        end_time: datetime,
        sample_interval: Optional[int]
    ) -> Optional[List[Dict]]:
        """Fetch metrics for a single time slice with rate limiting."""
        try:
            with self.rate_limit_lock:
                self._enforce_rate_limit()

            # Fetch basic metrics
            basic_metrics = self._fetch_slice_with_retry(
                zone_id,
                start_time,
                end_time,
                sample_interval,
                ZONE_METRICS_BASIC_QUERY,
                'basic'
            )

            if not basic_metrics:
                return None

            # Fetch detailed metrics
            with self.rate_limit_lock:
                self._enforce_rate_limit()
                
            detailed_metrics = self._fetch_slice_with_retry(
                zone_id,
                start_time,
                end_time,
                sample_interval,
                ZONE_METRICS_DETAILED_QUERY,
                'detailed'
            )

            # Merge metrics if both queries successful
            if basic_metrics and detailed_metrics:
                return self._merge_metrics(basic_metrics, detailed_metrics)

            return basic_metrics

        except Exception as e:
            logger.error(f"Error fetching slice metrics: {str(e)}")
            return None

    def _fetch_slice_with_retry(
        self,
        zone_id: str,
        start_time: datetime,
        end_time: datetime,
        sample_interval: Optional[int],
        query: str,
        query_type: str
    ) -> Optional[List[Dict]]:
        """Fetch metrics for a single time slice with enhanced error handling."""
        for attempt in range(self.max_retries):
            try:
                # Format datetime strings properly
                start_str = start_time.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
                end_str = end_time.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                variables = {
                    "zoneTag": zone_id,
                    "filter": {
                        "datetimeMinute_gt": start_str,
                        "datetimeMinute_lt": end_str
                    }
                }
                
                if sample_interval is not None:
                    variables["filter"]["sampleInterval"] = sample_interval

                response = requests.post(
                    f"{self.base_url}/graphql",
                    headers=self.headers,
                    json={
                        "query": query,
                        "variables": variables
                    },
                    timeout=self.request_timeout
                )

                if response.status_code == 429:  # Rate limit hit
                    wait_time = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit hit, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue

                # Parse response
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON response: {str(e)}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                # Check for HTTP errors
                if response.status_code != 200:
                    logger.error(f"""
HTTP Error ({query_type}):
----------
Status Code: {response.status_code}
Response Headers: {dict(response.headers)}
Response Body: {response.text[:2000]}
""")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                # Check for explicit GraphQL errors
                if 'errors' in data and data['errors']:
                    logger.error(f"""
GraphQL Errors:
-------------
{json.dumps(data.get('errors', []), indent=2)}
Query Variables: {json.dumps(variables, indent=2)}
""")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                # Extract metrics
                metrics = (data.get('data', {})
                         .get('viewer', {})
                         .get('zones', [{}])[0]
                         .get('httpRequestsAdaptiveGroups', []))

                return metrics

            except Exception as e:
                logger.error(f"Error in fetch attempt {attempt + 1}: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue

        return None

    def _enforce_rate_limit(self) -> None:
        """Enhanced rate limiting with sliding window."""
        current_time = time.time()
        
        # Remove timestamps outside the window
        self.query_timestamps = [
            t for t in self.query_timestamps 
            if current_time - t <= RATE_WINDOW_MINUTES * 60
        ]
        
        # If at rate limit, wait until oldest query expires
        if len(self.query_timestamps) >= MAX_QUERIES_PER_5_MIN:
            wait_time = (self.query_timestamps[0] + RATE_WINDOW_MINUTES * 60) - current_time
            if wait_time > 0:
                logger.info(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
        
        # Add current query timestamp
        self.query_timestamps.append(current_time)

    def _merge_metrics(self, basic_metrics: List[Dict], detailed_metrics: List[Dict]) -> List[Dict]:
        """Merge basic and detailed metrics based on datetimeMinute."""
        try:
            # Create lookup dictionary for detailed metrics
            detailed_dict = {
                m['dimensions']['datetimeMinute']: m 
                for m in detailed_metrics
            }

            merged_metrics = []
            for basic_metric in basic_metrics:
                datetime_key = basic_metric['dimensions']['datetimeMinute']
                detailed_metric = detailed_dict.get(datetime_key, {})

                # Merge dimensions
                merged_dimensions = {**basic_metric['dimensions']}
                if detailed_metric:
                    merged_dimensions.update(detailed_metric.get('dimensions', {}))

                # Create merged metric
                merged_metric = {
                    'dimensions': merged_dimensions,
                    'avg': {**basic_metric['avg']},
                    'sum': {**basic_metric['sum']},
                    'count': basic_metric['count'],
                    'ratio': basic_metric.get('ratio', {})
                }

                # Add detailed metrics if available
                if detailed_metric:
                    if 'avg' in detailed_metric:
                        merged_metric['avg'].update(detailed_metric['avg'])
                    if 'sum' in detailed_metric:
                        merged_metric['sum'].update(detailed_metric['sum'])
                    if 'quantiles' in detailed_metric:
                        merged_metric['quantiles'] = detailed_metric['quantiles']

                merged_metrics.append(merged_metric)

            return merged_metrics

        except Exception as e:
            logger.error(f"Error merging metrics: {str(e)}")
            # Return basic metrics if merge fails
            return basic_metrics

    def _deduplicate_metrics(self, metrics: List[Dict]) -> List[Dict]:
        """Deduplicate metrics with enhanced validation."""
        try:
            seen = set()
            unique_metrics = []
            duplicates = 0
            
            for metric in metrics:
                if not isinstance(metric, dict):
                    logger.warning(f"Skipping invalid metric type: {type(metric)}")
                    continue
                    
                dimensions = metric.get('dimensions', {})
                if not dimensions:
                    logger.warning("Skipping metric without dimensions")
                    continue
                    
                datetime_key = dimensions.get('datetimeMinute')
                if not datetime_key:
                    logger.warning("Skipping metric without datetimeMinute")
                    continue
                
                if datetime_key not in seen:
                    seen.add(datetime_key)
                    unique_metrics.append(metric)
                else:
                    duplicates += 1
            
            # Sort metrics by datetime
            sorted_metrics = sorted(
                unique_metrics,
                key=lambda x: x['dimensions']['datetimeMinute']
            )
            
            if sorted_metrics:
                logger.info(f"""
Deduplication Results:
-------------------
Original metrics: {len(metrics)}
Unique metrics: {len(sorted_metrics)}
Duplicates removed: {duplicates}
Time range: {sorted_metrics[0]['dimensions']['datetimeMinute']} to {sorted_metrics[-1]['dimensions']['datetimeMinute']}
""")
            
            return sorted_metrics
            
        except Exception as e:
            logger.error(f"Error deduplicating metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def get_zones(self) -> List[Dict]:
        """Fetch all zones with enhanced error handling."""
        try:
            logger.info("Fetching zones from Cloudflare")
            
            with self.rate_limit_lock:
                response = requests.get(
                    f"{self.base_url}/zones",
                    headers=self.headers,
                    timeout=self.request_timeout
                )
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 300))
                logger.warning(f"Rate limit hit on zones request, retrying after {retry_after} seconds")
                time.sleep(retry_after + 5)
                return self.get_zones()

            if response.status_code != 200:
                logger.error(f"""
Failed to fetch zones:
-------------------
Status Code: {response.status_code}
Response: {response.text}
""")
                return []

            data = response.json()
            
            if not data.get('success'):
                logger.error(f"API request unsuccessful: {data.get('errors', [])}")
                return []

            zones = data.get('result', [])
            
            logger.info(f"""
Zones Retrieved:
--------------
Total Zones: {len(zones)}
Active Zones: {sum(1 for z in zones if z.get('status') == 'active')}
Development Mode: {sum(1 for z in zones if z.get('development_mode', 0) == 1)}
""")

            return [
                {
                    'id': zone['id'],
                    'name': zone['name'],
                    'status': zone['status'],
                    'development_mode': zone.get('development_mode', 0) == 1
                }
                for zone in zones
            ]

        except Exception as e:
            logger.error(f"Error fetching zones: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def _save_raw_response(
        self,
        response_data: Dict,
        zone_id: str,
        sample_interval: Optional[int],
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """Save raw response data for debugging."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = self.config.json_dir / f"raw_response_{zone_id}_{timestamp}.json"
            
            metadata = {
                'zone_id': zone_id,
                'sample_interval': sample_interval,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'timestamp': timestamp,
                'query_count': len(self.query_timestamps),
                'processing_time': (datetime.now(timezone.utc) - start_time).total_seconds()
            }
            
            data_to_save = {
                'metadata': metadata,
                'response': response_data
            }
            
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w') as f:
                json.dump(data_to_save, f, indent=2)
                
            logger.debug(f"Raw response saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving raw response: {str(e)}")
            logger.error(traceback.format_exc())

    def cleanup(self):
        """Cleanup resources."""
        try:
            if self.executor:
                self.executor.shutdown(wait=True)
            logger.info("API client resources cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
