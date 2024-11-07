# api_client.py
import requests
import json
import logging
from datetime import datetime, timedelta, timezone
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
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.rate_limit_lock = Lock()
        self.query_count = 0
        self.last_query_time = datetime.now(timezone.utc)
        self.metrics_queue = Queue()
        self.max_retries = 3
        self.min_retry_wait = 1  # seconds
        self.request_timeout = 30  # seconds
        self.debug_mode = True  # Enable detailed logging

    def fetch_zone_metrics(
        self,
        zone_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        sample_interval: Optional[int] = None
    ) -> Optional[Dict]:
        """Fetch zone metrics with enhanced debugging and validation."""
        try:
            # Validate and normalize time range
            end = end_time or datetime.now(timezone.utc)
            start = start_time or (end - timedelta(hours=24))

            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)

            # Enhanced debugging information
            logger.info(f"""
Fetching metrics with parameters:
-------------------------------
Zone ID: {zone_id}
Time Range: {start.isoformat()} to {end.isoformat()}
Duration: {end - start}
Sample Interval: {sample_interval}
Current Time: {datetime.now(timezone.utc).isoformat()}
UTC Offset: {datetime.now(timezone.utc).utcoffset()}
Time Window: {INITIAL_SLICE_MINUTES} minutes
Rate Limit Status: {self.query_count}/{MAX_QUERIES_PER_5_MIN} queries used
""")

            if not validate_time_range(start, end):
                return None

            # Process metrics in time slices
            all_metrics = []
            current_start = start
            current_slice_minutes = INITIAL_SLICE_MINUTES

            while current_start < end and current_slice_minutes >= MIN_SLICE_MINUTES:
                slice_end = min(current_start + timedelta(minutes=current_slice_minutes), end)
                logger.info(f"""
Processing time slice:
-------------------
Start: {current_start.isoformat()}
End: {slice_end.isoformat()}
Duration: {slice_end - current_start}
Slice Size: {current_slice_minutes} minutes
""")

                # Fetch both basic and detailed metrics
                metrics = self._fetch_combined_metrics(
                    zone_id,
                    current_start,
                    slice_end,
                    sample_interval
                )

                if metrics is not None:
                    logger.info(f"Successfully fetched {len(metrics)} metrics for slice")
                    all_metrics.extend(metrics)
                    current_start = slice_end
                    current_slice_minutes = min(current_slice_minutes * 2, INITIAL_SLICE_MINUTES)
                else:
                    current_slice_minutes = reduce_slice_size(current_slice_minutes)
                    logger.warning(f"Reducing slice size to {current_slice_minutes} minutes")
                    if current_slice_minutes < MIN_SLICE_MINUTES:
                        logger.error("Slice size below minimum, aborting")
                        break

                time.sleep(self.min_retry_wait)

            if not all_metrics:
                logger.error("No metrics data found in any time slice")
                return None

            unique_metrics = self._deduplicate_metrics(all_metrics)
            
            # Log metrics summary
            self._log_metrics_summary(unique_metrics, start, end)

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

    def _fetch_combined_metrics(
        self,
        zone_id: str,
        start_time: datetime,
        end_time: datetime,
        sample_interval: Optional[int]
    ) -> Optional[List[Dict]]:
        """Fetch and combine both basic and detailed metrics."""
        try:
            # Fetch basic metrics first
            basic_metrics = self._fetch_slice_with_retry(
                zone_id,
                start_time,
                end_time,
                sample_interval,
                ZONE_METRICS_BASIC_QUERY,
                'basic'
            )

            if not basic_metrics:
                logger.error("Failed to fetch basic metrics")
                return None

            # Fetch detailed metrics
            detailed_metrics = self._fetch_slice_with_retry(
                zone_id,
                start_time,
                end_time,
                sample_interval,
                ZONE_METRICS_DETAILED_QUERY,
                'detailed'
            )

            if not detailed_metrics:
                logger.warning("Failed to fetch detailed metrics, continuing with basic metrics only")
                return basic_metrics

            # Merge metrics based on datetimeMinute
            return self._merge_metrics(basic_metrics, detailed_metrics)

        except Exception as e:
            logger.error(f"Error in fetch_combined_metrics: {str(e)}")
            logger.error(traceback.format_exc())
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

                logger.info(f"""
    Fetch attempt {attempt + 1}/{self.max_retries}:
    ------------------------------------------
    Query Type: {query_type}
    Time Range: {start_str} to {end_str}
    Sample Interval: {sample_interval}
    Variables: {json.dumps(variables, indent=2)}
    """)

                with self.rate_limit_lock:
                    self._handle_rate_limiting()

                response = requests.post(
                    f"{self.base_url}/graphql",
                    headers=self.headers,
                    json={
                        "query": query,
                        "variables": variables
                    },
                    timeout=self.request_timeout
                )

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
                if 'errors' in data and data['errors']:  # Only consider non-empty errors
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

                if metrics:
                    logger.info(f"""
    Success:
    -------
    Retrieved {len(metrics)} metrics
    First Timestamp: {metrics[0]['dimensions']['datetimeMinute'] if metrics else 'N/A'}
    Last Timestamp: {metrics[-1]['dimensions']['datetimeMinute'] if metrics else 'N/A'}
    """)
                    return metrics
                else:
                    logger.warning(f"""
    No metrics found in response:
    -------------------------
    Response Data: {json.dumps(data.get('data', {}), indent=2)}
    """)
                    return []

            except Exception as e:
                logger.error(f"""
    Error in fetch attempt {attempt + 1}:
    Type: {type(e).__name__}
    Message: {str(e)}
    Stack Trace:
    {traceback.format_exc()}
    """)
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue

        return None

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
                    'ratio': basic_metric['ratio']
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

            logger.info(f"""
Metrics Merge Results:
-------------------
Basic Metrics: {len(basic_metrics)}
Detailed Metrics: {len(detailed_metrics)}
Merged Metrics: {len(merged_metrics)}
""")

            return merged_metrics

        except Exception as e:
            logger.error(f"Error merging metrics: {str(e)}")
            logger.error(traceback.format_exc())
            # Return basic metrics if merge fails
            return basic_metrics

    def _log_graphql_error(self, error_data: Any, query_variables: Dict, query_type: str) -> None:
        """Log detailed information about GraphQL errors."""
        try:
            logger.error(f"""
GraphQL Query Failed ({query_type})
==================""")
            
            # Log complete error data in debug mode
            if self.debug_mode:
                logger.debug(f"Complete error data: {json.dumps(error_data, indent=2)}")

            # Extract and log errors
            if isinstance(error_data, dict):
                if 'errors' in error_data and isinstance(error_data['errors'], list):
                    errors = error_data['errors']
                    for idx, error in enumerate(errors, 1):
                        logger.error(f"""
Error {idx}:
----------
Message: {error.get('message', 'No message')}
Path: {' -> '.join(str(p) for p in error.get('path', []))}
Extensions: {json.dumps(error.get('extensions', {}), indent=2)}
""")

                # Log response data structure if present
                if 'data' in error_data:
                    logger.debug(f"Response data structure: {json.dumps(error_data['data'], indent=2)}")

            # Log query details
            logger.error(f"""
Query Details:
------------
Zone ID: {query_variables.get('zoneTag')}
Time Range: {query_variables['filter'].get('datetimeMinute_gt')} to {query_variables['filter'].get('datetimeMinute_lt')}
Sample Interval: {query_variables['filter'].get('sampleInterval', 'Not specified')}
Query Type: {query_type}
""")

            # Log request context
            logger.error(f"""
Request Context:
--------------
Timestamp: {datetime.now(timezone.utc).isoformat()}
Rate Limit Status: {self.query_count}/{MAX_QUERIES_PER_5_MIN}
Time Since Last Query: {(datetime.now(timezone.utc) - self.last_query_time).total_seconds():.2f}s
""")

        except Exception as e:
            logger.error(f"Error while logging GraphQL error: {str(e)}")
            logger.error(traceback.format_exc())

    def _handle_rate_limiting(self) -> None:
        """Handle rate limiting with enhanced logging."""
        current_time = datetime.now(timezone.utc)
        time_since_last = (current_time - self.last_query_time).total_seconds()
        
        logger.debug(f"""
Rate Limit Check:
--------------
Current Time: {current_time.isoformat()}
Time Since Last Query: {time_since_last:.2f}s
Query Count: {self.query_count}/{MAX_QUERIES_PER_5_MIN}
Window Size: {RATE_WINDOW_MINUTES} minutes
""")
        
        if time_since_last < RATE_WINDOW_MINUTES * 60:
            if self.query_count >= MAX_QUERIES_PER_5_MIN:
                wait_time = (RATE_WINDOW_MINUTES * 60) - time_since_last + 5
                logger.info(f"Rate limit reached, waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
                self.query_count = 0
                self.last_query_time = current_time
            else:
                time.sleep(self.min_retry_wait)
        else:
            self.query_count = 0
            self.last_query_time = current_time

        self.query_count += 1
        logger.debug(f"New query count: {self.query_count}")

    def _log_metrics_summary(self, metrics: List[Dict], start: datetime, end: datetime) -> None:
        """Log summary of collected metrics."""
        try:
            if not metrics:
                logger.warning("No metrics to summarize")
                return

            # Extract timestamps and sort them
            timestamps = sorted(m['dimensions']['datetimeMinute'] for m in metrics)
            first_metric = timestamps[0]
            last_metric = timestamps[-1]
            
            # Calculate time coverage
            total_duration = end - start
            covered_duration = datetime.fromisoformat(last_metric) - datetime.fromisoformat(first_metric)
            coverage_percent = (covered_duration.total_seconds() / total_duration.total_seconds()) * 100

            logger.info(f"""
Metrics Summary:
-------------
Total Metrics: {len(metrics)}
Time Range Requested: {start.isoformat()} to {end.isoformat()}
First Metric: {first_metric}
Last Metric: {last_metric}
Time Coverage: {coverage_percent:.1f}%
Average Interval: {covered_duration.total_seconds() / max(len(metrics)-1, 1):.1f} seconds
""")

            # Analyze gaps
            if len(timestamps) > 1:
                gaps = self._analyze_time_gaps(timestamps)
                if gaps:
                    logger.warning(f"""
Time Gaps Detected:
----------------
{json.dumps(gaps, indent=2)}
""")

        except Exception as e:
            logger.error(f"Error generating metrics summary: {str(e)}")
            logger.error(traceback.format_exc())

    def _analyze_time_gaps(self, timestamps: List[str]) -> List[Dict]:
        """Analyze gaps in time series data."""
        try:
            gaps = []
            prev_time = datetime.fromisoformat(timestamps[0])
            
            for timestamp in timestamps[1:]:
                current_time = datetime.fromisoformat(timestamp)
                gap = current_time - prev_time
                
                # Consider gaps longer than 5 minutes significant
                if gap > timedelta(minutes=5):
                    gaps.append({
                        'start': prev_time.isoformat(),
                        'end': current_time.isoformat(),
                        'duration_minutes': gap.total_seconds() / 60
                    })
                
                prev_time = current_time
            
            return gaps

        except Exception as e:
            logger.error(f"Error analyzing time gaps: {str(e)}")
            return []

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
            
            # Log response details in debug mode
            if self.debug_mode:
                logger.debug(f"""
Zone Request Response:
-------------------
Status Code: {response.status_code}
Headers: {dict(response.headers)}
""")
            
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
            
            # Log detailed zone information
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

    def _save_raw_response(self, response_data: Dict, zone_id: str, sample_interval: Optional[int],
                          start_time: datetime, end_time: datetime) -> None:
        """Save raw response data for debugging."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = self.config.json_dir / f"raw_response_{zone_id}_{timestamp}.json"
            
            metadata = {
                'zone_id': zone_id,
                'sample_interval': sample_interval,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'timestamp': timestamp
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
