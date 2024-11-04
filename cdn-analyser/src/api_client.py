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
    ZONE_METRICS_QUERY,
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
        """Fetch zone metrics with progressive slice handling and validation."""
        try:
            # Validate and normalize time range
            end = end_time or datetime.now(timezone.utc)
            start = start_time or (end - timedelta(hours=24))

            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)

            if not validate_time_range(start, end):
                return None

            if self.debug_mode:
                logger.debug(f"Initial request parameters:")
                logger.debug(f"Zone ID: {zone_id}")
                logger.debug(f"Time range: {start.isoformat()} to {end.isoformat()}")
                logger.debug(f"Sample interval: {sample_interval}")

            logger.info(f"Fetching metrics from {start.isoformat()} to {end.isoformat()}")
            
            all_metrics = []
            current_start = start
            current_slice_minutes = INITIAL_SLICE_MINUTES

            while current_start < end and current_slice_minutes >= MIN_SLICE_MINUTES:
                slice_end = min(current_start + timedelta(minutes=current_slice_minutes), end)
                logger.info(f"Attempting slice: {current_start.isoformat()} to {slice_end.isoformat()} "
                          f"({current_slice_minutes} minutes)")

                metrics = self._fetch_slice_with_retry(
                    zone_id,
                    current_start,
                    slice_end,
                    sample_interval
                )

                if metrics is not None:
                    all_metrics.extend(metrics)
                    current_start = slice_end
                    current_slice_minutes = min(current_slice_minutes * 2, INITIAL_SLICE_MINUTES)
                    logger.info(f"Slice successful, moving to next slice with size {current_slice_minutes} minutes")
                else:
                    current_slice_minutes = reduce_slice_size(current_slice_minutes)
                    logger.warning(f"Slice failed, reducing size to {current_slice_minutes} minutes")
                    if current_slice_minutes < MIN_SLICE_MINUTES:
                        logger.error("Slice size below minimum, aborting")
                        break

                time.sleep(self.min_retry_wait)

            if not all_metrics:
                logger.error("No metrics data found in any time slice")
                return None

            unique_metrics = self._deduplicate_metrics(all_metrics)
            logger.info(f"Collected {len(unique_metrics)} unique metrics")

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

    def _log_graphql_error(self, error_data: Any, query_variables: Dict) -> None:
        """Log detailed information about GraphQL errors with improved error parsing."""
        try:
            logger.error("GraphQL Query Failed")
            logger.error("=" * 50)
            
            # Log complete error data in debug mode
            if self.debug_mode:
                logger.debug(f"Complete error data: {json.dumps(error_data, indent=2)}")

            # Extract and log errors
            if isinstance(error_data, dict):
                # Check for errors array
                if 'errors' in error_data and isinstance(error_data['errors'], list):
                    errors = error_data['errors']
                    if errors:  # If errors array is not empty
                        for idx, error in enumerate(errors, 1):
                            logger.error(f"\nError {idx}:")
                            if isinstance(error, dict):
                                # Log all available error fields
                                for key, value in error.items():
                                    logger.error(f"  {key}: {value}")
                    else:
                        logger.error("Empty errors array received")
                
                # Check for data field
                if 'data' in error_data:
                    data = error_data['data']
                    if data is None:
                        logger.error("\nNull data received in response")
                    elif isinstance(data, dict):
                        # Log data structure for debugging
                        logger.error("\nResponse data structure:")
                        self._log_data_structure(data)
                    else:
                        logger.error(f"\nUnexpected data type in response: {type(data)}")

            # Log query details
            logger.error("\nQuery Details:")
            filter_data = query_variables.get('filter', {})
            logger.error(f"Zone ID: {query_variables.get('zoneTag')}")
            logger.error(f"Time Range: {filter_data.get('datetime_geq')} to {filter_data.get('datetime_leq')}")
            logger.error(f"Sample Interval: {filter_data.get('sampleInterval', 'Not specified')}")

            # Log request context
            logger.error("\nRequest Context:")
            logger.error(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
            logger.error(f"Rate Limit Status: {self.query_count}/{MAX_QUERIES_PER_5_MIN} queries used")
            logger.error(f"Time Since Last Query: {(datetime.now(timezone.utc) - self.last_query_time).total_seconds():.2f}s")
            
            logger.error("=" * 50)

        except Exception as e:
            logger.error(f"Error while logging GraphQL error: {str(e)}")
            logger.error(f"Stack trace: {traceback.format_exc()}")

    def _log_data_structure(self, data: Dict, prefix: str = "") -> None:
        """Recursively log the structure of the data object."""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    logger.error(f"{prefix}{key}:")
                    self._log_data_structure(value, prefix + "  ")
                else:
                    logger.error(f"{prefix}{key}: {type(value).__name__}")
        elif isinstance(data, list):
            if data:
                logger.error(f"{prefix}Array of {len(data)} items:")
                self._log_data_structure(data[0], prefix + "  ")
            else:
                logger.error(f"{prefix}Empty array")

    def _fetch_slice_with_retry(
        self,
        zone_id: str,
        start_time: datetime,
        end_time: datetime,
        sample_interval: Optional[int]
    ) -> Optional[List[Dict]]:
        """Fetch metrics for a single time slice with improved error handling."""
        for attempt in range(self.max_retries):
            try:
                variables = {
                    "zoneTag": zone_id,
                    "filter": {
                        "datetime_geq": start_time.isoformat(),
                        "datetime_leq": end_time.isoformat()
                    }
                }
                
                if sample_interval is not None:
                    variables["filter"]["sampleInterval"] = sample_interval

                logger.info(f"Fetching slice: {start_time.isoformat()} to {end_time.isoformat()} "
                          f"(Attempt {attempt + 1}/{self.max_retries})")

                # Handle rate limiting
                with self.rate_limit_lock:
                    self._handle_rate_limiting()

                # Log the actual query if in debug mode
                if self.debug_mode:
                    logger.debug("GraphQL Query:")
                    logger.debug(ZONE_METRICS_QUERY)
                    logger.debug(f"Variables: {json.dumps(variables, indent=2)}")

                response = requests.post(
                    f"{self.base_url}/graphql",
                    headers=self.headers,
                    json={
                        "query": ZONE_METRICS_QUERY,
                        "variables": variables
                    },
                    timeout=self.request_timeout
                )

                # Log response details in debug mode
                if self.debug_mode:
                    logger.debug(f"Response Status: {response.status_code}")
                    logger.debug(f"Response Headers: {dict(response.headers)}")

                # Handle HTTP errors
                if response.status_code != 200:
                    logger.error(f"HTTP {response.status_code} Error:")
                    logger.error(f"Headers: {dict(response.headers)}")
                    try:
                        error_body = response.json()
                        logger.error(f"Body: {json.dumps(error_body, indent=2)}")
                    except:
                        logger.error(f"Body: {response.text[:1000]}...")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                # Parse response
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    logger.error(f"JSON parse error: {str(e)}")
                    logger.error(f"Raw response: {response.text[:1000]}...")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                # Validate response
                if not isinstance(data, dict):
                    logger.error(f"Unexpected response type: {type(data)}")
                    return None

                # Check for GraphQL errors
                if data.get('errors') or data.get('data') is None:
                    self._log_graphql_error(data, variables)
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                # Extract metrics
                metrics = (data.get('data', {})
                         .get('viewer', {})
                         .get('zones', [{}])[0]
                         .get('httpRequestsAdaptiveGroups', []))

                if not metrics:
                    logger.warning("No metrics found in response")
                    # Log the full response structure in debug mode
                    if self.debug_mode:
                        logger.debug("Response structure:")
                        self._log_data_structure(data)
                    return []

                logger.info(f"Retrieved {len(metrics)} metrics")
                return metrics

            except requests.Timeout:
                logger.error(f"Request timeout on attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
            except Exception as e:
                logger.error(f"Error in attempt {attempt + 1}: {str(e)}")
                logger.error(f"Stack trace: {traceback.format_exc()}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue

        return None

    def _handle_rate_limiting(self) -> None:
        """Handle rate limiting with improved logging."""
        current_time = datetime.now(timezone.utc)
        time_since_last = (current_time - self.last_query_time).total_seconds()
        
        if time_since_last < RATE_WINDOW_MINUTES * 60:
            if self.query_count >= MAX_QUERIES_PER_5_MIN:
                wait_time = (RATE_WINDOW_MINUTES * 60) - time_since_last + 5
                logger.info(f"Rate limit approaching ({self.query_count}/{MAX_QUERIES_PER_5_MIN} queries), "
                          f"waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
                self.query_count = 0
                self.last_query_time = current_time
            else:
                time.sleep(self.min_retry_wait)
        else:
            self.query_count = 0
            self.last_query_time = current_time

        self.query_count += 1

    def get_zones(self) -> List[Dict]:
        """Fetch all zones with enhanced error handling."""
        try:
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
                logger.error(f"Failed to fetch zones: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return []

            data = response.json()
            
            if not data.get('success'):
                logger.error(f"API request unsuccessful: {data.get('errors', [])}")
                return []

            zones = data.get('result', [])
            logger.info(f"Successfully fetched {len(zones)} zones")

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

    def _deduplicate_metrics(self, metrics: List[Dict]) -> List[Dict]:
        """Deduplicate metrics with enhanced validation."""
        try:
            seen = set()
            unique_metrics = []
            
            for metric in metrics:
                if not isinstance(metric, dict):
                    logger.warning(f"Skipping invalid metric type: {type(metric)}")
                    continue
                    
                dimensions = metric.get('dimensions', {})
                if not dimensions:
                    logger.warning("Skipping metric without dimensions")
                    continue
                    
                datetime_key = dimensions.get('datetime')
                if not datetime_key:
                    logger.warning("Skipping metric without datetime")
                    continue
                
                if datetime_key not in seen:
                    seen.add(datetime_key)
                    unique_metrics.append(metric)
            
            # Sort metrics by datetime
            sorted_metrics = sorted(
                unique_metrics,
                key=lambda x: x['dimensions']['datetime']
            )
            
            if self.debug_mode:
                logger.debug(f"Deduplication: {len(metrics)} total metrics -> {len(sorted_metrics)} unique metrics")
                if sorted_metrics:
                    logger.debug(f"Time range: {sorted_metrics[0]['dimensions']['datetime']} to "
                               f"{sorted_metrics[-1]['dimensions']['datetime']}")
            
            return sorted_metrics
            
        except Exception as e:
            logger.error(f"Error deduplicating metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def _save_raw_response(
        self,
        data: Dict,
        zone_id: str,
        sample_interval: Optional[int],
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """Save raw response with enhanced debug information."""
        try:
            debug_data = {
                "metadata": {
                    "zone_id": zone_id,
                    "sample_interval": sample_interval,
                    "time_range": {
                        "start": start_time.isoformat(),
                        "end": end_time.isoformat(),
                        "duration_minutes": (end_time - start_time).total_seconds() / 60
                    },
                    "query_timestamp": datetime.now(timezone.utc).isoformat(),
                    "rate_limiting": {
                        "query_count": self.query_count,
                        "time_since_last": (datetime.now(timezone.utc) - self.last_query_time).total_seconds(),
                        "max_queries_per_window": MAX_QUERIES_PER_5_MIN,
                        "window_minutes": RATE_WINDOW_MINUTES
                    }
                },
                "response": data
            }

            # Add metrics summary if data is present
            metrics = (data.get('data', {})
                     .get('viewer', {})
                     .get('zones', [{}])[0]
                     .get('httpRequestsAdaptiveGroups', []))
            
            if metrics:
                metrics_summary = {
                    "total_metrics": len(metrics),
                    "time_range": {
                        "first_metric": metrics[0]['dimensions']['datetime'],
                        "last_metric": metrics[-1]['dimensions']['datetime']
                    },
                    "sample_intervals": {
                        "min": min(m['avg'].get('sampleInterval', 0) for m in metrics),
                        "max": max(m['avg'].get('sampleInterval', 0) for m in metrics),
                        "avg": sum(m['avg'].get('sampleInterval', 0) for m in metrics) / len(metrics)
                    }
                }
                debug_data["metrics_summary"] = metrics_summary

            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = self.config.json_dir / f"raw_response_{zone_id}_{timestamp}.json"
            
            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Save with pretty printing for readability
            with open(filepath, 'w') as f:
                json.dump(debug_data, f, indent=2)
            
            logger.info(f"Raw response saved to {filepath}")
            
            if self.debug_mode:
                logger.debug(f"Response metadata: {json.dumps(debug_data['metadata'], indent=2)}")
                if 'metrics_summary' in debug_data:
                    logger.debug(f"Metrics summary: {json.dumps(debug_data['metrics_summary'], indent=2)}")
            
        except Exception as e:
            logger.error(f"Error saving raw response: {str(e)}")
            logger.error(traceback.format_exc())
