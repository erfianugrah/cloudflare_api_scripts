# api_client.py
import aiohttp
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Union
import traceback

from .graphql_queries import ZONE_METRICS_QUERY

logger = logging.getLogger(__name__)

class CloudflareAPIClient:
    def __init__(self, config):
        """Initialize the Cloudflare API client with configuration."""
        self.config = config
        self.base_url = "https://api.cloudflare.com/client/v4"
        self.headers = {
            'X-Auth-Email': config.email,
            'X-Auth-Key': config.api_key,
            'Content-Type': 'application/json'
        }
        self.session = None

    async def fetch_zone_metrics(
        self,
        zone_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        sample_interval: Optional[int] = None
    ) -> Optional[Dict]:
        """Fetch zone metrics asynchronously with sampling interval control."""
        try:
            end = end_time or datetime.now(timezone.utc)
            start = start_time or (end - timedelta(hours=24))
            
            variables = {
                "zoneTag": zone_id,
                "filter": {
                    "datetime_geq": start.isoformat(),
                    "datetime_leq": end.isoformat()
                }
            }
            
            if sample_interval is not None:
                variables["filter"]["sampleInterval"] = sample_interval

            # Debug logging
            logger.debug(f"Fetching metrics for zone {zone_id}")
            logger.debug(f"Time range: {start.isoformat()} to {end.isoformat()}")
            logger.debug(f"Sample interval: {sample_interval}")
            logger.debug(f"Variables: {json.dumps(variables, indent=2)}")

            async with self.session.post(
                f"{self.base_url}/graphql",
                headers=self.headers,
                json={
                    "query": ZONE_METRICS_QUERY,
                    "variables": variables
                }
            ) as response:
                # Log raw response for debugging
                response_text = await response.text()
                logger.debug(f"Raw GraphQL response: {response_text}")

                if response.status != 200:
                    logger.error(f"GraphQL request failed with status {response.status}: {response_text}")
                    return None

                try:
                    data = json.loads(response_text)
                except json.JSONDecodeError:
                    logger.error("Failed to parse JSON response")
                    return None

                # Check for GraphQL errors
                if data.get('errors'):
                    errors = data.get('errors', [])
                    error_messages = [error.get('message', 'Unknown error') for error in errors]
                    logger.error(f"GraphQL errors: {error_messages}")
                    return None

                # Validate response structure
                if not data.get('data'):
                    logger.error("No data in GraphQL response")
                    return None

                viewer = data.get('data', {}).get('viewer')
                if not viewer:
                    logger.error("No viewer data in GraphQL response")
                    return None

                zones = viewer.get('zones', [])
                if not zones:
                    logger.error("No zones data in GraphQL response")
                    return None

                zone_data = zones[0]
                if not zone_data:
                    logger.error("Empty zone data in GraphQL response")
                    return None

                metrics = zone_data.get('httpRequestsAdaptiveGroups', [])
                if not metrics:
                    logger.error("No metrics data in GraphQL response")
                    return None

                # Save raw response for debugging
                await self._save_raw_response(data, zone_id, sample_interval, start, end)

                return {
                    'data': {
                        'viewer': {
                            'zones': [{
                                'httpRequestsAdaptiveGroups': metrics
                            }]
                        }
                    }
                }

        except aiohttp.ClientError as e:
            logger.error(f"HTTP request error: {str(e)}")
            return None
        except asyncio.TimeoutError:
            logger.error("Request timed out")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching metrics: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def get_zones(self) -> List[Dict]:
        """Fetch all zones accessible to the account asynchronously."""
        try:
            async with self.session.get(
                f"{self.base_url}/zones",
                headers=self.headers
            ) as response:
                response_text = await response.text()
                
                if response.status != 200:
                    logger.error(f"Failed to fetch zones: {response.status} - {response_text}")
                    return []

                try:
                    data = json.loads(response_text)
                except json.JSONDecodeError:
                    logger.error("Failed to parse zones response")
                    return []

                if not data.get('success'):
                    logger.error(f"API request unsuccessful: {data.get('errors', [])}")
                    return []

                zones = data.get('result', [])
                if not zones:
                    logger.warning("No zones found in account")
                    return []

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

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=60)  # Increased timeout
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _save_raw_response(
        self,
        data: Dict,
        zone_id: str,
        sample_interval: Optional[int],
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """Save raw response for debugging."""
        try:
            debug_data = {
                "metadata": {
                    "zone_id": zone_id,
                    "sample_interval": sample_interval,
                    "time_range": {
                        "start": start_time.isoformat(),
                        "end": end_time.isoformat()
                    },
                    "timestamp": datetime.now(timezone.utc).isoformat()
                },
                "response": data
            }

            filepath = self.config.json_dir / f"debug_response_{zone_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(filepath, 'w') as f:
                json.dump(debug_data, f, indent=2)
            
        except Exception as e:
            logger.error(f"Error saving debug data: {str(e)}")
