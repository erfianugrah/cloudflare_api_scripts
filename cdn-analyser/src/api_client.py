import requests
import json
import logging
from datetime import datetime, timedelta, UTC
from typing import Dict, List, Optional
from .types import ZoneMetrics

logger = logging.getLogger(__name__)

class CloudflareAPIClient:
    def __init__(self, config):
        self.config = config
    
    def get_zones(self) -> List[Dict]:
        """Fetch all zones from Cloudflare."""
        try:
            response = requests.get(
                f"{self.config.base_url}/zones",
                headers=self.config.headers,
                params={
                    'per_page': 50,
                    'account.id': self.config.account_id,
                    'status': 'active'
                }
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to fetch zones: {response.text}")
            
            data = response.json()
            
            with open(self.config.json_dir / 'zones.json', 'w') as f:
                json.dump(data, f, indent=2)
            
            return data['result']
            
        except Exception as e:
            logger.error(f"Error fetching zones: {str(e)}")
            raise

    def fetch_zone_metrics(
        self, 
        zone_id: str,
        query: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> ZoneMetrics:
        """Fetch metrics for a specific zone."""
        try:
            end = end_time or datetime.now(UTC)
            start = start_time or (end - timedelta(hours=24))
            
            variables = {
                "zoneTag": zone_id,
                "filter": {
                    "datetime_gt": start.isoformat(),
                    "datetime_leq": end.isoformat()
                }
            }
            
            response = requests.post(
                f"{self.config.base_url}/graphql",
                headers=self.config.headers,
                json={"query": query, "variables": variables}
            )
            
            if response.status_code != 200:
                raise Exception(f"API request failed: {response.status_code}")
            
            data = response.json()
            
            # Save raw response
            response_file = self.config.json_dir / f'metrics_{zone_id}_{end.strftime("%Y%m%d_%H%M%S")}.json'
            with open(response_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching metrics for zone {zone_id}: {str(e)}")
            raise
