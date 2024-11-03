import os
import sys
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta, UTC
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Union
import logging
import json
from pathlib import Path
import traceback
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('cloudflare_analytics.log')
    ]
)
logger = logging.getLogger(__name__)

# Set matplotlib style
plt.style.use('bmh')
sns.set(style="darkgrid", palette="husl")

# Set default figure size
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9

class NumpyJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types."""
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                          np.int16, np.int32, np.int64, np.uint8,
                          np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        return super().default(obj)

def convert_to_serializable(obj):
    """Convert numpy/pandas types to JSON serializable Python types."""
    if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                      np.int16, np.int32, np.int64, np.uint8,
                      np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    elif isinstance(obj, dict):
        return {str(k): convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_serializable(item) for item in obj]
    return obj

class CloudflareAnalytics:
    def __init__(self):
        """Initialize the Cloudflare Analytics client."""
        # Load environment variables
        self.account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        self.api_key = os.getenv('CLOUDFLARE_API_KEY')
        self.email = os.getenv('CLOUDFLARE_EMAIL')
        self.zone_mapping = {}
        
        # Validate environment variables
        if not all([self.account_id, self.api_key, self.email]):
            raise ValueError("Missing required environment variables: CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_KEY, CLOUDFLARE_EMAIL")
        
        # Set up API headers
        self.headers = {
            'X-Auth-Key': self.api_key,
            'X-Auth-Email': self.email,
            'Content-Type': 'application/json'
        }
        
        self.base_url = "https://api.cloudflare.com/client/v4"
        
        # Set up directory structure
        self.setup_directories()

        # Cache status mapping
        self.CACHE_CATEGORIES = {
            'HIT': ['hit', 'stream_hit'],
            'MISS': ['miss', 'expired', 'updating', 'stale'],
            'BYPASS': ['bypass', 'ignored'],
            'REVALIDATED': ['revalidated'],
            'DYNAMIC': ['dynamic'],
            'DEFERRED': ['deferred'],
            'UNKNOWN': ['unknown']
        }

        logger.info("CloudflareAnalytics initialized successfully")

    def setup_directories(self):
        """Create necessary directories for output."""
        try:
            # Create reports directory
            self.reports_dir = Path('reports')
            self.reports_dir.mkdir(exist_ok=True)
            
            # Create subdirectories
            self.json_dir = self.reports_dir / 'json'
            self.images_dir = self.reports_dir / 'images'
            self.logs_dir = self.reports_dir / 'logs'
            
            for directory in [self.json_dir, self.images_dir, self.logs_dir]:
                directory.mkdir(exist_ok=True)
                
            logger.info("Directory structure created successfully")
        except Exception as e:
            logger.error(f"Failed to create directory structure: {str(e)}")
            raise

    def get_zones(self) -> List[Dict]:
        """Fetch all zones and build zone mapping."""
        try:
            response = requests.get(
                f"{self.base_url}/zones",
                headers=self.headers,
                params={
                    'per_page': 50,
                    'account.id': self.account_id,
                    'status': 'active'
                }
            )
            
            if response.status_code != 200:
                error_msg = f"Failed to fetch zones: {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            data = response.json()
            
            # Save zones data
            with open(self.json_dir / 'zones.json', 'w') as f:
                json.dump(data, f, indent=2)
            
            zones = data['result']
            self.zone_mapping = {zone['id']: zone['name'] for zone in zones}
            
            logger.info(f"Found {len(zones)} zones")
            return zones

        except requests.exceptions.RequestException as e:
            logger.error(f"Network error while fetching zones: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse zones response: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while fetching zones: {str(e)}")
            raise

    def select_zones(self, zones: List[Dict]) -> List[Dict]:
        """Allow user to select specific zones to analyze."""
        print("\nAvailable zones:")
        for idx, zone in enumerate(zones, 1):
            print(f"{idx}. {zone['name']} ({zone['id']})")
        
        while True:
            try:
                selection = input("\nEnter zone numbers to analyze (comma-separated, or 'all' for all zones): ").strip()
                if selection.lower() == 'all':
                    logger.info("Selected all zones for analysis")
                    return zones
                
                selected_indices = [int(i.strip()) for i in selection.split(',')]
                selected_zones = [zones[i-1] for i in selected_indices if 0 < i <= len(zones)]
                
                if selected_zones:
                    logger.info(f"Selected {len(selected_zones)} zones for analysis")
                    return selected_zones
                    
                print("No valid zones selected. Please try again.")
                
            except (ValueError, IndexError):
                print("Invalid input. Please enter numbers separated by commas or 'all'.")
            except Exception as e:
                logger.error(f"Error during zone selection: {str(e)}")
                print("An error occurred during selection. Please try again.")

    def fetch_zone_metrics(self, zone_id: str) -> Optional[pd.DataFrame]:
        """Fetch performance and cache metrics for a zone."""
        query = """
        query ZoneMetrics($zoneTag: string!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject!) {
          viewer {
            zones(filter: { zoneTag: $zoneTag }) {
              httpRequestsAdaptiveGroups(
                limit: 100,
                filter: $filter
              ) {
                dimensions {
                  datetime
                  clientCountryName
                  clientRequestHTTPHost
                  clientRequestPath
                  clientRequestHTTPProtocol
                  clientRequestHTTPMethodName
                  edgeResponseContentTypeName
                  edgeResponseStatus
                  cacheStatus
                  coloCode
                }
                avg {
                  edgeTimeToFirstByteMs
                  originResponseDurationMs
                  edgeDnsResponseTimeMs
                  sampleInterval
                }
                quantiles {
                  edgeDnsResponseTimeMsP50
                  edgeDnsResponseTimeMsP95
                  edgeDnsResponseTimeMsP99
                  edgeTimeToFirstByteMsP50
                  edgeTimeToFirstByteMsP95
                  edgeTimeToFirstByteMsP99
                  originResponseDurationMsP50
                  originResponseDurationMsP95
                  originResponseDurationMsP99
                }
                sum {
                  edgeResponseBytes
                  visits
                }
                ratio {
                  status4xx
                  status5xx
                }
                count
              }
            }
          }
        }
        """

        try:
            # Get last 24 hours
            end = datetime.now(UTC)
            start = end - timedelta(hours=24)

            variables = {
                "zoneTag": zone_id,
                "filter": {
                    "datetime_gt": start.isoformat(),
                    "datetime_leq": end.isoformat()
                }
            }

            logger.debug(f"Querying zone {zone_id} with variables: {json.dumps(variables, indent=2)}")

            # Make the GraphQL request
            response = requests.post(
                f"{self.base_url}/graphql",
                headers=self.headers,
                json={"query": query, "variables": variables}
            )

            # Save raw response
            response_file = self.json_dir / f'metrics_{zone_id}_{end.strftime("%Y%m%d_%H%M%S")}.json'
            try:
                response_data = response.json()
                with open(response_file, 'w') as f:
                    json.dump(response_data, f, indent=2)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode response for zone {zone_id}: {str(e)}")
                logger.error(f"Raw response: {response.text[:1000]}...")
                return None

            # Check for errors
            if response.status_code != 200:
                logger.error(f"API request failed for zone {zone_id}: Status {response.status_code}")
                return None

            if 'errors' in response_data and response_data['errors']:
                logger.error(f"GraphQL errors for zone {zone_id}: {json.dumps(response_data['errors'], indent=2)}")
                return None

            # Process the data
            viewer_data = response_data.get('data', {}).get('viewer', {})
            zones_data = viewer_data.get('zones', [])
            
            if not zones_data:
                logger.error(f"No zones data found for zone {zone_id}")
                return None
                
            requests_data = zones_data[0].get('httpRequestsAdaptiveGroups', [])
            
            if not requests_data:
                logger.warning(f"No HTTP requests data available for zone {zone_id}")
                return None

            metrics = []
            for group in requests_data:
                try:
                    # Get cache status and category
                    cache_status = group['dimensions'].get('cacheStatus', 'unknown').lower()
                    cache_category = 'UNKNOWN'
                    for category, statuses in self.CACHE_CATEGORIES.items():
                        if cache_status in statuses:
                            cache_category = category
                            break

                    # Base metrics
                    metric = {
                        'zone_id': zone_id,
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

                    # Performance metrics
                    avg_data = group.get('avg', {})
                    metric.update({
                        'ttfb_avg': avg_data.get('edgeTimeToFirstByteMs', 0),
                        'origin_time_avg': avg_data.get('originResponseDurationMs', 0),
                        'dns_time_avg': avg_data.get('edgeDnsResponseTimeMs', 0),
                        'sample_interval': avg_data.get('sampleInterval', 1),
                    })

                    # Percentile metrics
                    quantiles = group.get('quantiles', {})
                    metric.update({
                        'dns_time_p50': quantiles.get('edgeDnsResponseTimeMsP50', 0),
                        'dns_time_p95': quantiles.get('edgeDnsResponseTimeMsP95', 0),
                        'dns_time_p99': quantiles.get('edgeDnsResponseTimeMsP99', 0),
                        'ttfb_p50': quantiles.get('edgeTimeToFirstByteMsP50', 0),
                        'ttfb_p95': quantiles.get('edgeTimeToFirstByteMsP95', 0),
                        'ttfb_p99': quantiles.get('edgeTimeToFirstByteMsP99', 0),
                        'origin_time_p50': quantiles.get('originResponseDurationMsP50', 0),
                        'origin_time_p95': quantiles.get('originResponseDurationMsP95', 0),
                        'origin_time_p99': quantiles.get('originResponseDurationMsP99', 0),
                    })

                    # Traffic metrics
                    sum_data = group.get('sum', {})
                    metric.update({
                        'bytes': sum_data.get('edgeResponseBytes', 0),
                        'visits': sum_data.get('visits', 0),
                    })

                    # Error ratios
                    ratio_data = group.get('ratio', {})
                    metric.update({
                        'error_4xx_ratio': ratio_data.get('status4xx', 0),
                        'error_5xx_ratio': ratio_data.get('status5xx', 0),
                    })

                    metrics.append(metric)

                except Exception as e:
                    logger.warning(f"Error processing metric for zone {zone_id}: {str(e)}")
                    logger.debug(f"Problematic metric data: {json.dumps(group, indent=2)}")
                    continue

            if not metrics:
                logger.warning(f"No valid metrics collected for zone {zone_id}")
                return None

            df = pd.DataFrame(metrics)
            df['cache_hit_ratio'] = (df['cache_status'].isin(['hit', 'stale', 'revalidated'])).astype(float) * 100
            logger.info(f"Successfully collected {len(metrics)} data points for zone {zone_id}")
            return df

        except Exception as e:
            logger.error(f"Error processing zone {zone_id}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def analyze_cache_performance(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze cache performance metrics for a zone."""
        try:
            logger.info(f"Starting cache analysis for zone {zone_name}")
            
            # Debug column names
            logger.debug(f"Available columns: {df.columns.tolist()}")

            cache_analysis = {
                'zone_name': zone_name,
                'overall': {},
                'by_content_type': {},
                'by_path': {},
                'by_country': {},
                'by_status': {},
                'by_time': {},
                'cache_status_distribution': {}
            }

            # Calculate overall cache metrics
            df['hit'] = df['cache_status'].isin(['hit', 'stale', 'revalidated'])
            total_samples = len(df)
            cache_hits = df['hit'].sum()
            total_bytes = float(df['bytes'].sum())

            cache_analysis['overall'] = {
                'total_samples': int(total_samples),
                'total_bytes': float(total_bytes),
                'cache_hits': int(cache_hits),
                'hit_ratio': float((cache_hits / total_samples * 100) if total_samples > 0 else 0),
                'bandwidth_saved_gb': float(total_bytes / (1024 ** 3))
            }

            # Cache status distribution
            status_counts = df['cache_status'].value_counts()
            total_statuses = status_counts.sum()
            cache_analysis['cache_status_distribution'] = {
                str(status): {
                    'count': int(count),
                    'percentage': float((count / total_statuses * 100).round(2))
                } for status, count in status_counts.items()
            }

            # Group analyses
            for group_by, name in [
                ('content_type', 'by_content_type'),
                ('path', 'by_path'),
                ('country', 'by_country'),
                ('status', 'by_status')
            ]:
                grouped = df.groupby(group_by).agg({
                    'bytes': 'sum',
                    'hit': 'mean'
                }).round(2)
                grouped['samples'] = df.groupby(group_by).size()
                grouped['hit_ratio'] = grouped['hit'] * 100
                
                result_dict = {}
                for idx in grouped.index:
                    result_dict[str(idx)] = {
                        'bytes': float(grouped.loc[idx, 'bytes']),
                        'hit_ratio': float(grouped.loc[idx, 'hit_ratio']),
                        'samples': int(grouped.loc[idx, 'samples'])
                    }
                
                cache_analysis[name] = result_dict

            # Hourly analysis
            df['hour'] = pd.to_datetime(df['datetime']).dt.hour
            hourly = df.groupby('hour').agg({
                'bytes': 'sum',
                'hit': 'mean'
            }).round(2)
            hourly['samples'] = df.groupby('hour').size()
            hourly['hit_ratio'] = hourly['hit'] * 100

            cache_analysis['by_time']['hourly'] = {
                str(hour): {
                    'bytes': float(row['bytes']),
                    'hit_ratio': float(row['hit_ratio']),
                    'samples': int(row['samples'])
                } for hour, row in hourly.iterrows()
            }

            # Save analysis
            analysis_file = self.reports_dir / f"{zone_name}_cache_analysis.json"
            with open(analysis_file, 'w') as f:
                json.dump(cache_analysis, f, indent=2, cls=NumpyJSONEncoder)

            return cache_analysis

        except Exception as e:
            logger.error(f"Error in cache analysis for zone {zone_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def analyze_performance_metrics(self, df: pd.DataFrame, zone_name: str) -> Optional[Dict]:
        """Analyze performance metrics for a zone."""
        try:
            logger.info(f"Starting performance analysis for zone {zone_name}")
            
            # Debug column names
            logger.debug(f"Available columns: {df.columns.tolist()}")

            perf_analysis = {
                'zone_name': zone_name,
                'overall': {},
                'by_path': {},
                'by_content_type': {},
                'by_country': {},
                'by_colo': {},
                'percentiles': {},
                'trends': {}
            }

            # Overall metrics
            perf_analysis['overall'] = {
                'total_samples': int(len(df)),
                'avg_ttfb': float(df['ttfb_avg'].mean()),
                'avg_origin_time': float(df['origin_time_avg'].mean()),
                'avg_dns_time': float(df['dns_time_avg'].mean()),
                'p95_ttfb': float(df['ttfb_p95'].mean()),
                'p95_origin_time': float(df['origin_time_p95'].mean()),
                'total_bytes': float(df['bytes'].sum()),
                'error_rate': float((df['error_4xx_ratio'].mean() + df['error_5xx_ratio'].mean()) * 100)
            }

            # Group analyses
            for group_by, name in [
                ('path', 'by_path'),
                ('content_type', 'by_content_type'),
                ('country', 'by_country'),
                ('colo', 'by_colo')
            ]:
                grouped = df.groupby(group_by).agg({
                    'ttfb_avg': 'mean',
                    'origin_time_avg': 'mean',
                    'dns_time_avg': 'mean',
                    'error_4xx_ratio': 'mean',
                    'error_5xx_ratio': 'mean',
                    'bytes': 'sum'
                }).round(2)
                
                grouped['samples'] = df.groupby(group_by).size()
                grouped['total_error_ratio'] = (grouped['error_4xx_ratio'] + grouped['error_5xx_ratio']) * 100
                
                result_dict = {}
                for idx in grouped.index:
                    result_dict[str(idx)] = {k: float(v) if isinstance(v, (np.floating, np.integer)) else v 
                                           for k, v in grouped.loc[idx].to_dict().items()}
                
                perf_analysis[name] = result_dict

            # Percentile metrics
            perf_analysis['percentiles'] = {
                'ttfb': {
                    'p50': float(df['ttfb_p50'].mean()),
                    'p95': float(df['ttfb_p95'].mean()),
                    'p99': float(df['ttfb_p99'].mean())
                },
                'origin_time': {
                    'p50': float(df['origin_time_p50'].mean()),
                    'p95': float(df['origin_time_p95'].mean()),
                    'p99': float(df['origin_time_p99'].mean())
                }
            }

            # Time-based trends
            df['hour'] = pd.to_datetime(df['datetime']).dt.hour
            hourly = df.groupby('hour').agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'error_4xx_ratio': 'mean',
                'error_5xx_ratio': 'mean',
                'bytes': 'sum'
            }).round(2)
            
            hourly['samples'] = df.groupby('hour').size()
            
            perf_analysis['trends']['hourly'] = {
                str(hour): {k: float(v) if isinstance(v, (np.floating, np.integer)) else v 
                           for k, v in row.to_dict().items()}
                for hour, row in hourly.iterrows()
            }

            # Save analysis
            analysis_file = self.reports_dir / f"{zone_name}_performance_analysis.json"
            with open(analysis_file, 'w') as f:
                json.dump(perf_analysis, f, indent=2, cls=NumpyJSONEncoder)

            return perf_analysis

        except Exception as e:
            logger.error(f"Error analyzing performance metrics for zone {zone_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def create_cache_visualizations(self, df: pd.DataFrame, cache_analysis: Dict, zone_name: str) -> None:
        """Create cache performance visualizations."""
        try:
            output_dir = self.images_dir / zone_name / 'cache'
            output_dir.mkdir(parents=True, exist_ok=True)

            # Prepare cache status data
            status_data = pd.Series({
                k: v['percentage'] 
                for k, v in cache_analysis['cache_status_distribution'].items()
            }).sort_values(ascending=True)

            # 1. Cache Status Distribution
            plt.figure(figsize=(12, 6))
            status_data.plot(kind='barh', color='skyblue')
            plt.title('Cache Status Distribution')
            plt.xlabel('Percentage (%)')
            plt.ylabel('Cache Status')
            plt.tight_layout()
            plt.savefig(output_dir / 'cache_status_distribution.png')
            plt.close()

            # 2. Hourly Cache Performance
            plt.figure(figsize=(12, 6))
            hourly_data = pd.DataFrame.from_dict(
                cache_analysis['by_time']['hourly'], 
                orient='index'
            )
            
            plt.plot(
                hourly_data.index, 
                hourly_data['hit_ratio'],
                marker='o',
                linestyle='-',
                color='purple',
                linewidth=2
            )
            plt.title('Cache Hit Ratio by Hour')
            plt.xlabel('Hour of Day')
            plt.ylabel('Hit Ratio (%)')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_dir / 'hourly_cache.png')
            plt.close()

            # 3. Content Type Performance
            plt.figure(figsize=(12, 8))
            content_data = pd.DataFrame.from_dict(
                cache_analysis['by_content_type'], 
                orient='index'
            )
            content_data_sorted = content_data.sort_values('hit_ratio').tail(10)
            
            content_data_sorted['hit_ratio'].plot(
                kind='barh',
                color='lightgreen'
            )
            plt.title('Cache Hit Ratio by Content Type (Top 10)')
            plt.xlabel('Hit Ratio (%)')
            plt.ylabel('Content Type')
            plt.tight_layout()
            plt.savefig(output_dir / 'content_type_cache.png')
            plt.close()

            # 4. Path Performance
            plt.figure(figsize=(12, 8))
            path_data = pd.DataFrame.from_dict(
                cache_analysis['by_path'],
                orient='index'
            )
            path_data_sorted = path_data.sort_values('samples', ascending=False).head(10)
            
            path_data_sorted['hit_ratio'].plot(
                kind='barh',
                color='lightcoral'
            )
            plt.title('Cache Hit Ratio by Path (Top 10 by Volume)')
            plt.xlabel('Hit Ratio (%)')
            plt.ylabel('Path')
            plt.tight_layout()
            plt.savefig(output_dir / 'path_cache.png')
            plt.close()

            # 5. Bandwidth Usage
            plt.figure(figsize=(12, 6))
            bytes_per_hour = hourly_data['bytes'] / (1024 * 1024)  # Convert to MB
            plt.bar(
                hourly_data.index,
                bytes_per_hour,
                color='teal',
                alpha=0.7
            )
            plt.title('Bandwidth Usage by Hour')
            plt.xlabel('Hour of Day')
            plt.ylabel('Bandwidth (MB)')
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_dir / 'bandwidth_usage.png')
            plt.close()

            logger.info(f"Created cache visualizations for zone {zone_name}")

        except Exception as e:
            logger.error(f"Error creating cache visualizations for zone {zone_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    def create_performance_visualizations(self, df: pd.DataFrame, perf_analysis: Dict, zone_name: str) -> None:
        """Create performance metric visualizations."""
        try:
            output_dir = self.images_dir / zone_name / 'performance'
            output_dir.mkdir(parents=True, exist_ok=True)

            # 1. Response Time Distribution
            plt.figure(figsize=(12, 10))
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            # TTFB Distribution
            sns.histplot(data=df, x='ttfb_avg', bins=50, ax=ax1, color='blue', alpha=0.6)
            ax1.set_title('TTFB Distribution')
            ax1.set_xlabel('Time to First Byte (ms)')
            ax1.set_ylabel('Count')
            
            # Origin Time Distribution
            sns.histplot(data=df, x='origin_time_avg', bins=50, ax=ax2, color='green', alpha=0.6)
            ax2.set_title('Origin Response Time Distribution')
            ax2.set_xlabel('Response Time (ms)')
            ax2.set_ylabel('Count')
            
            plt.tight_layout()
            plt.savefig(output_dir / 'response_time_distribution.png')
            plt.close()

            # 2. Performance Trends
            plt.figure(figsize=(12, 6))
            hourly_data = pd.DataFrame.from_dict(
                perf_analysis['trends']['hourly'],
                orient='index'
            )
            
            plt.plot(
                hourly_data.index,
                hourly_data['ttfb_avg'],
                marker='o',
                label='TTFB',
                color='blue'
            )
            plt.plot(
                hourly_data.index,
                hourly_data['origin_time_avg'],
                marker='o',
                label='Origin Time',
                color='green'
            )
            plt.title('Performance Trends Over Time')
            plt.xlabel('Hour of Day')
            plt.ylabel('Response Time (ms)')
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_dir / 'performance_trends.png')
            plt.close()

            # 3. Geographic Performance
            plt.figure(figsize=(12, 6))
            country_data = pd.DataFrame.from_dict(
                perf_analysis['by_country'],
                orient='index'
            )
            country_data_sorted = country_data.sort_values('ttfb_avg').tail(10)
            
            country_data_sorted['ttfb_avg'].plot(
                kind='barh',
                color='lightblue'
            )
            plt.title('TTFB by Country (Top 10)')
            plt.xlabel('Average TTFB (ms)')
            plt.ylabel('Country')
            plt.tight_layout()
            plt.savefig(output_dir / 'geographic_performance.png')
            plt.close()

            # 4. Status Code Distribution
            plt.figure(figsize=(10, 10))
            status_counts = df['status'].value_counts()
            plt.pie(
                status_counts.values,
                labels=status_counts.index,
                autopct='%1.1f%%',
                colors=sns.color_palette('husl', n_colors=len(status_counts))
            )
            plt.title('HTTP Status Code Distribution')
            plt.axis('equal')
            plt.savefig(output_dir / 'status_distribution.png')
            plt.close()

            # 5. Error Rate Trends
            plt.figure(figsize=(12, 6))
            plt.plot(
                hourly_data.index,
                hourly_data['error_4xx_ratio'] * 100,
                'r-',
                marker='o',
                label='4xx Errors'
            )
            plt.plot(
                hourly_data.index,
                hourly_data['error_5xx_ratio'] * 100,
                'b-',
                marker='o',
                label='5xx Errors'
            )
            plt.title('Error Rate Trends')
            plt.xlabel('Hour')
            plt.ylabel('Error Rate (%)')
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_dir / 'error_trends.png')
            plt.close()

            logger.info(f"Created performance visualizations for zone {zone_name}")

        except Exception as e:
            logger.error(f"Error creating performance visualizations for zone {zone_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    def generate_report(self, zone_name: str, cache_analysis: Dict, perf_analysis: Dict) -> str:
        """Generate comprehensive analysis report."""
        try:
            # Convert values for report formatting
            total_samples = int(cache_analysis['overall']['total_samples'])
            hit_ratio = float(cache_analysis['overall']['hit_ratio'])
            bandwidth_gb = float(cache_analysis['overall']['bandwidth_saved_gb'])

            report = f"""
            Cloudflare Analytics Report for {zone_name}
            {'=' * (24 + len(zone_name))}

            Generated at: {datetime.now(UTC).isoformat()}

            Cache Performance Summary
            -----------------------
            Overall Cache Hit Ratio: {hit_ratio:.2f}%
            Total Samples: {total_samples:,}
            Total Bandwidth: {bandwidth_gb:.2f} GB

            Cache Status Distribution:
            {'-' * 25}
            """ + "\n".join([
                f"{status}: {data['percentage']:.2f}% ({int(data['count']):,} samples)"
                for status, data in cache_analysis['cache_status_distribution'].items()
            ]) + f"""

            Performance Metrics Summary
            -------------------------
            Average TTFB: {float(perf_analysis['overall']['avg_ttfb']):.2f} ms
            P95 TTFB: {float(perf_analysis['percentiles']['ttfb']['p95']):.2f} ms
            Average Origin Response Time: {float(perf_analysis['overall']['avg_origin_time']):.2f} ms
            P95 Origin Response Time: {float(perf_analysis['percentiles']['origin_time']['p95']):.2f} ms
            Error Rate: {float(perf_analysis['overall']['error_rate']):.2f}%

            Top 5 Paths by Sample Volume:
            {'-' * 40}
            """

            # Add top paths
            for path, data in sorted(
                perf_analysis['by_path'].items(), 
                key=lambda x: x[1]['samples'], 
                reverse=True
            )[:5]:
                report += f"\nPath: {path}\n"
                report += f"  Samples: {int(data['samples']):,}\n"
                report += f"  Avg TTFB: {float(data['ttfb_avg']):.2f} ms\n"
                report += f"  Error Rate: {float(data['total_error_ratio']):.2f}%\n"

            # Save report
            report_file = self.reports_dir / f"{zone_name}_report.txt"
            with open(report_file, 'w') as f:
                f.write(report)

            logger.info(f"Generated report for zone {zone_name}")
            return report

        except Exception as e:
            logger.error(f"Error generating report for zone {zone_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return f"Error generating report for {zone_name}: {str(e)}"

    def analyze_zone(self, zone: Dict) -> None:
        """Analyze a single zone with all metrics."""
        try:
            zone_name = zone['name']
            zone_id = zone['id']
            logger.info(f"Starting analysis for zone: {zone_name}")

            # Fetch metrics
            df = self.fetch_zone_metrics(zone_id)
            if df is None or df.empty:
                logger.error(f"No data available for zone {zone_name}")
                return

            # Save raw data
            df.to_csv(self.reports_dir / f"{zone_name}_raw_data.csv", index=False)
            logger.info(f"Saved raw data for zone {zone_name}")

            # Perform cache analysis
            cache_analysis = self.analyze_cache_performance(df, zone_name)
            if cache_analysis:
                # Create cache visualizations
                self.create_cache_visualizations(df, cache_analysis, zone_name)
            else:
                logger.error(f"Cache analysis failed for zone {zone_name}")

            # Perform performance analysis
            perf_analysis = self.analyze_performance_metrics(df, zone_name)
            if perf_analysis:
                # Create performance visualizations
                self.create_performance_visualizations(df, perf_analysis, zone_name)
            else:
                logger.error(f"Performance analysis failed for zone {zone_name}")

            # Generate comprehensive report
            if cache_analysis and perf_analysis:
                self.generate_report(zone_name, cache_analysis, perf_analysis)
                logger.info(f"Completed analysis for zone {zone_name}")
            else:
                logger.error(f"Could not generate report for zone {zone_name}")

        except Exception as e:
            logger.error(f"Error analyzing zone {zone_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    def analyze_selected_zones(self, selected_zones: List[Dict]) -> None:
        """Analyze all selected zones and generate summary."""
        try:
            start_time = datetime.now()
            logger.info(f"Starting analysis for {len(selected_zones)} zones at {start_time}")

            # Process each zone
            for zone in selected_zones:
                self.analyze_zone(zone)

            # Generate overall summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            summary = f"""
            Cloudflare Analytics Summary
            ==========================

            Analysis completed at: {end_time}
            Total duration: {duration}
            Zones analyzed: {len(selected_zones)}

            Zone List:
            ---------
            """ + "\n".join([f"- {zone['name']}" for zone in selected_zones])

            with open(self.reports_dir / "analysis_summary.txt", 'w') as f:
                f.write(summary)

            logger.info("Analysis complete! Check the 'reports' directory for results.")

        except Exception as e:
            logger.error(f"Error in analyze_selected_zones: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

def main():
    """Main execution function."""
    try:
        # Initialize analytics
        analytics = CloudflareAnalytics()
        
        # Get all zones
        logger.info("Fetching zones...")
        zones = analytics.get_zones()
        
        # Select zones to analyze
        selected_zones = analytics.select_zones(zones)
        logger.info(f"Selected {len(selected_zones)} zones for analysis")
        
        # Analyze selected zones
        analytics.analyze_selected_zones(selected_zones)
        
    except KeyboardInterrupt:
        logger.warning("Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
