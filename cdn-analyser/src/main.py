# main.py
import logging
import sys
import json
from typing import Optional, List, Dict
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import asyncio
from pathlib import Path

from .config import Config, setup_logging
from .api_client import CloudflareAPIClient
from .data_processor import DataProcessor
from .analyzer import Analyzer
from .visualizer import Visualizer
from .reporter import Reporter
from .ui import UserInterface

logger = logging.getLogger(__name__)

class CloudflareAnalytics:
    def __init__(self):
        self.config = Config()
        self.data_processor = DataProcessor()
        self.analyzer = Analyzer(self.config)
        self.visualizer = Visualizer(self.config)
        self.reporter = Reporter(self.config)
        self.ui = UserInterface()

    def run_analysis(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        sample_interval: Optional[int] = None
    ) -> None:
        """Run complete analysis."""
        try:
            analysis_start = datetime.now(timezone.utc)
            
            # Create async event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Run async API calls in the loop
            async def fetch_data():
                async with CloudflareAPIClient(self.config) as api_client:
                    # Fetch and select zones
                    zones = await api_client.get_zones()
                    selected_zones = self.ui.select_zones(zones)
                    
                    if not selected_zones:
                        logger.warning("No zones selected for analysis")
                        return None

                    results = []
                    for zone in selected_zones:
                        try:
                            zone_name = zone['name']
                            logger.info(f"Analyzing zone: {zone_name}")
                            
                            # Fetch metrics with sampling control
                            raw_data = await api_client.fetch_zone_metrics(
                                zone['id'],
                                start_time=start_time,
                                end_time=end_time,
                                sample_interval=sample_interval
                            )
                            
                            if not raw_data:
                                logger.error(f"Failed to fetch metrics for zone {zone_name}")
                                continue
                            
                            # Process data synchronously
                            df = self.data_processor.process_zone_metrics(raw_data)
                            if df is None or df.empty:
                                logger.warning(f"No data available for zone {zone_name}")
                                continue
                            
                            # Process analysis synchronously
                            cache_analysis = self.analyzer.analyze_cache(df, zone_name)
                            perf_analysis = self.analyzer.analyze_performance(df, zone_name)
                            
                            if not cache_analysis or not perf_analysis:
                                logger.warning(f"Analysis failed for zone {zone_name}")
                                continue
                            
                            # Generate visualizations synchronously
                            self.visualizer.create_visualizations(
                                df, cache_analysis, zone_name, 'cache'
                            )
                            self.visualizer.create_visualizations(
                                df, perf_analysis, zone_name, 'performance'
                            )
                            
                            results.append({
                                'zone_name': zone_name,
                                'cache_analysis': cache_analysis,
                                'perf_analysis': perf_analysis,
                                'sampling_metrics': self._get_sampling_metrics(df)
                            })
                            
                        except Exception as e:
                            logger.error(f"Error processing zone {zone_name}: {str(e)}")
                            continue
                    
                    return results

            # Run the async function in the loop
            results = loop.run_until_complete(fetch_data())
            loop.close()
            
            if not results:
                logger.error("No zones were successfully analyzed")
                return

            # Generate summary report synchronously
            self.reporter.generate_summary(results, analysis_start)
            logger.info("Analysis complete! Check the 'reports' directory for results.")
            
        except Exception as e:
            logger.error(f"Fatal error in analysis: {str(e)}")
            raise

    def _get_sampling_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate sampling metrics."""
        return {
            'avg_sampling_rate': float(df['sampling_rate'].mean()),
            'min_sampling_rate': float(df['sampling_rate'].min()),
            'max_sampling_rate': float(df['sampling_rate'].max()),
            'total_samples': int(df['visits'].sum()),
            'estimated_total': int(df['visits_adjusted'].sum()),
            'avg_confidence_score': float(df['confidence_score'].mean())
        }

    def _save_json(self, file_path: Path, data: Dict) -> None:
        """Helper method for saving JSON files."""
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)

def configure_analysis():
    """Configure analysis parameters with user input."""
    try:
        print("\nCloudflare Analytics Configuration")
        print("=================================")
        
        print("\nTime Range Options:")
        print("1. Last 24 hours (default)")
        print("2. Last 7 days")
        print("3. Last 30 days")
        print("4. Custom range")
        
        choice = input("\nSelect time range option [1-4]: ").strip() or "1"
        
        end_time = datetime.now(timezone.utc)
        if choice == "1":
            start_time = end_time - timedelta(hours=24)
        elif choice == "2":
            start_time = end_time - timedelta(days=7)
        elif choice == "3":
            start_time = end_time - timedelta(days=30)
        elif choice == "4":
            days = int(input("Enter number of days to analyze: "))
            start_time = end_time - timedelta(days=days)
        else:
            logger.warning("Invalid choice, using default (24 hours)")
            start_time = end_time - timedelta(hours=24)
        
        print("\nSampling Options:")
        print("1. Auto (Cloudflare optimized)")
        print("2. High precision (100% sampling)")
        print("3. Balanced (10% sampling)")
        print("4. Low precision (1% sampling)")
        print("5. Custom sampling rate")
        
        sampling_choice = input("\nSelect sampling option [1-5]: ").strip() or "1"
        
        if sampling_choice == "1":
            sample_interval = None
        elif sampling_choice == "2":
            sample_interval = 1
        elif sampling_choice == "3":
            sample_interval = 10
        elif sampling_choice == "4":
            sample_interval = 100
        elif sampling_choice == "5":
            rate = float(input("Enter sampling rate (1-100): "))
            sample_interval = int(100 / rate)
        else:
            logger.warning("Invalid choice, using auto sampling")
            sample_interval = None
        
        return start_time, end_time, sample_interval
        
    except Exception as e:
        logger.error(f"Error in configuration: {str(e)}")
        return datetime.now(timezone.utc) - timedelta(hours=24), datetime.now(timezone.utc), None

def main():
    """Main execution function."""
    setup_logging()
    
    try:
        analytics = CloudflareAnalytics()
        
        # Get analysis configuration
        start_time, end_time, sample_interval = configure_analysis()
        
        # Run analysis
        analytics.run_analysis(
            start_time=start_time,
            end_time=end_time,
            sample_interval=sample_interval
        )
        
    except KeyboardInterrupt:
        logger.warning("Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
