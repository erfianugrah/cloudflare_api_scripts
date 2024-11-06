# main.py
import logging
import sys
import json
from typing import Optional, List, Dict
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
import concurrent.futures
from threading import Lock
from queue import Queue

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
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.process_pool = concurrent.futures.ProcessPoolExecutor()
        self.lock = Lock()
        self.results_queue = Queue()

    def run_analysis(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        sample_interval: Optional[int] = None
    ) -> None:
        """Run complete analysis with multi-threading."""
        try:
            analysis_start = datetime.now(timezone.utc)
            logger.info(f"Starting analysis at {analysis_start}")
            
            api_client = CloudflareAPIClient(self.config)
            
            # Fetch and select zones
            zones = api_client.get_zones()
            if not zones:
                logger.error("No zones available for analysis")
                return None
                
            selected_zones = self.ui.select_zones(zones)
            if not selected_zones:
                logger.warning("No zones selected for analysis")
                return None

            # Process zones in parallel
            futures = []
            for zone in selected_zones:
                future = self.thread_pool.submit(
                    self.process_zone,
                    api_client,
                    zone,
                    start_time,
                    end_time,
                    sample_interval
                )
                futures.append((zone['name'], future))

            # Collect results with progress tracking
            results = []
            total_zones = len(futures)
            completed_zones = 0
            
            for zone_name, future in futures:
                try:
                    self.ui.show_analysis_progress(
                        zone_name,
                        "Processing",
                        completed_zones / total_zones,
                        0,  # Will be updated with actual sampling rate
                        0   # Will be updated with actual confidence score
                    )
                    
                    result = future.result()
                    if result:
                        results.append(result)
                        # Update progress with actual metrics
                        sampling_rate = result['sampling_metrics']['sampling_rates']['mean']
                        confidence = result['sampling_metrics']['confidence_scores']['mean']
                        self.ui.show_analysis_progress(
                            zone_name,
                            "Complete",
                            1.0,
                            sampling_rate,
                            confidence
                        )
                    else:
                        logger.warning(f"Analysis failed for zone: {zone_name}")
                        
                except Exception as e:
                    logger.error(f"Error processing zone {zone_name}: {str(e)}")
                
                completed_zones += 1

            if not results:
                logger.error("No zones were successfully analyzed")
                return

            # Generate final report
            summary = self.reporter.generate_summary(results, analysis_start)
            if summary:
                logger.info("Analysis summary generated successfully")
                print("\nAnalysis Summary:")
                print("================")
                print(summary)
            
            # Show completion summary
            self.ui.show_completion_summary(
                analysis_start,
                len(results),
                sum(r['cache_analysis']['overall']['total_requests'] for r in results),
                np.mean([r['sampling_metrics']['sampling_rates']['mean'] for r in results]),
                np.mean([r['sampling_metrics']['confidence_scores']['mean'] for r in results])
            )
            
        except Exception as e:
            logger.error(f"Fatal error in analysis: {str(e)}")
            raise
        finally:
            self.cleanup()

    def process_zone(
        self,
        api_client: CloudflareAPIClient,
        zone: Dict,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        sample_interval: Optional[int]
    ) -> Optional[Dict]:
        """Process a single zone with thread safety."""
        try:
            zone_name = zone['name']
            with self.lock:
                logger.info(f"Processing zone: {zone_name}")
            
            # Fetch metrics
            raw_data = api_client.fetch_zone_metrics(
                zone['id'],
                start_time=start_time,
                end_time=end_time,
                sample_interval=sample_interval
            )
            
            if not raw_data:
                logger.error(f"Failed to fetch metrics for zone {zone_name}")
                return None

            # Process data
            df = self.data_processor.process_zone_metrics(raw_data)
            
            if df is None or df.empty:
                logger.warning(f"No data available for zone {zone_name}")
                return None
            
            # Log DataFrame information
            logger.debug(f"Processed DataFrame shape: {df.shape}")
            logger.debug(f"Processed DataFrame columns: {df.columns.tolist()}")
            
            # Run analysis
            analysis_result = self.analyzer.analyze_metrics(df, zone_name)
            
            if not analysis_result:
                logger.warning(f"Analysis failed for zone {zone_name}")
                return None
            
            # Store the DataFrame for origin analysis
            analysis_result['raw_data'] = df
            
            # Generate visualizations if analysis was successful
            try:
                self.visualizer.create_visualizations(
                    df=df,
                    analysis=analysis_result,
                    zone_name=zone_name
                )
            except Exception as viz_error:
                logger.error(f"Error creating visualizations: {str(viz_error)}")
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Error processing zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def cleanup(self):
        """Cleanup resources."""
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
        if self.process_pool:
            self.process_pool.shutdown(wait=True)

def main():
    """Main execution function."""
    setup_logging()
    
    try:
        analytics = CloudflareAnalytics()
        
        start_time, end_time, sample_interval = configure_analysis()
        
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

def configure_analysis() -> tuple:
    """Configure analysis parameters with user input."""
    try:
        print("\nCloudflare Analytics Configuration")
        print("=================================")
        
        # Time range configuration
        print("\nTime Range Options:")
        print("1. Last 3 hours")
        print("2. Last 24 hours")
        print("3. Last 7 days")
        print("4. Last 30 days")
        print("5. Custom range")
        
        choice = input("\nSelect time range option [1-5]: ").strip() or "2"
        
        end_time = datetime.now(timezone.utc)
        if choice == "1":
            start_time = end_time - timedelta(hours=3)
        elif choice == "2":
            start_time = end_time - timedelta(hours=24)
        elif choice == "3":
            start_time = end_time - timedelta(days=7)
        elif choice == "4":
            start_time = end_time - timedelta(days=30)
        elif choice == "5":
            days = int(input("Enter number of days to analyze: "))
            start_time = end_time - timedelta(days=days)
        else:
            logger.warning("Invalid choice, using default (24 hours)")
            start_time = end_time - timedelta(hours=24)
        
        # Sampling configuration
        print("\nSampling Options:")
        print("1. Auto (Cloudflare optimized)")
        print("2. High precision (100% sampling)")
        print("3. Balanced (10% sampling)")
        print("4. Low precision (1% sampling)")
        print("5. Custom sampling rate")
        
        sampling_choice = input("\nSelect sampling option [1-5]: ").strip() or "1"
        
        # Store the sampling description along with the interval
        if sampling_choice == "1":
            sample_interval = None
            sampling_desc = "Auto (Cloudflare optimized)"
        elif sampling_choice == "2":
            sample_interval = 1
            sampling_desc = "High precision (100% sampling)"
        elif sampling_choice == "3":
            sample_interval = 10
            sampling_desc = "Balanced (10% sampling)"
        elif sampling_choice == "4":
            sample_interval = 100
            sampling_desc = "Low precision (1% sampling)"
        elif sampling_choice == "5":
            rate = float(input("Enter sampling rate (1-100): "))
            sample_interval = int(100 / rate)
            sampling_desc = f"Custom ({rate:.1f}% sampling)"
        else:
            logger.warning("Invalid choice, using auto sampling")
            sample_interval = None
            sampling_desc = "Auto (Cloudflare optimized)"

        # Show configuration summary
        print("\nAnalysis Configuration:")
        print("======================")
        print(f"Start Time: {start_time.isoformat()}")
        print(f"End Time: {end_time.isoformat()}")
        print(f"Duration: {end_time - start_time}")
        print(f"Sampling: {sampling_desc}")
        
        # Confirm configuration
        confirm = input("\nProceed with analysis? (y/n): ").strip().lower()
        if confirm != 'y':
            logger.info("Analysis cancelled by user")
            sys.exit(0)
        
        return start_time, end_time, sample_interval
        
    except Exception as e:
        logger.error(f"Error in configuration: {str(e)}")
        # Return safe defaults
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)
        return start_time, end_time, None

if __name__ == "__main__":
    main()
