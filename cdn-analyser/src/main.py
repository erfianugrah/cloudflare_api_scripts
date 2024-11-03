from datetime import datetime, timezone
import sys
import logging

from src.config import Config, setup_logging, setup_plotting
from src.api_client import CloudflareAPIClient
from src.data_processor import DataProcessor
from src.analyzer import Analyzer
from src.visualizer import Visualizer
from src.reporter import Reporter
from src.ui import UserInterface
from src.graphql_queries import ZONE_METRICS_QUERY

def main():
    """Main execution function."""
    try:
        # Setup
        start_time = datetime.now(timezone.utc)  # Make start_time timezone-aware
        logger = setup_logging()
        setup_plotting()
        config = Config()
        
        # Initialize components
        api_client = CloudflareAPIClient(config)
        data_processor = DataProcessor()
        analyzer = Analyzer(config)
        visualizer = Visualizer(config)
        reporter = Reporter(config)
        ui = UserInterface()
        
        # Get and select zones
        logger.info("Fetching zones...")
        zones = api_client.get_zones()
        selected_zones = ui.select_zones(zones)
        
        # Process each zone
        for zone in selected_zones:
            try:
                zone_name = zone['name']
                logger.info(f"Processing zone: {zone_name}")
                
                # Fetch metrics
                raw_data = api_client.fetch_zone_metrics(zone['id'], ZONE_METRICS_QUERY)
                
                # Process data
                df = data_processor.process_zone_metrics(raw_data)
                if df is None:
                    logger.warning(f"No data available for zone {zone_name}")
                    continue
                
                # Save raw data
                df.to_csv(config.reports_dir / f"{zone_name}_raw_data.csv", index=False)
                
                # Perform analysis
                cache_analysis = analyzer.analyze_cache(df, zone_name)
                perf_analysis = analyzer.analyze_performance(df, zone_name)
                
                # Create visualizations
                if cache_analysis:
                    visualizer.create_visualizations(df, cache_analysis, zone_name, 'cache')
                if perf_analysis:
                    visualizer.create_visualizations(df, perf_analysis, zone_name, 'performance')
                
                # Generate report
                if cache_analysis and perf_analysis:
                    reporter.generate_report(zone_name, cache_analysis, perf_analysis)
                
            except Exception as e:
                logger.error(f"Error processing zone {zone_name}: {str(e)}")
                continue
        
        # Generate final summary
        summary = reporter.generate_summary(selected_zones, start_time)
        logger.info("Analysis complete! Check the 'reports' directory for results.")
        
    except KeyboardInterrupt:
        logger.warning("Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
