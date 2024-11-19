# src/dash_main.py
import logging
import sys
import argparse
from pathlib import Path
from typing import Optional
import json
import pandas as pd

from .config import Config, setup_logging
from .visualizer import Visualizer

logger = logging.getLogger(__name__)

def load_previous_analysis(json_dir: Path, zone_name: Optional[str] = None):
    """Load most recent analysis results and raw data."""
    try:
        # Find analysis files
        if zone_name:
            analysis_files = list(json_dir.glob(f"{zone_name}_analysis_*.json"))
        else:
            analysis_files = list(json_dir.glob("*_analysis_*.json"))
            
        if not analysis_files:
            raise FileNotFoundError(
                f"No analysis files found for {zone_name if zone_name else 'any zone'}"
            )
            
        # Sort by modification time to get most recent
        analysis_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        results = {}
        
        for analysis_file in analysis_files:
            try:
                # Extract zone name from filename
                zone = analysis_file.stem.split('_analysis_')[0]
                
                # Load analysis results
                with open(analysis_file) as f:
                    analysis = json.load(f)
                
                # Look for corresponding raw data
                raw_files = list(json_dir.glob(f"raw_response_{zone}_*.json"))
                if raw_files:
                    # Get most recent raw data file
                    raw_file = sorted(raw_files, key=lambda x: x.stat().st_mtime)[-1]
                    with open(raw_file) as f:
                        raw_data = json.load(f)
                        
                    results[zone] = {
                        'analysis': analysis,
                        'raw_data': raw_data['response'] if 'response' in raw_data else raw_data
                    }
                else:
                    logger.warning(f"No raw data found for zone {zone}, using analysis results only")
                    results[zone] = {
                        'analysis': analysis,
                        'raw_data': None
                    }
                    
            except Exception as e:
                logger.error(f"Error loading data for {analysis_file}: {str(e)}")
                continue
                
        return results
        
    except Exception as e:
        logger.error(f"Error loading previous analysis: {str(e)}")
        return None

def process_raw_data(raw_data: dict) -> Optional[pd.DataFrame]:
    """Process raw data into DataFrame if available."""
    if raw_data:
        try:
            from .data_processor import DataProcessor
            processor = DataProcessor()
            return processor.process_zone_metrics(raw_data)
        except Exception as e:
            logger.error(f"Error processing raw data: {str(e)}")
    return None

def main():
    """Main function for running the dashboard from previous analysis."""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='Cloudflare Analytics Dashboard')
    parser.add_argument('--zone', help='Specific zone to display dashboard for')
    parser.add_argument('--port', type=int, help='Port to run dashboard on (default: 8050)')
    args = parser.parse_args()
    
    try:
        config = Config()
        visualizer = Visualizer(config)
        
        # Load previous analysis results
        results = load_previous_analysis(config.json_dir, args.zone)
        
        if not results:
            logger.error("No previous analysis results found")
            sys.exit(1)
            
        # Process and visualize each zone
        for zone_name, data in results.items():
            try:
                logger.info(f"Processing zone: {zone_name}")
                
                # Process raw data if available
                df = process_raw_data(data['raw_data'])
                
                # Calculate port if not specified
                port = args.port or (8050 + abs(hash(zone_name)) % 1000)
                
                logger.info(f"Starting dashboard for {zone_name} on port {port}")
                visualizer.create_visualizations(
                    df=df,
                    analysis=data['analysis'],
                    zone_name=zone_name
                )
                
            except Exception as e:
                logger.error(f"Error processing zone {zone_name}: {str(e)}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Dashboard interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error running dashboard: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
