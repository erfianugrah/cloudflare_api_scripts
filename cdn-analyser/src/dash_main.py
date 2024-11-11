from .config import Config
from .api_client import CloudflareAPIClient
from .dash_app import DashAnalytics
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize configuration
        config = Config()
        
        # Initialize API client
        api_client = CloudflareAPIClient(config)
        
        # Get initial data
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)  # Default to 7 days of data
        
        # Get zones
        zones = api_client.get_zones()
        if not zones:
            logger.error("No zones available")
            return
            
        # Get data for first zone
        zone = zones[0]
        raw_data = api_client.fetch_zone_metrics(
            zone['id'],
            start_time=start_time,
            end_time=end_time
        )
        
        # Initialize dash app
        dash_app = DashAnalytics(config)
        
        # Update data
        if raw_data:
            df = dash_app.data_processor.process_zone_metrics(raw_data)
            dash_app.update_data(df)
        
        # Run server
        dash_app.run_server(debug=True, port=8050)
        
    except Exception as e:
        logger.error(f"Error running Dash application: {str(e)}")

if __name__ == '__main__':
    main()
