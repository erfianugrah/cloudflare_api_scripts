import logging
import traceback
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .dashboards import (
    create_origin_response_time_dashboard,
    create_origin_error_dashboard,
    create_origin_geographic_dashboard,
    create_origin_endpoint_dashboard,
    create_origin_asn_dashboard
)

logger = logging.getLogger(__name__)

class OriginVisualizer:
    """Visualizer for origin server performance metrics."""
    
    def __init__(self, config):
        self.config = config
        self.colors = {
            'edge': '#2E86C1',         # Strong blue
            'origin': '#E74C3C',       # Clear red
            'cache_hit': '#27AE60',    # Vibrant green
            'cache_miss': '#95A5A6',   # Medium gray
            'error': '#F39C12',        # Orange
            'warning': '#F1C40F',      # Yellow
            'success': '#2ECC71',      # Light green
            'primary': '#3498DB',      # Light blue
            'secondary': '#9B59B6'     # Purple
        }
        
        # Create output directory
        self.output_dir = self.config.images_dir / 'origin'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Store for generated figures
        self.figures = {}

    def create_origin_visualizations(
        self,
        df: pd.DataFrame,
        analysis_results: dict,
        zone_name: str
    ) -> Dict:
        """Create comprehensive origin performance visualizations."""
        try:
            if df is None or df.empty or not analysis_results:
                logger.error(f"No data available for visualization for zone {zone_name}")
                return {}

            # Create visualization group
            try:
                figures = {
                    'origin_response_time': create_origin_response_time_dashboard(
                        df, analysis_results, self.colors
                    ),
                    'origin_error': create_origin_error_dashboard(
                        df, analysis_results, self.colors
                    ),
                    'origin_geographic': create_origin_geographic_dashboard(
                        df, analysis_results, self.colors
                    ),
                    'origin_endpoints': create_origin_endpoint_dashboard(
                        df, analysis_results, self.colors
                    ),
                    'origin_asn': create_origin_asn_dashboard(
                        df, analysis_results, self.colors
                    )
                }

                # Save individual visualizations
                self._save_visualizations(figures, zone_name)
                
                # Store figures for potential reuse
                self.figures.update(figures)
                
                return figures

            except Exception as e:
                logger.error(f"Error creating origin visualizations for zone {zone_name}: {str(e)}")
                logger.error(traceback.format_exc())
                return {}

        except Exception as e:
            logger.error(f"Error in origin visualization creation for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _save_visualizations(self, figures: Dict, zone_name: str) -> None:
        """Save visualizations to files."""
        try:
            zone_dir = self.output_dir / zone_name
            zone_dir.mkdir(parents=True, exist_ok=True)

            for name, fig in figures.items():
                html_path = zone_dir / f"{name}_dashboard.html"
                fig.write_html(
                    str(html_path),
                    include_plotlyjs='cdn',
                    full_html=True,
                    config={
                        'displayModeBar': True,
                        'responsive': True
                    }
                )
                logger.info(f"Saved {name} dashboard for zone {zone_name} to {html_path}")

        except Exception as e:
            logger.error(f"Error saving visualizations: {str(e)}")
            logger.error(traceback.format_exc())

    def cleanup(self) -> None:
        """Clean up resources."""
        try:
            # Clear all figures to free memory
            if hasattr(self, 'figures'):
                self.figures.clear()
            
            logger.info("Origin visualizer cleanup completed successfully")
            
        except Exception as e:
            logger.error(f"Error during origin visualizer cleanup: {str(e)}")
            logger.error(traceback.format_exc())
