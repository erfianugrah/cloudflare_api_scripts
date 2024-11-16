from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from pathlib import Path
import traceback
import sys
from threading import Timer
from .origin_visualizer import OriginVisualizer
from .dashboards import (
    create_cache_dashboard,
    create_error_dashboard,
    create_performance_dashboard,
    create_geographic_dashboard,
    create_rps_dashboard
)

logger = logging.getLogger(__name__)

class Visualizer:
    """Modern visualizer for Cloudflare analytics using Dash and Plotly."""
    
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
        
        # Initialize Dash app
        self.app = Dash(
            __name__,
            external_stylesheets=[dbc.themes.DARKLY]
        )
        
        # Initialize origin visualizer
        self.origin_visualizer = OriginVisualizer(config)
        
        # Store for generated figures
        self.figures = {}

    def create_visualizations(
        self, 
        df: pd.DataFrame, 
        analysis: dict, 
        zone_name: str
    ) -> None:
        """Create comprehensive visualizations with multi-zone support."""
        try:
            if df is None or df.empty or not analysis:
                logger.error(f"No data available for visualization for zone {zone_name}")
                return

            output_dir = self.config.images_dir / zone_name 
            output_dir.mkdir(parents=True, exist_ok=True)

            # Initialize figures dictionary for this zone if not exists
            if not hasattr(self, 'zone_figures'):
                self.zone_figures = {}
            if zone_name not in self.zone_figures:
                self.zone_figures[zone_name] = {}

            # Create visualization groups for this zone
            try:
                # Create main dashboards
                self.zone_figures[zone_name]['performance'] = create_performance_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['cache'] = create_cache_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['error'] = create_error_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['geographic'] = create_geographic_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['rps'] = create_rps_dashboard(df, analysis, self.colors)
                
                # Create origin visualizations
                origin_figures = self.origin_visualizer.create_origin_visualizations(df, analysis, zone_name)
                self.zone_figures[zone_name].update(origin_figures)

            except Exception as e:
                logger.error(f"Error creating dashboards for zone {zone_name}: {str(e)}")
                logger.error(traceback.format_exc())

            # Save visualizations
            try:
                for name, fig in self.zone_figures[zone_name].items():
                    html_path = output_dir / f"{name}_dashboard.html"
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
                logger.error(f"Error saving visualizations for zone {zone_name}: {str(e)}")

            # Create dashboard for this zone
            try:
                self._create_dashboard(zone_name)
            except Exception as e:
                logger.error(f"Error creating dashboard for zone {zone_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error in visualization creation for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())

    def _create_dashboard(self, zone_name: str) -> None:
        """Create zone-specific dashboard including origin metrics."""
        try:
            zone_figures = self.zone_figures.get(zone_name, {})
            port = 8050 + abs(hash(zone_name)) % 1000
            
            logger.info(f"Creating dashboard for zone {zone_name} on port {port}")

            tab_style = {
                'backgroundColor': '#1e1e1e',
                'color': '#ffffff',
                'padding': '6px 12px',
                'border': '1px solid #333',
                'borderRadius': '3px 3px 0 0',
                'marginRight': '2px',
                'height': '32px',
                'fontSize': '13px',
                'lineHeight': '20px'
            }
            
            selected_tab_style = {
                'backgroundColor': '#2d2d2d',
                'color': '#ffffff',
                'padding': '6px 12px',
                'border': '1px solid #333',
                'borderRadius': '3px 3px 0 0',
                'marginRight': '2px',
                'height': '32px',
                'fontSize': '13px',
                'lineHeight': '20px',
                'borderBottom': '2px solid #3498db'
            }

            # Create graph containers with individual legends
            def create_graph_container(figure, tab_name):
                return html.Div([
                    # Title container
                    html.Div(
                        tab_name,
                        style={
                            'textAlign': 'center',
                            'fontSize': '18px',
                            'fontWeight': 'bold',
                            'padding': '10px',
                            'color': 'white'
                        }
                    ),
                    # Graph container
                    dcc.Graph(
                        figure=figure,
                        style={'height': '2000px'},
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': [
                                'select2d',
                                'lasso2d',
                                'autoScale2d'
                            ],
                        }
                    ),
                    # Legend container
                    html.Div(
                        style={
                            'backgroundColor': 'rgba(0,0,0,0.5)',
                            'padding': '10px',
                            'marginTop': '10px',
                            'borderRadius': '4px',
                            'border': '1px solid rgba(255,255,255,0.2)'
                        }
                    )
                ], style={
                    'marginBottom': '40px',
                    'backgroundColor': '#1e1e1e',
                    'padding': '20px',
                    'borderRadius': '8px'
                })

            app_layout = html.Div([
                html.H1(
                    f"Cloudflare Analytics - {zone_name}",
                    style={
                        'textAlign': 'center',
                        'margin': '20px 0',
                        'color': '#ffffff',
                        'fontSize': '24px',
                        'fontFamily': 'Arial, sans-serif'
                    }
                ),
                
                html.Div([
                    dcc.Tabs([
                        dcc.Tab(
                            label='Performance',
                            children=[
                                create_graph_container(
                                    zone_figures.get('performance', self._create_error_figure("No performance data available")),
                                    "Performance Metrics"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Cache',
                            children=[
                                create_graph_container(
                                    zone_figures.get('cache', self._create_error_figure("No cache data available")),
                                    "Cache Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Errors',
                            children=[
                                create_graph_container(
                                    zone_figures.get('error', self._create_error_figure("No error data available")),
                                    "Error Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Geographic',
                            children=[
                                create_graph_container(
                                    zone_figures.get('geographic', self._create_error_figure("No geographic data available")),
                                    "Geographic Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='RPS Analysis',
                            children=[
                                create_graph_container(
                                    zone_figures.get('rps', self._create_error_figure("No RPS data available")),
                                    "Requests Per Second Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Response Time',
                            children=[
                                create_graph_container(
                                    zone_figures.get('origin_response_time', self._create_error_figure("No origin response time data available")),
                                    "Origin Response Time Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin ASN Analysis',
                            children=[
                                create_graph_container(
                                    zone_figures.get('origin_asn', self._create_error_figure("No ASN analysis data available")),
                                    "Origin ASN Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Errors',
                            children=[
                                create_graph_container(
                                    zone_figures.get('origin_error', self._create_error_figure("No origin error data available")),
                                    "Origin Error Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Geographic',
                            children=[
                                create_graph_container(
                                    zone_figures.get('origin_geographic', self._create_error_figure("No origin geographic data available")),
                                    "Origin Geographic Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Endpoints',
                            children=[
                                create_graph_container(
                                    zone_figures.get('origin_endpoints', self._create_error_figure("No origin endpoint data available")),
                                    "Origin Endpoints Analysis"
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        )
                    ])
                ], style={'margin': '0 12px'})
            ], style={
                'backgroundColor': '#121212',
                'minHeight': '100vh',
                'padding': '12px'
            })

            self.app.layout = app_layout

            logger.info(f"Starting dashboard for zone {zone_name}")
            self.app.run_server(
                debug=False,
                port=port,
                use_reloader=False,
                dev_tools_hot_reload=False,
                host='127.0.0.1'
            )

        except Exception as e:
            logger.error(f"Error creating dashboard for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())

    def _create_error_figure(self, message: str) -> go.Figure:
        """Create an error figure with improved styling."""
        fig = go.Figure()
        
        fig.add_annotation(
            x=0.5,
            y=0.5,
            text=message,
            font=dict(
                size=16,
                color='#ffffff'
            ),
            showarrow=False,
            xref="paper",
            yref="paper"
        )
        
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='#1e1e1e',
            plot_bgcolor='#1e1e1e',
            margin=dict(l=40, r=40, t=40, b=40),
            height=600
        )
        
        return fig

    def _save_visualizations(self, output_dir: Path) -> None:
        """Save all visualizations as HTML files."""
        try:
            for name, fig in self.figures.items():
                html_path = output_dir / f"{name}_dashboard.html"
                fig.write_html(
                    str(html_path),
                    include_plotlyjs='cdn',
                    full_html=True,
                    config={
                        'displayModeBar': True,
                        'responsive': True
                    }
                )
                logger.info(f"Saved {name} dashboard to {html_path}")
        except Exception as e:
            logger.error(f"Error saving visualizations: {str(e)}")

    def cleanup(self):
        """Clean up resources and shutdown dashboard properly."""
        try:
            # Clear all figures to free memory
            if hasattr(self, 'figures'):
                self.figures.clear()
            
            # Close all open matplotlib figures
            plt.close('all')
            
            # Reset matplotlib settings if needed
            plt.style.use('default')
            
            # Clean up origin visualizer
            if hasattr(self, 'origin_visualizer'):
                self.origin_visualizer.cleanup()
            
            # Clear any cached data
            if hasattr(self, 'app') and self.app:
                if hasattr(self.app, 'server'):
                    try:
                        self.app.server.do_teardown_appcontext()
                    except Exception as e:
                        logger.warning(f"Error during server teardown: {str(e)}")
                
            logger.info("Visualizer cleanup completed successfully")
            
        except Exception as e:
            logger.error(f"Error during visualizer cleanup: {str(e)}")
            logger.error(traceback.format_exc())
