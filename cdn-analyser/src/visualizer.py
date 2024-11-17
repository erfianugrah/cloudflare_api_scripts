from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import logging
from pathlib import Path
import traceback
from threading import Timer

from .dashboards import (
    create_cache_dashboard,
    create_error_dashboard,
    create_performance_dashboard,
    create_geographic_dashboard,
    create_rps_dashboard,
    create_origin_response_time_dashboard,
    create_origin_error_dashboard,
    create_origin_geographic_dashboard,
    create_origin_endpoint_dashboard,
    create_origin_asn_dashboard
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
        
        # Store for generated figures
        self.figures = {}
        self.zone_figures = {}

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

            # Initialize zone figures if not exists
            if zone_name not in self.zone_figures:
                self.zone_figures[zone_name] = {}

            # Create visualization groups for this zone
            try:
                # Main dashboards
                self.zone_figures[zone_name].update({
                    'performance': create_performance_dashboard(df, analysis, self.colors),
                    'cache': create_cache_dashboard(df, analysis, self.colors),
                    'error': create_error_dashboard(df, analysis, self.colors),
                    'geographic': create_geographic_dashboard(df, analysis, self.colors),
                    'rps': create_rps_dashboard(df, analysis, self.colors),
                    'origin_response_time': create_origin_response_time_dashboard(df, analysis, self.colors),
                    'origin_error': create_origin_error_dashboard(df, analysis, self.colors),
                    'origin_geographic': create_origin_geographic_dashboard(df, analysis, self.colors),
                    'origin_endpoints': create_origin_endpoint_dashboard(df, analysis, self.colors),
                    'origin_asn': create_origin_asn_dashboard(df, analysis, self.colors)
                })

            except Exception as e:
                logger.error(f"Error creating dashboards for zone {zone_name}: {str(e)}")
                logger.error(traceback.format_exc())

            # Save visualizations
            try:
                self._save_visualizations(zone_name, output_dir)
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
        """Create zone-specific dashboard including all metrics."""
        try:
            zone_figures = self.zone_figures.get(zone_name, {})
            port = 8050 + abs(hash(zone_name)) % 1000
            
            logger.info(f"Creating dashboard for zone {zone_name} on port {port}")

            # Define tab styles
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

            # Create graph containers
            def create_graph_container(figure, tab_name):
                return html.Div([
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
                    )
                ], style={
                    'marginBottom': '40px',
                    'backgroundColor': '#1e1e1e',
                    'padding': '20px',
                    'borderRadius': '8px'
                })

            # Create app layout with all dashboards
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
                            children=[create_graph_container(zone_figures['performance'], "Performance Metrics")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Cache',
                            children=[create_graph_container(zone_figures['cache'], "Cache Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Errors',
                            children=[create_graph_container(zone_figures['error'], "Error Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Geographic',
                            children=[create_graph_container(zone_figures['geographic'], "Geographic Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='RPS Analysis',
                            children=[create_graph_container(zone_figures['rps'], "Requests Per Second Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Response Time',
                            children=[create_graph_container(zone_figures['origin_response_time'], "Origin Response Time Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Errors',
                            children=[create_graph_container(zone_figures['origin_error'], "Origin Error Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Geographic',
                            children=[create_graph_container(zone_figures['origin_geographic'], "Origin Geographic Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Endpoints',
                            children=[create_graph_container(zone_figures['origin_endpoints'], "Origin Endpoints Analysis")],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin ASN',
                            children=[create_graph_container(zone_figures['origin_asn'], "Origin ASN Analysis")],
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

    def _save_visualizations(self, zone_name: str, output_dir: Path) -> None:
        """Save all visualizations for a zone as HTML files."""
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
            logger.error(f"Error saving visualizations: {str(e)}")

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

    def cleanup(self):
        """Clean up resources and shutdown dashboard properly."""
        try:
            # Clear all figures to free memory
            if hasattr(self, 'figures'):
                self.figures.clear()
                
            if hasattr(self, 'zone_figures'):
                self.zone_figures.clear()
            
            # Clean up any cached data
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
