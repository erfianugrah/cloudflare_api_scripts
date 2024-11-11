from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path

from .analyzer import Analyzer
from .data_processor import DataProcessor
from .origin_analyzer import OriginAnalyzer

logger = logging.getLogger(__name__)

class DashAnalytics:
    def __init__(self, config):
        self.config = config
        self.analyzer = Analyzer(config)
        self.data_processor = DataProcessor()
        self.origin_analyzer = OriginAnalyzer()
        
        # Initialize Dash app
        self.app = Dash(
            __name__,
            external_stylesheets=[dbc.themes.DARKLY],
            suppress_callback_exceptions=True
        )
        
        # Set up the layout
        self.app.layout = self._create_layout()
        
        # Register callbacks
        self._register_callbacks()

    def _create_layout(self):
        """Create the main layout of the dashboard."""
        return html.Div([
            # Navigation Bar
            dbc.NavbarSimple(
                children=[
                    dbc.NavItem(dbc.NavLink("Tiered Cache", href="#tiered")),
                    dbc.NavItem(dbc.NavLink("Performance", href="#performance")),
                    dbc.NavItem(dbc.NavLink("Cache", href="#cache")),
                    dbc.NavItem(dbc.NavLink("Errors", href="#errors")),
                    dbc.NavItem(dbc.NavLink("Geographic", href="#geographic")),
                ],
                brand="Cloudflare Analytics",
                brand_href="#",
                color="primary",
                dark=True,
            ),
            
            # Main Content
            dbc.Container([
                # Controls
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                html.H5("Time Range Selection"),
                                dbc.Row([
                                    dbc.Col([
                                        dcc.DatePickerRange(
                                            id='date-range',
                                            min_date_allowed=datetime(2020, 1, 1),
                                            max_date_allowed=datetime.now(),
                                            start_date=datetime.now() - timedelta(days=7),
                                            end_date=datetime.now(),
                                        ),
                                    ]),
                                    dbc.Col([
                                        dbc.ButtonGroup([
                                            dbc.Button("1h", id="btn-1h", n_clicks=0),
                                            dbc.Button("6h", id="btn-6h", n_clicks=0),
                                            dbc.Button("12h", id="btn-12h", n_clicks=0),
                                            dbc.Button("24h", id="btn-24h", n_clicks=0),
                                            dbc.Button("7d", id="btn-7d", n_clicks=0),
                                        ]),
                                    ]),
                                ]),
                            ])
                        ], className="mb-4"),
                    ], width=12),
                ]),

                # Dashboard Content
                dbc.Row([
                    dbc.Col([
                        # Tiered Cache Section
                        html.Div([
                            html.H3("Tiered Cache Performance", id="tiered"),
                            dbc.Row([
                                dbc.Col(dcc.Graph(id='tiered-perf-graph'), width=6),
                                dbc.Col(dcc.Graph(id='tier-distribution'), width=6),
                            ]),
                            dbc.Row([
                                dbc.Col(dcc.Graph(id='geo-coverage'), width=6),
                                dbc.Col(dcc.Graph(id='tier-response-times'), width=6),
                            ]),
                        ], className="mb-5"),

                        # Performance Section
                        html.Div([
                            html.H3("Performance Metrics", id="performance"),
                            dbc.Row([
                                dbc.Col(dcc.Graph(id='edge-origin-graph'), width=6),
                                dbc.Col(dcc.Graph(id='request-volume'), width=6),
                            ]),
                        ], className="mb-5"),

                        # Cache Section
                        html.Div([
                            html.H3("Cache Analysis", id="cache"),
                            dbc.Row([
                                dbc.Col(dcc.Graph(id='cache-hit-ratio'), width=6),
                                dbc.Col(dcc.Graph(id='cache-distribution'), width=6),
                            ]),
                        ], className="mb-5"),
                    ], width=12),
                ]),
            ], fluid=True),
        ])

    def _register_callbacks(self):
        """Register all callbacks for interactivity."""
        
        @self.app.callback(
            [Output('tiered-perf-graph', 'figure'),
             Output('tier-distribution', 'figure'),
             Output('geo-coverage', 'figure'),
             Output('tier-response-times', 'figure')],
            [Input('date-range', 'start_date'),
             Input('date-range', 'end_date')]
        )
        def update_tiered_cache_graphs(start_date, end_date):
            try:
                filtered_df = self._filter_data(start_date, end_date)
                analysis = self.analyzer.analyze_metrics(filtered_df, 'selected_zone')
                
                # Create tiered performance graph
                perf_fig = self._create_tiered_perf_figure(analysis)
                dist_fig = self._create_tier_distribution_figure(analysis)
                geo_fig = self._create_geo_coverage_figure(analysis)
                resp_fig = self._create_tier_response_times_figure(analysis)
                
                return perf_fig, dist_fig, geo_fig, resp_fig
                
            except Exception as e:
                logger.error(f"Error updating tiered cache graphs: {str(e)}")
                return [go.Figure() for _ in range(4)]

        # Add more callbacks for other sections...

    def _create_tiered_perf_figure(self, analysis):
        """Create tiered performance comparison figure."""
        try:
            fig = go.Figure()
            
            perf_metrics = analysis.get('tiered_cache_analysis', {}).get('performance', {})
            
            # Add bars for tiered and direct performance
            fig.add_trace(go.Bar(
                x=['Tiered', 'Direct'],
                y=[
                    perf_metrics.get('tiered_requests', {}).get('avg_ttfb', 0),
                    perf_metrics.get('direct_requests', {}).get('avg_ttfb', 0)
                ],
                marker_color=[self.config.colors['edge'], self.config.colors['origin']]
            ))

            fig.update_layout(
                title='Tiered vs Direct Performance',
                template='plotly_dark',
                showlegend=False,
                yaxis_title='Response Time (ms)'
            )

            return fig
            
        except Exception as e:
            logger.error(f"Error creating tiered performance figure: {str(e)}")
            return go.Figure()

    def run_server(self, debug=True, port=8050):
        """Run the Dash server."""
        self.app.run_server(debug=debug, port=port)

    def _filter_data(self, start_date, end_date):
        """Filter DataFrame based on date range."""
        if not hasattr(self, 'df'):
            return pd.DataFrame()
            
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        mask = (self.df['timestamp'] >= start) & (self.df['timestamp'] <= end)
        return self.df[mask]

    def update_data(self, df: pd.DataFrame):
        """Update the data used by the dashboard."""
        self.df = df
