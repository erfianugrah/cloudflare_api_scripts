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

logger = logging.getLogger(__name__)

class Visualizer:
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
        self.chart_font = dict(family="Arial", size=12, color="white")
        self.title_font = dict(family="Arial", size=14, color="white")

    def _truncate_endpoint(self, endpoint: str, max_length: int = 30) -> str:
        """Truncate long endpoint URLs while keeping important parts."""
        if len(endpoint) <= max_length:
            return endpoint
        
        # Split by common separators
        parts = endpoint.split('/')
        if len(parts) <= 2:
            return endpoint[:max_length] + '...'
            
        # Keep domain and last path component
        return f"{parts[0]}/.../{parts[-1]}"

    def _format_hoverlabel(self, text: str) -> str:
        """Format hover label with line breaks for better readability."""
        return text.replace(', ', '<br>')

    # def _add_chart_annotations(
    #     self, 
    #     fig: go.Figure,
    #     row: int,
    #     col: int,
    #     title: str,
    #     subtitle: Optional[str] = None
    # ) -> None:
    #     """Add title and optional subtitle to chart."""
    #     fig.add_annotation(
    #         text=title,
    #         xref="x domain",
    #         yref="y domain",
    #         x=0.5,
    #         y=1.1,
    #         showarrow=False,
    #         font=self.title_font,
    #         row=row,
    #         col=col
    #     )
    #     
    #     if subtitle:
    #         fig.add_annotation(
    #             text=subtitle,
    #             xref="x domain",
    #             yref="y domain",
    #             x=0.5,
    #             y=1.05,
    #             showarrow=False,
    #             font=dict(
    #                 family=self.chart_font['family'],
    #                 size=10,
    #                 color='#999'
    #             ),
    #             row=row,
    #             col=col
    #         )

    def _create_dashboard(self, zone_name: str, port: int) -> None:
        """Create zone-specific dashboard with all visualizations."""
        try:
            zone_figures = self.zone_figures.get(zone_name, {})
            
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

            # Create new Dash app instance for this zone
            app = Dash(
                f"cloudflare-analytics-{zone_name}",
                external_stylesheets=[dbc.themes.DARKLY]
            )

            app.layout = html.Div([
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
                                dcc.Graph(
                                    figure=zone_figures.get('performance', self._create_error_figure("No performance data available")),
                                    style={'height': '85vh'}
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Cache',
                            children=[
                                dcc.Graph(
                                    figure=zone_figures.get('cache', self._create_error_figure("No cache data available")),
                                    style={'height': '85vh'}
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Errors',
                            children=[
                                dcc.Graph(
                                    figure=zone_figures.get('error', self._create_error_figure("No error data available")),
                                    style={'height': '85vh'}
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Geographic',
                            children=[
                                dcc.Graph(
                                    figure=zone_figures.get('geographic', self._create_error_figure("No geographic data available")),
                                    style={'height': '85vh'}
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Endpoints',
                            children=[
                                dcc.Graph(
                                    figure=zone_figures.get('endpoints', self._create_error_figure("No endpoints data available")),
                                    style={'height': '85vh'}
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        )
                    ])
                ],
                style={
                    'margin': '0 12px'
                }),
                
                html.Div([
                    html.Button(
                        'Shutdown Dashboard',
                        id='shutdown-button',
                        className='mt-4 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700',
                        n_clicks=0,
                        style={
                            'marginTop': '20px',
                            'padding': '8px 16px',
                            'backgroundColor': '#e53e3e',
                            'color': 'white',
                            'border': 'none',
                            'borderRadius': '4px',
                            'cursor': 'pointer'
                        }
                    )
                ], style={
                    'textAlign': 'center',
                    'marginTop': '20px',
                    'marginBottom': '20px'
                }),
                
                html.Div(id='shutdown-trigger', style={'display': 'none'})
            ],
            style={
                'backgroundColor': '#121212',
                'minHeight': '100vh',
                'padding': '12px'
            })

            @app.callback(
                Output('shutdown-trigger', 'children'),
                Input('shutdown-button', 'n_clicks')
            )
            def shutdown_server(n_clicks):
                if n_clicks and n_clicks > 0:
                    logger.info(f"Shutdown requested for zone {zone_name} dashboard")
                    def shutdown():
                        try:
                            self.cleanup()
                        finally:
                            sys.exit(0)
                    Timer(1.0, shutdown).start()
                    return "Shutting down..."
                return ""

            logger.info(f"Starting dashboard for zone {zone_name} on port {port}")
            app.run_server(
                debug=False,
                port=port,
                use_reloader=False,
                dev_tools_hot_reload=False,
                host='127.0.0.1'
            )

        except Exception as e:
            logger.error(f"Error creating dashboard for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())

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
                # Create consolidated dashboards
                self.zone_figures[zone_name]['performance'] = self._create_performance_dashboard(df, analysis)
                self.zone_figures[zone_name]['cache'] = self._create_cache_dashboard(df, analysis)
                self.zone_figures[zone_name]['error'] = self._create_error_dashboard(df, analysis)
                self.zone_figures[zone_name]['geographic'] = self._create_geographic_dashboard(df, analysis)
                self.zone_figures[zone_name]['endpoints'] = self._create_endpoint_dashboard(df, analysis)

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
                port = 8050 + abs(hash(zone_name)) % 1000  # Generate unique port for each zone
                self._create_dashboard(zone_name, port)
            except Exception as e:
                logger.error(f"Error creating dashboard for zone {zone_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error in visualization creation for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())

    def _create_performance_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create enhanced performance dashboard with better readability."""
        try:
            fig = make_subplots(
                rows=3,
                cols=2,
                subplot_titles=['' for _ in range(6)],  # We'll add custom titles later
                specs=[
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "choropleth"}]
                ],
                vertical_spacing=0.2,    # Increased spacing
                horizontal_spacing=0.15,
                row_heights=[0.4, 0.3, 0.3]  # Main chart gets more space
            )

            # Process time series data
            df_time = df.set_index('timestamp').sort_index()
            rolling_window = '5min'

            # 1. Edge vs Origin Response Times with Quantiles
            metrics_groups = {
                'Edge': [
                    ('ttfb_avg', 'TTFB', 'solid', 2),
                    ('ttfb_p95', 'P95', 'dash', 1),
                    ('ttfb_p99', 'P99', 'dot', 1)
                ],
                'Origin': [
                    ('origin_time_avg', 'Response', 'solid', 2),
                    ('origin_p95', 'P95', 'dash', 1),
                    ('origin_p99', 'P99', 'dot', 1)
                ]
            }

            for group_name, metrics in metrics_groups.items():
                color = self.colors['edge'] if group_name == 'Edge' else self.colors['origin']
                
                for metric, label, dash_style, width in metrics:
                    fig.add_trace(
                        go.Scatter(
                            x=df_time.index,
                            y=df_time[metric].rolling(rolling_window).mean(),
                            name=f'{group_name} {label}',
                            line=dict(
                                color=color,
                                dash=dash_style,
                                width=width
                            ),
                            opacity=0.8 if dash_style != 'solid' else 1,
                            hovertemplate=(
                                '%{x}<br>' +
                                f'{group_name} {label}: ' + '%{y:.2f}ms<br>' +
                                '<extra></extra>'
                            )
                        ),
                        row=1, col=1
                    )

            # Add annotations
            self._add_chart_annotations(
                fig, 1, 1,
                "Edge vs Origin Response Times",
                "Showing average, P95, and P99 response times"
            )

            # 2. Request Volume by Cache Status
            cache_statuses = df_time.groupby(
                [pd.Grouper(freq=rolling_window), 'cache_status']
            )['visits_adjusted'].sum().unstack(fill_value=0)

            colors = {
                'hit': self.colors['cache_hit'],
                'miss': self.colors['cache_miss'],
                'expired': self.colors['warning'],
                'error': self.colors['error'],
                'bypass': self.colors['secondary']
            }

            for status in cache_statuses.columns:
                fig.add_trace(
                    go.Scatter(
                        x=cache_statuses.index,
                        y=cache_statuses[status],
                        name=f'Cache {status.title()}',
                        stackgroup='cache',
                        line=dict(width=0),
                        fillcolor=colors.get(status, self.colors['secondary']),
                        hovertemplate=(
                            '%{x}<br>' +
                            f'Cache {status.title()}: ' + '%{y:,.0f} requests<br>' +
                            '<extra></extra>'
                        )
                    ),
                    row=1, col=2
                )

            self._add_chart_annotations(
                fig, 1, 2,
                "Request Volume Distribution",
                "Stacked by cache status"
            )

            # 3. ASN Performance Analysis
            if 'clientAsn' in df.columns:
                asn_perf = df.groupby('clientAsn').agg({
                    'origin_time_avg': 'mean',
                    'origin_p95': 'mean',
                    'ttfb_avg': 'mean',
                    'visits_adjusted': 'sum'
                }).nlargest(10, 'visits_adjusted')

                # Edge Performance
                fig.add_trace(
                    go.Scatter(
                        x=asn_perf.index,
                        y=asn_perf['ttfb_avg'],
                        name='Edge TTFB',
                        mode='markers+lines',
                        marker=dict(
                            size=10,
                            color=self.colors['edge']
                        ),
                        line=dict(color=self.colors['edge']),
                        hovertemplate=(
                            'ASN: %{x}<br>' +
                            'Edge TTFB: %{y:.1f}ms<br>' +
                            '<extra></extra>'
                        )
                    ),
                    row=2, col=1
                )

                # Origin Performance
                fig.add_trace(
                    go.Scatter(
                        x=asn_perf.index,
                        y=asn_perf['origin_time_avg'],
                        name='Origin Response',
                        mode='markers+lines',
                        marker=dict(
                            size=10,
                            color=self.colors['origin']
                        ),
                        line=dict(color=self.colors['origin']),
                        hovertemplate=(
                            'ASN: %{x}<br>' +
                            'Origin Response: %{y:.1f}ms<br>' +
                            '<extra></extra>'
                        )
                    ),
                    row=2, col=1
                )

                # Add size reference for request volume
                fig.add_trace(
                    go.Scatter(
                        x=asn_perf.index,
                        y=[0] * len(asn_perf),
                        mode='markers',
                        marker=dict(
                            size=asn_perf['visits_adjusted'] / asn_perf['visits_adjusted'].max() * 50,
                            color=asn_perf['visits_adjusted'],
                            colorscale='Viridis',
                            showscale=True,
                            colorbar=dict(
                                title='Request Volume',
                                x=1.1
                            )
                        ),
                        showlegend=False,
                        hovertemplate=(
                            'ASN: %{x}<br>' +
                            'Requests: %{marker.color:,.0f}<br>' +
                            '<extra></extra>'
                        )
                    ),
                    row=2, col=1
                )

            # 4. Protocol Performance
            protocol_perf = df.groupby('protocol').agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()

            # Create subplots for protocols
            fig.add_trace(
                go.Bar(
                    name='Edge TTFB',
                    x=protocol_perf['protocol'],
                    y=protocol_perf['ttfb_avg'],
                    marker_color=self.colors['edge'],
                    customdata=protocol_perf[['visits_adjusted', 'origin_time_avg']],
                    hovertemplate=(
                        'Protocol: %{x}<br>' +
                        'Edge TTFB: %{y:.1f}ms<br>' +
                        'Requests: %{customdata[0]:,.0f}<br>' +
                        '<extra></extra>'
                    )
                ),
                row=2, col=2
            )

            fig.add_trace(
                go.Bar(
                    name='Origin Time',
                    x=protocol_perf['protocol'],
                    y=protocol_perf['origin_time_avg'],
                    marker_color=self.colors['origin'],
                    customdata=protocol_perf[['visits_adjusted']],
                    hovertemplate=(
                        'Protocol: %{x}<br>' +
                        'Origin Time: %{y:.1f}ms<br>' +
                        'Requests: %{customdata[0]:,.0f}<br>' +
                        '<extra></extra>'
                    )
                ),
                row=2, col=2
            )

            # 5. Top Endpoints Performance
            top_endpoints = df.groupby('endpoint').agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')

            fig.add_trace(
                go.Bar(
                    name='Edge TTFB',
                    x=top_endpoints.index,
                    y=top_endpoints['ttfb_avg'],
                    marker_color=self.colors['edge'],
                    customdata=top_endpoints[['visits_adjusted']],
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        'Edge TTFB: %{y:.1f}ms<br>' +
                        'Requests: %{customdata[0]:,.0f}<br>' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=1
            )

            fig.add_trace(
                go.Bar(
                    name='Origin Time',
                    x=top_endpoints.index,
                    y=top_endpoints['origin_time_avg'],
                    marker_color=self.colors['origin'],
                    customdata=top_endpoints[['visits_adjusted']],
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        'Origin Time: %{y:.1f}ms<br>' +
                        'Requests: %{customdata[0]:,.0f}<br>' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=1
            )

            # 6. Geographic Response Times
            geo_metrics = df.groupby('country').agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()

            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['origin_time_avg'],
                    colorscale='Viridis',
                    reversescale=True,
                    colorbar_title='Response Time (ms)',
                    locationmode='country names',
                    customdata=np.stack((
                        geo_metrics['ttfb_avg'],
                        geo_metrics['visits_adjusted']
                    ), axis=1),
                    hovertemplate=(
                        '%{location}<br>' +
                        'Origin Time: %{z:.1f}ms<br>' +
                        'Edge TTFB: %{customdata[0]:.1f}ms<br>' +
                        'Requests: %{customdata[1]:,.0f}<br>' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=2
            )

            # Update layout
            fig.update_layout(
                height=1200,
                showlegend=True,
                template='plotly_dark',
                paper_bgcolor='#1e1e1e',
                plot_bgcolor='#1e1e1e',
                margin=dict(l=60, r=60, t=100, b=60),  # Increased top margin for titles
                legend=dict(
                    bgcolor='rgba(0,0,0,0.5)',
                    bordercolor='#333',
                    borderwidth=1,
                    font=self.chart_font,
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01,
                    itemsizing='constant'
                ),
                hoverlabel=dict(
                    font=self.chart_font,
                    bgcolor='rgba(0,0,0,0.8)',
                    bordercolor='#333'
                )
            )

            # Update axes
            fig.update_xaxes(
                gridcolor='#333',
                showgrid=True,
                griddash='dash',
                zeroline=False,
                title_font=self.title_font,
                tickfont=self.chart_font,
                title_standoff=20
            )

            fig.update_yaxes(
                gridcolor='#333',
                showgrid=True,
                griddash='dash',
                zeroline=False,
                title_font=self.title_font,
                tickfont=self.chart_font,
                title_standoff=20
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating performance dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating performance dashboard")

    def _create_error_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create enhanced error analysis dashboard with improved readability."""
        try:
            fig = make_subplots(
                rows=3,
                cols=2,
                subplot_titles=['' for _ in range(6)],
                specs=[
                    [{"secondary_y": True}, {"type": "bar"}],
                    [{"type": "bar"}, {"type": "bar"}],
                    [{"type": "bar"}, {"type": "choropleth"}]
                ],
                vertical_spacing=0.2,
                horizontal_spacing=0.15,
                row_heights=[0.4, 0.3, 0.3]
            )

            # Process time series data with better aggregation
            df_time = df.set_index('timestamp').resample('5min').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean'
            }).reset_index()

            # 1. Error Rates Over Time with Request Volume
            # Add 4xx errors
            fig.add_trace(
                go.Scatter(
                    x=df_time['timestamp'],
                    y=df_time['error_rate_4xx'] * 100,
                    name='4xx Errors',
                    line=dict(
                        color=self.colors['warning'],
                        width=2
                    ),
                    fill='tonexty',
                    fillcolor=f'rgba{tuple(list(int(self.colors["warning"].lstrip("#")[i:i+2], 16) for i in (0, 2, 4)) + [0.2])}',
                    hovertemplate=(
                        '%{x}<br>' +
                        '4xx Rate: %{y:.2f}%<br>' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=1
            )

            # Add 5xx errors
            fig.add_trace(
                go.Scatter(
                    x=df_time['timestamp'],
                    y=df_time['error_rate_5xx'] * 100,
                    name='5xx Errors',
                    line=dict(
                        color=self.colors['error'],
                        width=2
                    ),
                    fill='tonexty',
                    fillcolor=f'rgba{tuple(list(int(self.colors["error"].lstrip("#")[i:i+2], 16) for i in (0, 2, 4)) + [0.2])}',
                    hovertemplate=(
                        '%{x}<br>' +
                        '5xx Rate: %{y:.2f}%<br>' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=1
            )

            # Add request volume on secondary axis
            fig.add_trace(
                go.Scatter(
                    x=df_time['timestamp'],
                    y=df_time['visits_adjusted'],
                    name='Request Volume',
                    line=dict(
                        color=self.colors['primary'],
                        dash='dot',
                        width=1
                    ),
                    hovertemplate=(
                        '%{x}<br>' +
                        'Requests: %{y:,.0f}<br>' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=1,
                secondary_y=True
            )

            self._add_chart_annotations(
                fig, 1, 1,
                "Error Rates Over Time",
                "4xx and 5xx errors with request volume"
            )

            # 2. Error Status Distribution with Response Time
            error_df = df[df['status'] >= 400]
            status_counts = error_df['status'].value_counts()
            
            # Calculate average response time per status
            status_times = error_df.groupby('status')['origin_time_avg'].mean()

            # Create color gradient based on error severity
            status_colors = [
                self.colors['warning'] if s < 500 else self.colors['error']
                for s in status_counts.index
            ]

            fig.add_trace(
                go.Bar(
                    x=status_counts.index.astype(str),
                    y=status_counts.values,
                    marker_color=status_colors,
                    name='Error Count',
                    customdata=np.stack((
                        status_counts.values / len(df) * 100,  # Error rate percentage
                        status_times[status_counts.index]      # Average response time
                    ), axis=1),
                    hovertemplate=(
                        'Status: %{x}<br>' +
                        'Count: %{y:,}<br>' +
                        'Rate: %{customdata[0]:.2f}%<br>' +
                        'Avg Response: %{customdata[1]:.1f}ms' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=2
            )

            self._add_chart_annotations(
                fig, 1, 2,
                "Error Status Distribution",
                "With average response times"
            )

            # 3. Error Rates by ASN (Top 10 by volume)
            if 'clientAsn' in df.columns:
                asn_errors = df.groupby('clientAsn').agg({
                    'error_rate_4xx': 'mean',
                    'error_rate_5xx': 'mean',
                    'visits_adjusted': 'sum',
                    'origin_time_avg': 'mean'
                }).nlargest(10, 'visits_adjusted')

                # Create bar chart for each error type
                for error_type, color in [('error_rate_4xx', self.colors['warning']),
                                        ('error_rate_5xx', self.colors['error'])]:
                    fig.add_trace(
                        go.Bar(
                            name=f"{error_type.split('_')[1].upper()} Errors",
                            x=[self._truncate_asn(asn) for asn in asn_errors.index],
                            y=asn_errors[error_type] * 100,
                            marker_color=color,
                            customdata=np.stack((
                                asn_errors.index,
                                asn_errors['visits_adjusted'],
                                asn_errors['origin_time_avg']
                            ), axis=1),
                            hovertemplate=(
                                'ASN: %{customdata[0]}<br>' +
                                'Error Rate: %{y:.2f}%<br>' +
                                'Requests: %{customdata[1]:,.0f}<br>' +
                                'Avg Response: %{customdata[2]:.1f}ms' +
                                '<extra></extra>'
                            )
                        ),
                        row=2, col=1
                    )

            self._add_chart_annotations(
                fig, 2, 1,
                "Error Rates by ASN",
                "Top 10 ASNs by request volume"
            )

            # 4. Protocol Error Distribution
            protocol_errors = df.groupby('protocol').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean'
            }).reset_index()

            # Add bars for each protocol
            for error_type, color in [('error_rate_4xx', self.colors['warning']),
                                    ('error_rate_5xx', self.colors['error'])]:
                fig.add_trace(
                    go.Bar(
                        name=f"{error_type.split('_')[1].upper()} Errors",
                        x=protocol_errors['protocol'],
                        y=protocol_errors[error_type] * 100,
                        marker_color=color,
                        customdata=protocol_errors[['visits_adjusted', 'origin_time_avg']],
                        hovertemplate=(
                            'Protocol: %{x}<br>' +
                            'Error Rate: %{y:.2f}%<br>' +
                            'Requests: %{customdata[0]:,.0f}<br>' +
                            'Avg Response: %{customdata[1]:.1f}ms' +
                            '<extra></extra>'
                        )
                    ),
                    row=2, col=2
                )

            self._add_chart_annotations(
                fig, 2, 2,
                "Protocol Error Distribution",
                "Error rates by HTTP protocol version"
            )

            # 5. Top Endpoints by Error Rate
            endpoint_errors = df.groupby('endpoint').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean'
            }).nlargest(10, 'visits_adjusted')

            # Add bars for each endpoint
            for error_type, color in [('error_rate_4xx', self.colors['warning']),
                                    ('error_rate_5xx', self.colors['error'])]:
                fig.add_trace(
                    go.Bar(
                        name=f"{error_type.split('_')[1].upper()} Errors",
                        x=[self._truncate_endpoint(ep) for ep in endpoint_errors.index],
                        y=endpoint_errors[error_type] * 100,
                        marker_color=color,
                        customdata=np.stack((
                            endpoint_errors.index,
                            endpoint_errors['visits_adjusted'],
                            endpoint_errors['origin_time_avg']
                        ), axis=1),
                        hovertemplate=(
                            'Endpoint: %{customdata[0]}<br>' +
                            'Error Rate: %{y:.2f}%<br>' +
                            'Requests: %{customdata[1]:,.0f}<br>' +
                            'Avg Response: %{customdata[2]:.1f}ms' +
                            '<extra></extra>'
                        )
                    ),
                    row=3, col=1
                )

            self._add_chart_annotations(
                fig, 3, 1,
                "Top Endpoints by Error Rate",
                "Top 10 endpoints by request volume"
            )

            # 6. Geographic Error Distribution
            geo_errors = df.groupby('country').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean'
            }).reset_index()

            # Calculate total error rate for choropleth
            geo_errors['total_error_rate'] = (
                geo_errors['error_rate_4xx'] + geo_errors['error_rate_5xx']
            ) * 100

            fig.add_trace(
                go.Choropleth(
                    locations=geo_errors['country'],
                    z=geo_errors['total_error_rate'],
                    colorscale=[
                        [0, '#1e1e1e'],
                        [0.2, self.colors['warning']],
                        [1, self.colors['error']]
                    ],
                    colorbar_title='Error Rate (%)',
                    locationmode='country names',
                    customdata=np.stack((
                        geo_errors['error_rate_4xx'] * 100,
                        geo_errors['error_rate_5xx'] * 100,
                        geo_errors['visits_adjusted'],
                        geo_errors['origin_time_avg']
                    ), axis=1),
                    hovertemplate=(
                        '%{location}<br>' +
                        'Total Error Rate: %{z:.2f}%<br>' +
                        '4xx Rate: %{customdata[0]:.2f}%<br>' +
                        '5xx Rate: %{customdata[1]:.2f}%<br>' +
                        'Requests: %{customdata[2]:,.0f}<br>' +
                        'Avg Response: %{customdata[3]:.1f}ms' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=2
            )

            self._add_chart_annotations(
                fig, 3, 2,
                "Geographic Error Distribution",
                "Total error rate by country"
            )

            # Update layout with improved styling
            self._update_layout(fig)
            
            # Add secondary y-axis title
            fig.update_yaxes(
                title_text="Error Rate (%)",
                secondary_y=False,
                row=1, col=1
            )
            fig.update_yaxes(
                title_text="Requests",
                secondary_y=True,
                row=1, col=1
            )

            # Update axes with improved styling
            self._update_axes(fig)
            
            # Update geo layout
            self._update_geo_layout(fig)

            return fig

        except Exception as e:
            logger.error(f"Error creating error dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating error dashboard")

    def _create_cache_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create enhanced cache performance dashboard with improved readability."""
        try:
            fig = make_subplots(
                rows=3,
                cols=2,
                subplot_titles=['' for _ in range(6)],  # Custom titles added later
                specs=[
                    [{"secondary_y": True}, {"type": "domain"}],  # Pie chart needs domain type
                    [{"type": "bar"}, {"type": "bar"}],
                    [{"type": "bar"}, {"type": "choropleth"}]
                ],
                vertical_spacing=0.2,
                horizontal_spacing=0.15,
                row_heights=[0.4, 0.3, 0.3]
            )

            # Process time series data
            df_time = df.set_index('timestamp').sort_index()
            rolling_window = '5min'

            # 1. Cache Hit Ratio Over Time with Request Volume
            cache_ratio = df_time.resample(rolling_window).agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean'
            })

            # Add Cache Hit Ratio line
            fig.add_trace(
                go.Scatter(
                    x=cache_ratio.index,
                    y=cache_ratio['cache_status'],
                    name='Cache Hit Ratio',
                    fill='tozeroy',
                    line=dict(
                        color=self.colors['cache_hit'],
                        width=2
                    ),
                    hovertemplate=(
                        '%{x}<br>' +
                        'Hit Ratio: %{y:.1f}%<br>' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=1
            )

            # Add Request Volume on secondary axis
            fig.add_trace(
                go.Scatter(
                    x=cache_ratio.index,
                    y=cache_ratio['visits_adjusted'],
                    name='Request Volume',
                    line=dict(
                        color=self.colors['secondary'],
                        dash='dot',
                        width=1
                    ),
                    hovertemplate=(
                        '%{x}<br>' +
                        'Requests: %{y:,.0f}<br>' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=1,
                secondary_y=True
            )

            self._add_chart_annotations(
                fig, 1, 1,
                "Cache Performance Over Time",
                "Hit ratio and request volume"
            )

            # 2. Cache Status Distribution Pie Chart
            cache_dist = df['cache_status'].value_counts()
            cache_colors = [
                self.colors['cache_hit'] if status in ['hit', 'stale', 'revalidated']
                else self.colors['cache_miss'] if status == 'miss'
                else self.colors['warning'] if status == 'expired'
                else self.colors['error'] if status == 'error'
                else self.colors['secondary']
                for status in cache_dist.index
            ]

            fig.add_trace(
                go.Pie(
                    labels=cache_dist.index,
                    values=cache_dist.values,
                    name='Cache Status',
                    hole=0.4,
                    marker_colors=cache_colors,
                    textposition='outside',
                    textinfo='label+percent',
                    hovertemplate=(
                        '%{label}<br>' +
                        'Count: %{value:,}<br>' +
                        'Percentage: %{percent}<extra></extra>'
                    ),
                    textfont=self.chart_font,
                    insidetextfont=self.chart_font
                ),
                row=1, col=2
            )

            self._add_chart_annotations(
                fig, 1, 2,
                "Cache Status Distribution",
                "Breakdown of cache statuses"
            )

            # 3. Cache Performance by Content Type
            content_perf = df.groupby('content_type').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean'
            }).nlargest(10, 'visits_adjusted')

            fig.add_trace(
                go.Bar(
                    x=content_perf.index,
                    y=content_perf['cache_status'],
                    name='Hit Ratio',
                    marker_color=self.colors['cache_hit'],
                    customdata=np.stack((
                        content_perf['visits_adjusted'],
                        content_perf['origin_time_avg']
                    ), axis=1),
                    hovertemplate=(
                        '%{x}<br>' +
                        'Hit Ratio: %{y:.1f}%<br>' +
                        'Requests: %{customdata[0]:,.0f}<br>' +
                        'Origin Time: %{customdata[1]:.1f}ms' +
                        '<extra></extra>'
                    )
                ),
                row=2, col=1
            )

            self._add_chart_annotations(
                fig, 2, 1,
                "Cache Performance by Content Type",
                "Top 10 content types by request volume"
            )

            # 4. Origin Response Time by Cache Status
            cache_timing = df.groupby('cache_status').agg({
                'origin_time_avg': ['mean', 'std'],
                'visits_adjusted': 'sum',
                'origin_p95': 'mean'
            }).reset_index()

            # Add error bars for response time variability
            fig.add_trace(
                go.Bar(
                    x=cache_timing['cache_status'],
                    y=cache_timing['origin_time_avg']['mean'],
                    name='Avg Origin Time',
                    marker_color=self.colors['origin'],
                    error_y=dict(
                        type='data',
                        array=cache_timing['origin_time_avg']['std'],
                        visible=True,
                        color=self.colors['origin']
                    ),
                    customdata=np.stack((
                        cache_timing['visits_adjusted'],
                        cache_timing['origin_p95']
                    ), axis=1),
                    hovertemplate=(
                        '%{x}<br>' +
                        'Avg Time: %{y:.1f}ms<br>' +
                        'P95: %{customdata[1]:.1f}ms<br>' +
                        'Requests: %{customdata[0]:,.0f}' +
                        '<extra></extra>'
                    )
                ),
                row=2, col=2
            )

            self._add_chart_annotations(
                fig, 2, 2,
                "Response Time by Cache Status",
                "Including standard deviation"
            )

            # 5. Cache Efficiency by ASN
            if 'clientAsn' in df.columns:
                asn_cache = df.groupby('clientAsn').agg({
                    'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                    'visits_adjusted': 'sum',
                    'origin_time_avg': 'mean'
                }).nlargest(10, 'visits_adjusted')

                fig.add_trace(
                    go.Bar(
                        x=[self._truncate_asn(asn) for asn in asn_cache.index],
                        y=asn_cache['cache_status'],
                        name='Hit Ratio by ASN',
                        marker=dict(
                            color=asn_cache['cache_status'],
                            colorscale=[[0, self.colors['cache_miss']], 
                                      [1, self.colors['cache_hit']]],
                            showscale=True,
                            colorbar=dict(
                                title='Hit Ratio (%)',
                                titlefont=self.chart_font,
                                tickfont=self.chart_font
                            )
                        ),
                        customdata=np.stack((
                            asn_cache.index,  # Full ASN for hover
                            asn_cache['visits_adjusted'],
                            asn_cache['origin_time_avg']
                        ), axis=1),
                        hovertemplate=(
                            'ASN: %{customdata[0]}<br>' +
                            'Hit Ratio: %{y:.1f}%<br>' +
                            'Requests: %{customdata[1]:,.0f}<br>' +
                            'Origin Time: %{customdata[2]:.1f}ms' +
                            '<extra></extra>'
                        )
                    ),
                    row=3, col=1
                )

            self._add_chart_annotations(
                fig, 3, 1,
                "Cache Efficiency by ASN",
                "Top 10 ASNs by request volume"
            )

            # 6. Geographic Cache Performance
            geo_cache = df.groupby('country').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean'
            }).reset_index()

            fig.add_trace(
                go.Choropleth(
                    locations=geo_cache['country'],
                    z=geo_cache['cache_status'],
                    colorscale=[[0, self.colors['cache_miss']], 
                               [1, self.colors['cache_hit']]],
                    colorbar_title='Hit Ratio (%)',
                    locationmode='country names',
                    customdata=np.stack((
                        geo_cache['visits_adjusted'],
                        geo_cache['origin_time_avg']
                    ), axis=1),
                    hovertemplate=(
                        '%{location}<br>' +
                        'Hit Ratio: %{z:.1f}%<br>' +
                        'Requests: %{customdata[0]:,.0f}<br>' +
                        'Origin Time: %{customdata[1]:.1f}ms' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=2
            )

            self._add_chart_annotations(
                fig, 3, 2,
                "Geographic Cache Performance",
                "Cache hit ratio by country"
            )

            # Update layout with improved styling
            self._update_layout(fig)
            self._update_axes(fig)
            self._update_geo_layout(fig)

            return fig

        except Exception as e:
            logger.error(f"Error creating cache dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating cache dashboard")

    def _truncate_asn(self, asn: str) -> str:
        """Truncate ASN for display while keeping important information."""
        if len(asn) <= 20:
            return asn
        parts = asn.split()
        if len(parts) <= 2:
            return asn[:17] + '...'
        return f"{parts[0]} ... {parts[-1]}"

    def _update_layout(self, fig: go.Figure) -> None:
        """Apply consistent layout styling."""
        fig.update_layout(
            height=1200,
            showlegend=True,
            template='plotly_dark',
            paper_bgcolor='#1e1e1e',
            plot_bgcolor='#1e1e1e',
            margin=dict(l=60, r=60, t=100, b=60),
            legend=dict(
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=self.chart_font,
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                itemsizing='constant'
            ),
            hoverlabel=dict(
                font=self.chart_font,
                bgcolor='rgba(0,0,0,0.8)',
                bordercolor='#333'
            )
        )

    def _update_axes(self, fig: go.Figure) -> None:
        """Apply consistent axes styling."""
        fig.update_xaxes(
            gridcolor='#333',
            showgrid=True,
            griddash='dash',
            zeroline=False,
            title_font=self.title_font,
            tickfont=self.chart_font,
            title_standoff=20,
            tickangle=45
        )

        fig.update_yaxes(
            gridcolor='#333',
            showgrid=True,
            griddash='dash',
            zeroline=False,
            title_font=self.title_font,
            tickfont=self.chart_font,
            title_standoff=20
        )

    def _create_geographic_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create enhanced geographic analysis dashboard with improved readability."""
        try:
            fig = make_subplots(
                rows=3,
                cols=2,
                subplot_titles=['' for _ in range(6)],
                specs=[
                    [{"type": "scattergeo"}, {"type": "choropleth"}],
                    [{"type": "choropleth"}, {"type": "choropleth"}],
                    [{"type": "xy"}, {"type": "bar"}]
                ],
                vertical_spacing=0.2,
                horizontal_spacing=0.15,
                row_heights=[0.4, 0.4, 0.2]
            )

            # Calculate comprehensive geographic metrics
            geo_metrics = df.groupby('country').agg({
                'ttfb_avg': 'mean',
                'ttfb_p95': 'mean',
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            # 1. Response Time Map with Edge vs Origin
            # Create scattergeo for top countries by volume
            top_countries = geo_metrics.nlargest(10, 'visits_adjusted')
            
            fig.add_trace(
                go.Scattergeo(
                    locations=top_countries['country'],
                    locationmode='country names',
                    text=[
                        f"Country: {country}<br>" +
                        f"Edge TTFB: {row['ttfb_avg']:.1f}ms<br>" +
                        f"Origin: {row['origin_time_avg']:.1f}ms<br>" +
                        f"Requests: {row['visits_adjusted']:,.0f}"
                        for country, row in top_countries.iterrows()
                    ],
                    mode='markers',
                    marker=dict(
                        size=top_countries['visits_adjusted'] / top_countries['visits_adjusted'].max() * 50 + 10,
                        color=top_countries['ttfb_avg'],
                        colorscale='Viridis',
                        reversescale=True,
                        showscale=True,
                        colorbar=dict(
                            title='Response Time (ms)',
                            titlefont=self.chart_font,
                            tickfont=self.chart_font
                        )
                    ),
                    name='Top Countries'
                ),
                row=1, col=1
            )

            # 2. Request Volume Distribution
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['visits_adjusted'],
                    colorscale='Blues',
                    colorbar_title='Requests',
                    locationmode='country names',
                    customdata=np.stack((
                        geo_metrics['bytes_adjusted'] / (1024 * 1024),  # MB
                        geo_metrics['visits_adjusted'] / geo_metrics['visits_adjusted'].sum() * 100
                    ), axis=1),
                    hovertemplate=(
                        '%{location}<br>' +
                        'Requests: %{z:,.0f}<br>' +
                        'Traffic: %{customdata[0]:.1f} MB<br>' +
                        'Share: %{customdata[1]:.1f}%' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=2
            )

            # 3. Cache Hit Ratio Map
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['cache_status'],
                    colorscale=[
                        [0, self.colors['cache_miss']],
                        [0.5, '#1e1e1e'],
                        [1, self.colors['cache_hit']]
                    ],
                    colorbar_title='Cache Hit Ratio (%)',
                    locationmode='country names',
                    customdata=np.stack((
                        geo_metrics['visits_adjusted'],
                        geo_metrics['origin_time_avg']
                    ), axis=1),
                    hovertemplate=(
                        '%{location}<br>' +
                        'Hit Ratio: %{z:.1f}%<br>' +
                        'Requests: %{customdata[0]:,.0f}<br>' +
                        'Origin Time: %{customdata[1]:.1f}ms' +
                        '<extra></extra>'
                    )
                ),
                row=2, col=1
            )

            # 4. Error Rate Distribution
            geo_metrics['total_error_rate'] = (
                geo_metrics['error_rate_4xx'] + geo_metrics['error_rate_5xx']
            ) * 100

            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['total_error_rate'],
                    colorscale=[
                        [0, '#1e1e1e'],
                        [0.3, self.colors['warning']],
                        [1, self.colors['error']]
                    ],
                    colorbar_title='Error Rate (%)',
                    locationmode='country names',
                    customdata=np.stack((
                        geo_metrics['error_rate_4xx'] * 100,
                        geo_metrics['error_rate_5xx'] * 100,
                        geo_metrics['visits_adjusted']
                    ), axis=1),
                    hovertemplate=(
                        '%{location}<br>' +
                        'Total Error Rate: %{z:.2f}%<br>' +
                        '4xx Rate: %{customdata[0]:.2f}%<br>' +
                        '5xx Rate: %{customdata[1]:.2f}%<br>' +
                        'Requests: %{customdata[2]:,.0f}' +
                        '<extra></extra>'
                    )
                ),
                row=2, col=2
            )

            # 5. Time Series by Region
            df_time = df.copy()
            top_5_countries = geo_metrics.nlargest(5, 'visits_adjusted')['country']
            
            for country in top_5_countries:
                country_data = df_time[df_time['country'] == country]
                time_series = country_data.set_index('timestamp').resample('5min').agg({
                    'ttfb_avg': 'mean',
                    'origin_time_avg': 'mean',
                    'visits_adjusted': 'sum'
                }).reset_index()
                
                fig.add_trace(
                    go.Scatter(
                        x=time_series['timestamp'],
                        y=time_series['ttfb_avg'],
                        name=f'{country} Edge',
                        mode='lines',
                        line=dict(dash='solid', width=1),
                        hovertemplate=(
                            '%{x}<br>' +
                            f'{country} Edge TTFB: %{{y:.1f}}ms' +
                            '<extra></extra>'
                        )
                    ),
                    row=3, col=1
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=time_series['timestamp'],
                        y=time_series['origin_time_avg'],
                        name=f'{country} Origin',
                        mode='lines',
                        line=dict(dash='dot', width=1),
                        hovertemplate=(
                            '%{x}<br>' +
                            f'{country} Origin: %{{y:.1f}}ms' +
                            '<extra></extra>'
                        )
                    ),
                    row=3, col=1
                )

            # 6. Regional ASN Distribution
            if 'clientAsn' in df.columns:
                region_asn = df[df['country'].isin(top_5_countries)]
                asn_dist = region_asn.groupby(['country', 'clientAsn']).agg({
                    'visits_adjusted': 'sum',
                    'origin_time_avg': 'mean'
                }).reset_index()

                # Get top 3 ASNs per country
                top_asns = []
                for country in top_5_countries:
                    country_asns = asn_dist[asn_dist['country'] == country]
                    top_asns.extend(country_asns.nlargest(3, 'visits_adjusted').to_dict('records'))

                asn_df = pd.DataFrame(top_asns)

                fig.add_trace(
                    go.Bar(
                        x=[f"{row['country']}-{self._truncate_asn(row['clientAsn'])}" 
                           for _, row in asn_df.iterrows()],
                        y=asn_df['visits_adjusted'],
                        marker_color=self.colors['primary'],
                        customdata=np.stack((
                            asn_df['clientAsn'],
                            asn_df['origin_time_avg']
                        ), axis=1),
                        hovertemplate=(
                            'ASN: %{customdata[0]}<br>' +
                            'Requests: %{y:,.0f}<br>' +
                            'Origin Time: %{customdata[1]:.1f}ms' +
                            '<extra></extra>'
                        )
                    ),
                    row=3, col=2
                )

            # Add annotations
            self._add_chart_annotations(fig, 1, 1, "Response Time by Region", "Circle size indicates request volume")
            self._add_chart_annotations(fig, 1, 2, "Request Volume Distribution", "Total requests by country")
            self._add_chart_annotations(fig, 2, 1, "Cache Hit Ratio by Region", "Cache effectiveness by country")
            self._add_chart_annotations(fig, 2, 2, "Error Rate Distribution", "Combined 4xx and 5xx errors")
            self._add_chart_annotations(fig, 3, 1, "Regional Performance Trends", "Top 5 countries by volume")
            self._add_chart_annotations(fig, 3, 2, "ASN Distribution by Region", "Top 3 ASNs per country")

            # Update layout with improved styling
            self._update_layout(fig)
            self._update_axes(fig)
            self._update_geo_layout(fig)

            return fig

        except Exception as e:
            logger.error(f"Error creating geographic dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating geographic dashboard")

    def _update_geo_layout(self, fig: go.Figure) -> None:
        """Apply consistent geo layout styling."""
        fig.update_geos(
            showcoastlines=True,
            coastlinecolor='#666',
            showland=True,
            landcolor='#1e1e1e',
            showframe=False,
            showocean=True,
            oceancolor='#1e1e1e',
            showlakes=True,
            lakecolor='#1e1e1e',
            showcountries=True,
            countrycolor='#666',
            projection_type='equirectangular',
            resolution=110,
            framecolor='#666',
            bgcolor='#1e1e1e',
            lonaxis_range=[-180, 180],
            lataxis_range=[-60, 90],
            showrivers=False,
            countrywidth=0.5,
            showsubunits=False,
            subunitwidth=0.5
        )

    def _format_number(self, num: float) -> str:
        """Format numbers for display with appropriate units."""
        if num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.1f}K"
        return f"{num:.1f}"

    def _get_readable_time_unit(self, seconds: float) -> str:
        """Convert seconds to human-readable time unit."""
        if seconds >= 1:
            return f"{seconds:.2f}s"
        return f"{seconds*1000:.0f}ms"

    def _create_error_figure(self, message: str) -> go.Figure:
        """Create an enhanced error figure."""
        fig = go.Figure()
        
        fig.add_annotation(
            x=0.5,
            y=0.5,
            text=message,
            font=dict(
                family=self.chart_font['family'],
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

    def _create_endpoint_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create comprehensive endpoint performance dashboard."""
        try:
            fig = make_subplots(
                rows=3, cols=2,
                subplot_titles=(
                    'Top Endpoints Response Time',
                    'Request Volume Distribution',
                    'Endpoint Performance Over Time',
                    'Cache Hit Ratio by Endpoint',
                    'Error Distribution',
                    'Response Size Distribution'
                ),
                specs=[
                    [{"type": "bar"}, {"type": "bar"}],
                    [{"type": "xy"}, {"type": "bar"}],
                    [{"type": "bar"}, {"type": "bar"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Get top endpoints by request volume
            endpoint_metrics = df.groupby('endpoint').agg({
                'ttfb_avg': 'mean',
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'origin_p95': 'mean',
                'ttfb_p95': 'mean'
            }).reset_index()

            top_endpoints = endpoint_metrics.nlargest(10, 'visits_adjusted')

            # 1. Response Time Comparison
            fig.add_trace(
                go.Bar(
                    name='Edge TTFB',
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['ttfb_avg'],
                    marker_color=self.colors['edge'],
                    customdata=np.stack((
                        top_endpoints['ttfb_p95'],
                        top_endpoints['visits_adjusted']
                    ), axis=1),
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        'Avg TTFB: %{y:.1f}ms<br>' +
                        'P95 TTFB: %{customdata[0]:.1f}ms<br>' +
                        'Requests: %{customdata[1]:,.0f}' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=1
            )

            fig.add_trace(
                go.Bar(
                    name='Origin Time',
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['origin_time_avg'],
                    marker_color=self.colors['origin'],
                    customdata=np.stack((
                        top_endpoints['origin_p95'],
                        top_endpoints['visits_adjusted']
                    ), axis=1),
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        'Avg Origin: %{y:.1f}ms<br>' +
                        'P95 Origin: %{customdata[0]:.1f}ms<br>' +
                        'Requests: %{customdata[1]:,.0f}' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=1
            )

            # 2. Request Volume Distribution
            fig.add_trace(
                go.Bar(
                    name='Request Volume',
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['visits_adjusted'],
                    marker_color=self.colors['primary'],
                    customdata=top_endpoints['bytes_adjusted'] / (1024 * 1024),  # MB
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        'Requests: %{y:,.0f}<br>' +
                        'Traffic: %{customdata:.1f} MB' +
                        '<extra></extra>'
                    )
                ),
                row=1, col=2
            )

            # 3. Performance Over Time for top 5 endpoints
            top_5_endpoints = top_endpoints.nlargest(5, 'visits_adjusted')['endpoint']
            df_time = df[df['endpoint'].isin(top_5_endpoints)].copy()
            
            for endpoint in top_5_endpoints:
                endpoint_data = df_time[df_time['endpoint'] == endpoint]
                time_series = endpoint_data.set_index('timestamp').resample('5min').agg({
                    'origin_time_avg': 'mean',
                    'ttfb_avg': 'mean'
                }).reset_index()
                
                # Add origin time series
                fig.add_trace(
                    go.Scatter(
                        x=time_series['timestamp'],
                        y=time_series['origin_time_avg'],
                        name=f'{endpoint} (Origin)',
                        mode='lines',
                        line=dict(width=1)
                    ),
                    row=2, col=1
                )
                
                # Add edge time series
                fig.add_trace(
                    go.Scatter(
                        x=time_series['timestamp'],
                        y=time_series['ttfb_avg'],
                        name=f'{endpoint} (Edge)',
                        mode='lines',
                        line=dict(dash='dot', width=1)
                    ),
                    row=2, col=1
                )

            # 4. Cache Hit Ratio
            fig.add_trace(
                go.Bar(
                    name='Cache Hit Ratio',
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['cache_status'],
                    marker=dict(
                        color=top_endpoints['cache_status'],
                        colorscale=[[0, self.colors['cache_miss']], 
                                  [1, self.colors['cache_hit']]],
                        showscale=True,
                        colorbar=dict(title='Hit Ratio (%)')
                    ),
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        'Hit Ratio: %{y:.1f}%' +
                        '<extra></extra>'
                    )
                ),
                row=2, col=2
            )

            # 5. Error Distribution
            fig.add_trace(
                go.Bar(
                    name='4xx Errors',
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['error_rate_4xx'] * 100,
                    marker_color=self.colors['warning'],
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        '4xx Rate: %{y:.2f}%' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=1
            )

            fig.add_trace(
                go.Bar(
                    name='5xx Errors',
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['error_rate_5xx'] * 100,
                    marker_color=self.colors['error'],
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        '5xx Rate: %{y:.2f}%' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=1
            )

            # 6. Response Size Distribution
            fig.add_trace(
                go.Bar(
                    name='Response Size',
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['bytes_adjusted'] / (1024 * 1024),  # Convert to MB
                    marker_color=self.colors['secondary'],
                    customdata=top_endpoints['visits_adjusted'],
                    hovertemplate=(
                        'Endpoint: %{x}<br>' +
                        'Size: %{y:.1f} MB<br>' +
                        'Requests: %{customdata:,.0f}' +
                        '<extra></extra>'
                    )
                ),
                row=3, col=2
            )

            # Update layout
            fig.update_layout(
                height=1200,
                showlegend=True,
                template='plotly_dark',
                paper_bgcolor='#1e1e1e',
                plot_bgcolor='#1e1e1e',
                margin=dict(l=60, r=60, t=80, b=60),
                legend=dict(
                    bgcolor='rgba(0,0,0,0.5)',
                    bordercolor='#333',
                    borderwidth=1,
                    font=dict(size=12, color='white'),
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01
                ),
                barmode='group'
            )

            # Update axes
            fig.update_xaxes(gridcolor='#333', showgrid=True)
            fig.update_yaxes(gridcolor='#333', showgrid=True)

            # Update specific axes labels and rotations
            fig.update_xaxes(tickangle=45)
            
            fig.update_xaxes(title_text="Endpoint", row=1, col=1)
            fig.update_xaxes(title_text="Endpoint", row=1, col=2)
            fig.update_xaxes(title_text="Time", row=2, col=1)
            fig.update_xaxes(title_text="Endpoint", row=2, col=2)
            fig.update_xaxes(title_text="Endpoint", row=3, col=1)
            fig.update_xaxes(title_text="Endpoint", row=3, col=2)

            fig.update_yaxes(title_text="Response Time (ms)", row=1, col=1)
            fig.update_yaxes(title_text="Requests", row=1, col=2)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
            fig.update_yaxes(title_text="Cache Hit Ratio (%)", row=2, col=2)
            fig.update_yaxes(title_text="Error Rate (%)", row=3, col=1)
            fig.update_yaxes(title_text="Response Size (MB)", row=3, col=2)

            return fig

        except Exception as e:
            logger.error(f"Error creating endpoint dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating endpoint dashboard")

    def _create_error_figure(self, message: str) -> go.Figure:
        """Create an error figure with improved styling."""
        fig = go.Figure()
        
        fig.add_annotation(
            x=0.5,
            y=0.5,
            text=message,
            font=dict(size=16, color='#ffffff'),
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
        """Clean up resources."""
        try:
            # Clear all figures to free memory
            if hasattr(self, 'figures'):
                self.figures.clear()
            
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
