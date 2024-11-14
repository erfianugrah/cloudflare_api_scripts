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
<<<<<<< HEAD
=======
from .dashboards import (
    create_cache_dashboard,
    create_error_dashboard,
    create_performance_dashboard,
    create_geographic_dashboard,
    create_rps_dashboard
)
>>>>>>> 05423a1 (center legends)

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
<<<<<<< HEAD
                self.zone_figures[zone_name]['performance'] = self._create_performance_dashboard(df, analysis)
                self.zone_figures[zone_name]['cache'] = self._create_cache_dashboard(df, analysis)
                self.zone_figures[zone_name]['error'] = self._create_error_dashboard(df, analysis)
                self.zone_figures[zone_name]['geographic'] = self._create_geographic_dashboard(df, analysis)
                self.zone_figures[zone_name]['rps'] = self._create_rps_dashboard(df, analysis)
=======
                self.zone_figures[zone_name]['performance'] = create_performance_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['cache'] = create_cache_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['error'] = create_error_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['geographic'] = create_geographic_dashboard(df, analysis, self.colors)
                self.zone_figures[zone_name]['rps'] = create_rps_dashboard(df, analysis, self.colors)
>>>>>>> 05423a1 (center legends)
                
                # Create origin visualizations
                origin_figures = self.origin_visualizer.create_origin_visualizations(df, analysis, zone_name)
                self.zone_figures[zone_name].update(origin_figures)
<<<<<<< HEAD

            except Exception as e:
                logger.error(f"Error creating dashboards for zone {zone_name}: {str(e)}")
                logger.error(traceback.format_exc())

=======

            except Exception as e:
                logger.error(f"Error creating dashboards for zone {zone_name}: {str(e)}")
                logger.error(traceback.format_exc())

>>>>>>> 05423a1 (center legends)
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
<<<<<<< HEAD

            # Create dashboard for this zone
            try:
                self._create_dashboard(zone_name)
            except Exception as e:
                logger.error(f"Error creating dashboard for zone {zone_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error in visualization creation for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())

    def _create_performance_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create performance dashboard with fixed choropleth specifications."""
        try:
            fig = make_subplots(
                rows=3, cols=2,
                subplot_titles=(
                    'Edge vs Origin Response Times',
                    'Request Volume Over Time',
                    'Response Time Distribution',
                    'Protocol Performance',
                    'Top Endpoints Performance',
                    'Geographic Response Times'
                ),
                specs=[
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "choropleth"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Process time series data
            df_time = df.set_index('timestamp').sort_index()
            rolling_window = '5min'

            # Edge vs Origin Times
            fig.add_trace(
                go.Scatter(
                    x=df_time.index,
                    y=df_time['ttfb_avg'].rolling(rolling_window).mean(),
                    name='Edge TTFB',
                    line=dict(color=self.colors['edge'], width=2),
                    hovertemplate='%{y:.2f} ms<extra>Edge TTFB</extra>'
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=df_time.index,
                    y=df_time['origin_time_avg'].rolling(rolling_window).mean(),
                    name='Origin Response',
                    line=dict(color=self.colors['origin'], width=2),
                    hovertemplate='%{y:.2f} ms<extra>Origin Response</extra>'
                ),
                row=1, col=1
            )

            # Request Volume
            fig.add_trace(
                go.Scatter(
                    x=df_time.index,
                    y=df_time['requests_adjusted'].rolling(rolling_window).mean(),
                    name='Requests',
                    line=dict(color=self.colors['primary']),
                    hovertemplate='%{y:.0f} requests<extra>Request Volume</extra>'
                ),
                row=1, col=2
            )

            # Response Time Distribution
            fig.add_trace(
                go.Histogram(
                    x=df['ttfb_avg'].clip(upper=df['ttfb_avg'].quantile(0.99)),
                    name='TTFB Distribution',
                    nbinsx=50,
                    marker_color=self.colors['edge'],
                    hovertemplate='%{y} occurrences<br>%{x:.2f} ms<extra>TTFB Distribution</extra>'
                ),
                row=2, col=1
            )

            # Protocol Performance
            protocol_perf = df.groupby('protocol').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum'
            }).reset_index()
            
            fig.add_trace(
                go.Bar(
                    x=protocol_perf['protocol'],
                    y=protocol_perf['ttfb_avg'],
                    name='Protocol Performance',
                    marker_color=self.colors['primary'],
                    hovertemplate='%{x}<br>%{y:.2f} ms<extra>Protocol Performance</extra>'
                ),
                row=2, col=2
            )

            # Top Endpoints Performance
            top_endpoints = df.groupby('endpoint').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum'
            }).nlargest(10, 'requests_adjusted')
            
            fig.add_trace(
                go.Bar(
                    x=top_endpoints.index,
                    y=top_endpoints['ttfb_avg'],
                    name='Top Endpoints',
                    marker_color=self.colors['secondary'],
                    hovertemplate='%{x}<br>%{y:.2f} ms<extra>Endpoint Performance</extra>'
                ),
                row=3, col=1
            )

            # Geographic Performance - Fixed choropleth implementation
            geo_perf = df.groupby('country').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum'
            }).reset_index()
            
            fig.add_trace(
                go.Choropleth(
                    locations=geo_perf['country'],
                    z=geo_perf['ttfb_avg'],
                    colorscale='Viridis',
                    reversescale=True,
                    name='Geographic Performance',
                    colorbar_title='TTFB (ms)',
                    locationmode='country names',
                    showscale=True,
                    zmin=0,
                    zmax=geo_perf['ttfb_avg'].quantile(0.95),
                    hovertemplate='%{location}<br>TTFB: %{z:.2f}ms<extra></extra>'
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
                    x=0.01,
                    itemsizing='constant'
                )
            )

            # Update axes
            fig.update_xaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False
            )
            
            fig.update_yaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False
            )

            # Update geo layout with complete configuration
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
                bgcolor='#1e1e1e'
            )

            # Update subplot titles
            for i in fig['layout']['annotations']:
                i['font'] = dict(size=14, color='white')

            return fig

        except Exception as e:
            logger.error(f"Error creating performance dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating performance dashboard")

    def _create_cache_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create cache analysis dashboard."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Cache Hit Ratio Over Time',
                    'Cache Status Distribution',
                    'Cache Performance by Content Type',
                    'Bandwidth Distribution'
                ),
                specs=[
                    [{"secondary_y": True}, {"type": "pie"}],
                    [{"type": "bar"}, {"type": "pie"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Cache Hit Ratio Over Time
            df_time = df.set_index('timestamp').sort_index()
            cache_ratio = df_time.resample('5min').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'requests_adjusted': 'sum'
            })

            # First trace: Cache Hit Ratio
            fig.add_trace(
                go.Scatter(
                    x=cache_ratio.index,
                    y=cache_ratio['cache_status'],
                    name='Cache Hit Ratio',
                    fill='tozeroy',
                    line=dict(color=self.colors['cache_hit']),
                    hovertemplate='%{y:.1f}%<extra>Cache Hit Ratio</extra>'
                ),
                row=1, col=1
            )

            # Second trace: Request Volume
            fig.add_trace(
                go.Scatter(
                    x=cache_ratio.index,
                    y=cache_ratio['requests_adjusted'],
                    name='Request Volume',
                    line=dict(color=self.colors['secondary'], dash='dot'),
                    hovertemplate='%{y:.0f} requests<extra>Request Volume</extra>'
                ),
                row=1, col=1,
                secondary_y=True
            )

            # Cache Status Distribution
            cache_dist = df['cache_status'].value_counts()
            fig.add_trace(
                go.Pie(
                    labels=cache_dist.index,
                    values=cache_dist.values,
                    name='Cache Status',
                    hole=0.4,
                    marker_colors=[
                        self.colors['cache_hit'],  # hit
                        self.colors['warning'],    # miss
                        self.colors['error'],      # error
                        self.colors['secondary']   # other
                    ],
                    textposition='outside',
                    textinfo='label+percent',
                    hovertemplate="%{label}<br>%{value:,.0f} requests<br>%{percent:.1f}%<extra></extra>",
                    rotation=90,
                    textfont=dict(size=12, color='white')
                ),
                row=1, col=2
            )

            # Cache Performance by Content Type
            content_type_perf = df.groupby('content_type').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'requests_adjusted': 'sum'
            }).reset_index()

            # Sort by request volume and get top 10
            content_type_perf = content_type_perf.nlargest(10, 'requests_adjusted')

            fig.add_trace(
                go.Bar(
                    x=content_type_perf['content_type'],
                    y=content_type_perf['cache_status'],
                    name='Content Type Hit Ratio',
                    marker_color=self.colors['cache_hit'],
                    hovertemplate='%{x}<br>Hit Ratio: %{y:.1f}%<br>%{text}<extra></extra>',
                    text=[f"{x:,.0f} requests" for x in content_type_perf['requests_adjusted']],
                    textposition='auto'
                ),
                row=2, col=1
            )

            # Bandwidth Distribution
            bandwidth = df.groupby('cache_status').agg({
                'bytes_adjusted': 'sum'
            })
            
            bandwidth_gb = bandwidth['bytes_adjusted'] / (1024 ** 3)  # Convert to GB

            fig.add_trace(
                go.Pie(
                    labels=bandwidth.index,
                    values=bandwidth_gb,
                    name='Bandwidth',
                    hole=0.4,
                    marker_colors=[
                        self.colors['cache_hit'],  # hit
                        self.colors['warning'],    # miss
                        self.colors['error'],      # error
                        self.colors['secondary']   # other
                    ],
                    textposition='outside',
                    textinfo='label+percent',
                    hovertemplate="%{label}<br>%{value:.2f} GB<br>%{percent:.1f}%<extra></extra>",
                    rotation=90,
                    textfont=dict(size=12, color='white')
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=1000,
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
                    x=0.01,
                    itemsizing='constant'
                )
            )

            # Update axes
            fig.update_xaxes(
                title_text="Time",
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False,
                row=1, col=1
            )

            fig.update_xaxes(
                title_text="Content Type",
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False,
                tickangle=45,
                row=2, col=1
            )

            fig.update_yaxes(
                title_text="Cache Hit Ratio (%)",
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False,
                secondary_y=False,
                row=1, col=1
            )

            fig.update_yaxes(
                title_text="Requests",
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False,
                secondary_y=True,
                row=1, col=1
            )

            fig.update_yaxes(
                title_text="Cache Hit Ratio (%)",
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False,
                range=[0, 100],  # Force y-axis to show 0-100%
                row=2, col=1
            )

            # Update subplot titles
            for i in fig['layout']['annotations']:
                i['font'] = dict(size=14, color='white')

            return fig

        except Exception as e:
            logger.error(f"Error creating cache dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating cache dashboard")

    def _create_error_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create error analysis dashboard with fixed choropleth."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Error Rate Over Time',
                    'Error Status Distribution',
                    'Top Error Endpoints',
                    'Geographic Error Distribution'
                ),
                specs=[
                    [{"secondary_y": True}, {"type": "bar"}],
                    [{"type": "bar"}, {"type": "choropleth"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Error Rate Over Time
            df_time = df.set_index('timestamp').sort_index()
            error_rates = df_time.resample('5min').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum'
            })

            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_4xx'] * 100,
                    name='4xx Errors',
                    line=dict(color=self.colors['warning']),
                    hovertemplate='%{y:.2f}%<extra>4xx Error Rate</extra>'
                ),
                row=1, col=1
            )

            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_5xx'] * 100,
                    name='5xx Errors',
                    line=dict(color=self.colors['error']),
                    hovertemplate='%{y:.2f}%<extra>5xx Error Rate</extra>'
                ),
                row=1, col=1
            )

            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['requests_adjusted'],
                    name='Request Volume',
                    line=dict(color=self.colors['secondary'], dash='dot'),
                    hovertemplate='%{y:.0f} requests<extra>Request Volume</extra>'
                ),
                row=1, col=1,
                secondary_y=True
            )

            # Error Status Distribution
            error_df = df[df['status'] >= 400]
            status_dist = error_df['status'].value_counts().sort_index()
            
            fig.add_trace(
                go.Bar(
                    x=status_dist.index.astype(str),
                    y=status_dist.values,
                    name='Error Distribution',
                    marker_color=[self.colors['warning'] if s < 500 else self.colors['error'] 
                                for s in status_dist.index],
                    hovertemplate='Status %{x}<br>Count: %{y:,}<extra></extra>'
                ),
                row=1, col=2
            )

            # Top Error Endpoints
            endpoint_errors = df.groupby('endpoint').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum'
            }).nlargest(10, 'requests_adjusted')

            fig.add_trace(
                go.Bar(
                    x=endpoint_errors.index,
                    y=endpoint_errors['error_rate_5xx'] * 100,
                    name='5xx Error Rate',
                    marker_color=self.colors['error'],
                    hovertemplate='%{x}<br>5xx Rate: %{y:.2f}%<extra></extra>'
                ),
                row=2, col=1
            )

            fig.add_trace(
                go.Bar(
                    x=endpoint_errors.index,
                    y=endpoint_errors['error_rate_4xx'] * 100,
                    name='4xx Error Rate',
                    marker_color=self.colors['warning'],
                    hovertemplate='%{x}<br>4xx Rate: %{y:.2f}%<extra></extra>'
                ),
                row=2, col=1
            )

            # Geographic Error Distribution - Fixed choropleth implementation
            geo_errors = df.groupby('country').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum'
            }).reset_index()

            total_error_rate = geo_errors['error_rate_4xx'] + geo_errors['error_rate_5xx']
            
            fig.add_trace(
                go.Choropleth(
                    locations=geo_errors['country'],
                    z=total_error_rate * 100,
                    colorscale='Reds',
                    name='Error Rate by Country',
                    colorbar_title='Error Rate (%)',
                    locationmode='country names',
                    showscale=True,
                    zmin=0,
                    zmax=min(100, total_error_rate.max() * 100),
                    hovertemplate='%{location}<br>Error Rate: %{z:.2f}%<extra></extra>'
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=1000,
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
                    x=0.01,
                    itemsizing='constant'
                ),
                barmode='stack'
            )

            # Update axes
            fig.update_xaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False
            )
            
            fig.update_yaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False
            )

            # Update geo layout with complete configuration
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
                bgcolor='#1e1e1e'
            )

            # Update subplot titles
            for i in fig['layout']['annotations']:
                i['font'] = dict(size=14, color='white')

            return fig

        except Exception as e:
            logger.error(f"Error creating error dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating error dashboard")

    def _create_geographic_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create geographic analysis dashboard with properly configured choropleth maps."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Geographic Performance Heatmap',
                    'Top Countries by Traffic',
                    'Response Time by Country',
                    'Error Rates by Country'
                ),
                specs=[
                    [{"type": "choropleth"}, {"type": "bar"}],
                    [{"type": "bar"}, {"type": "bar"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Calculate geographic metrics
            geo_metrics = df.groupby('country').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100
            }).reset_index()

            # Geographic Performance Heatmap
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['ttfb_avg'],
                    colorscale='Viridis',
                    reversescale=True,
                    name='Response Time',
                    colorbar_title='TTFB (ms)',
                    locationmode='country names',
                    showscale=True,
                    zmin=0,
                    zmax=geo_metrics['ttfb_avg'].quantile(0.95),
                    hovertemplate='%{location}<br>TTFB: %{z:.2f}ms<extra></extra>'
                ),
                row=1, col=1
            )

            # Top Countries by Traffic
            top_countries = geo_metrics.nlargest(10, 'requests_adjusted')
            fig.add_trace(
                go.Bar(
                    x=top_countries['country'],
                    y=top_countries['requests_adjusted'],
                    name='Request Volume',
                    marker_color=self.colors['primary'],
                    hovertemplate='%{x}<br>Requests: %{y:,.0f}<extra></extra>'
                ),
                row=1, col=2
            )

            # Response Time by Country
            fig.add_trace(
                go.Bar(
                    x=top_countries['country'],
                    y=top_countries['ttfb_avg'],
                    name='Response Time',
                    marker_color=self.colors['edge'],
                    hovertemplate='%{x}<br>TTFB: %{y:.2f}ms<extra></extra>'
                ),
                row=2, col=1
            )

            # Error Rates by Country
            error_rates = top_countries['error_rate_4xx'] + top_countries['error_rate_5xx']
            fig.add_trace(
                go.Bar(
                    x=top_countries['country'],
                    y=error_rates * 100,
                    name='Error Rate',
                    marker_color=self.colors['error'],
                    hovertemplate='%{x}<br>Error Rate: %{y:.2f}%<extra></extra>'
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=1000,
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
                    x=0.01,
                    itemsizing='constant'
                )
            )

            # Update axes
            fig.update_xaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False,
                tickangle=45
            )
            
            fig.update_yaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False
            )

            # Update geo layout with complete configuration
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
                bgcolor='#1e1e1e'
            )

            # Add axis titles
            fig.update_yaxes(title_text="Requests", row=1, col=2)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=2)

            # Update subplot titles
            for i in fig['layout']['annotations']:
                i['font'] = dict(size=14, color='white')

            return fig

        except Exception as e:
            logger.error(f"Error creating geographic dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating geographic dashboard")
=======

            # Create dashboard for this zone
            try:
                self._create_dashboard(zone_name)
            except Exception as e:
                logger.error(f"Error creating dashboard for zone {zone_name}: {str(e)}")

        except Exception as e:
            logger.error(f"Error in visualization creation for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())
>>>>>>> 05423a1 (center legends)

    def _create_dashboard(self, zone_name: str) -> None:
        """Create zone-specific dashboard including origin metrics."""
        try:
            zone_figures = self.zone_figures.get(zone_name, {})
<<<<<<< HEAD
            port = 8050 + abs(hash(zone_name)) % 1000  # Keep port number reasonable
            
            logger.info(f"Creating dashboard for zone {zone_name} on port {port}")

            # Create new Dash app instance for this zone
            app = Dash(
                f"cloudflare-analytics-{zone_name}",
                external_stylesheets=[dbc.themes.DARKLY]
            )
=======
            port = 8050 + abs(hash(zone_name)) % 1000
            
            logger.info(f"Creating dashboard for zone {zone_name} on port {port}")

>>>>>>> 05423a1 (center legends)
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

<<<<<<< HEAD
=======
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

>>>>>>> 05423a1 (center legends)
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
<<<<<<< HEAD
                                dcc.Graph(
                                    figure=zone_figures.get('rps', self._create_error_figure("No RPS data available")),
                                    style={'height': '85vh'}
=======
                                create_graph_container(
                                    zone_figures.get('rps', self._create_error_figure("No RPS data available")),
                                    "Requests Per Second Analysis"
>>>>>>> 05423a1 (center legends)
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
<<<<<<< HEAD
                        # Origin Analysis Tabs
                        dcc.Tab(
                            label='Origin Response Time',
                            children=[
                                dcc.Graph(
                                    figure=zone_figures.get('origin_response_time', self._create_error_figure("No origin response time data available")),
                                    style={'height': '85vh'}
=======
                        dcc.Tab(
                            label='Origin Response Time',
                            children=[
                                create_graph_container(
                                    zone_figures.get('origin_response_time', self._create_error_figure("No origin response time data available")),
                                    "Origin Response Time Analysis"
>>>>>>> 05423a1 (center legends)
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin ASN Analysis',
                            children=[
<<<<<<< HEAD
                                dcc.Graph(
                                    figure=zone_figures.get('origin_asn', self._create_error_figure("No ASN analysis data available")),
                                    style={'height': '85vh'}
=======
                                create_graph_container(
                                    zone_figures.get('origin_asn', self._create_error_figure("No ASN analysis data available")),
                                    "Origin ASN Analysis"
>>>>>>> 05423a1 (center legends)
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Errors',
                            children=[
<<<<<<< HEAD
                                dcc.Graph(
                                    figure=zone_figures.get('origin_error', self._create_error_figure("No origin error data available")),
                                    style={'height': '85vh'}
=======
                                create_graph_container(
                                    zone_figures.get('origin_error', self._create_error_figure("No origin error data available")),
                                    "Origin Error Analysis"
>>>>>>> 05423a1 (center legends)
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Geographic',
                            children=[
<<<<<<< HEAD
                                dcc.Graph(
                                    figure=zone_figures.get('origin_geographic', self._create_error_figure("No origin geographic data available")),
                                    style={'height': '85vh'}
=======
                                create_graph_container(
                                    zone_figures.get('origin_geographic', self._create_error_figure("No origin geographic data available")),
                                    "Origin Geographic Analysis"
>>>>>>> 05423a1 (center legends)
                                )
                            ],
                            style=tab_style,
                            selected_style=selected_tab_style
                        ),
                        dcc.Tab(
                            label='Origin Endpoints',
                            children=[
<<<<<<< HEAD
                                dcc.Graph(
                                    figure=zone_figures.get('origin_endpoints', self._create_error_figure("No origin endpoint data available")),
                                    style={'height': '85vh'}
=======
                                create_graph_container(
                                    zone_figures.get('origin_endpoints', self._create_error_figure("No origin endpoint data available")),
                                    "Origin Endpoints Analysis"
>>>>>>> 05423a1 (center legends)
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
<<<<<<< HEAD

            @self.app.callback(
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
=======
>>>>>>> 05423a1 (center legends)

            logger.info(f"Starting dashboard for zone {zone_name}")
            self.app.run_server(
                debug=False,
                port=port,  # Use single port since we're not creating multiple instances
                use_reloader=False,
                dev_tools_hot_reload=False,
                host='127.0.0.1'
            )

        except Exception as e:
            logger.error(f"Error creating dashboard for zone {zone_name}: {str(e)}")
            logger.error(traceback.format_exc())

<<<<<<< HEAD
    def _create_rps_dashboard(self, df: pd.DataFrame, analysis: dict) -> go.Figure:
        """Create requests per second (RPS) analysis dashboard with fixed choropleth."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Requests per Second Over Time',
                    'RPS by HTTP Method',
                    'Top Endpoints by RPS',
                    'RPS Geographic Distribution'
                ),
                specs=[
                    [{"secondary_y": True}, {"type": "bar"}],
                    [{"type": "bar"}, {"type": "choropleth"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Calculate RPS over time
            df_time = df.set_index('timestamp').sort_index()
            rps = df_time.resample('1min').agg({
                'requests_adjusted': 'sum',
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            })
            
            # Convert to requests per second
            rps['rps'] = rps['requests_adjusted'] / 60

            # RPS Over Time
            fig.add_trace(
                go.Scatter(
                    x=rps.index,
                    y=rps['rps'],
                    name='RPS',
                    line=dict(color=self.colors['primary'], width=2),
                    hovertemplate='%{y:.1f} req/s<extra>RPS</extra>'
                ),
                row=1, col=1
            )

            # Add cache hit ratio on secondary y-axis
            fig.add_trace(
                go.Scatter(
                    x=rps.index,
                    y=rps['cache_status'],
                    name='Cache Hit Ratio',
                    line=dict(color=self.colors['cache_hit'], dash='dot'),
                    hovertemplate='%{y:.1f}%<extra>Cache Hit Ratio</extra>'
                ),
                row=1, col=1,
                secondary_y=True
            )

            # RPS by HTTP Method
            method_rps = df.groupby('clientRequestMethod').agg({
                'requests_adjusted': 'sum'
            }).reset_index()
            method_rps['rps'] = method_rps['requests_adjusted'] / (df['timestamp'].max() - df['timestamp'].min()).total_seconds()

            fig.add_trace(
                go.Bar(
                    x=method_rps['clientRequestMethod'],
                    y=method_rps['rps'],
                    name='RPS by Method',
                    marker_color=self.colors['secondary'],
                    hovertemplate='%{x}<br>%{y:.1f} req/s<extra>RPS by Method</extra>'
                ),
                row=1, col=2
            )

            # Top Endpoints by RPS
            endpoint_rps = df.groupby('endpoint').agg({
                'requests_adjusted': 'sum'
            }).reset_index()
            endpoint_rps['rps'] = endpoint_rps['requests_adjusted'] / (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            top_endpoints = endpoint_rps.nlargest(10, 'rps')

            fig.add_trace(
                go.Bar(
                    x=top_endpoints['endpoint'],
                    y=top_endpoints['rps'],
                    name='Top Endpoints',
                    marker_color=self.colors['primary'],
                    hovertemplate='%{x}<br>%{y:.1f} req/s<extra>RPS by Endpoint</extra>'
                ),
                row=2, col=1
            )

            # Geographic RPS Distribution - Fixed choropleth implementation
            geo_rps = df.groupby('country').agg({
                'requests_adjusted': 'sum'
            }).reset_index()
            total_seconds = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
            geo_rps['rps'] = geo_rps['requests_adjusted'] / total_seconds

            fig.add_trace(
                go.Choropleth(
                    locations=geo_rps['country'],
                    z=geo_rps['rps'],
                    colorscale='Viridis',
                    name='RPS by Country',
                    colorbar_title='Requests/Second',
                    locationmode='country names',
                    showscale=True,
                    zmin=0,
                    zmax=geo_rps['rps'].quantile(0.95),
                    hovertemplate='%{location}<br>%{z:.1f} req/s<extra>RPS by Country</extra>'
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=1000,
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
                    x=0.01,
                    itemsizing='constant'
                )
            )

            # Update axes
            fig.update_xaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False,
                tickangle=45
            )
            
            fig.update_yaxes(
                title_font=dict(size=14, color='white'),
                tickfont=dict(size=12, color='white'),
                gridcolor='#333',
                title_standoff=20,
                zeroline=False
            )

            # Update axis titles
            fig.update_yaxes(title_text="Requests/Second", row=1, col=1, secondary_y=False)
            fig.update_yaxes(title_text="Cache Hit Ratio (%)", row=1, col=1, secondary_y=True)
            fig.update_yaxes(title_text="Requests/Second", row=1, col=2)
            fig.update_yaxes(title_text="Requests/Second", row=2, col=1)

            # Update geo layout with complete configuration
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
                bgcolor='#1e1e1e'
            )

            # Update subplot titles
            for i in fig['layout']['annotations']:
                i['font'] = dict(size=14, color='white')

            return fig

        except Exception as e:
            logger.error(f"Error creating RPS dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating RPS dashboard")

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

=======
>>>>>>> 05423a1 (center legends)
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

<<<<<<< HEAD
=======
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

>>>>>>> 05423a1 (center legends)
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
