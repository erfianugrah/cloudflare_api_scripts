import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional
from datetime import datetime
import traceback

logger = logging.getLogger(__name__)

class Visualizer:
    """Modern visualizer for Cloudflare analytics using interactive Plotly charts."""
    
    def __init__(self, config):
        self.config = config
        # Enhanced color palette with better contrast and accessibility
        self.colors = {
            'edge': '#2E86C1',         # Strong blue
            'origin': '#E74C3C',       # Clear red
            'cache_hit': '#27AE60',    # Vibrant green
            'cache_miss': '#95A5A6',   # Medium gray
            'error': '#F39C12',        # Bright orange
            'warning': '#F1C40F',      # Yellow
            'success': '#2ECC71',      # Light green
            'primary': '#3498DB',      # Light blue
            'secondary': '#9B59B6',    # Purple
            'background': '#F8F9FA',   # Light background
            'grid': '#DFE4EA'          # Light gray grid
        }
        
        # Color sequences for consistent data visualization
        self.color_sequences = {
            'main': [
                self.colors['edge'],
                self.colors['origin'],
                self.colors['cache_hit'],
                self.colors['error']
            ],
            'status': [
                self.colors['success'],
                self.colors['warning'],
                self.colors['error']
            ],
            'cache': [
                self.colors['cache_hit'],
                self.colors['cache_miss'],
                self.colors['error']
            ]
        }

    def create_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str) -> None:
        """Create all visualizations for the analysis results."""
        try:
            if df is None or df.empty or not analysis:
                logger.error("No data available for visualization")
                return

            output_dir = self.config.images_dir / zone_name
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create main performance dashboard
            logger.info("Generating performance dashboard...")
            perf_dashboard = self._create_performance_dashboard(df, analysis)
            perf_dashboard.write_html(str(output_dir / 'performance_dashboard.html'))
            perf_dashboard.write_image(str(output_dir / 'performance_dashboard.png'), 
                                     width=1920, height=1080, scale=2)

            # Create cache analysis dashboard
            logger.info("Generating cache dashboard...")
            cache_dashboard = self._create_cache_dashboard(df, analysis)
            cache_dashboard.write_html(str(output_dir / 'cache_dashboard.html'))
            cache_dashboard.write_image(str(output_dir / 'cache_dashboard.png'), 
                                      width=1920, height=1080, scale=2)

            # Create error analysis dashboard
            logger.info("Generating error dashboard...")
            error_dashboard = self._create_error_dashboard(df, analysis)
            error_dashboard.write_html(str(output_dir / 'error_dashboard.html'))
            error_dashboard.write_image(str(output_dir / 'error_dashboard.png'), 
                                      width=1920, height=1080, scale=2)

            # Create geographic analysis dashboard
            logger.info("Generating geographic dashboard...")
            geo_dashboard = self._create_geographic_dashboard(df, analysis)
            geo_dashboard.write_html(str(output_dir / 'geographic_dashboard.html'))
            geo_dashboard.write_image(str(output_dir / 'geographic_dashboard.png'), 
                                    width=1920, height=1080, scale=2)

            logger.info(f"Successfully generated all visualizations for {zone_name}")

        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            logger.error(traceback.format_exc())

    def _create_performance_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create main performance dashboard."""
        try:
            fig = make_subplots(
                rows=3, cols=2,
                subplot_titles=(
                    'Edge vs Origin Response Times',
                    'Request Volume Over Time',
                    'Protocol Performance',
                    'Top Endpoints by Traffic',
                    'Response Time Distribution',
                    'Traffic Patterns by Hour'
                ),
                specs=[
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Edge vs Origin Response Times
            df_time = df.set_index('timestamp').sort_index()
            rolling_window = '5min'
            edge_time = df_time['ttfb_avg'].rolling(rolling_window).mean()
            origin_time = df_time['origin_time_avg'].rolling(rolling_window).mean()

            fig.add_trace(
                go.Scatter(
                    x=df_time.index,
                    y=edge_time,
                    name='Edge TTFB',
                    line=dict(color=self.colors['edge'], width=2),
                    hovertemplate='%{y:.1f}ms'
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=df_time.index,
                    y=origin_time,
                    name='Origin Response',
                    line=dict(color=self.colors['origin'], width=2),
                    hovertemplate='%{y:.1f}ms'
                ),
                row=1, col=1
            )

            # Request Volume
            volume = df_time.resample(rolling_window).agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            })
            
            fig.add_trace(
                go.Scatter(
                    x=volume.index,
                    y=volume['requests_adjusted'],
                    name='Requests',
                    line=dict(color=self.colors['primary'], width=2),
                    hovertemplate='%{y:,.0f}'
                ),
                row=1, col=2
            )
            fig.add_trace(
                go.Scatter(
                    x=volume.index,
                    y=volume['visits_adjusted'],
                    name='Unique Visitors',
                    line=dict(color=self.colors['secondary'], width=2),
                    hovertemplate='%{y:,.0f}'
                ),
                row=1, col=2
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
                    name='Response Time',
                    marker_color=self.colors['edge'],
                    hovertemplate='%{y:.1f}ms',
                    showlegend=False
                ),
                row=2, col=1
            )

            # Top Endpoints
            top_endpoints = df.groupby('endpoint').agg({
                'requests_adjusted': 'sum',
                'ttfb_avg': 'mean'
            }).nlargest(10, 'requests_adjusted')
            
            fig.add_trace(
                go.Bar(
                    x=top_endpoints.index,
                    y=top_endpoints['requests_adjusted'],
                    name='Top Endpoints',
                    marker_color=self.colors['primary'],
                    hovertemplate='%{y:,.0f} requests',
                    showlegend=False
                ),
                row=2, col=2
            )

            # Response Time Distribution
            fig.add_trace(
                go.Histogram(
                    x=df['ttfb_avg'],
                    name='TTFB Distribution',
                    marker_color=self.colors['edge'],
                    nbinsx=50,
                    hovertemplate='%{y:,.0f} samples<br>%{x:.1f}ms',
                    showlegend=False
                ),
                row=3, col=1
            )

            # Hourly Traffic Pattern
            hourly_traffic = df.groupby(df['timestamp'].dt.hour).agg({
                'requests_adjusted': 'mean'
            }).reset_index()
            
            fig.add_trace(
                go.Scatter(
                    x=hourly_traffic['timestamp'],
                    y=hourly_traffic['requests_adjusted'],
                    name='Hourly Traffic',
                    mode='lines+markers',
                    line=dict(color=self.colors['primary'], width=2),
                    marker=dict(size=8),
                    hovertemplate='Hour %{x}: %{y:,.0f} requests',
                    showlegend=False
                ),
                row=3, col=2
            )

            # Update layout and formatting
            fig.update_layout(
                title={
                    'text': "Performance Dashboard",
                    'y':0.98,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                template='plotly_white',
                height=1200,
                font=dict(size=12),
                hovermode='x unified'
            )

            # Update axes labels and formatting
            fig.update_xaxes(title_text="Time", row=1, col=1)
            fig.update_xaxes(title_text="Time", row=1, col=2)
            fig.update_xaxes(title_text="Protocol", row=2, col=1)
            fig.update_xaxes(title_text="Endpoint", row=2, col=2, tickangle=45)
            fig.update_xaxes(title_text="Response Time (ms)", row=3, col=1)
            fig.update_xaxes(title_text="Hour of Day", row=3, col=2)

            fig.update_yaxes(title_text="Response Time (ms)", row=1, col=1)
            fig.update_yaxes(title_text="Requests", row=1, col=2)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
            fig.update_yaxes(title_text="Requests", row=2, col=2)
            fig.update_yaxes(title_text="Count", row=3, col=1)
            fig.update_yaxes(title_text="Average Requests", row=3, col=2)

            return fig

        except Exception as e:
            logger.error(f"Error creating performance dashboard: {str(e)}")
            raise

    def _create_cache_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
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
                    [{"type": "xy"}, {"type": "domain"}],
                    [{"type": "xy"}, {"type": "domain"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Cache Hit Ratio Over Time
            df_time = df.set_index('timestamp').sort_index()
            cache_ratio = df_time.resample('5min').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100
            })
            
            fig.add_trace(
                go.Scatter(
                    x=cache_ratio.index,
                    y=cache_ratio['cache_status'],
                    name='Cache Hit Ratio',
                    fill='tozeroy',
                    line=dict(color=self.colors['cache_hit'], width=2),
                    hovertemplate='%{y:.1f}%'
                ),
                row=1, col=1
            )

            # Cache Status Distribution
            cache_dist = df.groupby('cache_status').size()
            fig.add_trace(
                go.Pie(
                    labels=cache_dist.index,
                    values=cache_dist.values,
                    name='Cache Status',
                    marker_colors=self.color_sequences['cache'],
                    textinfo='label+percent',
                    hovertemplate='%{label}<br>%{value:,} requests<br>%{percent}'
                ),
                row=1, col=2
            )

            # Cache Performance by Content Type
            content_cache = df.groupby('content_type').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'requests_adjusted': 'sum'
            }).nlargest(10, 'requests_adjusted')
            
            fig.add_trace(
                go.Bar(
                    x=content_cache.index,
                    y=content_cache['cache_status'],
                    name='Hit Ratio by Content',
                    marker_color=self.colors['cache_hit'],
                    hovertemplate='%{y:.1f}%',
                    showlegend=False
                ),
                row=2, col=1
            )

            # Bandwidth Distribution
            bandwidth = df.groupby('cache_status').agg({
                'bytes_adjusted': 'sum'
            })
            
            fig.add_trace(
                go.Pie(
                    labels=bandwidth.index,
                    values=bandwidth['bytes_adjusted'],
                    name='Bandwidth',
                    marker_colors=self.color_sequences['cache'],
                    textinfo='label+percent',
                    hovertemplate='%{label}<br>%{value:,} bytes<br>%{percent}'
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                title={
                    'text': "Cache Analysis Dashboard",
                    'y':0.98,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                template='plotly_white',
                height=1000,
                font=dict(size=12),
                hovermode='x unified'
            )

            # Update axes
            fig.update_xaxes(title_text="Time", row=1, col=1)
            fig.update_xaxes(title_text="Content Type", row=2, col=1, tickangle=45)
            
            fig.update_yaxes(title_text="Cache Hit Ratio (%)", row=1, col=1)
            fig.update_yaxes(title_text="Cache Hit Ratio (%)", row=1, col=1)
            fig.update_yaxes(title_text="Cache Hit Ratio (%)", row=2, col=1)

            return fig

        except Exception as e:
            logger.error(f"Error creating cache dashboard: {str(e)}")
            raise

    def _create_error_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create error analysis dashboard."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Error Rate Over Time',
                    'Error Status Distribution',
                    'Top Endpoints by Error Rate',
                    'Response Time by Status'
                ),
                specs=[
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Error Rate Over Time
            df_time = df.set_index('timestamp').sort_index()
            error_rates = df_time.resample('5min').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }) * 100
            
            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_4xx'],
                    name='4xx Errors',
                    line=dict(color=self.colors['warning'], width=2),
                    hovertemplate='%{y:.2f}%'
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_5xx'],
                    name='5xx Errors',
                    line=dict(color=self.colors['error'], width=2),
                    hovertemplate='%{y:.2f}%'
                ),
                row=1, col=1
            )

            # Error Status Distribution
            status_dist = df[df['status'] >= 400]['status'].value_counts().sort_index()
            colors = [self.colors['warning'] if s < 500 else self.colors['error'] 
                     for s in status_dist.index]
            
            fig.add_trace(
                go.Bar(
                    x=status_dist.index.astype(str),
                    y=status_dist.values,
                    name='Error Distribution',
                    marker_color=colors,
                    hovertemplate='Status %{x}<br>Count: %{y:,}',
                    showlegend=False
                ),
                row=1, col=2
            )

            # Top Endpoints by Error Rate
            endpoint_errors = df.groupby('endpoint').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum'
            }).nlargest(10, 'requests_adjusted')
            
            fig.add_trace(
                go.Bar(
                    x=endpoint_errors.index,
                    y=endpoint_errors['error_rate_4xx'] * 100,
                    name='4xx Rate',
                    marker_color=self.colors['warning'],
                    hovertemplate='%{x}<br>4xx Rate: %{y:.2f}%'
                ),
                row=2, col=1
            )
            fig.add_trace(
                go.Bar(
                    x=endpoint_errors.index,
                    y=endpoint_errors['error_rate_5xx'] * 100,
                    name='5xx Rate',
                    marker_color=self.colors['error'],
                    hovertemplate='%{x}<br>5xx Rate: %{y:.2f}%'
                ),
                row=2, col=1
            )

            # Response Time by Status
            status_groups = ['2xx', '3xx', '4xx', '5xx']
            for status_group in status_groups:
                if status_group == '2xx':
                    mask = df['status'].between(200, 299)
                    color = self.colors['success']
                elif status_group == '3xx':
                    mask = df['status'].between(300, 399)
                    color = self.colors['primary']
                elif status_group == '4xx':
                    mask = df['status'].between(400, 499)
                    color = self.colors['warning']
                else:  # 5xx
                    mask = df['status'].between(500, 599)
                    color = self.colors['error']
                
                if mask.any():
                    fig.add_trace(
                        go.Box(
                            name=status_group,
                            y=df[mask]['ttfb_avg'],
                            boxmean=True,
                            marker_color=color,
                            hovertemplate='Status: %{x}<br>Response Time: %{y:.1f}ms',
                            showlegend=False
                        ),
                        row=2, col=2
                    )

            # Update layout
            fig.update_layout(
                title={
                    'text': "Error Analysis Dashboard",
                    'y':0.98,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                template='plotly_white',
                height=1000,
                font=dict(size=12),
                hovermode='x unified',
                barmode='group'
            )

            # Update axes
            fig.update_xaxes(title_text="Time", row=1, col=1)
            fig.update_xaxes(title_text="Status Code", row=1, col=2)
            fig.update_xaxes(title_text="Endpoint", row=2, col=1, tickangle=45)
            fig.update_xaxes(title_text="Status Group", row=2, col=2)
            
            fig.update_yaxes(title_text="Error Rate (%)", row=1, col=1)
            fig.update_yaxes(title_text="Count", row=1, col=2)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=1)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=2)

            return fig

        except Exception as e:
            logger.error(f"Error creating error dashboard: {str(e)}")
            raise

    def _create_geographic_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create geographic analysis dashboard."""
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
                    [{"type": "choropleth"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}]
                ],
                vertical_spacing=0.15,
                horizontal_spacing=0.12
            )

            # Geographic Performance Heatmap
            geo_metrics = df.groupby('country').agg({
                'ttfb_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['ttfb_avg'],
                    colorscale='RdYlBu_r',
                    showscale=True,
                    locationmode='country names',
                    name='Response Time',
                    colorbar_title='TTFB (ms)',
                    hovertemplate='%{location}<br>TTFB: %{z:.1f}ms'
                ),
                row=1, col=1
            )

            # Top Countries by Traffic
            top_countries = geo_metrics.nlargest(10, 'visits_adjusted')
            fig.add_trace(
                go.Bar(
                    x=top_countries['country'],
                    y=top_countries['visits_adjusted'],
                    name='Traffic Volume',
                    marker_color=self.colors['primary'],
                    hovertemplate='%{x}<br>Visits: %{y:,.0f}',
                    showlegend=False
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
                    hovertemplate='%{x}<br>TTFB: %{y:.1f}ms',
                    showlegend=False
                ),
                row=2, col=1
            )

            # Error Rates by Country
            country_errors = df.groupby('country').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')
            
            fig.add_trace(
                go.Bar(
                    x=country_errors.index,
                    y=country_errors['error_rate_4xx'] * 100,
                    name='4xx Errors',
                    marker_color=self.colors['warning'],
                    hovertemplate='%{x}<br>4xx Rate: %{y:.2f}%'
                ),
                row=2, col=2
            )
            fig.add_trace(
                go.Bar(
                    x=country_errors.index,
                    y=country_errors['error_rate_5xx'] * 100,
                    name='5xx Errors',
                    marker_color=self.colors['error'],
                    hovertemplate='%{x}<br>5xx Rate: %{y:.2f}%'
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                title={
                    'text': "Geographic Analysis Dashboard",
                    'y':0.98,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                template='plotly_white',
                height=1000,
                font=dict(size=12),
                hovermode='x unified',
                barmode='group'
            )

            # Update axes
            fig.update_xaxes(title_text="Country", row=1, col=2, tickangle=45)
            fig.update_xaxes(title_text="Country", row=2, col=1, tickangle=45)
            fig.update_xaxes(title_text="Country", row=2, col=2, tickangle=45)
            
            fig.update_yaxes(title_text="Visitors", row=1, col=2)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=2)

            # Update geo layout
            fig.update_geos(
                showcoastlines=True,
                coastlinecolor="RebeccaPurple",
                showland=True,
                landcolor="LightGray",
                showocean=True,
                oceancolor="LightBlue"
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating geographic dashboard: {str(e)}")
            raise
