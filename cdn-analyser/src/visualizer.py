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

    def create_visualizations(
        self, 
        df: pd.DataFrame, 
        analysis: Dict, 
        zone_name: str
    ) -> None:
        """Create comprehensive visualizations."""
        try:
            if df is None or df.empty or not analysis:
                logger.error("No data available for visualization")
                return

            output_dir = self.config.images_dir / zone_name
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create visualization groups
            logger.info("Generating performance dashboard...")
            perf_dashboard = self._create_performance_dashboard(df, analysis)
            perf_dashboard.write_html(str(output_dir / 'performance_dashboard.html'))
            perf_dashboard.write_image(str(output_dir / 'performance_dashboard.png'), 
                                     width=1920, height=1080, scale=2)

            logger.info("Generating cache dashboard...")
            cache_dashboard = self._create_cache_dashboard(df, analysis)
            cache_dashboard.write_html(str(output_dir / 'cache_dashboard.html'))
            cache_dashboard.write_image(str(output_dir / 'cache_dashboard.png'), 
                                      width=1920, height=1080, scale=2)

            logger.info("Generating error dashboard...")
            error_dashboard = self._create_error_dashboard(df, analysis)
            error_dashboard.write_html(str(output_dir / 'error_dashboard.html'))
            error_dashboard.write_image(str(output_dir / 'error_dashboard.png'), 
                                      width=1920, height=1080, scale=2)

            logger.info("Generating geographic dashboard...")
            geo_dashboard = self._create_geographic_dashboard(df, analysis)
            geo_dashboard.write_html(str(output_dir / 'geographic_dashboard.html'))
            geo_dashboard.write_image(str(output_dir / 'geographic_dashboard.png'), 
                                    width=1920, height=1080, scale=2)

            logger.info("Generating tiered cache dashboard...")
            tiered_dashboard = self._create_tiered_cache_dashboard(df, analysis)
            tiered_dashboard.write_html(str(output_dir / 'tiered_cache_dashboard.html'))
            tiered_dashboard.write_image(str(output_dir / 'tiered_cache_dashboard.png'),
                                       width=1920, height=1080, scale=2)

            logger.info(f"Successfully generated all visualizations for {zone_name}")

        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _create_time_range_buttons(self, df_time: pd.DataFrame) -> list:
        """Create time range selection buttons."""
        return [
            dict(
                label="1h",
                method="relayout",
                args=[{"xaxis.range": [df_time.index[-1] - pd.Timedelta(hours=1), df_time.index[-1]]}]
            ),
            dict(
                label="6h",
                method="relayout",
                args=[{"xaxis.range": [df_time.index[-1] - pd.Timedelta(hours=6), df_time.index[-1]]}]
            ),
            dict(
                label="12h",
                method="relayout",
                args=[{"xaxis.range": [df_time.index[-1] - pd.Timedelta(hours=12), df_time.index[-1]]}]
            ),
            dict(
                label="24h",
                method="relayout",
                args=[{"xaxis.range": [df_time.index[-1] - pd.Timedelta(hours=24), df_time.index[-1]]}]
            ),
            dict(
                label="All",
                method="relayout",
                args=[{"xaxis.range": [df_time.index[0], df_time.index[-1]]}]
            )
        ]

    def _update_layout_common(self, fig: go.Figure, title: str) -> None:
        """Apply common layout updates to all dashboards."""
        fig.update_layout(
            height=1600,
            width=1800,
            title_text=title,
            title_x=0.5,
            title_y=0.98,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor='rgba(255,255,255,0.9)',
                bordercolor='rgba(0,0,0,0.2)',
                borderwidth=1
            ),
            template='plotly_white',
            hovermode='x unified',
            font=dict(
                family="Arial, sans-serif",
                size=12
            ),
            plot_bgcolor='white',
            paper_bgcolor='white',
            margin=dict(t=120, l=80, r=80, b=80)
        )

        # Update axes
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128,128,128,0.2)',
            zeroline=False,
            showspikes=True,
            spikemode='across',
            spikesnap='cursor',
            showline=True,
            linewidth=1,
            linecolor='rgba(128,128,128,0.2)',
            tickangle=45,
            title_standoff=15
        )

        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128,128,128,0.2)',
            zeroline=False,
            showspikes=True,
            spikesnap='cursor',
            showline=True,
            linewidth=1,
            linecolor='rgba(128,128,128,0.2)',
            title_standoff=15
        )

        # Update subplot titles
        fig.update_annotations(font_size=14)

    def _create_performance_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create main performance dashboard with improved layout."""
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
                vertical_spacing=0.25,
                horizontal_spacing=0.20
            )

            df_time = df.set_index('timestamp').sort_index()
            rolling_window = '5min'
            edge_time = df_time['ttfb_avg'].rolling(rolling_window).mean()
            origin_time = df_time['origin_time_avg'].rolling(rolling_window).mean()

            # Edge vs Origin Response Times
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

            # Request Volume Over Time
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
                    hovertemplate='%{y:,.0f} requests'
                ),
                row=1, col=2
            )
            fig.add_trace(
                go.Scatter(
                    x=volume.index,
                    y=volume['visits_adjusted'],
                    name='Unique Visitors',
                    line=dict(color=self.colors['secondary'], width=2),
                    hovertemplate='%{y:,.0f} visitors'
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
                    name='Protocol Performance',
                    marker_color=self.colors['edge'],
                    hovertemplate='%{y:.1f}ms',
                    width=0.6
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
                    width=0.6
                ),
                row=2, col=2
            )

            # Response Time Distribution
            fig.add_trace(
                go.Histogram(
                    x=df['ttfb_avg'],
                    name='TTFB Distribution',
                    marker_color=self.colors['edge'],
                    nbinsx=30,
                    hovertemplate='%{y:,.0f} samples<br>%{x:.1f}ms'
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
                    hovertemplate='Hour %{x}: %{y:,.0f} requests'
                ),
                row=3, col=2
            )

            # Update layout and axes
            self._update_layout_common(fig, "Performance Dashboard")

            # Add time range buttons
            fig.update_layout(
                updatemenus=[dict(
                    type="buttons",
                    direction="right",
                    x=0.1,
                    y=1.05,
                    xanchor="left",
                    yanchor="top",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    buttons=self._create_time_range_buttons(df_time)
                )]
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating performance dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _create_cache_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create cache analysis dashboard with improved layout."""
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
                    [{"type": "xy", "secondary_y": True}, {"type": "domain"}],
                    [{"type": "xy"}, {"type": "domain"}]
                ],
                vertical_spacing=0.25,
                horizontal_spacing=0.20
            )

            df_time = df.set_index('timestamp').sort_index()
            cache_ratio = df_time.resample('5min').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'requests_adjusted': 'sum'
            })

            # Cache Hit Ratio Over Time
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

            # Add request volume on secondary y-axis
            fig.add_trace(
                go.Scatter(
                    x=cache_ratio.index,
                    y=cache_ratio['requests_adjusted'],
                    name='Request Volume',
                    line=dict(color=self.colors['secondary'], width=1, dash='dot'),
                    hovertemplate='%{y:,.0f} requests',
                    opacity=0.7
                ),
                row=1, col=1,
                secondary_y=True
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
                    hovertemplate='%{label}<br>%{value:,} requests<br>%{percent:.1f}%',
                    hole=0.4,
                    textposition='outside',
                    textfont=dict(size=12),
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
                    hovertemplate='%{x}<br>Hit Ratio: %{y:.1f}%',
                    text=content_cache['cache_status'].round(1).astype(str) + '%',
                    textposition='auto',
                    width=0.6
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
                    hovertemplate='%{label}<br>%{value:.2f} GB<br>%{percent:.1f}%',
                    hole=0.4,
                    textposition='outside',
                    textfont=dict(size=12)
                ),
                row=2, col=2
            )

            # Update layout and axes
            self._update_layout_common(fig, "Cache Analysis Dashboard")

            # Update axis labels
            fig.update_yaxes(title_text="Cache Hit Ratio (%)", row=1, col=1)
            fig.update_yaxes(title_text="Requests", row=1, col=1, secondary_y=True)
            fig.update_yaxes(title_text="Hit Ratio (%)", row=2, col=1)
            fig.update_xaxes(title_text="Time", row=1, col=1)
            fig.update_xaxes(title_text="Content Type", row=2, col=1)

            # Add time range buttons
            fig.update_layout(
                updatemenus=[dict(
                    type="buttons",
                    direction="right",
                    x=0.1,
                    y=1.05,
                    xanchor="left",
                    yanchor="top",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    buttons=self._create_time_range_buttons(df_time)
                )]
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating cache dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _create_tiered_cache_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create tiered cache performance dashboard."""
        try:
            tiered_analysis = analysis.get('tiered_cache_analysis', {})
            if not tiered_analysis:
                logger.warning("No tiered cache analysis data available")
                return None

            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Tiered vs Direct Cache Performance',
                    'Upper Tier Distribution',
                    'Geographic Coverage',
                    'Response Times by Upper Tier'
                ),
                specs=[
                    [{"type": "xy"}, {"type": "pie"}],
                    [{"type": "choropleth"}, {"type": "xy"}]
                ],
                vertical_spacing=0.25,
                horizontal_spacing=0.20
            )

            # 1. Tiered vs Direct Performance Comparison
            perf_metrics = tiered_analysis['performance']
            categories = ['Tiered', 'Direct']
            ttfb_values = [
                perf_metrics['tiered_requests']['avg_ttfb'],
                perf_metrics['direct_requests']['avg_ttfb']
            ]
            cache_hit_values = [
                perf_metrics['tiered_requests']['cache_hit_ratio'],
                perf_metrics['direct_requests']['cache_hit_ratio']
            ]

            fig.add_trace(
                go.Bar(
                    name='TTFB',
                    x=categories,
                    y=ttfb_values,
                    marker_color=self.colors['edge'],
                    text=[f"{v:.1f}ms" for v in ttfb_values],
                    textposition='auto',
                ),
                row=1, col=1
            )

            fig.add_trace(
                go.Bar(
                    name='Cache Hit Ratio',
                    x=categories,
                    y=cache_hit_values,
                    marker_color=self.colors['cache_hit'],
                    text=[f"{v:.1f}%" for v in cache_hit_values],
                    textposition='auto',
                ),
                row=1, col=1
            )

            # 2. Upper Tier Distribution Pie Chart
            tier_dist = tiered_analysis['tier_distribution']
            fig.add_trace(
                go.Pie(
                    labels=list(tier_dist.keys()),
                    values=[d['traffic']['requests'] for d in tier_dist.values()],
                    hole=0.4,
                    marker_colors=self.color_sequences['main'],
                    textinfo='label+percent',
                    hovertemplate="<b>%{label}</b><br>" +
                                "Requests: %{value:,.0f}<br>" +
                                "Percentage: %{percent:.1f}%<extra></extra>"
                ),
                row=1, col=2
            )

            # 3. Geographic Coverage Heatmap
            geo_dist = tiered_analysis['geographic_distribution']
            fig.add_trace(
                go.Choropleth(
                    locations=list(geo_dist.keys()),
                    z=[data['avg_ttfb'] for data in geo_dist.values()],
                    colorscale='RdYlBu_r',
                    colorbar_title='TTFB (ms)',
                    locationmode='country names',
                    hovertemplate="<b>%{location}</b><br>" +
                                "TTFB: %{z:.1f}ms<extra></extra>"
                ),
                row=2, col=1
            )

            # 4. Response Times by Upper Tier
            tier_names = list(tier_dist.keys())
            ttfb_values = [tier_dist[tier]['performance']['ttfb'] for tier in tier_names]
            origin_times = [tier_dist[tier]['performance']['origin_time'] for tier in tier_names]

            fig.add_trace(
                go.Bar(
                    name='TTFB',
                    x=tier_names,
                    y=ttfb_values,
                    marker_color=self.colors['edge'],
                    text=[f"{v:.1f}ms" for v in ttfb_values],
                    textposition='auto',
                ),
                row=2, col=2
            )

            fig.add_trace(
                go.Bar(
                    name='Origin Time',
                    x=tier_names,
                    y=origin_times,
                    marker_color=self.colors['origin'],
                    text=[f"{v:.1f}ms" for v in origin_times],
                    textposition='auto',
                ),
                row=2, col=2
            )

            # Update layout
            self._update_layout_common(fig, "Tiered Cache Performance Dashboard")
            
            # Add time range selector
            fig.update_layout(
                updatemenus=[dict(
                    type="buttons",
                    direction="right",
                    x=0.1,
                    y=1.05,
                    xanchor="left",
                    yanchor="top",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    buttons=self._create_time_range_buttons(df.set_index('timestamp'))
                )]
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating tiered cache dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _create_error_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create error analysis dashboard with improved layout."""
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
                    [{"type": "xy", "secondary_y": True}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}]
                ],
                vertical_spacing=0.25,
                horizontal_spacing=0.20
            )

            df_time = df.set_index('timestamp').sort_index()
            error_rates = df_time.resample('5min').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'requests_adjusted': 'sum'
            })
            
            # Error Rate Over Time
            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_4xx'] * 100,
                    name='4xx Errors',
                    line=dict(color=self.colors['warning'], width=2),
                    hovertemplate='%{y:.2f}%'
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_5xx'] * 100,
                    name='5xx Errors',
                    line=dict(color=self.colors['error'], width=2),
                    hovertemplate='%{y:.2f}%'
                ),
                row=1, col=1
            )

            # Add request volume on secondary y-axis
            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['requests_adjusted'],
                    name='Request Volume',
                    line=dict(color=self.colors['secondary'], width=1, dash='dot'),
                    hovertemplate='%{y:,.0f} requests',
                    opacity=0.7
                ),
                row=1, col=1,
                secondary_y=True
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
                    text=status_dist.values,
                    textposition='auto',
                    width=0.6
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
                    hovertemplate='%{x}<br>4xx Rate: %{y:.2f}%',
                    width=0.6
                ),
                row=2, col=1
            )
            fig.add_trace(
                go.Bar(
                    x=endpoint_errors.index,
                    y=endpoint_errors['error_rate_5xx'] * 100,
                    name='5xx Rate',
                    marker_color=self.colors['error'],
                    hovertemplate='%{x}<br>5xx Rate: %{y:.2f}%',
                    width=0.6
                ),
                row=2, col=1
            )

            # Response Time by Status
            status_groups = ['2xx', '3xx', '4xx', '5xx']
            colors = {
                '2xx': self.colors['success'],
                '3xx': self.colors['primary'],
                '4xx': self.colors['warning'],
                '5xx': self.colors['error']
            }
            
            for status_group in status_groups:
                if status_group == '2xx':
                    mask = df['status'].between(200, 299)
                elif status_group == '3xx':
                    mask = df['status'].between(300, 399)
                elif status_group == '4xx':
                    mask = df['status'].between(400, 499)
                else:  # 5xx
                    mask = df['status'].between(500, 599)
                
                if mask.any():
                    fig.add_trace(
                        go.Box(
                            name=status_group,
                            y=df[mask]['ttfb_avg'],
                            boxmean=True,
                            marker_color=colors[status_group],
                            hovertemplate='Status: %{x}<br>Response Time: %{y:.1f}ms'
                        ),
                        row=2, col=2
                    )

            # Update layout and axes
            self._update_layout_common(fig, "Error Analysis Dashboard")

            # Update axis labels
            fig.update_yaxes(title_text="Error Rate (%)", row=1, col=1)
            fig.update_yaxes(title_text="Requests", row=1, col=1, secondary_y=True)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=1)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=2)

            fig.update_xaxes(title_text="Time", row=1, col=1)
            fig.update_xaxes(title_text="Status Code", row=1, col=2)
            fig.update_xaxes(title_text="Endpoint", row=2, col=1)
            fig.update_xaxes(title_text="Status Group", row=2, col=2)

            # Add time range buttons
            fig.update_layout(
                updatemenus=[dict(
                    type="buttons",
                    direction="right",
                    x=0.1,
                    y=1.05,
                    xanchor="left",
                    yanchor="top",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    buttons=self._create_time_range_buttons(df_time)
                )]
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating error dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _create_geographic_dashboard(self, df: pd.DataFrame, analysis: Dict) -> go.Figure:
        """Create geographic analysis dashboard with improved layout."""
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
                vertical_spacing=0.25,
                horizontal_spacing=0.20
            )

            # Calculate geographic metrics
            geo_metrics = df.groupby('country').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).reset_index()

            # Geographic Performance Heatmap
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['ttfb_avg'],
                    colorscale='RdYlBu_r',
                    showscale=True,
                    locationmode='country names',
                    name='Response Time',
                    colorbar_title='TTFB (ms)',
                    hovertemplate='%{location}<br>TTFB: %{z:.1f}ms<extra></extra>'
                ),
                row=1, col=1
            )

            # Top Countries by Traffic
            df_time = df.set_index('timestamp')
            top_countries = geo_metrics.nlargest(10, 'requests_adjusted')['country'].tolist()
            
            for country in top_countries:
                country_data = df_time[df_time['country'] == country].resample('1h').agg({
                    'requests_adjusted': 'sum'
                })
                
                fig.add_trace(
                    go.Scatter(
                        x=country_data.index,
                        y=country_data['requests_adjusted'],
                        name=country,
                        mode='lines',
                        hovertemplate='%{x}<br>%{y:,.0f} requests<extra></extra>'
                    ),
                    row=1, col=2
                )

            # Response Time by Country
            fig.add_trace(
                go.Bar(
                    x=top_countries,
                    y=geo_metrics[geo_metrics['country'].isin(top_countries)]['ttfb_avg'],
                    name='Response Time',
                    marker_color=self.colors['edge'],
                    hovertemplate='%{x}<br>TTFB: %{y:.1f}ms',
                    text=geo_metrics[geo_metrics['country'].isin(top_countries)]['ttfb_avg'].round(1),
                    textposition='auto',
                    width=0.6
                ),
                row=2, col=1
            )

            # Error Rates by Country
            for error_type, color, name in [
                ('error_rate_4xx', self.colors['warning'], '4xx Errors'),
                ('error_rate_5xx', self.colors['error'], '5xx Errors')
            ]:
                fig.add_trace(
                    go.Bar(
                        x=top_countries,
                        y=geo_metrics[geo_metrics['country'].isin(top_countries)][error_type] * 100,
                        name=name,
                        marker_color=color,
                        hovertemplate='%{x}<br>Error Rate: %{y:.2f}%',
                        width=0.6
                    ),
                    row=2, col=2
                )

            # Update layout and axes
            self._update_layout_common(fig, "Geographic Analysis Dashboard")

            # Update geographic map settings
            fig.update_geos(
                showcoastlines=True,
                coastlinecolor="RebeccaPurple",
                showland=True,
                landcolor="LightGray",
                showocean=True,
                oceancolor="LightBlue",
                projection_type="equirectangular",
                showframe=False,
                showlakes=True,
                lakecolor="LightBlue",
                showcountries=True,
                countrycolor="Gray",
                countrywidth=0.5
            )

            # Update axis labels
            fig.update_yaxes(title_text="Requests", row=1, col=2)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=2)

            fig.update_xaxes(title_text="Time", row=1, col=2)
            fig.update_xaxes(title_text="Country", row=2, col=1)
            fig.update_xaxes(title_text="Country", row=2, col=2)

            # Add time range buttons
            fig.update_layout(
                updatemenus=[dict(
                    type="buttons",
                    direction="right",
                    x=0.1,
                    y=1.05,
                    xanchor="left",
                    yanchor="top",
                    pad={"r": 10, "t": 10},
                    showactive=True,
                    buttons=self._create_time_range_buttons(df_time)
                )]
            )

            # Adjust subplot heights
            fig.update_layout(
                height=1800,  # Increased height for geographic visualizations
                grid=dict(
                    rows=2,
                    columns=2,
                    pattern='independent',
                    roworder='top to bottom'
                )
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating geographic dashboard: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def _update_axes_style(self, fig: go.Figure) -> None:
        """Helper method to apply consistent axis styling across all subplots."""
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128,128,128,0.2)',
            zeroline=False,
            showspikes=True,
            spikemode='across',
            spikesnap='cursor',
            showline=True,
            linewidth=1,
            linecolor='rgba(128,128,128,0.2)',
            tickangle=45,
            title_standoff=15,
            tickfont=dict(size=10),
            title_font=dict(size=12)
        )

        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128,128,128,0.2)',
            zeroline=False,
            showspikes=True,
            spikesnap='cursor',
            showline=True,
            linewidth=1,
            linecolor='rgba(128,128,128,0.2)',
            title_standoff=15,
            tickfont=dict(size=10),
            title_font=dict(size=12)
        )

    def _save_plot(self, fig: go.Figure, filepath: Path) -> None:
        """Safely save plot with error handling and optimized settings."""
        try:
            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as HTML with optimized settings
            fig.write_html(
                str(filepath.with_suffix('.html')),
                include_plotlyjs='cdn',
                include_mathjax='cdn',
                full_html=True,
                config={
                    'displayModeBar': True,
                    'responsive': True,
                    'scrollZoom': True,
                    'showLink': False,
                    'toImageButtonOptions': {
                        'format': 'png',
                        'filename': filepath.stem,
                        'height': 1080,
                        'width': 1920,
                        'scale': 2
                    }
                }
            )
            
            # Save as static image
            fig.write_image(
                str(filepath.with_suffix('.png')),
                width=1920,
                height=1080,
                scale=2,
                engine='kaleido'
            )
            
            logger.info(f"Plot saved successfully to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving plot to {filepath}: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # Clean up
            fig.data = []
            fig.layout = {}

    def cleanup(self):
        """Clean up resources and temporary files."""
        try:
            # Clean up any temporary files or resources here
            logger.info("Visualizer cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during visualizer cleanup: {str(e)}")
            logger.error(traceback.format_exc())

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
