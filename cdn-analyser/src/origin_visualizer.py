import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from pathlib import Path
import traceback

logger = logging.getLogger(__name__)

class OriginVisualizer:
    """Visualizer for origin server performance metrics using Plotly."""
    
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
        
        # Store for generated figures
        self.figures = {}

    def create_origin_visualizations(
        self,
        df: pd.DataFrame,
        analysis_results: dict,
        zone_name: str
    ) -> dict:
        """Create comprehensive origin performance visualizations."""
        try:
            if df is None or df.empty or not analysis_results:
                logger.error("No data available for visualization")
                return {}

            # Create visualization group
            try:
                figures = {
                    'origin_response_time': self._create_response_time_analysis(df, analysis_results),
                    'origin_error': self._create_error_analysis(df, analysis_results),
                    'origin_geographic': self._create_geographic_analysis(df, analysis_results),
                    'origin_endpoints': self._create_endpoint_analysis(df, analysis_results),
                    'origin_asn': self._create_asn_analysis(df, analysis_results)
                }
                
                return figures

            except Exception as e:
                logger.error(f"Error creating origin visualizations: {str(e)}")
                logger.error(traceback.format_exc())
                return {}

        except Exception as e:
            logger.error(f"Error in origin visualization creation: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    def _create_response_time_analysis(
        self,
        df: pd.DataFrame,
        analysis: dict
    ) -> go.Figure:
        """Create response time analysis visualization."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Origin Response Time Distribution with Quantiles',
                    'Response Time by Cache Status',
                    'Response Time Quantiles Over Time',
                    'Response Time vs Request Volume'
                ),
                specs=[
                    [{"type": "xy"}, {"type": "xy"}],
                    [{"type": "xy"}, {"type": "xy"}]
                ]
            )

            # 1. Response Time Distribution with Quantiles
            ttfb_dist = df['origin_time_avg'].clip(upper=df['origin_time_avg'].quantile(0.99))
            fig.add_trace(
                go.Histogram(
                    x=ttfb_dist,
                    name='Response Time',
                    nbinsx=50,
                    marker_color=self.colors['origin'],
                    opacity=0.7
                ),
                row=1, col=1
            )

            # Add quantile lines
            for quantile, name, color in [
                ('origin_p50', 'P50', self.colors['success']),
                ('origin_p95', 'P95', self.colors['warning']),
                ('origin_p99', 'P99', self.colors['error'])
            ]:
                value = df[quantile].mean()
                fig.add_vline(
                    x=value,
                    line=dict(
                        dash="dash",
                        color=color,
                        width=2
                    ),
                    annotation=dict(
                        text=f"{name}: {value:.1f}ms",
                        font=dict(color=color),
                        align="right"
                    ),
                    row=1, col=1
                )

            # 2. Response Time by Cache Status with Quantiles
            for status in df['cache_status'].unique():
                status_data = df[df['cache_status'] == status]
                
                # Box plot for distribution
                fig.add_trace(
                    go.Box(
                        y=status_data['origin_time_avg'],
                        name=status,
                        marker_color=self.colors['origin'],
                        boxpoints='outliers',
                        showlegend=False
                    ),
                    row=1, col=2
                )
                
                # Add quantile markers
                for quantile, marker_color in [
                    ('origin_p95', self.colors['warning']),
                    ('origin_p99', self.colors['error'])
                ]:
                    fig.add_trace(
                        go.Scatter(
                            x=[status],
                            y=[status_data[quantile].mean()],
                            mode='markers',
                            marker=dict(
                                color=marker_color,
                                symbol='diamond',
                                size=10
                            ),
                            name=f'{quantile} ({status})',
                            showlegend=False
                        ),
                        row=1, col=2
                    )

            # 3. Response Time Quantiles Over Time
            df_time = df.set_index('timestamp').resample('5min').agg({
                'origin_time_avg': 'mean',
                'origin_p50': 'mean',
                'origin_p95': 'mean',
                'origin_p99': 'mean'
            }).reset_index()

            # Plot each line
            for metric, name, color in [
                ('origin_time_avg', 'Average', self.colors['origin']),
                ('origin_p50', 'P50', self.colors['success']),
                ('origin_p95', 'P95', self.colors['warning']),
                ('origin_p99', 'P99', self.colors['error'])
            ]:
                fig.add_trace(
                    go.Scatter(
                        x=df_time['timestamp'],
                        y=df_time[metric],
                        name=name,
                        line=dict(color=color),
                        hovertemplate='%{y:.1f}ms'
                    ),
                    row=2, col=1
                )
            # 4. Response Time vs Request Volume Heatmap
            df_vol = df.set_index('timestamp').resample('1h').agg({
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum',
                'origin_p95': 'mean'
            }).reset_index()

            fig.add_trace(
                go.Scatter(
                    x=df_vol['visits_adjusted'],
                    y=df_vol['origin_time_avg'],
                    mode='markers',
                    marker=dict(
                        color=df_vol['origin_p95'],
                        colorscale='Viridis',
                        size=10,
                        showscale=True,
                        colorbar=dict(
                            title='P95 Response Time (ms)',
                            x=1.1
                        )
                    ),
                    name='Response Time',
                    hovertemplate=(
                        'Requests: %{x:,.0f}<br>' +
                        'Avg Time: %{y:.1f}ms<br>' +
                        'P95 Time: %{marker.color:.1f}ms'
                    )
                ),
                row=2, col=2
            )

            # Add trendline
            z = np.polyfit(df_vol['visits_adjusted'], df_vol['origin_time_avg'], 1)
            p = np.poly1d(z)
            
            fig.add_trace(
                go.Scatter(
                    x=df_vol['visits_adjusted'],
                    y=p(df_vol['visits_adjusted']),
                    name='Trend',
                    line=dict(
                        color='red',
                        dash='dash'
                    ),
                    hovertemplate='Trend: %{y:.1f}ms'
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=800,
                showlegend=True,
                template='plotly_dark',
                paper_bgcolor='#1e1e1e',
                plot_bgcolor='#1e1e1e',
                margin=dict(l=60, r=60, t=80, b=60)
            )

            # Update axes
            fig.update_xaxes(
                title_text='Response Time (ms)',
                gridcolor='#333',
                row=1, col=1
            )
            fig.update_xaxes(
                title_text='Cache Status',
                gridcolor='#333',
                row=1, col=2
            )
            fig.update_xaxes(
                title_text='Time',
                gridcolor='#333',
                row=2, col=1
            )
            fig.update_xaxes(
                title_text='Requests per Hour',
                gridcolor='#333',
                row=2, col=2
            )

            fig.update_yaxes(
                title_text='Count',
                gridcolor='#333',
                row=1, col=1
            )
            fig.update_yaxes(
                title_text='Response Time (ms)',
                gridcolor='#333',
                row=1, col=2
            )
            fig.update_yaxes(
                title_text='Response Time (ms)',
                gridcolor='#333',
                row=2, col=1
            )
            fig.update_yaxes(
                title_text='Response Time (ms)',
                gridcolor='#333',
                row=2, col=2
            )

            return fig

        except Exception as e:
            logger.error(f"Error creating response time analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating response time analysis")

    def _create_asn_analysis(
        self,
        df: pd.DataFrame,
        analysis: dict
    ) -> go.Figure:
        """Create ASN analysis visualization."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'ASN Performance Distribution',
                    'Top ASNs by Request Volume',
                    'ASN Response Time Correlation',
                    'ASN Performance Over Time'
                )
            )

            # 1. ASN Performance Distribution
            asn_perf = df.groupby('clientAsn').agg({
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()

            # Get top ASNs by request volume
            top_asns = asn_perf.nlargest(10, 'visits_adjusted')

            fig.add_trace(
                go.Box(
                    x=df['clientAsn'].loc[df['clientAsn'].isin(top_asns['clientAsn'])],
                    y=df['origin_time_avg'].loc[df['clientAsn'].isin(top_asns['clientAsn'])],
                    name='Response Time',
                    marker_color=self.colors['origin'],
                    boxpoints='outliers'
                ),
                row=1, col=1
            )

            # 2. Top ASNs by Volume
            fig.add_trace(
                go.Bar(
                    x=top_asns['clientAsn'],
                    y=top_asns['visits_adjusted'],
                    name='Request Volume',
                    marker_color=self.colors['primary']
                ),
                row=1, col=2
            )

            # 3. ASN Response Time Correlation (Avg vs P95)
            fig.add_trace(
                go.Scatter(
                    x=top_asns['origin_time_avg'],
                    y=top_asns['origin_p95'],
                    mode='markers+text',
                    marker=dict(
                        size=top_asns['visits_adjusted'] / top_asns['visits_adjusted'].max() * 50,
                        color=top_asns['visits_adjusted'],
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(title='Request Volume')
                    ),
                    text=top_asns['clientAsn'],
                    textposition="top center",
                    name='ASN Performance'
                ),
                row=2, col=1
            )

            # Add diagonal reference line
            max_val = max(
                top_asns['origin_time_avg'].max(),
                top_asns['origin_p95'].max()
            )
            fig.add_trace(
                go.Scatter(
                    x=[0, max_val],
                    y=[0, max_val],
                    mode='lines',
                    line=dict(color='red', dash='dash'),
                    name='1:1 Reference'
                ),
                row=2, col=1
            )

            # 4. ASN Performance Over Time
            # Calculate moving averages for top 3 ASNs
            top_3_asns = top_asns.nlargest(3, 'visits_adjusted')['clientAsn']
            df_time = df[df['clientAsn'].isin(top_3_asns)].copy()
            
            for asn in top_3_asns:
                asn_data = df_time[df_time['clientAsn'] == asn]
                time_series = asn_data.set_index('timestamp').resample('5min').agg({
                    'origin_time_avg': 'mean'
                }).reset_index()
                
                fig.add_trace(
                    go.Scatter(
                        x=time_series['timestamp'],
                        y=time_series['origin_time_avg'],
                        name=f'ASN {asn}',
                        mode='lines'
                    ),
                    row=2, col=2
                )

            # Update layout
            fig.update_layout(
                height=800,
                showlegend=True,
                template='plotly_dark',
                paper_bgcolor='#1e1e1e',
                plot_bgcolor='#1e1e1e',
                margin=dict(l=60, r=60, t=80, b=60)
            )

            # Update axes
            fig.update_xaxes(gridcolor='#333')
            fig.update_yaxes(gridcolor='#333')

            fig.update_xaxes(title_text="ASN", row=1, col=1)
            fig.update_xaxes(title_text="ASN", row=1, col=2)
            fig.update_xaxes(title_text="Average Response Time (ms)", row=2, col=1)
            fig.update_xaxes(title_text="Time", row=2, col=2)

            fig.update_yaxes(title_text="Response Time (ms)", row=1, col=1)
            fig.update_yaxes(title_text="Request Volume", row=1, col=2)
            fig.update_yaxes(title_text="P95 Response Time (ms)", row=2, col=1)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=2)

            return fig

        except Exception as e:
            logger.error(f"Error creating ASN analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating ASN analysis")

    def _create_error_analysis(
        self,
        df: pd.DataFrame,
        analysis: dict
    ) -> go.Figure:
        """Create error analysis visualization."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Error Status Distribution',
                    'Error Rate Over Time',
                    'Top Error Endpoints',
                    'Error Rate by ASN'
                )
            )

            # 1. Error Status Distribution
            error_df = df[df['status'] >= 400]
            status_counts = error_df['status'].value_counts()

            fig.add_trace(
                go.Bar(
                    x=status_counts.index.astype(str),
                    y=status_counts.values,
                    marker_color=[
                        self.colors['warning'] if s < 500 else self.colors['error']
                        for s in status_counts.index
                    ],
                    name='Error Count',
                    hovertemplate='Status %{x}<br>Count: %{y:,}'
                ),
                row=1, col=1
            )

            # 2. Error Rate Over Time with Quantiles
            df_time = df.set_index('timestamp').resample('5min').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()

            fig.add_trace(
                go.Scatter(
                    x=df_time['timestamp'],
                    y=df_time['error_rate_4xx'] * 100,
                    name='4xx Error Rate',
                    line=dict(color=self.colors['warning']),
                    hovertemplate='%{y:.2f}%'
                ),
                row=1, col=2
            )

            fig.add_trace(
                go.Scatter(
                    x=df_time['timestamp'],
                    y=df_time['error_rate_5xx'] * 100,
                    name='5xx Error Rate',
                    line=dict(color=self.colors['error']),
                    hovertemplate='%{y:.2f}%'
                ),
                row=1, col=2
            )

            # Add request volume as secondary axis
            fig.add_trace(
                go.Scatter(
                    x=df_time['timestamp'],
                    y=df_time['visits_adjusted'],
                    name='Request Volume',
                    line=dict(color=self.colors['primary'], dash='dot'),
                    yaxis='y2',
                    hovertemplate='%{y:,.0f} requests'
                ),
                row=1, col=2
            )

            # 3. Top Error Endpoints
            endpoint_errors = df.groupby('endpoint').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')

            fig.add_trace(
                go.Bar(
                    x=endpoint_errors.index,
                    y=endpoint_errors['error_rate_5xx'] * 100,
                    name='5xx Error Rate',
                    marker_color=self.colors['error']
                ),
                row=2, col=1
            )

            fig.add_trace(
                go.Bar(
                    x=endpoint_errors.index,
                    y=endpoint_errors['error_rate_4xx'] * 100,
                    name='4xx Error Rate',
                    marker_color=self.colors['warning']
                ),
                row=2, col=1
            )

            # 4. Error Rate by ASN
            asn_errors = df.groupby('clientAsn').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')

            fig.add_trace(
                go.Bar(
                    x=asn_errors.index,
                    y=asn_errors['error_rate_5xx'] * 100,
                    name='5xx Error Rate',
                    marker_color=self.colors['error']
                ),
                row=2, col=2
            )

            fig.add_trace(
                go.Bar(
                    x=asn_errors.index,
                    y=asn_errors['error_rate_4xx'] * 100,
                    name='4xx Error Rate',
                    marker_color=self.colors['warning']
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=800,
                showlegend=True,
                template='plotly_dark',
                paper_bgcolor='#1e1e1e',
                plot_bgcolor='#1e1e1e',
                margin=dict(l=60, r=60, t=80, b=60),
                barmode='stack'
            )

            # Add secondary y-axis for request volume
            fig.update_layout(
                yaxis2=dict(
                    title="Requests",
                    overlaying="y",
                    side="right",
                    showgrid=False
                )
            )

            # Update axes
            fig.update_xaxes(gridcolor='#333')
            fig.update_yaxes(gridcolor='#333')

            # Update specific axis labels
            fig.update_xaxes(title_text="Error Status", row=1, col=1)
            fig.update_xaxes(title_text="Time", row=1, col=2)
            fig.update_xaxes(title_text="Endpoint", row=2, col=1, tickangle=45)
            fig.update_xaxes(title_text="ASN", row=2, col=2, tickangle=45)

            fig.update_yaxes(title_text="Count", row=1, col=1)
            fig.update_yaxes(title_text="Error Rate (%)", row=1, col=2)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=1)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=2)

            return fig

        except Exception as e:
            logger.error(f"Error creating error analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating error analysis")

    def _create_geographic_analysis(
        self,
        df: pd.DataFrame,
        analysis: dict
    ) -> go.Figure:
        """Create geographic analysis visualization."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Response Time by Country',
                    'Error Rate by Country',
                    'Request Volume by Country',
                    'Cache Performance by Country'
                ),
                specs=[
                    [{"type": "choropleth"}, {"type": "choropleth"}],
                    [{"type": "choropleth"}, {"type": "choropleth"}]
                ]
            )

            # Calculate metrics by country
            geo_metrics = df.groupby('country').agg({
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum',
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100
            }).reset_index()

            # 1. Response Time Choropleth
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['origin_time_avg'],
                    colorscale='Viridis',
                    reversescale=True,
                    name='Response Time',
                    colorbar_title='Response Time (ms)',
                    locationmode='country names',
                    hovertemplate=(
                        '%{location}<br>' +
                        'Avg: %{z:.1f}ms<br>' +
                        'P95: %{customdata:.1f}ms'
                    ),
                    customdata=geo_metrics['origin_p95']
                ),
                row=1, col=1
            )

            # 2. Error Rate Choropleth
            geo_metrics['total_error_rate'] = (
                geo_metrics['error_rate_4xx'] + geo_metrics['error_rate_5xx']
            ) * 100
            
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['total_error_rate'],
                    colorscale='Reds',
                    name='Error Rate',
                    colorbar_title='Error Rate (%)',
                    locationmode='country names',
                    hovertemplate=(
                        '%{location}<br>' +
                        'Error Rate: %{z:.2f}%<br>' +
                        '4xx: %{customdata[0]:.2f}%<br>' +
                        '5xx: %{customdata[1]:.2f}%'
                    ),
                    customdata=np.stack((
                        geo_metrics['error_rate_4xx'] * 100,
                        geo_metrics['error_rate_5xx'] * 100
                    ), axis=1)
                ),
                row=1, col=2
            )

            # 3. Request Volume Choropleth
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['visits_adjusted'],
                    colorscale='Blues',
                    name='Request Volume',
                    colorbar_title='Requests',
                    locationmode='country names',
                    hovertemplate='%{location}<br>Requests: %{z:,.0f}'
                ),
                row=2, col=1
            )

            # 4. Cache Hit Ratio Choropleth
            fig.add_trace(
                go.Choropleth(
                    locations=geo_metrics['country'],
                    z=geo_metrics['cache_status'],
                    colorscale='Greens',
                    name='Cache Hit Ratio',
                    colorbar_title='Cache Hit Ratio (%)',
                    locationmode='country names',
                    hovertemplate='%{location}<br>Cache Hit Ratio: %{z:.1f}%'
                ),
                row=2, col=2
            )

            # Update layout
            fig.update_layout(
                height=1000,
                showlegend=False,
                template='plotly_dark',
                paper_bgcolor='#1e1e1e',
                plot_bgcolor='#1e1e1e',
                margin=dict(l=0, r=0, t=80, b=0)
            )

            # Update geo settings for all subplots
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

            return fig

        except Exception as e:
            logger.error(f"Error creating geographic analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating geographic analysis")

    def _create_endpoint_analysis(
        self,
        df: pd.DataFrame,
        analysis: dict
    ) -> go.Figure:
        """Create endpoint performance analysis visualization."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Response Time by Top Endpoints',
                    'Request Volume vs Response Time',
                    'Error Rate by Endpoint',
                    'Response Time Trends by Endpoint'
                )
            )

            # Get top 10 endpoints by request volume
            top_endpoints = df.groupby('endpoint').agg({
                'origin_time_avg': 'mean',
                'origin_p95': 'mean',
                'origin_p99': 'mean',
                'visits_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).nlargest(10, 'visits_adjusted')

            # 1. Response Time by Endpoint with Quantiles
            endpoints = top_endpoints.index
            
            # Base bars for average response time
            fig.add_trace(
                go.Bar(
                    x=endpoints,
                    y=top_endpoints['origin_time_avg'],
                    name='Avg Response Time',
                    marker_color=self.colors['origin']
                ),
                row=1, col=1
            )

            # Add P95 markers
            fig.add_trace(
                go.Scatter(
                    x=endpoints,
                    y=top_endpoints['origin_p95'],
                    mode='markers',
                    name='P95',
                    marker=dict(
                        symbol='diamond',
                        size=10,
                        color=self.colors['warning']
                    )
                ),
                row=1, col=1
            )

            # Add P99 markers
            fig.add_trace(
                go.Scatter(
                    x=endpoints,
                    y=top_endpoints['origin_p99'],
                    mode='markers',
                    name='P99',
                    marker=dict(
                        symbol='diamond',
                        size=10,
                        color=self.colors['error']
                    )
                ),
                row=1, col=1
            )

            # 2. Request Volume vs Response Time Scatter
            fig.add_trace(
                go.Scatter(
                    x=top_endpoints['visits_adjusted'],
                    y=top_endpoints['origin_time_avg'],
                    mode='markers+text',
                    text=endpoints,
                    textposition="top center",
                    marker=dict(
                        size=top_endpoints['origin_p95'] / top_endpoints['origin_p95'].max() * 50,
                        color=top_endpoints['origin_p95'],
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(title='P95 Response Time (ms)')
                    ),
                    hovertemplate=(
                        'Endpoint: %{text}<br>' +
                        'Requests: %{x:,.0f}<br>' +
                        'Avg Time: %{y:.1f}ms<br>' +
                        'P95 Time: %{marker.color:.1f}ms'
                    )
                ),
                row=1, col=2
            )

            # 3. Error Rate by Endpoint
            fig.add_trace(
                go.Bar(
                    x=endpoints,
                    y=top_endpoints['error_rate_5xx'] * 100,
                    name='5xx Errors',
                    marker_color=self.colors['error']
                ),
                row=2, col=1
            )

            fig.add_trace(
                go.Bar(
                    x=endpoints,
                    y=top_endpoints['error_rate_4xx'] * 100,
                    name='4xx Errors',
                    marker_color=self.colors['warning']
                ),
                row=2, col=1
            )

            # 4. Response Time Trends for Top 5 Endpoints
            top_5_endpoints = top_endpoints.nlargest(5, 'visits_adjusted').index
            df_time = df[df['endpoint'].isin(top_5_endpoints)].copy()
            
            for endpoint in top_5_endpoints:
                endpoint_data = df_time[df_time['endpoint'] == endpoint]
                time_series = endpoint_data.set_index('timestamp').resample('5min').agg({
                    'origin_time_avg': 'mean',
                    'origin_p95': 'mean'
                }).reset_index()
                
                # Add average line
                fig.add_trace(
                    go.Scatter(
                        x=time_series['timestamp'],
                        y=time_series['origin_time_avg'],
                        name=f'{endpoint} (avg)',
                        mode='lines',
                        line=dict(width=1)
                    ),
                    row=2, col=2
                )
                
                # Add P95 line
                fig.add_trace(
                    go.Scatter(
                        x=time_series['timestamp'],
                        y=time_series['origin_p95'],
                        name=f'{endpoint} (P95)',
                        mode='lines',
                        line=dict(
                            width=1,
                            dash='dash'
                        )
                    ),
                    row=2, col=2
                )

            # Update layout
            fig.update_layout(
                height=800,
                showlegend=True,
                template='plotly_dark',
                paper_bgcolor='#1e1e1e',
                plot_bgcolor='#1e1e1e',
                margin=dict(l=60, r=60, t=80, b=60),
                legend=dict(
                    bgcolor='rgba(0,0,0,0.5)',
                    bordercolor='#333',
                    borderwidth=1
                ),
                barmode='stack'
            )

            # Update axes
            fig.update_xaxes(
                gridcolor='#333',
                tickangle=45
            )
            fig.update_yaxes(
                gridcolor='#333'
            )

            # Update specific axis labels
            fig.update_xaxes(title_text="Endpoint", row=1, col=1)
            fig.update_xaxes(title_text="Request Volume", row=1, col=2)
            fig.update_xaxes(title_text="Endpoint", row=2, col=1)
            fig.update_xaxes(title_text="Time", row=2, col=2)

            fig.update_yaxes(title_text="Response Time (ms)", row=1, col=1)
            fig.update_yaxes(title_text="Response Time (ms)", row=1, col=2)
            fig.update_yaxes(title_text="Error Rate (%)", row=2, col=1)
            fig.update_yaxes(title_text="Response Time (ms)", row=2, col=2)

            return fig

        except Exception as e:
            logger.error(f"Error creating endpoint analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return self._create_error_figure("Error generating endpoint analysis")

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
            
            logger.info("Origin visualizer cleanup completed successfully")
            
        except Exception as e:
            logger.error(f"Error during origin visualizer cleanup: {str(e)}")
            logger.error(traceback.format_exc())
