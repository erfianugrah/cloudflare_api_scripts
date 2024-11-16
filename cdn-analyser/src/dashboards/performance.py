import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
import traceback

logger = logging.getLogger(__name__)

def create_performance_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create performance dashboard with individual legends per subplot."""
    try:
        # Initialize subplots
        fig = make_subplots(
            rows=3, cols=2,
            specs=[
                [{"type": "xy", "secondary_y": True}, {"type": "xy", "secondary_y": True}],
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "xy"}, {"type": "choropleth"}]
            ],
            vertical_spacing=0.22,
            horizontal_spacing=0.15,
            subplot_titles=(
                "Edge & Origin Response Times", "Request Volume Over Time",
                "ASN Performance", "Protocol Performance",
                "Top Endpoints Performance", "Geographic Response Times"
            ),
            row_heights=[0.33, 0.33, 0.33]
        )

        # Process time series data
        df_time = df.set_index('timestamp').sort_index()
        rolling_window = '5min'
        df_time = df_time.replace([np.inf, -np.inf], np.nan)

        # 1. Edge & Origin Times
        edge_times = df_time['ttfb_avg'].rolling(rolling_window).mean().fillna(0)
        origin_times = df_time['origin_time_avg'].rolling(rolling_window).mean().fillna(0)

        # Edge TTFB
        fig.add_trace(
            go.Scatter(
                x=df_time.index,
                y=edge_times,
                name='Edge TTFB',
                line=dict(color=colors['edge'], width=2),
                legend='legend1',
                hovertemplate='Time: %{x}<br>TTFB: %{y:.2f} ms<extra>Edge TTFB</extra>'
            ),
            row=1, col=1
        )

        # Edge percentiles
        for quantile, name, color_opacity in [
            ('ttfb_p50', 'Edge P50', 0.6),
            ('ttfb_p95', 'Edge P95', 0.4),
            ('ttfb_p99', 'Edge P99', 0.2)
        ]:
            fig.add_trace(
                go.Scatter(
                    x=df_time.index,
                    y=df_time[quantile].rolling(rolling_window).mean().fillna(0),
                    name=name,
                    line=dict(
                        color=f'rgba(46, 134, 193, {color_opacity})',
                        width=1,
                        dash='dot'
                    ),
                    legend='legend1',
                    hovertemplate=f'Time: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
                ),
                row=1, col=1
            )

        # Origin times
        fig.add_trace(
            go.Scatter(
                x=df_time.index,
                y=origin_times,
                name='Origin',
                line=dict(color=colors['origin'], width=2),
                legend='legend1',
                hovertemplate='Time: %{x}<br>Response Time: %{y:.2f} ms<extra>Origin Response</extra>'
            ),
            row=1, col=1
        )

        # Origin percentiles
        for quantile, name, color_opacity in [
            ('origin_p50', 'Origin P50', 0.6),
            ('origin_p95', 'Origin P95', 0.4),
            ('origin_p99', 'Origin P99', 0.2)
        ]:
            fig.add_trace(
                go.Scatter(
                    x=df_time.index,
                    y=df_time[quantile].rolling(rolling_window).mean().fillna(0),
                    name=name,
                    line=dict(
                        color=f'rgba(231, 76, 60, {color_opacity})',
                        width=1,
                        dash='dot'
                    ),
                    legend='legend1',
                    hovertemplate=f'Time: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
                ),
                row=1, col=1
            )

        # 2. Request Volume with separate legend
        request_volume = df_time['requests_adjusted'].rolling(rolling_window).mean().fillna(0)
        fig.add_trace(
            go.Scatter(
                x=df_time.index,
                y=request_volume,
                name='Requests',
                line=dict(color=colors['success'], width=2),
                fill='tozeroy',
                fillcolor='rgba(46, 204, 113, 0.1)',
                legend='legend2',
                hovertemplate='Time: %{x}<br>Requests: %{y:.0f}<extra></extra>'
            ),
            row=1, col=2
        )

        # Moving average
        fig.add_trace(
            go.Scatter(
                x=df_time.index,
                y=request_volume.rolling('30min').mean().fillna(0),
                name='30min Average',
                line=dict(color=colors['primary'], width=1, dash='dot'),
                legend='legend2',
                hovertemplate='Time: %{x}<br>Average: %{y:.0f}<extra></extra>'
            ),
            row=1, col=2
        )

        # 3. ASN Performance with separate legend
        asn_metrics = df.groupby('clientAsn').agg({
            'ttfb_avg': 'mean',
            'requests_adjusted': 'sum'
        }).dropna()
        
        top_asns = asn_metrics.nlargest(10, 'requests_adjusted').index
        
        for idx, asn in enumerate(top_asns):
            asn_data = df_time[df_time['clientAsn'] == asn]
            asn_series = asn_data['ttfb_avg'].rolling(rolling_window).mean().fillna(0)
            
            if not asn_series.empty:
                fig.add_trace(
                    go.Scatter(
                        x=asn_data.index,
                        y=asn_series,
                        name=f'ASN {asn}',
                        legend='legend3',
                        hovertemplate=f'ASN: {asn}<br>TTFB: %{{y:.2f}} ms<extra></extra>'
                    ),
                    row=2, col=1
                )

        # 4. Protocol Performance with separate legend
        protocol_metrics = df.groupby('protocol').agg({
            'ttfb_avg': 'mean',
            'ttfb_p50': 'mean',
            'ttfb_p95': 'mean',
            'ttfb_p99': 'mean',
            'requests_adjusted': 'sum'
        }).dropna()

        if not protocol_metrics.empty:
            fig.add_trace(
                go.Bar(
                    x=protocol_metrics.index,
                    y=protocol_metrics['ttfb_avg'],
                    name='Average TTFB',
                    marker_color=colors['edge'],
                    legend='legend4',
                    hovertemplate='Protocol: %{x}<br>Avg TTFB: %{y:.2f} ms<extra></extra>'
                ),
                row=2, col=2
            )

            for metric, name, color_opacity in [
                ('ttfb_p50', 'P50', 0.6),
                ('ttfb_p95', 'P95', 0.4),
                ('ttfb_p99', 'P99', 0.2)
            ]:
                fig.add_trace(
                    go.Scatter(
                        x=protocol_metrics.index,
                        y=protocol_metrics[metric],
                        name=name,
                        mode='markers',
                        marker=dict(
                            symbol='diamond',
                            size=12,
                            color=f'rgba(46, 134, 193, {color_opacity})',
                            line=dict(width=1, color='white')
                        ),
                        legend='legend4',
                        hovertemplate=f'Protocol: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
                    ),
                    row=2, col=2
                )

        # 5. Endpoints with separate legend
        endpoint_metrics = df.groupby('endpoint').agg({
            'ttfb_avg': 'mean',
            'ttfb_p50': 'mean',
            'ttfb_p95': 'mean',
            'ttfb_p99': 'mean',
            'requests_adjusted': 'sum'
        }).dropna()

        top_endpoints = endpoint_metrics.nlargest(10, 'requests_adjusted')

        if not top_endpoints.empty:
            fig.add_trace(
                go.Bar(
                    x=top_endpoints.index,
                    y=top_endpoints['ttfb_avg'],
                    name='Average TTFB',
                    marker_color=colors['edge'],
                    legend='legend5',
                    hovertemplate='Endpoint: %{x}<br>Avg TTFB: %{y:.2f} ms<extra></extra>'
                ),
                row=3, col=1
            )

            for metric, name, color_opacity in [
                ('ttfb_p50', 'P50', 0.6),
                ('ttfb_p95', 'P95', 0.4),
                ('ttfb_p99', 'P99', 0.2)
            ]:
                fig.add_trace(
                    go.Scatter(
                        x=top_endpoints.index,
                        y=top_endpoints[metric],
                        name=name,
                        mode='markers',
                        marker=dict(
                            symbol='diamond',
                            size=12,
                            color=f'rgba(46, 134, 193, {color_opacity})',
                            line=dict(width=1, color='white')
                        ),
                        legend='legend5',
                        hovertemplate=f'Endpoint: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
                    ),
                    row=3, col=1
                )

        # Update layout with properly positioned individual legends
        fig.update_layout(
            height=1500,
            template='plotly_dark',
            paper_bgcolor='#1e1e1e',
            plot_bgcolor='#1e1e1e',
            margin=dict(l=60, r=60, t=80, b=60),
            showlegend=True,
            # Individual legend settings
            legend1=dict(
                yanchor="top",
                y=0.99,  # Positioned right under first chart
                xanchor="right",
                x=0.46,
                bgcolor='rgba(0,0,0,0.5)',
                orientation='v'  # Make legend horizontal
            ),
            legend2=dict(
                yanchor="top",
                y=0.99,  # Positioned right under first chart
                xanchor="right",
                x=1.02,
                bgcolor='rgba(0,0,0,0.5)',
                orientation='v'  # Make legend horizontal
            ),
            legend3=dict(
                yanchor="top",
                y=0.60,  # Positioned right under second chart
                xanchor="right",
                x=0.46,
                bgcolor='rgba(0,0,0,0.5)',
                orientation='v'  # Make legend horizontal
            ),
            legend4=dict(
                yanchor="top",
                y=0.60,  # Positioned right under second chart
                xanchor="left",
                x=0.95,
                bgcolor='rgba(0,0,0,0.5)',
                orientation='v'  # Make legend horizontal
            ),
            legend5=dict(
                yanchor="top",
                y=0.18,  # Positioned right under third chart
                xanchor="left",
                x=0.40,
                bgcolor='rgba(0,0,0,0.5)',
                orientation='v'  # Make legend horizontal
            )
        )

        # Update axes styling
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(255, 255, 255, 0.1)',
            zeroline=False,
            tickfont=dict(size=10),
            title_font=dict(size=11)
        )

        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(255, 255, 255, 0.1)',
            zeroline=False,
            tickfont=dict(size=10),
            title_font=dict(size=11)
        )

        # Update subplot titles
        fig.update_annotations(font_size=14, font_color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating performance dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating performance dashboard")

def _create_error_figure(message: str) -> go.Figure:
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
