import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
import traceback

logger = logging.getLogger(__name__)

def create_origin_response_time_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create origin server response time analysis dashboard."""
    try:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Origin Response Time Distribution',
                'Response Time by Cache Status',
                'Response Time Over Time',
                'Response Time vs Request Volume'
            ),
            specs=[
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "xy", "secondary_y": True}, {"type": "xy"}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # 1. Response Time Distribution with Quantiles
        ttfb_dist = df['origin_time_avg'].clip(upper=df['origin_time_avg'].quantile(0.99))
        fig.add_trace(
            go.Histogram(
                x=ttfb_dist,
                name='Response Time',
                nbinsx=50,
                marker_color=colors['origin'],
                opacity=0.7,
                hovertemplate='Response Time: %{x:.1f}ms<br>Count: %{y}<extra></extra>',
                legend='legend1'
            ),
            row=1, col=1
        )

        # Add quantile lines
        for quantile, name, color in [
            ('origin_p50', 'P50', colors['success']),
            ('origin_p95', 'P95', colors['warning']),
            ('origin_p99', 'P99', colors['error'])
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

        # 2. Response Time by Cache Status
        for status in df['cache_status'].unique():
            status_data = df[df['cache_status'] == status]
            
            fig.add_trace(
                go.Box(
                    y=status_data['origin_time_avg'],
                    name=status,
                    marker_color=colors['origin'],
                    boxpoints='outliers',
                    hovertemplate=(
                        'Cache Status: %{x}<br>' +
                        'Response Time: %{y:.1f}ms<br>' +
                        '<extra></extra>'
                    ),
                    legend='legend2'
                ),
                row=1, col=2
            )

            # Add quantile markers
            for quantile, marker_color in [
                ('origin_p95', colors['warning']),
                ('origin_p99', colors['error'])
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
                        hovertemplate=(
                            'Cache Status: %{x}<br>' +
                            f'{quantile}: %{{y:.1f}}ms<br>' +
                            '<extra></extra>'
                        ),
                        legend='legend2'
                    ),
                    row=1, col=2
                )

        # 3. Response Time Over Time with Request Volume
        df_time = df.set_index('timestamp').resample('5min').agg({
            'origin_time_avg': 'mean',
            'origin_p50': 'mean',
            'origin_p95': 'mean',
            'origin_p99': 'mean',
            'requests_adjusted': 'sum'
        }).reset_index()

        # Plot response time metrics
        metrics = [
            ('origin_time_avg', 'Average', colors['origin']),
            ('origin_p50', 'P50', colors['success']),
            ('origin_p95', 'P95', colors['warning']),
            ('origin_p99', 'P99', colors['error'])
        ]

        for metric, name, color in metrics:
            fig.add_trace(
                go.Scatter(
                    x=df_time['timestamp'],
                    y=df_time[metric],
                    name=name,
                    line=dict(color=color),
                    hovertemplate=(
                        'Time: %{x}<br>' +
                        f'{name}: %{{y:.1f}}ms' +
                        '<extra></extra>'
                    ),
                    legend='legend3'
                ),
                row=2, col=1
            )

        # Add request volume on secondary y-axis
        fig.add_trace(
            go.Scatter(
                x=df_time['timestamp'],
                y=df_time['requests_adjusted'],
                name='Requests',
                line=dict(color=colors['secondary'], dash='dot'),
                hovertemplate=(
                    'Time: %{x}<br>' +
                    'Requests: %{y:,.0f}' +
                    '<extra></extra>'
                ),
                legend='legend3'
            ),
            row=2, col=1,
            secondary_y=True
        )

        # 4. Response Time vs Request Volume Heatmap
        df_vol = df.set_index('timestamp').resample('1h').agg({
            'origin_time_avg': 'mean',
            'requests_adjusted': 'sum',
            'origin_p95': 'mean'
        }).reset_index()

        fig.add_trace(
            go.Scatter(
                x=df_vol['requests_adjusted'],
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
                    'P95 Time: %{marker.color:.1f}ms' +
                    '<extra></extra>'
                ),
                legend='legend4'
            ),
            row=2, col=2
        )

        # Add trendline
        z = np.polyfit(df_vol['requests_adjusted'], df_vol['origin_time_avg'], 1)
        p = np.poly1d(z)
        
        fig.add_trace(
            go.Scatter(
                x=df_vol['requests_adjusted'],
                y=p(df_vol['requests_adjusted']),
                name='Trend',
                line=dict(
                    color='red',
                    dash='dash'
                ),
                hovertemplate=(
                    'Requests: %{x:,.0f}<br>' +
                    'Trend: %{y:.1f}ms' +
                    '<extra></extra>'
                ),
                legend='legend4'
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
            
            # Position legends for each subplot
            legend1=dict(
                x=0.45,
                y=0.95,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend2=dict(
                x=0.95,
                y=0.95,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend3=dict(
                x=0.45,
                y=0.45,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend4=dict(
                x=0.95,
                y=0.45,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            )
        )

        # Update axes
        fig.update_xaxes(
            gridcolor='#333',
            title_font=dict(size=12),
            tickfont=dict(size=10),
            zeroline=False
        )
        fig.update_yaxes(
            gridcolor='#333',
            title_font=dict(size=12),
            tickfont=dict(size=10),
            zeroline=False
        )

        # Update specific axis labels
        fig.update_xaxes(title_text='Response Time (ms)', row=1, col=1)
        fig.update_xaxes(title_text='Cache Status', row=1, col=2)
        fig.update_xaxes(title_text='Time', row=2, col=1)
        fig.update_xaxes(title_text='Requests per Hour', row=2, col=2)

        fig.update_yaxes(title_text='Count', row=1, col=1)
        fig.update_yaxes(title_text='Response Time (ms)', row=1, col=2)
        fig.update_yaxes(title_text='Response Time (ms)', row=2, col=1)
        fig.update_yaxes(title_text='Response Time (ms)', row=2, col=2)
        fig.update_yaxes(title_text='Requests', secondary_y=True, row=2, col=1)

        # Update subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating origin response time dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating origin response time dashboard")

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
