import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
import traceback

logger = logging.getLogger(__name__)

def create_origin_endpoint_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create origin server endpoint performance dashboard."""
    try:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Top Endpoints by Response Time',
                'Endpoint Load Distribution',
                'Response Time Trends',
                'Error Rate by Endpoint'
            ),
            specs=[
                [{"type": "bar"}, {"type": "scatter"}],
                [{"type": "scatter"}, {"type": "bar"}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # Get top endpoints by request volume
        top_endpoints = df.groupby('endpoint').agg({
            'origin_time_avg': 'mean',
            'origin_p50': 'mean',
            'origin_p95': 'mean',
            'origin_p99': 'mean',
            'requests_adjusted': 'sum',
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean'
        }).nlargest(10, 'requests_adjusted')

        # 1. Top Endpoints by Response Time
        fig.add_trace(
            go.Bar(
                x=top_endpoints.index,
                y=top_endpoints['origin_time_avg'],
                name='Avg Response Time',
                marker_color=colors['origin'],
                hovertemplate=(
                    '%{x}<br>' +
                    'Avg Time: %{y:.1f}ms' +
                    '<extra>Response Time</extra>'
                ),
                legend='legend1'
            ),
            row=1, col=1
        )

        # Add percentile markers
        for percentile, name, color in [
            ('origin_p50', 'P50', colors['success']),
            ('origin_p95', 'P95', colors['warning']),
            ('origin_p99', 'P99', colors['error'])
        ]:
            fig.add_trace(
                go.Scatter(
                    x=top_endpoints.index,
                    y=top_endpoints[percentile],
                    name=name,
                    mode='markers',
                    marker=dict(
                        symbol='diamond',
                        size=10,
                        color=color
                    ),
                    hovertemplate=(
                        '%{x}<br>' +
                        f'{name}: %{{y:.1f}}ms' +
                        '<extra></extra>'
                    ),
                    legend='legend1'
                ),
                row=1, col=1
            )

        # 2. Endpoint Load Distribution
        fig.add_trace(
            go.Scatter(
                x=top_endpoints['requests_adjusted'],
                y=top_endpoints['origin_time_avg'],
                mode='markers+text',
                text=top_endpoints.index,
                textposition="top center",
                marker=dict(
                    size=top_endpoints['requests_adjusted'] / top_endpoints['requests_adjusted'].max() * 50,
                    color=top_endpoints['origin_p95'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(
                        title='P95 Response Time (ms)',
                        x=1.0
                    )
                ),
                hovertemplate=(
                    '%{text}<br>' +
                    'Requests: %{x:,.0f}<br>' +
                    'Avg Time: %{y:.1f}ms<br>' +
                    'P95: %{marker.color:.1f}ms' +
                    '<extra>Load Distribution</extra>'
                ),
                legend='legend2'
            ),
            row=1, col=2
        )

        # 3. Response Time Trends
        # Get rolling averages for top 5 endpoints
        df_time = df.set_index('timestamp')
        top_5_endpoints = top_endpoints.head().index

        for endpoint in top_5_endpoints:
            endpoint_data = df_time[df_time['endpoint'] == endpoint]
            endpoint_series = endpoint_data['origin_time_avg'].resample('5min').mean()
            
            fig.add_trace(
                go.Scatter(
                    x=endpoint_series.index,
                    y=endpoint_series.values,
                    name=endpoint,
                    mode='lines',
                    hovertemplate=(
                        '%{x}<br>' +
                        'Time: %{y:.1f}ms' +
                        f'<extra>{endpoint}</extra>'
                    ),
                    legend='legend3'
                ),
                row=2, col=1
            )

        # 4. Error Rate by Endpoint
        fig.add_trace(
            go.Bar(
                x=top_endpoints.index,
                y=top_endpoints['error_rate_5xx'] * 100,
                name='5xx Errors',
                marker_color=colors['error'],
                hovertemplate=(
                    '%{x}<br>' +
                    '5xx Rate: %{y:.2f}%' +
                    '<extra>5xx Errors</extra>'
                ),
                legend='legend4'
            ),
            row=2, col=2
        )

        fig.add_trace(
            go.Bar(
                x=top_endpoints.index,
                y=top_endpoints['error_rate_4xx'] * 100,
                name='4xx Errors',
                marker_color=colors['warning'],
                hovertemplate=(
                    '%{x}<br>' +
                    '4xx Rate: %{y:.2f}%' +
                    '<extra>4xx Errors</extra>'
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
            barmode='stack',
            
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
        fig.update_xaxes(title_text='Endpoint', tickangle=45, row=1, col=1)
        fig.update_xaxes(title_text='Requests', row=1, col=2)
        fig.update_xaxes(title_text='Time', row=2, col=1)
        fig.update_xaxes(title_text='Endpoint', tickangle=45, row=2, col=2)

        fig.update_yaxes(title_text='Response Time (ms)', row=1, col=1)
        fig.update_yaxes(title_text='Response Time (ms)', row=1, col=2)
        fig.update_yaxes(title_text='Response Time (ms)', row=2, col=1)
        fig.update_yaxes(title_text='Error Rate (%)', row=2, col=2)

        # Update subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating origin endpoint dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating origin endpoint dashboard")

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
