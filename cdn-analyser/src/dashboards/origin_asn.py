import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
import traceback

logger = logging.getLogger(__name__)

def create_origin_asn_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create origin server ASN analysis dashboard."""
    try:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'ASN Performance Distribution',
                'Top ASNs by Request Volume',
                'ASN Response Time Trends',
                'Error Rates by ASN'
            ),
            specs=[
                [{"type": "box"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "bar"}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # Get aggregated ASN metrics
        asn_metrics = df.groupby('client_asn').agg({
            'origin_time_avg': 'mean',
            'origin_p50': 'mean',
            'origin_p95': 'mean',
            'origin_p99': 'mean',
            'requests_adjusted': 'sum',
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean'
        })

        # Get top ASNs by request volume
        top_asns = asn_metrics.nlargest(10, 'requests_adjusted')

        # 1. ASN Response Time Distribution
        # Filter data for top ASNs
        asn_data = df[df['client_asn'].isin(top_asns.index)]
        
        fig.add_trace(
            go.Box(
                x=asn_data['client_asn'],
                y=asn_data['origin_time_avg'],
                name='Response Time',
                marker_color=colors['origin'],
                boxpoints='outliers',
                hovertemplate=(
                    'ASN: %{x}<br>' +
                    'Response Time: %{y:.1f}ms' +
                    '<extra></extra>'
                ),
                legend='legend1'
            ),
            row=1, col=1
        )

        # Add percentile markers
        for metric, name, color in [
            ('origin_p95', 'P95', colors['warning']),
            ('origin_p99', 'P99', colors['error'])
        ]:
            fig.add_trace(
                go.Scatter(
                    x=top_asns.index,
                    y=top_asns[metric],
                    name=name,
                    mode='markers',
                    marker=dict(
                        symbol='diamond',
                        size=10,
                        color=color
                    ),
                    hovertemplate=(
                        'ASN: %{x}<br>' +
                        f'{name}: %{{y:.1f}}ms' +
                        '<extra></extra>'
                    ),
                    legend='legend1'
                ),
                row=1, col=1
            )

        # 2. Top ASNs by Volume
        fig.add_trace(
            go.Bar(
                x=top_asns.index,
                y=top_asns['requests_adjusted'],
                name='Request Volume',
                marker_color=colors['primary'],
                hovertemplate=(
                    'ASN: %{x}<br>' +
                    'Requests: %{y:,.0f}' +
                    '<extra>Request Volume</extra>'
                ),
                legend='legend2'
            ),
            row=1, col=2
        )

        # Add average response time line
        fig.add_trace(
            go.Scatter(
                x=top_asns.index,
                y=top_asns['origin_time_avg'],
                name='Avg Response Time',
                mode='lines+markers',
                line=dict(color=colors['origin']),
                yaxis='y2',
                hovertemplate=(
                    'ASN: %{x}<br>' +
                    'Avg Time: %{y:.1f}ms' +
                    '<extra>Response Time</extra>'
                ),
                legend='legend2'
            ),
            row=1, col=2
        )

        # 3. ASN Response Time Trends
        df_time = df.set_index('timestamp')
        for asn in top_asns.head().index:
            asn_data = df_time[df_time['client_asn'] == asn]
            time_series = asn_data['origin_time_avg'].resample('5min').mean()
            
            fig.add_trace(
                go.Scatter(
                    x=time_series.index,
                    y=time_series.values,
                    name=f'ASN {asn}',
                    mode='lines',
                    hovertemplate=(
                        'Time: %{x}<br>' +
                        'Response Time: %{y:.1f}ms' +
                        f'<extra>ASN {asn}</extra>'
                    ),
                    legend='legend3'
                ),
                row=2, col=1
            )

        # 4. Error Rates by ASN
        fig.add_trace(
            go.Bar(
                x=top_asns.index,
                y=top_asns['error_rate_5xx'] * 100,
                name='5xx Errors',
                marker_color=colors['error'],
                hovertemplate=(
                    'ASN: %{x}<br>' +
                    '5xx Rate: %{y:.2f}%' +
                    '<extra>5xx Errors</extra>'
                ),
                legend='legend4'
            ),
            row=2, col=2
        )

        fig.add_trace(
            go.Bar(
                x=top_asns.index,
                y=top_asns['error_rate_4xx'] * 100,
                name='4xx Errors',
                marker_color=colors['warning'],
                hovertemplate=(
                    'ASN: %{x}<br>' +
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

        # Add secondary y-axis for response time in the volume plot
        fig.update_layout(
            yaxis2=dict(
                title="Response Time (ms)",
                overlaying="y",
                side="right",
                showgrid=False
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
        fig.update_xaxes(title_text='ASN', tickangle=45, row=1, col=1)
        fig.update_xaxes(title_text='ASN', tickangle=45, row=1, col=2)
        fig.update_xaxes(title_text='Time', row=2, col=1)
        fig.update_xaxes(title_text='ASN', tickangle=45, row=2, col=2)

        fig.update_yaxes(title_text='Response Time (ms)', row=1, col=1)
        fig.update_yaxes(title_text='Requests', row=1, col=2)
        fig.update_yaxes(title_text='Response Time (ms)', row=2, col=1)
        fig.update_yaxes(title_text='Error Rate (%)', row=2, col=2)

        # Update subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating origin ASN dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating origin ASN dashboard")

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
