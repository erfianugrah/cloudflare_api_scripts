import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import logging
import traceback

logger = logging.getLogger(__name__)

def create_origin_error_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create origin server error analysis dashboard."""
    try:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Error Rate Over Time',
                'Error Status Distribution',
                'Error Rate by Response Time',
                'Top Error URLs'
            ),
            specs=[
                [{"secondary_y": True}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "bar"}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # 1. Error Rate Over Time with Request Volume
        df_time = df.set_index('timestamp').resample('5min').agg({
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean',
            'requests_adjusted': 'sum',
            'origin_time_avg': 'mean'
        }).reset_index()

        # 4xx Errors
        fig.add_trace(
            go.Scatter(
                x=df_time['timestamp'],
                y=df_time['error_rate_4xx'] * 100,
                name='4xx Errors',
                line=dict(color=colors['warning']),
                hovertemplate=(
                    'Time: %{x}<br>' +
                    'Error Rate: %{y:.2f}%' +
                    '<extra>4xx Errors</extra>'
                ),
                legend='legend1'
            ),
            row=1, col=1
        )

        # 5xx Errors
        fig.add_trace(
            go.Scatter(
                x=df_time['timestamp'],
                y=df_time['error_rate_5xx'] * 100,
                name='5xx Errors',
                line=dict(color=colors['error']),
                hovertemplate=(
                    'Time: %{x}<br>' +
                    'Error Rate: %{y:.2f}%' +
                    '<extra>5xx Errors</extra>'
                ),
                legend='legend1'
            ),
            row=1, col=1
        )

        # Request Volume
        fig.add_trace(
            go.Scatter(
                x=df_time['timestamp'],
                y=df_time['requests_adjusted'],
                name='Requests',
                line=dict(color=colors['secondary'], dash='dot'),
                hovertemplate=(
                    'Time: %{x}<br>' +
                    'Requests: %{y:,.0f}' +
                    '<extra>Request Volume</extra>'
                ),
                legend='legend1'
            ),
            row=1, col=1,
            secondary_y=True
        )

        # 2. Error Status Distribution
        error_df = df[df['status'] >= 400]
        status_dist = error_df['status'].value_counts().sort_index()
        
        fig.add_trace(
            go.Bar(
                x=status_dist.index.astype(str),
                y=status_dist.values,
                marker_color=[
                    colors['warning'] if s < 500 else colors['error']
                    for s in status_dist.index
                ],
                name='Error Distribution',
                hovertemplate=(
                    'Status: %{x}<br>' +
                    'Count: %{y:,}' +
                    '<extra>Error Status</extra>'
                ),
                legend='legend2'
            ),
            row=1, col=2
        )

        # 3. Error Rate by Response Time
        error_bins = pd.qcut(df['origin_time_avg'], q=10)
        error_by_time = df.groupby(error_bins).agg({
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean',
            'requests_adjusted': 'sum'
        }).reset_index()
        
        # Add 4xx error rate
        fig.add_trace(
            go.Scatter(
                x=error_by_time['origin_time_avg'].apply(lambda x: x.right),
                y=error_by_time['error_rate_4xx'] * 100,
                name='4xx Rate',
                mode='lines+markers',
                line=dict(color=colors['warning']),
                hovertemplate=(
                    'Response Time: %{x:.0f}ms<br>' +
                    'Error Rate: %{y:.2f}%' +
                    '<extra>4xx Error Rate</extra>'
                ),
                legend='legend3'
            ),
            row=2, col=1
        )

        # Add 5xx error rate
        fig.add_trace(
            go.Scatter(
                x=error_by_time['origin_time_avg'].apply(lambda x: x.right),
                y=error_by_time['error_rate_5xx'] * 100,
                name='5xx Rate',
                mode='lines+markers',
                line=dict(color=colors['error']),
                hovertemplate=(
                    'Response Time: %{x:.0f}ms<br>' +
                    'Error Rate: %{y:.2f}%' +
                    '<extra>5xx Error Rate</extra>'
                ),
                legend='legend3'
            ),
            row=2, col=1
        )

        # 4. Top Error URLs
        top_error_urls = df[df['status'] >= 400].groupby('endpoint').agg({
            'requests_adjusted': 'sum',
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean'
        }).sort_values('requests_adjusted', ascending=True).tail(10)

        # Add stacked bar for error rates
        fig.add_trace(
            go.Bar(
                y=top_error_urls.index,
                x=top_error_urls['error_rate_4xx'] * 100,
                name='4xx Errors',
                orientation='h',
                marker_color=colors['warning'],
                hovertemplate=(
                    '%{y}<br>' +
                    '4xx Rate: %{x:.2f}%' +
                    '<extra></extra>'
                ),
                legend='legend4'
            ),
            row=2, col=2
        )

        fig.add_trace(
            go.Bar(
                y=top_error_urls.index,
                x=top_error_urls['error_rate_5xx'] * 100,
                name='5xx Errors',
                orientation='h',
                marker_color=colors['error'],
                hovertemplate=(
                    '%{y}<br>' +
                    '5xx Rate: %{x:.2f}%' +
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
        fig.update_xaxes(title_text='Time', row=1, col=1)
        fig.update_xaxes(title_text='Status Code', row=1, col=2)
        fig.update_xaxes(title_text='Response Time (ms)', row=2, col=1)
        fig.update_xaxes(title_text='Error Rate (%)', row=2, col=2)

        fig.update_yaxes(title_text='Error Rate (%)', secondary_y=False, row=1, col=1)
        fig.update_yaxes(title_text='Requests', secondary_y=True, row=1, col=1)
        fig.update_yaxes(title_text='Count', row=1, col=2)
        fig.update_yaxes(title_text='Error Rate (%)', row=2, col=1)
        fig.update_yaxes(title_text='Endpoint', row=2, col=2)

        # Update subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating origin error dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating origin error dashboard")

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
