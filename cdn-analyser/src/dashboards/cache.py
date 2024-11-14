import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import logging
import traceback
logger = logging.getLogger(__name__)

def create_cache_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
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
                line=dict(color=colors['cache_hit']),
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
                line=dict(color=colors['secondary'], dash='dot'),
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
                    colors['cache_hit'],  # hit
                    colors['warning'],    # miss
                    colors['error'],      # error
                    colors['secondary']   # other
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
                marker_color=colors['cache_hit'],
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
                    colors['cache_hit'],  # hit
                    colors['warning'],    # miss
                    colors['error'],      # error
                    colors['secondary']   # other
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
        return _create_error_figure("Error generating cache dashboard")

def _create_error_figure(message: str) -> go.Figure:
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
