import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
import traceback

logger = logging.getLogger(__name__)

def create_performance_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create performance dashboard with individual legends."""
    try:
        # Initialize subplots with refined spacing and layout
        fig = make_subplots(
            rows=3, cols=2,
            specs=[
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "xy"}, {"type": "xy"}],
                [{"type": "xy"}, {"type": "choropleth"}]
            ],
            vertical_spacing=0.3,
            horizontal_spacing=0.15,
            row_heights=[0.35, 0.35, 0.30],
            subplot_titles=[
                "Edge & Origin Response Times", "Request Volume Over Time",
                "ASN Performance", "Protocol Performance",
                "Top Endpoints Performance", "Geographic Response Times"
            ]
        )

        # Process time series data
        df_time = df.set_index('timestamp').sort_index()
        rolling_window = '5min'

        # 1. Edge vs Origin Times with Quantiles
        add_trace_with_legend(fig, go.Scatter(
            x=df_time.index,
            y=df_time['ttfb_avg'].rolling(rolling_window).mean(),
            name='Edge TTFB',
            line=dict(color=colors['edge'], width=2),
            legendgroup='edge',
            legendgrouptitle_text="Edge & Origin Times",
            showlegend=True,
            hovertemplate='Time: %{x}<br>TTFB: %{y:.2f} ms<extra>Edge TTFB</extra>'
        ), 1, 1)

        for quantile, name, color in [
            ('ttfb_p50', 'Edge P50', 'rgba(46, 134, 193, 0.6)'),
            ('ttfb_p95', 'Edge P95', 'rgba(46, 134, 193, 0.4)'),
            ('ttfb_p99', 'Edge P99', 'rgba(46, 134, 193, 0.2)')
        ]:
            add_trace_with_legend(fig, go.Scatter(
                x=df_time.index,
                y=df_time[quantile].rolling(rolling_window).mean(),
                name=name,
                line=dict(color=color, width=1, dash='dot'),
                legendgroup='edge',
                showlegend=True,
                hovertemplate=f'Time: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
            ), 1, 1)

        add_trace_with_legend(fig, go.Scatter(
            x=df_time.index,
            y=df_time['origin_time_avg'].rolling(rolling_window).mean(),
            name='Origin',
            line=dict(color=colors['origin'], width=2),
            legendgroup='origin',
            showlegend=True,
            hovertemplate='Time: %{x}<br>Response Time: %{y:.2f} ms<extra>Origin Response</extra>'
        ), 1, 1)

        for quantile, name, color in [
            ('origin_p50', 'Origin P50', 'rgba(231, 76, 60, 0.6)'),
            ('origin_p95', 'Origin P95', 'rgba(231, 76, 60, 0.4)'),
            ('origin_p99', 'Origin P99', 'rgba(231, 76, 60, 0.2)')
        ]:
            add_trace_with_legend(fig, go.Scatter(
                x=df_time.index,
                y=df_time[quantile].rolling(rolling_window).mean(),
                name=name,
                line=dict(color=color, width=1, dash='dot'),
                legendgroup='origin',
                showlegend=True,
                hovertemplate=f'Time: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
            ), 1, 1)

        # 2. Request Volume
        add_trace_with_legend(fig, go.Scatter(
            x=df_time.index,
            y=df_time['requests_adjusted'].rolling(rolling_window).mean(),
            name='Requests',
            line=dict(color=colors['success'], width=2),
            fill='tozeroy',
            fillcolor="rgba(46, 204, 113, 0.1)",
            legendgroup='requests',
            legendgrouptitle_text="Request Volume",
            showlegend=True,
            hovertemplate='Time: %{x}<br>Requests: %{y:.0f}<extra></extra>'
        ), 1, 2)

        # 3. ASN Performance
        top_asns = (df.groupby('clientAsn')['requests_adjusted']
                   .sum()
                   .nlargest(10)
                   .index)
        
        asn_time_data = df_time[df_time['clientAsn'].isin(top_asns)]
        
        for idx, asn in enumerate(top_asns):
            asn_data = asn_time_data[asn_time_data['clientAsn'] == asn]
            rolling_avg = asn_data['ttfb_avg'].rolling(rolling_window).mean()
            
            add_trace_with_legend(fig, go.Scatter(
                x=asn_data.index,
                y=rolling_avg,
                name=f'ASN {asn}',
                mode='lines',
                line=dict(color=px.colors.qualitative.Set3[idx % len(px.colors.qualitative.Set3)], width=2),
                legendgroup='asn',
                legendgrouptitle_text="ASN Performance",
                showlegend=True,
                hovertemplate=(
                    f'ASN: {asn}<br>'
                    'Time: %{x}<br>'
                    'TTFB: %{y:.2f} ms<extra></extra>'
                )
            ), 2, 1)

        # 4. Protocol Performance
        protocol_perf = df.groupby('protocol').agg({
            'ttfb_avg': 'mean',
            'ttfb_p50': 'mean',
            'ttfb_p95': 'mean',
            'ttfb_p99': 'mean',
            'requests_adjusted': 'sum'
        })

        add_trace_with_legend(fig, go.Bar(
            x=protocol_perf.index,
            y=protocol_perf['ttfb_avg'],
            name='TTFB',
            marker_color=colors['edge'],
            legendgroup='protocol',
            legendgrouptitle_text="Protocol Performance",
            showlegend=True,
            hovertemplate='%{x}<br>Avg TTFB: %{y:.2f} ms<extra></extra>'
        ), 2, 2)

        for metric, name, color in [
            ('ttfb_p50', 'P50', 'rgba(46, 134, 193, 0.6)'),
            ('ttfb_p95', 'P95', 'rgba(46, 134, 193, 0.4)'),
            ('ttfb_p99', 'P99', 'rgba(46, 134, 193, 0.2)')
        ]:
            add_trace_with_legend(fig, go.Scatter(
                x=protocol_perf.index,
                y=protocol_perf[metric],
                name=name,
                mode='markers',
                marker=dict(
                    symbol='diamond',
                    size=12,
                    color=color,
                    line=dict(width=1, color='white')
                ),
                legendgroup='protocol',
                showlegend=True,
                hovertemplate=f'Protocol: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
            ), 2, 2)

        # 5. Top Endpoints Performance
        top_endpoints = df.groupby('endpoint').agg({
            'ttfb_avg': 'mean',
            'ttfb_p50': 'mean',
            'ttfb_p95': 'mean',
            'ttfb_p99': 'mean',
            'requests_adjusted': 'sum'
        }).nlargest(10, 'requests_adjusted')

        add_trace_with_legend(fig, go.Bar(
            x=top_endpoints.index,
            y=top_endpoints['ttfb_avg'],
            name='TTFB',
            marker_color=colors['edge'],
            legendgroup='endpoints',
            legendgrouptitle_text="Endpoint Performance",
            showlegend=True,
            hovertemplate='Endpoint: %{x}<br>Avg TTFB: %{y:.2f} ms<extra></extra>'
        ), 3, 1)

        for metric, name, color in [
            ('ttfb_p50', 'P50', 'rgba(46, 134, 193, 0.6)'),
            ('ttfb_p95', 'P95', 'rgba(46, 134, 193, 0.4)'),
            ('ttfb_p99', 'P99', 'rgba(46, 134, 193, 0.2)')
        ]:
            add_trace_with_legend(fig, go.Scatter(
                x=top_endpoints.index,
                y=top_endpoints[metric],
                name=name,
                mode='markers',
                marker=dict(
                    symbol='diamond',
                    size=12,
                    color=color,
                    line=dict(width=1, color='white')
                ),
                legendgroup='endpoints',
                showlegend=True,
                hovertemplate=f'Endpoint: %{{x}}<br>{name}: %{{y:.2f}} ms<extra></extra>'
            ), 3, 1)

        # 6. Geographic Performance
        geo_perf = df.groupby('country').agg({
            'ttfb_avg': 'mean',
            'ttfb_p95': 'mean',
            'requests_adjusted': 'sum'
        }).reset_index()
        
        add_trace_with_legend(fig, go.Choropleth(
            locations=geo_perf['country'],
            z=geo_perf['ttfb_avg'],
            colorscale=[
                [0, colors['edge']],
                [0.5, 'rgba(46, 134, 193, 0.4)'],
                [1, colors['origin']]
            ],
            reversescale=True,
            name='Geographic Performance',
            colorbar=dict(
                title=dict(
                    text='TTFB (ms)',
                    font=dict(size=12, color='white')
                ),
                thickness=15,
                len=0.8,
                tickfont=dict(size=10, color='white'),
                yanchor='middle',
                y=0.5,
                xanchor='left',
                x=1.02
            ),
            locationmode='country names',
            showscale=True,
            zmin=0,
            zmax=geo_perf['ttfb_avg'].quantile(0.95),
            hovertemplate=(
                '%{location}<br>'
                'Avg TTFB: %{z:.2f} ms<br>'
                'P95: %{customdata:.2f} ms'
                '<extra></extra>'
            ),
            customdata=geo_perf['ttfb_p95']
        ), 3, 2)

        # Update layout
        fig.update_layout(
            height=1500,
            showlegend=True,
            template='plotly_dark',
            paper_bgcolor='#1e1e1e',
            plot_bgcolor='#1e1e1e',
            margin=dict(l=60, r=60, t=80, b=60),
            legend=dict(
                orientation="h",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12, color='white'),
                yanchor="bottom",
                y=1.03,
                xanchor="right",
                x=0.75,
                itemsizing='constant'
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

        # Enhanced geo layout
        fig.update_geos(
            showcoastlines=True,
            coastlinecolor='rgba(255, 255, 255, 0.2)',
            showland=True,
            landcolor='#181b1f',
            showocean=True,
            oceancolor='#181b1f',
            showframe=False,
            framecolor='rgba(255, 255, 255, 0.2)',
            showlakes=True,
            lakecolor='#181b1f',
            showcountries=True,
            countrycolor='rgba(255, 255, 255, 0.2)',
            bgcolor='#181b1f',
            projection_type='equirectangular',
            resolution=110,
            lonaxis_range=[-180, 180],
            lataxis_range=[-90, 90]
        )

        # Update axis titles
        fig.update_xaxes(title_text="Time", row=1, col=1)
        fig.update_xaxes(title_text="Time", row=1, col=2)
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_xaxes(title_text="Protocol", row=2, col=2)
        fig.update_xaxes(title_text="Endpoint", row=3, col=1, tickangle=45)

        fig.update_yaxes(title_text="Response Time (ms)", row=1, col=1)
        fig.update_yaxes(title_text="Requests", row=1, col=2)
        fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
        fig.update_yaxes(title_text="Response Time (ms)", row=2, col=2)
        fig.update_yaxes(title_text="Response Time (ms)", row=3, col=1)

        return fig

    except Exception as e:
        logger.error(f"Error creating performance dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating performance dashboard")

def add_trace_with_legend(fig, trace, row, col):
    """Add a trace to the figure and assign it to the correct legend."""
    fig.add_trace(trace, row=row, col=col)
    legend_name = f"legend{row}{col}"
    trace.update(legend=legend_name)

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
        paper_bgcolor='#181b1f',
        plot_bgcolor='#181b1f',
        margin=dict(l=40, r=40, t=40, b=40),
        height=600
    )

    return fig

# Add this function to handle legend positioning
def update_legend_layout(fig):
    """Update the layout with multiple legends."""
    legend_positions = {
        (1, 1): {'x': 1.02, 'y': 0.95},  # Edge & Origin Times
        (1, 2): {'x': 1.02, 'y': 0.95},  # Request Volume
        (2, 1): {'x': 1.02, 'y': 0.65},  # ASN Performance
        (2, 2): {'x': 1.02, 'y': 0.65},  # Protocol Performance
        (3, 1): {'x': 1.02, 'y': 0.35},  # Top Endpoints
        (3, 2): {'x': 1.02, 'y': 0.35}   # Geographic
    }
    
    for (row, col), pos in legend_positions.items():
        legend_name = f"legend{row}{col}"
        fig.update_layout(**{
            legend_name: dict(
                yanchor="top",
                y=pos['y'],
                xanchor="left",
                x=pos['x'],
                bgcolor='rgba(24, 27, 31, 0.8)',
                bordercolor='rgba(255, 255, 255, 0.2)',
                borderwidth=1,
                font=dict(size=10)
            )
        })
