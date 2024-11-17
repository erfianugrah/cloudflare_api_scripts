import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import logging
import traceback

logger = logging.getLogger(__name__)

def create_origin_geographic_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create origin server geographic analysis dashboard."""
    try:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Origin Response Time by Country',
                'Error Distribution by Country',
                'Request Volume Distribution',
                'Performance Metrics by Region'
            ),
            specs=[
                [{"type": "choropleth"}, {"type": "choropleth"}],
                [{"type": "choropleth"}, {"type": "bar"}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # Calculate metrics by country
        geo_metrics = df.groupby('country').agg({
            'origin_time_avg': 'mean',
            'origin_p95': 'mean',
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean',
            'requests_adjusted': 'sum',
            'bytes_adjusted': 'sum'
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
                showscale=True,
                zmin=0,
                zmax=geo_metrics['origin_time_avg'].quantile(0.95),
                hovertemplate=(
                    '%{location}<br>' +
                    'Avg Time: %{z:.1f}ms<br>' +
                    'P95: %{customdata:.1f}ms' +
                    '<extra>Response Time</extra>'
                ),
                customdata=geo_metrics['origin_p95'],
                legend='legend1'
            ),
            row=1, col=1
        )

        # 2. Error Rate Choropleth
        total_error_rate = (geo_metrics['error_rate_4xx'] + geo_metrics['error_rate_5xx']) * 100
        
        fig.add_trace(
            go.Choropleth(
                locations=geo_metrics['country'],
                z=total_error_rate,
                colorscale='Reds',
                name='Error Rate',
                colorbar_title='Error Rate (%)',
                locationmode='country names',
                showscale=True,
                zmin=0,
                zmax=min(100, total_error_rate.max()),
                hovertemplate=(
                    '%{location}<br>' +
                    'Error Rate: %{z:.2f}%<br>' +
                    '4xx: %{customdata[0]:.2f}%<br>' +
                    '5xx: %{customdata[1]:.2f}%' +
                    '<extra>Error Rate</extra>'
                ),
                customdata=np.stack((
                    geo_metrics['error_rate_4xx'] * 100,
                    geo_metrics['error_rate_5xx'] * 100
                ), axis=1),
                legend='legend2'
            ),
            row=1, col=2
        )

        # 3. Request Volume Choropleth
        fig.add_trace(
            go.Choropleth(
                locations=geo_metrics['country'],
                z=geo_metrics['requests_adjusted'],
                colorscale='Blues',
                name='Request Volume',
                colorbar_title='Requests',
                locationmode='country names',
                showscale=True,
                hovertemplate=(
                    '%{location}<br>' +
                    'Requests: %{z:,.0f}<br>' +
                    'Bandwidth: %{customdata:.2f} GB' +
                    '<extra>Request Volume</extra>'
                ),
                customdata=geo_metrics['bytes_adjusted'] / (1024 ** 3),
                legend='legend3'
            ),
            row=2, col=1
        )

        # 4. Top Countries Performance Metrics
        top_countries = geo_metrics.nlargest(10, 'requests_adjusted')

        # Response Time bars
        fig.add_trace(
            go.Bar(
                x=top_countries['country'],
                y=top_countries['origin_time_avg'],
                name='Avg Response Time',
                marker_color=colors['origin'],
                hovertemplate=(
                    '%{x}<br>' +
                    'Response Time: %{y:.1f}ms' +
                    '<extra>Response Time</extra>'
                ),
                legend='legend4'
            ),
            row=2, col=2
        )

        # Add P95 markers
        fig.add_trace(
            go.Scatter(
                x=top_countries['country'],
                y=top_countries['origin_p95'],
                name='P95 Response Time',
                mode='markers',
                marker=dict(
                    size=10,
                    symbol='diamond',
                    color=colors['warning']
                ),
                hovertemplate=(
                    '%{x}<br>' +
                    'P95: %{y:.1f}ms' +
                    '<extra>P95 Response Time</extra>'
                ),
                legend='legend4'
            ),
            row=2, col=2
        )

        # Update layout
        fig.update_layout(
            height=1200,  # Increased height for better choropleth visibility
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

        # Update geo settings for choropleth maps
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

        # Update axes for the bar plot
        fig.update_xaxes(
            title_text='Country',
            gridcolor='#333',
            title_font=dict(size=12),
            tickfont=dict(size=10),
            tickangle=45,
            row=2, col=2
        )
        fig.update_yaxes(
            title_text='Response Time (ms)',
            gridcolor='#333',
            title_font=dict(size=12),
            tickfont=dict(size=10),
            row=2, col=2
        )

        # Update subplot titles
        for annotation in fig['layout']['annotations']:
            annotation['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating origin geographic dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating origin geographic dashboard")

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
