import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import logging
import traceback
logger = logging.getLogger(__name__)

def create_geographic_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create geographic analysis dashboard with properly configured choropleth maps."""
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
                [{"type": "choropleth"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # Calculate geographic metrics
        geo_metrics = df.groupby('country').agg({
            'ttfb_avg': 'mean',
            'requests_adjusted': 'sum',
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean',
            'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100
        }).reset_index()

        # Geographic Performance Heatmap
        fig.add_trace(
            go.Choropleth(
                locations=geo_metrics['country'],
                z=geo_metrics['ttfb_avg'],
                colorscale='Viridis',
                reversescale=True,
                name='Response Time',
                colorbar_title='TTFB (ms)',
                locationmode='country names',
                showscale=True,
                zmin=0,
                zmax=geo_metrics['ttfb_avg'].quantile(0.95),
                hovertemplate='%{location}<br>TTFB: %{z:.2f}ms<extra></extra>'
            ),
            row=1, col=1
        )

        # Top Countries by Traffic
        top_countries = geo_metrics.nlargest(10, 'requests_adjusted')
        fig.add_trace(
            go.Bar(
                x=top_countries['country'],
                y=top_countries['requests_adjusted'],
                name='Request Volume',
                marker_color=colors['primary'],
                hovertemplate='%{x}<br>Requests: %{y:,.0f}<extra></extra>'
            ),
            row=1, col=2
        )

        # Response Time by Country
        fig.add_trace(
            go.Bar(
                x=top_countries['country'],
                y=top_countries['ttfb_avg'],
                name='Response Time',
                marker_color=colors['edge'],
                hovertemplate='%{x}<br>TTFB: %{y:.2f}ms<extra></extra>'
            ),
            row=2, col=1
        )

        # Error Rates by Country
        error_rates = top_countries['error_rate_4xx'] + top_countries['error_rate_5xx']
        fig.add_trace(
            go.Bar(
                x=top_countries['country'],
                y=error_rates * 100,
                name='Error Rate',
                marker_color=colors['error'],
                hovertemplate='%{x}<br>Error Rate: %{y:.2f}%<extra></extra>'
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
            title_font=dict(size=14, color='white'),
            tickfont=dict(size=12, color='white'),
            gridcolor='#333',
            title_standoff=20,
            zeroline=False,
            tickangle=45
        )
        
        fig.update_yaxes(
            title_font=dict(size=14, color='white'),
            tickfont=dict(size=12, color='white'),
            gridcolor='#333',
            title_standoff=20,
            zeroline=False
        )

        # Update geo layout with complete configuration
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

        # Add axis titles
        fig.update_yaxes(title_text="Requests", row=1, col=2)
        fig.update_yaxes(title_text="Response Time (ms)", row=2, col=1)
        fig.update_yaxes(title_text="Error Rate (%)", row=2, col=2)

        # Update subplot titles
        for i in fig['layout']['annotations']:
            i['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating geographic dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating geographic dashboard")

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

