import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import logging
import traceback
logger = logging.getLogger(__name__)

def create_rps_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create requests per second (RPS) analysis dashboard with right-side legends."""
    try:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Requests per Second Over Time',
                'RPS by HTTP Method',
                'Top Endpoints by RPS',
                'RPS Geographic Distribution'
            ),
            specs=[
                [{"secondary_y": True}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "choropleth"}]
            ],
            horizontal_spacing=0.2,
            vertical_spacing=0.2
        )

        # Calculate RPS over time
        df_time = df.set_index('timestamp').sort_index()
        rps = df_time.resample('1min').agg({
            'requests_adjusted': 'sum',
            'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean'
        })
        
        # Convert to requests per second
        rps['rps'] = rps['requests_adjusted'] / 60

        fig.add_trace(
            go.Scatter(
                x=rps.index,
                y=rps['rps'],
                name='RPS',
                line=dict(color=colors['primary'], width=2),
                hovertemplate='%{y:.1f} req/s<extra>RPS</extra>',
                legend='legend1'
            ),
            row=1, col=1
        )

        # Add cache hit ratio on secondary y-axis
        fig.add_trace(
            go.Scatter(
                x=rps.index,
                y=rps['cache_status'],
                name='Cache Hit Ratio',
                line=dict(color=colors['cache_hit'], dash='dot'),
                hovertemplate='%{y:.1f}%<extra>Cache Hit Ratio</extra>',
                legend='legend1'
            ),
            row=1, col=1,
            secondary_y=True
        )

        # RPS by HTTP Method
        method_rps = df.groupby('clientRequestMethod').agg({
            'requests_adjusted': 'sum'
        }).reset_index()
        method_rps['rps'] = method_rps['requests_adjusted'] / (df['timestamp'].max() - df['timestamp'].min()).total_seconds()

        fig.add_trace(
            go.Bar(
                x=method_rps['clientRequestMethod'],
                y=method_rps['rps'],
                name='RPS by Method',
                marker_color=colors['secondary'],
                hovertemplate='%{x}<br>%{y:.1f} req/s<extra>RPS by Method</extra>',
                legend='legend2'
            ),
            row=1, col=2
        )

        # Top Endpoints by RPS
        endpoint_rps = df.groupby('endpoint').agg({
            'requests_adjusted': 'sum'
        }).reset_index()
        endpoint_rps['rps'] = endpoint_rps['requests_adjusted'] / (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
        top_endpoints = endpoint_rps.nlargest(10, 'rps')

        fig.add_trace(
            go.Bar(
                x=top_endpoints['endpoint'],
                y=top_endpoints['rps'],
                name='Top Endpoints',
                marker_color=colors['primary'],
                hovertemplate='%{x}<br>%{y:.1f} req/s<extra>RPS by Endpoint</extra>',
                legend='legend3'
            ),
            row=2, col=1
        )

        # Geographic RPS Distribution
        geo_rps = df.groupby('country').agg({
            'requests_adjusted': 'sum'
        }).reset_index()
        total_seconds = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
        geo_rps['rps'] = geo_rps['requests_adjusted'] / total_seconds

        fig.add_trace(
            go.Choropleth(
                locations=geo_rps['country'],
                z=geo_rps['rps'],
                colorscale='Viridis',
                name='RPS by Country',
                colorbar_title='Requests/Second',
                locationmode='country names',
                showscale=True,
                zmin=0,
                zmax=geo_rps['rps'].quantile(0.95),
                hovertemplate='%{location}<br>%{z:.1f} req/s<extra>RPS by Country</extra>',
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
            
            # Position legends with adjusted spacing
            legend1=dict(
                x=0.43,
                y=0.95,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend2=dict(
                x=0.94,
                y=0.95,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend3=dict(
                x=0.43,
                y=0.45,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend4=dict(
                x=0.94,
                y=0.45,
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            )
        )

        # Update axes styling
        fig.update_xaxes(
            title_font=dict(size=14, color='white'),
            tickfont=dict(size=12, color='white'),
            gridcolor='#333',
            title_standoff=20,
            zeroline=False
        )

        fig.update_yaxes(
            title_font=dict(size=14, color='white'),
            tickfont=dict(size=12, color='white'),
            gridcolor='#333',
            title_standoff=20,
            zeroline=False
        )

        # Update geo layout
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

        # Specific axis labels
        fig.update_xaxes(title_text="Time", row=1, col=1)
        fig.update_xaxes(title_text="HTTP Method", row=1, col=2)
        fig.update_xaxes(title_text="Endpoint", row=2, col=1, tickangle=45)
        
        fig.update_yaxes(title_text="Requests/Second", row=1, col=1, secondary_y=False)
        fig.update_yaxes(title_text="Cache Hit Ratio (%)", row=1, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Requests/Second", row=1, col=2)
        fig.update_yaxes(title_text="Requests/Second", row=2, col=1)

        # Update subplot titles
        for i in fig['layout']['annotations']:
            i['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating RPS dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating RPS dashboard")

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
