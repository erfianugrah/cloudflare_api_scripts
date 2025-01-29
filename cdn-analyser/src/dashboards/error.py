import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import logging
import traceback
logger = logging.getLogger(__name__)

def create_error_dashboard(df: pd.DataFrame, analysis: dict, colors: dict) -> go.Figure:
    """Create error analysis dashboard with legends matching performance.py pattern."""
    try:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Error Rate Over Time',
                'Error Status Distribution',
                'Top Error Endpoints',
                'Geographic Error Distribution'
            ),
            specs=[
                [{"secondary_y": True}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "choropleth"}]
            ],
            vertical_spacing=0.15,
            horizontal_spacing=0.12
        )

        # Error Rate Over Time
        df_time = df.set_index('timestamp').sort_index()
        error_rates = df_time.resample('5min').agg({
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean',
            'requests_adjusted': 'sum'
        })

        fig.add_trace(
            go.Scatter(
                x=error_rates.index,
                y=error_rates['error_rate_4xx'] * 100,
                name='4xx Errors',
                line=dict(color=colors['warning']),
                hovertemplate='%{y:.2f}%<extra>4xx Error Rate</extra>',
                legend='legend1'
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=error_rates.index,
                y=error_rates['error_rate_5xx'] * 100,
                name='5xx Errors',
                line=dict(color=colors['error']),
                hovertemplate='%{y:.2f}%<extra>5xx Error Rate</extra>',
                legend='legend1'
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=error_rates.index,
                y=error_rates['requests_adjusted'],
                name='Request Volume',
                line=dict(color=colors['secondary'], dash='dot'),
                hovertemplate='%{y:.0f} requests<extra>Request Volume</extra>',
                legend='legend1'
            ),
            row=1, col=1,
            secondary_y=True
        )

        # Error Status Distribution
        error_df = df[df['status_code'] >= 400]
        status_dist = error_df['status_code'].value_counts().sort_index()
        
        fig.add_trace(
            go.Bar(
                x=status_dist.index.astype(str),
                y=status_dist.values,
                name='Error Distribution',
                marker_color=[colors['warning'] if s < 500 else colors['error'] 
                            for s in status_dist.index],
                hovertemplate='Status %{x}<br>Count: %{y:,}<extra></extra>',
                legend='legend2'
            ),
            row=1, col=2
        )

        # Top Error Endpoints
        endpoint_errors = df.groupby('path').agg({
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean',
            'requests_adjusted': 'sum'
        }).nlargest(10, 'requests_adjusted')

        fig.add_trace(
            go.Bar(
                x=endpoint_errors.index,
                y=endpoint_errors['error_rate_5xx'] * 100,
                name='5xx Error Rate',
                marker_color=colors['error'],
                hovertemplate='%{x}<br>5xx Rate: %{y:.2f}%<extra></extra>',
                legend='legend3'
            ),
            row=2, col=1
        )

        fig.add_trace(
            go.Bar(
                x=endpoint_errors.index,
                y=endpoint_errors['error_rate_4xx'] * 100,
                name='4xx Error Rate',
                marker_color=colors['warning'],
                hovertemplate='%{x}<br>4xx Rate: %{y:.2f}%<extra></extra>',
                legend='legend3'
            ),
            row=2, col=1
        )

        # Geographic Error Distribution
        geo_errors = df.groupby('country').agg({
            'error_rate_4xx': 'mean',
            'error_rate_5xx': 'mean',
            'requests_adjusted': 'sum'
        }).reset_index()

        total_error_rate = geo_errors['error_rate_4xx'] + geo_errors['error_rate_5xx']
        
        fig.add_trace(
            go.Choropleth(
                locations=geo_errors['country'],
                z=total_error_rate * 100,
                colorscale='Reds',
                name='Error Rate by Country',
                colorbar_title='Error Rate (%)',
                locationmode='country names',
                showscale=True,
                zmin=0,
                zmax=min(100, total_error_rate.max() * 100),
                hovertemplate='%{location}<br>Error Rate: %{z:.2f}%<extra></extra>',
                legend='legend4'
            ),
            row=2, col=2
        )

        # Update layout with adjusted legend positions
        fig.update_layout(
            height=1000,
            showlegend=True,
            template='plotly_dark',
            paper_bgcolor='#1e1e1e',
            plot_bgcolor='#1e1e1e',
            margin=dict(l=60, r=60, t=80, b=60),
            
            # Position legends to the right of each subplot with adjusted x-coordinates
            legend1=dict(
                x=0.44,    # Right side of first plot
                y=0.96,    # Top of first plot
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend2=dict(
                x=0.94,    # Moved slightly left to avoid overlap
                y=0.96,    # Top of second plot
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend3=dict(
                x=0.43,    # Right side of third plot
                y=0.43,    # Top of third plot
                xanchor="left",
                yanchor="top",
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='#333',
                borderwidth=1,
                font=dict(size=12)
            ),
            legend4=dict(
                x=0.95,    # Moved slightly left to avoid overlap
                y=0.43,    # Top of fourth plot
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
        fig.update_xaxes(title_text="Status Code", row=1, col=2)
        fig.update_xaxes(title_text="Endpoint", row=2, col=1, tickangle=45)
        
        fig.update_yaxes(title_text="Error Rate (%)", secondary_y=False, row=1, col=1)
        fig.update_yaxes(title_text="Requests", secondary_y=True, row=1, col=1)
        fig.update_yaxes(title_text="Count", row=1, col=2)
        fig.update_yaxes(title_text="Error Rate (%)", row=2, col=1)

        # Update subplot titles
        for i in fig['layout']['annotations']:
            i['font'] = dict(size=14, color='white')

        return fig

    except Exception as e:
        logger.error(f"Error creating error dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_figure("Error generating error dashboard")

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
