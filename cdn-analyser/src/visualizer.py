import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional
from matplotlib.dates import DateFormatter, AutoDateLocator
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import traceback
import plotly.express as px
from datetime import datetime

logger = logging.getLogger(__name__)

class Visualizer:
    """Enhanced visualizer for Cloudflare analytics with improved charts."""
    
    def __init__(self, config):
        self.config = config
        self.setup_style()
        
        # Modern color palette for consistent visualization
        self.colors = {
            'edge': '#3498db',      # Blue for edge metrics
            'origin': '#e74c3c',    # Red for origin metrics
            'cache_hit': '#2ecc71', # Green for cache hits
            'cache_miss': '#95a5a6',# Gray for cache misses
            'error': '#e67e22',     # Orange for errors
            'background': '#f8f9fa', # Light background
            'grid': '#dce0e3'       # Light gray grid
        }
        
    def setup_style(self):
        """Configure visualization style."""
        plt.style.use('seaborn-v0_8-whitegrid')
        plt.rcParams.update({
            'figure.figsize': [12, 6],
            'figure.dpi': 100,
            'savefig.dpi': 300,
            'font.size': 10,
            'axes.titlesize': 12,
            'axes.labelsize': 10,
            'xtick.labelsize': 9,
            'ytick.labelsize': 9,
            'axes.grid': True,
            'grid.alpha': 0.3,
            'figure.autolayout': True
        })
        sns.set_palette("husl")

    def _create_cache_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create cache analysis visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # 1. Cache Hit Ratio Over Time
            df_time = df.set_index('timestamp')
            df_time['cache_hit'] = df_time['cache_status'].isin(['hit', 'stale', 'revalidated'])
            hit_ratio = df_time['cache_hit'].rolling('5min').mean() * 100
            
            ax1.plot(hit_ratio.index, hit_ratio.values, color=self.colors['cache_hit'])
            ax1.set_title('Cache Hit Ratio Over Time')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Hit Ratio (%)')
            self._setup_time_axis(ax1, hit_ratio.index)

            # 2. Cache Status Distribution
            cache_dist = df.groupby('cache_status').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            x = np.arange(len(cache_dist))
            width = 0.35
            ax2.bar(x - width/2, cache_dist['requests_adjusted'], width, 
                   label='Requests', color=self.colors['edge'])
            ax2.bar(x + width/2, cache_dist['visits_adjusted'], width, 
                   label='Visits', color=self.colors['origin'])
            ax2.set_title('Cache Status Distribution')
            ax2.set_xlabel('Cache Status')
            ax2.set_ylabel('Count')
            ax2.legend()
            ax2.set_xticks(x)
            ax2.set_xticklabels(cache_dist['cache_status'], rotation=45, ha='right')

            # 3. Cache Performance by Content Type
            content_cache = df.groupby('content_type').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).nlargest(10, 'requests_adjusted')
            
            ax3.bar(range(len(content_cache)), content_cache['cache_status'],
                   color=self.colors['cache_hit'])
            ax3.set_title('Cache Hit Ratio by Content Type')
            ax3.set_xlabel('Content Type')
            ax3.set_ylabel('Hit Ratio (%)')
            ax3.set_xticks(range(len(content_cache)))
            ax3.set_xticklabels(content_cache.index, rotation=45, ha='right')

            # 4. Bandwidth Savings
            cache_bandwidth = df.groupby('cache_status').agg({
                'bytes_adjusted': 'sum'
            })
            total_bytes = cache_bandwidth['bytes_adjusted'].sum()
            cache_bandwidth['percentage'] = cache_bandwidth['bytes_adjusted'] / total_bytes * 100
            
            ax4.pie(cache_bandwidth['percentage'],
                   labels=[f"{status}\n({pct:.1f}%)" for status, pct in 
                          zip(cache_bandwidth.index, cache_bandwidth['percentage'])],
                   colors=[self.colors['cache_hit'], self.colors['cache_miss'], 
                          self.colors['error']],
                   autopct='%1.1f%%')
            ax4.set_title('Bandwidth Distribution by Cache Status')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'cache_analysis.png')

        except Exception as e:
            logger.error(f"Error creating cache analysis: {str(e)}")
            plt.close('all')

    def _create_error_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create error analysis visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Error Rate Over Time
            df_time = df.set_index('timestamp')
            error_rates = df_time[df_time['status'] >= 400].resample('5min').size() / \
                         df_time.resample('5min').size() * 100
            
            ax1.plot(error_rates.index, error_rates.values, color=self.colors['error'])
            ax1.set_title('Error Rate Over Time')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Error Rate (%)')
            self._setup_time_axis(ax1, error_rates.index)

            # 2. Error Status Distribution
            error_status = df[df['status'] >= 400].groupby('status').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            x = np.arange(len(error_status))
            width = 0.35
            ax2.bar(x - width/2, error_status['requests_adjusted'], width, 
                   label='Requests', color=self.colors['edge'])
            ax2.bar(x + width/2, error_status['visits_adjusted'], width, 
                   label='Visits', color=self.colors['origin'])
            ax2.set_title('Error Status Distribution')
            ax2.set_xlabel('Status Code')
            ax2.set_ylabel('Count')
            ax2.legend()
            ax2.set_xticks(x)
            ax2.set_xticklabels(error_status['status'])

            # 3. Errors by Endpoint
            endpoint_errors = df.groupby('endpoint').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).nlargest(10, 'error_rate_4xx')
            
            ax3.barh(range(len(endpoint_errors)), 
                    endpoint_errors['error_rate_4xx'] * 100,
                    label='4xx', color=self.colors['edge'])
            ax3.barh(range(len(endpoint_errors)), 
                    endpoint_errors['error_rate_5xx'] * 100,
                    left=endpoint_errors['error_rate_4xx'] * 100,
                    label='5xx', color=self.colors['error'])
            ax3.set_title('Error Rates by Top Endpoints')
            ax3.set_xlabel('Error Rate (%)')
            ax3.set_yticks(range(len(endpoint_errors)))
            ax3.set_yticklabels(endpoint_errors.index)
            ax3.legend()

            # 4. Error Response Time Distribution
            ax4.boxplot([df[df['status'] < 400]['ttfb_avg'],
                        df[df['status'].between(400, 499)]['ttfb_avg'],
                        df[df['status'] >= 500]['ttfb_avg']],
                       labels=['Success', '4xx', '5xx'])
            ax4.set_title('Response Time Distribution by Status')
            ax4.set_ylabel('Response Time (ms)')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'error_analysis.png')

        except Exception as e:
            logger.error(f"Error creating error analysis: {str(e)}")
            plt.close('all')

    def create_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str) -> None:
        """Create all visualizations for the analysis results."""
        try:
            if df is None or df.empty or not analysis:
                logger.error("No data available for visualization")
                return

            output_dir = self.config.images_dir / zone_name
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create both traditional and enhanced visualizations
            self._create_performance_overview(df, analysis, output_dir)
            self._create_cache_analysis(df, analysis, output_dir)
            self._create_error_analysis(df, analysis, output_dir)
            self._create_geographic_analysis(df, analysis, output_dir)
            self._create_time_series(df, analysis, output_dir)
            self._create_endpoint_analysis(df, analysis, output_dir)
            
            # Enhanced visualizations
            self._create_edge_origin_comparison(df, analysis, output_dir)
            self._create_enhanced_cache_dashboard(df, analysis, output_dir)
            self._create_enhanced_latency_analysis(df, analysis, output_dir)

        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')

    def _create_edge_origin_comparison(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create enhanced edge vs origin performance comparison."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Edge vs Origin Response Time',
                    'Cache Hit Ratio Over Time',
                    'Bandwidth Savings',
                    'Error Rates Comparison'
                )
            )
            
            # 1. Response Time Comparison
            fig.add_trace(
                go.Scatter(
                    x=df['timestamp'],
                    y=df['ttfb_avg'],
                    name='Edge TTFB',
                    line=dict(color=self.colors['edge'])
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(
                    x=df['timestamp'],
                    y=df['origin_time_avg'],
                    name='Origin Response Time',
                    line=dict(color=self.colors['origin'])
                ),
                row=1, col=1
            )
            
            # 2. Cache Performance
            cache_ratio = df.set_index('timestamp').resample('5min').agg({
                'cache_status': lambda x: (x.isin(['hit', 'stale', 'revalidated'])).mean() * 100
            })
            fig.add_trace(
                go.Scatter(
                    x=cache_ratio.index,
                    y=cache_ratio['cache_status'],
                    name='Cache Hit Ratio',
                    fill='tozeroy',
                    line=dict(color=self.colors['cache_hit'])
                ),
                row=1, col=2
            )
            
            # 3. Bandwidth Analysis
            edge_bandwidth = df[df['cache_status'].isin(['hit', 'stale', 'revalidated'])]['bytes_adjusted'].sum()
            origin_bandwidth = df[~df['cache_status'].isin(['hit', 'stale', 'revalidated'])]['bytes_adjusted'].sum()
            fig.add_trace(
                go.Bar(
                    x=['Edge Served', 'Origin Served'],
                    y=[edge_bandwidth / 1e9, origin_bandwidth / 1e9],
                    name='Bandwidth (GB)',
                    marker_color=[self.colors['edge'], self.colors['origin']]
                ),
                row=2, col=1
            )
            
            # 4. Error Rate Comparison
            error_rates = df.set_index('timestamp').resample('5min').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }) * 100
            
            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_4xx'],
                    name='4xx Errors',
                    line=dict(color=self.colors['edge'])
                ),
                row=2, col=2
            )
            fig.add_trace(
                go.Scatter(
                    x=error_rates.index,
                    y=error_rates['error_rate_5xx'],
                    name='5xx Errors',
                    line=dict(color=self.colors['error'])
                ),
                row=2, col=2
            )
            
            # Update layout
            fig.update_layout(
                height=800,
                showlegend=True,
                template='plotly_white',
                title_text='Edge vs Origin Performance Analysis',
                title_x=0.5
            )
            
            # Save both interactive and static versions
            fig.write_html(str(output_dir / 'edge_origin_comparison.html'))
            fig.write_image(str(output_dir / 'edge_origin_comparison.png'))
            
        except Exception as e:
            logger.error(f"Error creating edge vs origin comparison: {str(e)}")

    def _create_enhanced_cache_dashboard(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create enhanced cache performance dashboard."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Cache Status Distribution',
                    'Cache Performance by Content Type',
                    'Cache Hit Ratio Trends',
                    'Response Size Distribution'
                ),
                specs=[[{'type': 'domain'}, {'type': 'bar'}],
                      [{'type': 'scatter'}, {'type': 'box'}]]
            )
            
            # 1. Cache Status Distribution
            cache_dist = df['cache_status'].value_counts()
            fig.add_trace(
                go.Pie(
                    labels=cache_dist.index,
                    values=cache_dist.values,
                    hole=0.4,
                    marker_colors=[self.colors['cache_hit'], 
                                 self.colors['cache_miss'],
                                 self.colors['error']]
                ),
                row=1, col=1
            )
            
            # 2. Cache Performance by Content Type
            content_metrics = df.groupby('content_type').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100,
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')
            
            fig.add_trace(
                go.Bar(
                    x=content_metrics.index,
                    y=content_metrics['cache_status'],
                    marker_color=self.colors['cache_hit']
                ),
                row=1, col=2
            )
            
            # 3. Cache Hit Ratio Trends
            cache_trends = df.set_index('timestamp').resample('1H').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean() * 100
            })
            
            fig.add_trace(
                go.Scatter(
                    x=cache_trends.index,
                    y=cache_trends['cache_status'],
                    fill='tozeroy',
                    line=dict(color=self.colors['edge'])
                ),
                row=2, col=1
            )
            
            # 4. Response Size Distribution
            fig.add_trace(
                go.Box(
                    x=df['cache_status'],
                    y=df['bytes_adjusted'].apply(lambda x: x/1024/1024), # Convert to MB
                    marker_color=self.colors['edge']
                ),
                row=2, col=2
            )
            
            # Update layout
            fig.update_layout(
                height=800,
                showlegend=False,
                template='plotly_white',
                title_text='Cache Performance Analysis',
                title_x=0.5
            )
            
            # Save visualizations
            fig.write_html(str(output_dir / 'cache_analysis_dashboard.html'))
            fig.write_image(str(output_dir / 'cache_analysis_dashboard.png'))
            
        except Exception as e:
            logger.error(f"Error creating cache dashboard: {str(e)}")

    def _create_enhanced_latency_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create enhanced latency analysis visualization."""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    'Edge vs Origin Response Time Distribution',
                    'Response Time Percentiles',
                    'Response Time by Cache Status',
                    'Response Time Correlation'
                )
            )
            
            # 1. Response Time Distribution
            fig.add_trace(
                go.Histogram(
                    x=df['ttfb_avg'],
                    name='Edge TTFB',
                    opacity=0.7,
                    marker_color=self.colors['edge']
                ),
                row=1, col=1
            )
            fig.add_trace(
                go.Histogram(
                    x=df['origin_time_avg'],
                    name='Origin Response',
                    opacity=0.7,
                    marker_color=self.colors['origin']
                ),
                row=1, col=1
            )
            
            # 2. Response Time Percentiles
            percentiles = [50, 75, 90, 95, 99]
            edge_percentiles = [np.percentile(df['ttfb_avg'].dropna(), p) for p in percentiles]
            origin_percentiles = [np.percentile(df['origin_time_avg'].dropna(), p) for p in percentiles]
            
            fig.add_trace(
                go.Bar(
                    x=[f'P{p}' for p in percentiles],
                    y=edge_percentiles,
                    name='Edge TTFB',
                    marker_color=self.colors['edge']
                ),
                row=1, col=2
            )
            fig.add_trace(
                go.Bar(
                    x=[f'P{p}' for p in percentiles],
                    y=origin_percentiles,
                    name='Origin Response',
                    marker_color=self.colors['origin']
                ),
                row=1, col=2
            )
            
            # 3. Response Time by Cache Status
            fig.add_trace(
                go.Box(
                    x=df['cache_status'],
                    y=df['ttfb_avg'],
                    name='Edge TTFB',
                    marker_color=self.colors['edge']
                ),
                row=2, col=1
            )
            
            # 4. Response Time Correlation
            fig.add_trace(
                go.Scatter(
                    x=df['ttfb_avg'],
                    y=df['origin_time_avg'],
                    mode='markers',
                    marker=dict(
                        color=df['visits_adjusted'],
                        colorscale='Viridis',
                        showscale=True,
                        size=8,
                        opacity=0.6
                    ),
                    name='Response Correlation'
                ),
                row=2, col=2
            )
            
            # Update layout
            fig.update_layout(
                height=800,
                showlegend=True,
                template='plotly_white',
                title_text='Latency Analysis Dashboard',
                title_x=0.5
            )
            
            # Save visualizations
            fig.write_html(str(output_dir / 'latency_analysis.html'))
            fig.write_image(str(output_dir / 'latency_analysis.png'))
            
        except Exception as e:
            logger.error(f"Error creating latency analysis: {str(e)}")

    # Keep existing methods but enhance them with better styling
    def _create_performance_overview(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create performance overview visualizations with improved styling."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # Enhanced styling for each plot
            self._plot_ttfb_distribution(df, ax1)
            self._plot_cache_status_performance(df, ax2)
            self._plot_status_distribution(df, ax3)
            self._plot_protocol_performance(df, ax4)

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'performance_overview.png')
            
        except Exception as e:
            logger.error(f"Error creating performance overview: {str(e)}")
            plt.close('all')

    def _plot_ttfb_distribution(self, df: pd.DataFrame, ax) -> None:
        """Plot TTFB distribution with enhanced styling."""
        sns.histplot(
            data=df['ttfb_avg'].dropna(),
            ax=ax,
            bins=30,
            color=self.colors['edge'],
            alpha=0.7
        )
        ax.set_title('TTFB Distribution', pad=20)
        ax.set_xlabel('TTFB (ms)')
        ax.set_ylabel('Count')
        ax.grid(True, alpha=0.3)

    def _plot_cache_status_performance(self, df: pd.DataFrame, ax) -> None:
        """Plot cache status performance with enhanced styling."""
        cache_perf = df.groupby('cache_status').agg({
            'ttfb_avg': 'mean',
            'requests_adjusted': 'sum',
            'visits_adjusted': 'sum'
        }).reset_index()
        
        bars = ax.bar(cache_perf['cache_status'], cache_perf['ttfb_avg'], color=self.colors['edge'])
        
        # Add request/visit annotations
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'R:{int(cache_perf.iloc[i]["requests_adjusted"]):,}\n'
                   f'V:{int(cache_perf.iloc[i]["visits_adjusted"]):,}',
                   ha='center', va='bottom')
        
        ax.set_title('Performance by Cache Status', pad=20)
        ax.set_xlabel('Cache Status')
        ax.set_ylabel('Average TTFB (ms)')
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax.grid(True, alpha=0.3)

    def _plot_status_distribution(self, df: pd.DataFrame, ax) -> None:
        """Plot status code distribution with enhanced styling."""
        status_counts = df.groupby('status').agg({
            'requests_adjusted': 'sum',
            'visits_adjusted': 'sum'
        }).reset_index()
        
        x = np.arange(len(status_counts))
        width = 0.35
        
        ax.bar(x - width/2, status_counts['requests_adjusted'], width,
               label='Requests', color=self.colors['edge'])
        ax.bar(x + width/2, status_counts['visits_adjusted'], width,
               label='Visits', color=self.colors['origin'])
        
        ax.set_title('Status Code Distribution', pad=20)
        ax.set_xlabel('Status Code')
        ax.set_ylabel('Count')
        ax.set_xticks(x)
        ax.set_xticklabels(status_counts['status'])
        ax.legend()
        ax.grid(True, alpha=0.3)

    def _plot_protocol_performance(self, df: pd.DataFrame, ax) -> None:
        """Plot protocol performance with enhanced styling."""
        protocol_perf = df.groupby('protocol').agg({
            'ttfb_avg': 'mean',
            'requests_adjusted': 'sum',
            'visits_adjusted': 'sum'
        }).reset_index()
        
        bars = ax.bar(protocol_perf['protocol'], protocol_perf['ttfb_avg'], 
                     color=self.colors['edge'])
        
        # Add request/visit annotations
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'R:{int(protocol_perf.iloc[i]["requests_adjusted"]):,}\n'
                   f'V:{int(protocol_perf.iloc[i]["visits_adjusted"]):,}',
                   ha='center', va='bottom')
        
        ax.set_title('Performance by Protocol', pad=20)
        ax.set_xlabel('Protocol')
        ax.set_ylabel('Average TTFB (ms)')
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax.grid(True, alpha=0.3)

    def _setup_time_axis(self, ax, timestamps) -> None:
        """Configure time axis formatting based on data range."""
        try:
            if len(timestamps) == 0:
                return

            time_range = timestamps.max() - timestamps.min()
            minutes = time_range.total_seconds() / 60
            
            locator = AutoDateLocator()
            formatter = DateFormatter('%Y-%m-%d %H:%M')
            
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
        except Exception as e:
            logger.error(f"Error setting up time axis: {str(e)}")

    def _save_fig_safely(self, fig: plt.Figure, filepath: Path, close: bool = True) -> None:
        """Safely save figure with error handling."""
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(filepath, bbox_inches='tight', dpi=300)
            if close:
                plt.close(fig)
        except Exception as e:
            logger.error(f"Error saving figure to {filepath}: {str(e)}")
            if close:
                plt.close(fig)

    def _create_colormap(self, values, cmap_name='YlOrRd'):
        """Create a colormap for the given values."""
        try:
            normalized = (values - values.min()) / (values.max() - values.min())
            cmap = plt.cm.get_cmap(cmap_name)
            return [cmap(x) for x in normalized]
        except Exception as e:
            logger.error(f"Error creating colormap: {str(e)}")
            return ['#3498db'] * len(values)  # Default blue color

    def _create_geographic_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create geographic analysis visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # Get top countries by requests
            top_countries = df.groupby('country').agg({
                'requests_adjusted': 'sum'
            }).nlargest(10, 'requests_adjusted').index

            country_metrics = df[df['country'].isin(top_countries)].groupby('country').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            })

            # 1. Traffic Distribution
            x = np.arange(len(country_metrics))
            width = 0.35
            
            ax1.bar(x - width/2, 
                   country_metrics['requests_adjusted'] / country_metrics['requests_adjusted'].sum() * 100,
                   width, label='Requests', color=self.colors['edge'])
            ax1.bar(x + width/2, 
                   country_metrics['visits_adjusted'] / country_metrics['visits_adjusted'].sum() * 100,
                   width, label='Visits', color=self.colors['origin'])
            ax1.set_title('Traffic Distribution by Country')
            ax1.set_ylabel('Percentage (%)')
            ax1.set_xticks(x)
            ax1.set_xticklabels(country_metrics.index, rotation=45, ha='right')
            ax1.legend()

            # 2. Response Time by Country
            bars = ax2.bar(country_metrics.index, country_metrics['ttfb_avg'],
                          color=self.colors['edge'])
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height,
                        f'R:{int(country_metrics.iloc[i]["requests_adjusted"]):,}\n'
                        f'V:{int(country_metrics.iloc[i]["visits_adjusted"]):,}',
                        ha='center', va='bottom')
            ax2.set_title('Average Response Time by Country')
            ax2.set_ylabel('TTFB (ms)')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 3. Bandwidth Usage
            bandwidth_gb = country_metrics['bytes_adjusted'] / (1024**3)
            ax3.bar(bandwidth_gb.index, bandwidth_gb, color=self.colors['edge'])
            ax3.set_title('Bandwidth Usage by Country')
            ax3.set_ylabel('Bandwidth (GB)')
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 4. Error Rates
            x = np.arange(len(country_metrics))
            ax4.bar(x - width/2, country_metrics['error_rate_4xx'] * 100, width,
                   label='4xx', color=self.colors['edge'])
            ax4.bar(x + width/2, country_metrics['error_rate_5xx'] * 100, width,
                   label='5xx', color=self.colors['error'])
            ax4.set_title('Error Rates by Country')
            ax4.set_ylabel('Error Rate (%)')
            ax4.set_xticks(x)
            ax4.set_xticklabels(country_metrics.index, rotation=45, ha='right')
            ax4.legend()

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'geographic_analysis.png')

        except Exception as e:
            logger.error(f"Error creating geographic analysis: {str(e)}")
            plt.close('all')

    def _create_time_series(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create time series visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            df_time = df.set_index('timestamp')
            
            # 1. Traffic Volume Over Time
            requests = df_time['requests_adjusted'].resample('5min').sum()
            visits = df_time['visits_adjusted'].resample('5min').sum()
            
            ax1.plot(requests.index, requests.values, color=self.colors['edge'],
                    label='Requests')
            ax1.plot(visits.index, visits.values, color=self.colors['origin'],
                    label='Visits')
            ax1.set_title('Traffic Volume Over Time')
            ax1.set_ylabel('Count')
            ax1.legend()
            self._setup_time_axis(ax1, requests.index)

            # 2. Response Time Over Time
            ttfb = df_time['ttfb_avg'].resample('5min').mean()
            ax2.plot(ttfb.index, ttfb.values, color=self.colors['edge'])
            ax2.set_title('Average TTFB Over Time')
            ax2.set_ylabel('TTFB (ms)')
            self._setup_time_axis(ax2, ttfb.index)

            # 3. Error Rates Over Time
            error_4xx = df_time['error_rate_4xx'].resample('5min').mean() * 100
            error_5xx = df_time['error_rate_5xx'].resample('5min').mean() * 100
            
            ax3.plot(error_4xx.index, error_4xx.values, label='4xx Errors',
                    color=self.colors['edge'])
            ax3.plot(error_5xx.index, error_5xx.values, label='5xx Errors',
                    color=self.colors['error'])
            ax3.set_title('Error Rates Over Time')
            ax3.set_ylabel('Error Rate (%)')
            ax3.legend()
            self._setup_time_axis(ax3, error_4xx.index)

            # 4. Cache Hit Ratio Over Time
            cache_hit_ratio = df_time['cache_status'].isin(['hit', 'stale', 'revalidated'])\
                            .resample('5min').mean() * 100
            ax4.plot(cache_hit_ratio.index, cache_hit_ratio.values,
                    color=self.colors['cache_hit'])
            ax4.set_title('Cache Hit Ratio Over Time')
            ax4.set_ylabel('Hit Ratio (%)')
            self._setup_time_axis(ax4, cache_hit_ratio.index)

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'time_series.png')

        except Exception as e:
            logger.error(f"Error creating time series: {str(e)}")
            plt.close('all')

    def _create_endpoint_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create endpoint analysis visualizations."""
        try:
            # Get top 10 endpoints by requests
            top_endpoints = df.groupby('endpoint').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).nlargest(10, 'requests_adjusted')

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Traffic Volume
            x = np.arange(len(top_endpoints))
            width = 0.35
            
            ax1.bar(x - width/2, top_endpoints['requests_adjusted'], width,
                   label='Requests', color=self.colors['edge'])
            ax1.bar(x + width/2, top_endpoints['visits_adjusted'], width,
                   label='Visits', color=self.colors['origin'])
            ax1.set_title('Traffic Volume by Top Endpoints')
            ax1.set_ylabel('Count')
            ax1.set_xticks(x)
            ax1.set_xticklabels(top_endpoints.index, rotation=45, ha='right')
            ax1.legend()

            # 2. Response Time
            ax2.bar(range(len(top_endpoints)), top_endpoints['ttfb_avg'],
                   color=self.colors['edge'])
            for i, v in enumerate(top_endpoints['ttfb_avg']):
                ax2.text(i, v, f'{v:.0f}ms', ha='center', va='bottom')
            ax2.set_title('Average Response Time by Endpoint')
            ax2.set_ylabel('TTFB (ms)')
            ax2.set_xticks(range(len(top_endpoints)))
            ax2.set_xticklabels(top_endpoints.index, rotation=45, ha='right')

            # 3. Error Rates
            x = np.arange(len(top_endpoints))
            ax3.bar(x - width/2, top_endpoints['error_rate_4xx'] * 100, width,
                   label='4xx Errors', color=self.colors['edge'])
            ax3.bar(x + width/2, top_endpoints['error_rate_5xx'] * 100, width,
                   label='5xx Errors', color=self.colors['error'])
            ax3.set_title('Error Rates by Top Endpoints')
            ax3.set_ylabel('Error Rate (%)')
            ax3.set_xticks(x)
            ax3.set_xticklabels(top_endpoints.index, rotation=45, ha='right')
            ax3.legend()

            # 4. Requests per Visit Ratio
            req_visit_ratio = top_endpoints['requests_adjusted'] / \
                            top_endpoints['visits_adjusted'].replace(0, float('nan'))
            ax4.bar(range(len(req_visit_ratio)), req_visit_ratio.fillna(0),
                   color=self.colors['edge'])
            ax4.set_title('Requests per Visit by Endpoint')
            ax4.set_ylabel('Requests/Visit Ratio')
            ax4.set_xticks(range(len(req_visit_ratio)))
            ax4.set_xticklabels(req_visit_ratio.index, rotation=45, ha='right')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'endpoint_analysis.png')

        except Exception as e:
            logger.error(f"Error creating endpoint analysis: {str(e)}")
            plt.close('all')
