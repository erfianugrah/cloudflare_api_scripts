import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional
from matplotlib.dates import DateFormatter, AutoDateLocator, HourLocator, DayLocator, MinuteLocator
import seaborn as sns
from datetime import datetime, timedelta, timezone
import traceback
from matplotlib.colors import LinearSegmentedColormap

logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self, config):
        self.config = config
        self.setup_style()
        self.colors = {
            'primary': '#2ecc71',
            'secondary': '#3498db',
            'tertiary': '#9b59b6',
            'warning': '#f1c40f',
            'error': '#e74c3c',
            'neutral': '#95a5a6'
        }
    
    def setup_style(self):
        """Configure visualization style."""
        plt.style.use('bmh')
        plt.rcParams['figure.figsize'] = [12, 6]
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['axes.grid'] = True
        plt.rcParams['grid.alpha'] = 0.3
        plt.rcParams['figure.autolayout'] = True
        
        # Set color palette
        sns.set_palette("husl")
        
        # Improve readability
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 12
        plt.rcParams['axes.labelsize'] = 10
        plt.rcParams['xtick.labelsize'] = 9
        plt.rcParams['ytick.labelsize'] = 9

    def create_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str) -> None:
        """Create all visualizations for the analysis results."""
        try:
            if df is None or df.empty or not analysis:
                logger.error("No data available for visualization")
                return

            # Convert timestamp to datetime if it's not already
            if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])

            output_dir = self.config.images_dir / zone_name
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create main visualization groups
            self._create_performance_overview(df, analysis, output_dir)
            self._create_cache_analysis(df, analysis, output_dir)
            self._create_error_analysis(df, analysis, output_dir)
            self._create_geographic_analysis(df, analysis, output_dir)
            self._create_time_series(df, analysis, output_dir)
            self._create_path_analysis(df, analysis, output_dir)

        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')

    def _create_performance_overview(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create performance overview visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # 1. TTFB Distribution
            ttfb_data = df['ttfb_avg'].dropna().astype(float)
            sns.histplot(data=ttfb_data, ax=ax1, bins=30, color=self.colors['primary'])
            ax1.set_title('TTFB Distribution')
            ax1.set_xlabel('TTFB (ms)')
            ax1.set_ylabel('Count')

            # 2. Performance by Cache Status
            cache_perf = df.groupby('cache_status').agg({
                'ttfb_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            cache_perf['ttfb_avg'] = cache_perf['ttfb_avg'].astype(float)
            ax2.bar(range(len(cache_perf)), cache_perf['ttfb_avg'], color=self.colors['secondary'])
            ax2.set_title('Average TTFB by Cache Status')
            ax2.set_xlabel('Cache Status')
            ax2.set_ylabel('TTFB (ms)')
            ax2.set_xticks(range(len(cache_perf)))
            ax2.set_xticklabels(cache_perf['cache_status'], rotation=45, ha='right')

            # 3. Status Code Distribution
            status_counts = df['status_code'].astype(int).value_counts().sort_index()
            ax3.bar(status_counts.index.astype(str), status_counts.values, color=self.colors['tertiary'])
            ax3.set_title('Status Code Distribution')
            ax3.set_xlabel('Status Code')
            ax3.set_ylabel('Count')
            
            # 4. Performance by HTTP Protocol
            protocol_perf = df.groupby('protocol').agg({
                'ttfb_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            protocol_perf['ttfb_avg'] = protocol_perf['ttfb_avg'].astype(float)
            ax4.bar(range(len(protocol_perf)), protocol_perf['ttfb_avg'], color=self.colors['error'])
            ax4.set_title('Average TTFB by HTTP Protocol')
            ax4.set_xlabel('Protocol')
            ax4.set_ylabel('TTFB (ms)')
            ax4.set_xticks(range(len(protocol_perf)))
            ax4.set_xticklabels(protocol_perf['protocol'], rotation=45, ha='right')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'performance_overview.png')

        except Exception as e:
            logger.error(f"Error creating performance overview: {str(e)}")
            plt.close('all')

    def _create_cache_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create cache analysis visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # 1. Cache Hit Ratio Over Time
            df_time = df.set_index('timestamp')
            df_time['cache_hit'] = df_time['cache_status'].isin(['hit', 'stale', 'revalidated'])
            hit_ratio = df_time['cache_hit'].rolling('5min').mean() * 100
            
            ax1.plot(df_time.index, hit_ratio, color=self.colors['primary'])
            ax1.set_title('Cache Hit Ratio Over Time')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Hit Ratio (%)')
            self._setup_time_axis(ax1, df_time.index)

            # 2. Cache Status Distribution
            cache_dist = df['cache_status'].value_counts()
            colors = plt.cm.Set3(np.linspace(0, 1, len(cache_dist)))
            ax2.pie(cache_dist.values, labels=cache_dist.index, colors=colors, autopct='%1.1f%%')
            ax2.set_title('Cache Status Distribution')

            # 3. Cache Performance by Content Type
            content_cache = df.groupby('content_type').agg({
                'cache_status': lambda x: (x.isin(['hit', 'stale', 'revalidated']).mean() * 100),
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')
            
            x = np.arange(len(content_cache))
            ax3.bar(x, content_cache['cache_status'], color=self.colors['secondary'])
            ax3.set_title('Cache Hit Ratio by Content Type')
            ax3.set_xlabel('Content Type')
            ax3.set_ylabel('Hit Ratio (%)')
            ax3.set_xticks(x)
            ax3.set_xticklabels(content_cache.index, rotation=45, ha='right')

            # 4. Bandwidth Savings
            cache_bandwidth = df.groupby('cache_status').agg({
                'bytes_adjusted': 'sum'
            })
            total_bytes = cache_bandwidth['bytes_adjusted'].sum()
            cache_bandwidth['percentage'] = cache_bandwidth['bytes_adjusted'] / total_bytes * 100
            
            colors = plt.cm.Set3(np.linspace(0, 1, len(cache_bandwidth)))
            ax4.pie(cache_bandwidth['percentage'], 
                   labels=cache_bandwidth.index,
                   colors=colors,
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
            error_4xx = df_time['error_rate_4xx'].rolling('5min').mean() * 100
            error_5xx = df_time['error_rate_5xx'].rolling('5min').mean() * 100
            
            ax1.plot(df_time.index, error_4xx, label='4xx Errors', color=self.colors['warning'])
            ax1.plot(df_time.index, error_5xx, label='5xx Errors', color=self.colors['error'])
            ax1.set_title('Error Rates Over Time')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Error Rate (%)')
            ax1.legend()
            self._setup_time_axis(ax1, df_time.index)

            # 2. Error Status Distribution
            error_status = df[df['status_code'] >= 400]['status_code'].astype(int).value_counts().sort_index()
            x = range(len(error_status))
            ax2.bar(x, error_status.values, color=self.colors['tertiary'])
            ax2.set_title('Error Status Code Distribution')
            ax2.set_xlabel('Status Code')
            ax2.set_ylabel('Count')
            ax2.set_xticks(x)
            ax2.set_xticklabels(error_status.index, rotation=45)

            # 3. Errors by Path
            path_errors = df.groupby('path_group').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')
            
            x = np.arange(len(path_errors))
            width = 0.35
            ax3.bar(x - width/2, 
                   path_errors['error_rate_4xx'] * 100,
                   width,
                   label='4xx',
                   color=self.colors['warning'])
            ax3.bar(x + width/2,
                   path_errors['error_rate_5xx'] * 100,
                   width,
                   label='5xx',
                   color=self.colors['error'])
            ax3.set_title('Error Rates by Top Paths')
            ax3.set_ylabel('Error Rate (%)')
            ax3.set_xticks(x)
            ax3.set_xticklabels(path_errors.index, rotation=45, ha='right')
            ax3.legend()

            # 4. Error Response Sizes
            error_sizes = df[df['status_code'] >= 400].groupby('status_code').agg({
                'bytes_adjusted': 'mean'
            }) / 1024  # Convert to KB
            
            x = range(len(error_sizes))
            ax4.bar(x, error_sizes['bytes_adjusted'], color=self.colors['primary'])
            ax4.set_title('Average Error Response Size')
            ax4.set_xlabel('Status Code')
            ax4.set_ylabel('Size (KB)')
            ax4.set_xticks(x)
            ax4.set_xticklabels(error_sizes.index.astype(int), rotation=45)

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'error_analysis.png')

        except Exception as e:
            logger.error(f"Error creating error analysis: {str(e)}")
            plt.close('all')

    def _create_geographic_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create geographic analysis visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # Get top 10 countries by traffic
            top_countries = df.groupby('country').agg({
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')

            country_metrics = df[df['country'].isin(top_countries.index)].groupby('country').agg({
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            })

            # 1. Traffic Distribution
            colors = plt.cm.Set3(np.linspace(0, 1, len(country_metrics)))
            ax1.pie(country_metrics['visits_adjusted'],
                   labels=country_metrics.index,
                   colors=colors,
                   autopct='%1.1f%%')
            ax1.set_title('Traffic Distribution by Country')

            # 2. Average TTFB by Country
            x = np.arange(len(country_metrics))  # Using numpy array instead of range
            ax2.bar(x, country_metrics['ttfb_avg'], color=self.colors['secondary'])
            ax2.set_title('Average TTFB by Country')
            ax2.set_ylabel('TTFB (ms)')
            ax2.set_xticks(x)
            ax2.set_xticklabels(country_metrics.index, rotation=45, ha='right')

            # 3. Bandwidth Usage
            bandwidth_gb = country_metrics['bytes_adjusted'] / (1024**3)
            ax3.bar(x, bandwidth_gb, color=self.colors['tertiary'])
            ax3.set_title('Bandwidth Usage by Country')
            ax3.set_ylabel('Bandwidth (GB)')
            ax3.set_xticks(x)
            ax3.set_xticklabels(country_metrics.index, rotation=45, ha='right')

            # 4. Error Rates by Country
            width = 0.35
            # Convert x to numpy array for arithmetic operations
            bar_positions_left = x - width/2
            bar_positions_right = x + width/2
            
            ax4.bar(bar_positions_left,
                   country_metrics['error_rate_4xx'] * 100,
                   width,
                   label='4xx',
                   color=self.colors['warning'])
            ax4.bar(bar_positions_right,
                   country_metrics['error_rate_5xx'] * 100,
                   width,
                   label='5xx',
                   color=self.colors['error'])
            ax4.set_title('Error Rates by Country')
            ax4.set_ylabel('Error Rate (%)')
            ax4.set_xticks(x)
            ax4.set_xticklabels(country_metrics.index, rotation=45, ha='right')
            ax4.legend()

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'geographic_analysis.png')

        except Exception as e:
            logger.error(f"Error creating geographic analysis: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')


    def _create_time_series(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create time series visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            df_time = df.set_index('timestamp')
            
            # 1. Request Volume Over Time
            requests = df_time['visits_adjusted'].resample('5min').sum()
            ax1.plot(requests.index, requests.values, color=self.colors['primary'])
            ax1.set_title('Request Volume Over Time')
            ax1.set_ylabel('Requests')
            self._setup_time_axis(ax1, requests.index)

            # 2. Average TTFB Over Time
            ttfb = df_time['ttfb_avg'].resample('5min').mean()
            ax2.plot(ttfb.index, ttfb.values, color=self.colors['secondary'])
            ax2.set_title('Average TTFB Over Time')
            ax2.set_ylabel('TTFB (ms)')
            self._setup_time_axis(ax2, ttfb.index)

            # 3. Bandwidth Usage Over Time
            bandwidth = df_time['bytes_adjusted'].resample('5min').sum() / (1024**3)
            ax3.plot(bandwidth.index, bandwidth.values, color=self.colors['tertiary'])
            ax3.set_title('Bandwidth Usage Over Time')
            ax3.set_ylabel('Bandwidth (GB)')
            self._setup_time_axis(ax3, bandwidth.index)

            # 4. Error Rates Over Time
            error_4xx = df_time['error_rate_4xx'].resample('5min').mean() * 100
            error_5xx = df_time['error_rate_5xx'].resample('5min').mean() * 100
            
            ax4.plot(error_4xx.index, error_4xx.values, label='4xx Errors', color=self.colors['warning'])
            ax4.plot(error_5xx.index, error_5xx.values, label='5xx Errors', color=self.colors['error'])
            ax4.set_title('Error Rates Over Time')
            ax4.set_ylabel('Error Rate (%)')
            ax4.legend()
            self._setup_time_axis(ax4, error_4xx.index)

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'time_series.png')

        except Exception as e:
            logger.error(f"Error creating time series: {str(e)}")
            plt.close('all')

    def _create_path_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create path-specific analysis visualizations."""
        try:
            # Get top 10 paths by traffic
            top_paths = df.groupby('path_group').agg({
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')

            path_metrics = df[df['path_group'].isin(top_paths.index)].groupby('path_group').agg({
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: (x.isin(['hit', 'stale', 'revalidated'])).mean() * 100
            })

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Request Distribution by Path
            colors = plt.cm.Set3(np.linspace(0, 1, len(path_metrics)))
            ax1.pie(path_metrics['visits_adjusted'], 
                   labels=[p[:30] + '...' if len(p) > 30 else p for p in path_metrics.index],
                   colors=colors,
                   autopct='%1.1f%%')
            ax1.set_title('Request Distribution by Path')

            # 2. Average TTFB by Path
            x = np.arange(len(path_metrics))  # Using numpy array instead of range
            ax2.bar(x, path_metrics['ttfb_avg'], color=self.colors['secondary'])
            ax2.set_title('Average TTFB by Path')
            ax2.set_ylabel('TTFB (ms)')
            ax2.set_xticks(x)
            ax2.set_xticklabels(
                [p[:30] + '...' if len(p) > 30 else p for p in path_metrics.index],
                rotation=45,
                ha='right'
            )

            # 3. Cache Hit Ratio by Path
            ax3.bar(x, path_metrics['cache_status'], color=self.colors['primary'])
            ax3.set_title('Cache Hit Ratio by Path')
            ax3.set_ylabel('Hit Ratio (%)')
            ax3.set_xticks(x)
            ax3.set_xticklabels(
                [p[:30] + '...' if len(p) > 30 else p for p in path_metrics.index],
                rotation=45,
                ha='right'
            )

            # 4. Error Rates by Path
            width = 0.35
            # Convert x to numpy array for arithmetic operations
            bar_positions_left = x - width/2
            bar_positions_right = x + width/2
            
            ax4.bar(bar_positions_left,
                   path_metrics['error_rate_4xx'] * 100,
                   width,
                   label='4xx Errors',
                   color=self.colors['warning'])
            ax4.bar(bar_positions_right,
                   path_metrics['error_rate_5xx'] * 100,
                   width,
                   label='5xx Errors',
                   color=self.colors['error'])
            ax4.set_title('Error Rates by Path')
            ax4.set_ylabel('Error Rate (%)')
            ax4.set_xticks(x)
            ax4.set_xticklabels(
                [p[:30] + '...' if len(p) > 30 else p for p in path_metrics.index],
                rotation=45,
                ha='right'
            )
            ax4.legend()

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'path_analysis.png')

        except Exception as e:
            logger.error(f"Error creating path analysis: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')

    def _setup_time_axis(self, ax, timestamps):
        """Configure time axis formatting based on data range."""
        try:
            if len(timestamps) == 0:
                return

            time_range = timestamps.max() - timestamps.min()
            minutes = time_range.total_seconds() / 60
            
            if minutes <= 60:  # 1 hour or less
                ax.xaxis.set_major_formatter(DateFormatter('%H:%M'))
                ax.xaxis.set_major_locator(MinuteLocator(interval=5))
            elif minutes <= 1440:  # 24 hours or less
                ax.xaxis.set_major_formatter(DateFormatter('%H:%M'))
                ax.xaxis.set_major_locator(HourLocator(interval=2))
            else:  # More than 24 hours
                ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
                ax.xaxis.set_major_locator(AutoDateLocator())
            
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
        except Exception as e:
            logger.error(f"Error setting up time axis: {str(e)}")

    def _save_fig_safely(self, fig: plt.Figure, filepath: Path, close: bool = True) -> None:
        """Safely save figure with error handling."""
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(filepath, bbox_inches='tight', dpi=300)
            logger.info(f"Saved figure to {filepath}")
            if close:
                plt.close(fig)
        except Exception as e:
            logger.error(f"Error saving figure to {filepath}: {str(e)}")
            if close:
                plt.close(fig)

    def _create_colormap(self, values, cmap_name='YlOrRd'):
        """Create a colormap for the given values."""
        try:
            if len(values) == 0:
                return []
            normalized = (values - values.min()) / (values.max() - values.min())
            cmap = plt.cm.get_cmap(cmap_name)
            return [cmap(x) for x in normalized]
        except Exception as e:
            logger.error(f"Error creating colormap: {str(e)}")
            return [self.colors['primary']] * len(values)
