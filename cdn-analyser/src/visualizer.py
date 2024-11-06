# visualizer.py
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

logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self, config):
        self.config = config
        self.setup_style()
    
    def setup_style(self):
        """Configure visualization style."""
        plt.style.use('bmh')
        plt.rcParams['figure.figsize'] = [12, 6]
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['axes.grid'] = True
        plt.rcParams['grid.alpha'] = 0.3
        plt.rcParams['figure.autolayout'] = True
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

            output_dir = self.config.images_dir / zone_name
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create main visualization groups
            self._create_performance_overview(df, analysis, output_dir)
            self._create_cache_analysis(df, analysis, output_dir)
            self._create_error_analysis(df, analysis, output_dir)
            self._create_geographic_analysis(df, analysis, output_dir)
            self._create_time_series(df, analysis, output_dir)
            self._create_endpoint_analysis(df, analysis, output_dir)

        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')

    def _create_endpoint_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create endpoint-specific analysis visualizations with improved readability."""
        try:
            # Get top 10 endpoints by traffic
            top_endpoints = df.groupby('endpoint').agg({
                'visits_adjusted': 'sum',
                'clientRequestPath': 'first',
                'clientRequestHTTPHost': 'first'
            }).nlargest(10, 'visits_adjusted')

            endpoint_metrics = df[df['endpoint'].isin(top_endpoints.index)].groupby('endpoint').agg({
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: (x == 'hit').mean(),
                'clientRequestPath': 'first',
                'clientRequestHTTPHost': 'first'
            })

            # Create better formatted endpoint labels
            def format_endpoint_label(host, path):
                if path == '/':
                    return host
                # Remove trailing slashes and clean up path
                path = path.rstrip('/')
                # Split into parts for better formatting
                return f"{host}\n{path}"

            endpoint_labels = [format_endpoint_label(row['clientRequestHTTPHost'], row['clientRequestPath'])
                             for _, row in endpoint_metrics.iterrows()]

            # Create figure with adjusted size and spacing
            fig = plt.figure(figsize=(24, 18))
            gs = plt.GridSpec(2, 2, figure=fig, hspace=0.4, wspace=0.3)

            # 1. Traffic Distribution (upper left)
            ax1 = fig.add_subplot(gs[0, 0])
            wedges, texts, autotexts = ax1.pie(endpoint_metrics['visits_adjusted'], 
                                             labels=endpoint_labels,
                                             autopct='%1.1f%%',
                                             labeldistance=1.1,
                                             pctdistance=0.8)
            # Enhance pie chart appearance
            plt.setp(autotexts, size=9, weight="bold")
            plt.setp(texts, size=8)
            ax1.set_title('Traffic Distribution by Endpoint', pad=20, size=12, weight='bold')

            # 2. Average TTFB (upper right)
            ax2 = fig.add_subplot(gs[0, 1])
            bars = ax2.bar(range(len(endpoint_metrics)), endpoint_metrics['ttfb_avg'],
                          color=plt.cm.Set3(np.linspace(0, 1, len(endpoint_metrics))))
            ax2.set_title('Average TTFB by Endpoint', pad=20, size=12, weight='bold')
            ax2.set_ylabel('TTFB (ms)', size=10)
            ax2.set_xticks(range(len(endpoint_metrics)))
            ax2.set_xticklabels(endpoint_labels, rotation=45, ha='right', va='top')
            ax2.grid(True, axis='y', alpha=0.3)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height)}ms',
                        ha='center', va='bottom', size=8)

            # 3. Cache Hit Ratio (lower left)
            ax3 = fig.add_subplot(gs[1, 0])
            bars = ax3.bar(range(len(endpoint_metrics)), endpoint_metrics['cache_status'] * 100,
                          color=plt.cm.Set3(np.linspace(0, 1, len(endpoint_metrics))))
            ax3.set_title('Cache Hit Ratio by Endpoint', pad=20, size=12, weight='bold')
            ax3.set_ylabel('Hit Ratio (%)', size=10)
            ax3.set_xticks(range(len(endpoint_metrics)))
            ax3.set_xticklabels(endpoint_labels, rotation=45, ha='right', va='top')
            ax3.grid(True, axis='y', alpha=0.3)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.1f}%',
                        ha='center', va='bottom', size=8)

            # 4. Error Rates (lower right)
            ax4 = fig.add_subplot(gs[1, 1])
            x = np.arange(len(endpoint_metrics))
            width = 0.35

            # Create bars with different colors for 4xx and 5xx
            bars1 = ax4.bar(x - width/2, endpoint_metrics['error_rate_4xx'] * 100, width, 
                           label='4xx Errors', color='#ff9999')
            bars2 = ax4.bar(x + width/2, endpoint_metrics['error_rate_5xx'] * 100, width, 
                           label='5xx Errors', color='#ff4d4d')

            ax4.set_title('Error Rates by Endpoint', pad=20, size=12, weight='bold')
            ax4.set_ylabel('Error Rate (%)', size=10)
            ax4.set_xticks(x)
            ax4.set_xticklabels(endpoint_labels, rotation=45, ha='right', va='top')
            ax4.legend(loc='upper right')
            ax4.grid(True, axis='y', alpha=0.3)

            # Add value labels on bars
            def add_value_labels(bars):
                for bar in bars:
                    height = bar.get_height()
                    if height > 0:  # Only show non-zero values
                        ax4.text(bar.get_x() + bar.get_width()/2., height,
                                f'{height:.1f}%',
                                ha='center', va='bottom', size=8)

            add_value_labels(bars1)
            add_value_labels(bars2)

            # Final adjustments
            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'endpoint_analysis.png')

        except Exception as e:
            logger.error(f"Error creating endpoint analysis: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')

    def _create_host_analysis(self, df: pd.DataFrame, output_dir: Path) -> None:
        """Create host-specific analysis visualizations."""
        try:
            # Group by host (separate from endpoint analysis)
            host_metrics = df.groupby('clientRequestHTTPHost').agg({
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'cache_status': lambda x: (x == 'hit').mean()
            }).nlargest(10, 'visits_adjusted')

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Traffic Distribution by Host
            ax1.pie(host_metrics['visits_adjusted'], 
                   labels=host_metrics.index, 
                   autopct='%1.1f%%')
            ax1.set_title('Traffic Distribution by Host')

            # 2. Performance by Host
            ax2.bar(range(len(host_metrics)), host_metrics['ttfb_avg'])
            ax2.set_title('Average TTFB by Host')
            ax2.set_ylabel('TTFB (ms)')
            ax2.set_xticks(range(len(host_metrics)))
            ax2.set_xticklabels(host_metrics.index, rotation=45, ha='right')

            # 3. Cache Performance by Host
            ax3.bar(range(len(host_metrics)), host_metrics['cache_status'] * 100)
            ax3.set_title('Cache Hit Ratio by Host')
            ax3.set_ylabel('Hit Ratio (%)')
            ax3.set_xticks(range(len(host_metrics)))
            ax3.set_xticklabels(host_metrics.index, rotation=45, ha='right')

            # 4. Error Rates by Host
            width = 0.35
            x = np.arange(len(host_metrics))
            ax4.bar(x - width/2, host_metrics['error_rate_4xx'] * 100, width, label='4xx Errors')
            ax4.bar(x + width/2, host_metrics['error_rate_5xx'] * 100, width, label='5xx Errors')
            ax4.set_title('Error Rates by Host')
            ax4.set_ylabel('Error Rate (%)')
            ax4.set_xticks(x)
            ax4.set_xticklabels(host_metrics.index, rotation=45, ha='right')
            ax4.legend()

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'host_analysis.png')

        except Exception as e:
            logger.error(f"Error creating host analysis: {str(e)}")
            plt.close('all')

    def _create_performance_overview(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create performance overview visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # 1. TTFB Distribution
            ttfb_data = df['ttfb_avg'].dropna()
            sns.histplot(data=ttfb_data, ax=ax1, bins=30, color='#2ecc71')
            ax1.set_title('TTFB Distribution')
            ax1.set_xlabel('TTFB (ms)')
            ax1.set_ylabel('Count')

            # 2. Performance by Cache Status
            cache_perf = df.groupby('cache_status').agg({
                'ttfb_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            ax2.bar(cache_perf['cache_status'], cache_perf['ttfb_avg'], color='#3498db')
            ax2.set_title('Average TTFB by Cache Status')
            ax2.set_xlabel('Cache Status')
            ax2.set_ylabel('TTFB (ms)')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 3. Status Code Distribution
            status_counts = df['status'].value_counts().sort_index()
            ax3.bar(status_counts.index.astype(str), status_counts.values, color='#9b59b6')
            ax3.set_title('Status Code Distribution')
            ax3.set_xlabel('Status Code')
            ax3.set_ylabel('Count')
            
            # 4. Performance by HTTP Protocol
            protocol_perf = df.groupby('protocol').agg({
                'ttfb_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            ax4.bar(protocol_perf['protocol'], protocol_perf['ttfb_avg'], color='#e74c3c')
            ax4.set_title('Average TTFB by HTTP Protocol')
            ax4.set_xlabel('Protocol')
            ax4.set_ylabel('TTFB (ms)')
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')

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
            # Create cache_hit column here
            df_time['cache_hit'] = df_time['cache_status'].isin(['hit', 'stale', 'revalidated'])
            hit_ratio = df_time['cache_hit'].rolling('5min').mean() * 100  # Changed from '5T' to '5min'
            
            ax1.plot(hit_ratio.index, hit_ratio.values, color='#2ecc71')
            ax1.set_title('Cache Hit Ratio Over Time')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Hit Ratio (%)')
            self._setup_time_axis(ax1, df_time.index)

            # 2. Cache Status Distribution
            cache_dist = df['cache_status'].value_counts()
            ax2.pie(cache_dist.values, labels=cache_dist.index, autopct='%1.1f%%')
            ax2.set_title('Cache Status Distribution')

            # 3. Cache Performance by Content Type
            content_cache = df.groupby('content_type').agg({
                'cache_status': lambda x: x.isin(['hit', 'stale', 'revalidated']).mean(),
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')
            
            ax3.bar(content_cache.index, content_cache['cache_status'] * 100)
            ax3.set_title('Cache Hit Ratio by Content Type')
            ax3.set_xlabel('Content Type')
            ax3.set_ylabel('Hit Ratio (%)')
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 4. Bandwidth Savings
            cache_bandwidth = df.groupby('cache_status').agg({
                'bytes_adjusted': 'sum'
            })
            total_bytes = cache_bandwidth['bytes_adjusted'].sum()
            cache_bandwidth['percentage'] = cache_bandwidth['bytes_adjusted'] / total_bytes * 100
            
            ax4.pie(cache_bandwidth['percentage'], 
                   labels=cache_bandwidth.index, 
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
            error_4xx = df_time['error_rate_4xx'].rolling('5min').mean() * 100  # Changed from '5T' to '5min'
            error_5xx = df_time['error_rate_5xx'].rolling('5min').mean() * 100  # Changed from '5T' to '5min'
            
            ax1.plot(error_4xx.index, error_4xx.values, label='4xx Errors', color='#f1c40f')
            ax1.plot(error_5xx.index, error_5xx.values, label='5xx Errors', color='#e74c3c')
            ax1.set_title('Error Rates Over Time')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Error Rate (%)')
            ax1.legend()
            self._setup_time_axis(ax1, df_time.index)

            # 2. Error Status Distribution
            error_status = df[df['status'] >= 400]['status'].value_counts().sort_index()
            ax2.bar(error_status.index.astype(str), error_status.values)
            ax2.set_title('Error Status Code Distribution')
            ax2.set_xlabel('Status Code')
            ax2.set_ylabel('Count')

            # 3. Errors by Endpoint
            endpoint_errors = df.groupby('endpoint').agg({
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean',
                'visits_adjusted': 'sum'
            }).nlargest(10, 'visits_adjusted')
            
            width = 0.35
            ax3.bar(np.arange(len(endpoint_errors)) - width/2, 
                   endpoint_errors['error_rate_4xx'] * 100, 
                   width, 
                   label='4xx')
            ax3.bar(np.arange(len(endpoint_errors)) + width/2, 
                   endpoint_errors['error_rate_5xx'] * 100, 
                   width, 
                   label='5xx')
            ax3.set_title('Error Rates by Top Endpoints')
            ax3.set_ylabel('Error Rate (%)')
            ax3.set_xticks(np.arange(len(endpoint_errors)))
            ax3.set_xticklabels(endpoint_errors.index, rotation=45, ha='right')
            ax3.legend()

            # 4. Error Response Sizes
            error_sizes = df[df['status'] >= 400].groupby('status').agg({
                'bytes_adjusted': 'mean'
            }) / 1024  # Convert to KB
            
            ax4.bar(error_sizes.index.astype(str), error_sizes['bytes_adjusted'])
            ax4.set_title('Average Error Response Size')
            ax4.set_xlabel('Status Code')
            ax4.set_ylabel('Size (KB)')

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
            }).nlargest(10, 'visits_adjusted').index

            country_metrics = df[df['country'].isin(top_countries)].groupby('country').agg({
                'visits_adjusted': 'sum',
                'ttfb_avg': 'mean',
                'bytes_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            })

            # 1. Traffic Distribution
            ax1.pie(country_metrics['visits_adjusted'], 
                   labels=country_metrics.index, 
                   autopct='%1.1f%%')
            ax1.set_title('Traffic Distribution by Country')

            # 2. Average TTFB by Country
            ax2.bar(country_metrics.index, country_metrics['ttfb_avg'])
            ax2.set_title('Average TTFB by Country')
            ax2.set_ylabel('TTFB (ms)')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 3. Bandwidth Usage
            bandwidth_gb = country_metrics['bytes_adjusted'] / (1024**3)
            ax3.bar(country_metrics.index, bandwidth_gb)
            ax3.set_title('Bandwidth Usage by Country')
            ax3.set_ylabel('Bandwidth (GB)')
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 4. Error Rates by Country
            width = 0.35
            ax4.bar(np.arange(len(country_metrics)) - width/2, 
                   country_metrics['error_rate_4xx'] * 100, 
                   width, 
                   label='4xx')
            ax4.bar(np.arange(len(country_metrics)) + width/2, 
                   country_metrics['error_rate_5xx'] * 100, 
                   width, 
                   label='5xx')
            ax4.set_title('Error Rates by Country')
            ax4.set_ylabel('Error Rate (%)')
            ax4.set_xticks(np.arange(len(country_metrics)))
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
            
            # 1. Request Volume Over Time
            requests = df_time['visits_adjusted'].resample('5min').sum()  # Changed from '5T' to '5min'
            ax1.plot(requests.index, requests.values, color='#2ecc71')
            ax1.set_title('Request Volume Over Time')
            ax1.set_ylabel('Requests')
            self._setup_time_axis(ax1, requests.index)

            # 2. Average TTFB Over Time
            ttfb = df_time['ttfb_avg'].resample('5min').mean()  # Changed from '5T' to '5min'
            ax2.plot(ttfb.index, ttfb.values, color='#3498db')
            ax2.set_title('Average TTFB Over Time')
            ax2.set_ylabel('TTFB (ms)')
            self._setup_time_axis(ax2, ttfb.index)

            # 3. Bandwidth Usage Over Time
            bandwidth = df_time['bytes_adjusted'].resample('5min').sum() / (1024**3)  # Changed from '5T' to '5min'
            ax3.plot(bandwidth.index, bandwidth.values, color='#9b59b6')
            ax3.set_title('Bandwidth Usage Over Time')
            ax3.set_ylabel('Bandwidth (GB)')
            self._setup_time_axis(ax3, bandwidth.index)

            # 4. Error Rates Over Time
            error_4xx = df_time['error_rate_4xx'].resample('5min').mean() * 100  # Changed from '5T' to '5min'
            error_5xx = df_time['error_rate_5xx'].resample('5min').mean() * 100  # Changed from '5T' to '5min'
            
            ax4.plot(error_4xx.index, error_4xx.values, label='4xx Errors', color='#f1c40f')
            ax4.plot(error_5xx.index, error_5xx.values, label='5xx Errors', color='#e74c3c')
            ax4.set_title('Error Rates Over Time')
            ax4.set_ylabel('Error Rate (%)')
            ax4.legend()
            self._setup_time_axis(ax4, error_4xx.index)

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'time_series.png')

        except Exception as e:
            logger.error(f"Error creating time series: {str(e)}")
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
