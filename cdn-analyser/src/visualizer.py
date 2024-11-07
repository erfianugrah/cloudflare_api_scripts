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

    def _create_performance_overview(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create performance overview visualizations with request and visit metrics."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # 1. TTFB Distribution
            ttfb_data = df['ttfb_avg'].dropna()
            sns.histplot(data=ttfb_data, ax=ax1, bins=30, color='#2ecc71')
            ax1.set_title('TTFB Distribution (All Requests)')
            ax1.set_xlabel('TTFB (ms)')
            ax1.set_ylabel('Count')

            # 2. Performance by Cache Status (with request/visit split)
            cache_perf = df.groupby('cache_status').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            ax2.bar(cache_perf['cache_status'], cache_perf['ttfb_avg'], color='#3498db')
            for i, row in cache_perf.iterrows():
                ax2.text(i, row['ttfb_avg'], 
                        f'R:{int(row["requests_adjusted"]):,}\nV:{int(row["visits_adjusted"]):,}', 
                        ha='center', va='bottom')
            ax2.set_title('Average TTFB by Cache Status\nwith Request (R) and Visit (V) Counts')
            ax2.set_xlabel('Cache Status')
            ax2.set_ylabel('TTFB (ms)')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 3. Status Code Distribution (Request vs Visit)
            status_counts = df.groupby('status').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            x = np.arange(len(status_counts))
            width = 0.35
            ax3.bar(x - width/2, status_counts['requests_adjusted'], width, 
                   label='Requests', color='#9b59b6')
            ax3.bar(x + width/2, status_counts['visits_adjusted'], width, 
                   label='Visits', color='#e74c3c')
            ax3.set_title('Status Code Distribution\nRequests vs Visits')
            ax3.set_xlabel('Status Code')
            ax3.set_ylabel('Count')
            ax3.legend()
            ax3.set_xticks(x)
            ax3.set_xticklabels(status_counts['status'])
            
            # 4. Performance by HTTP Protocol (with request/visit annotation)
            protocol_perf = df.groupby('protocol').agg({
                'ttfb_avg': 'mean',
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            ax4.bar(protocol_perf['protocol'], protocol_perf['ttfb_avg'], color='#e74c3c')
            for i, row in protocol_perf.iterrows():
                ax4.text(i, row['ttfb_avg'], 
                        f'R:{int(row["requests_adjusted"]):,}\nV:{int(row["visits_adjusted"]):,}', 
                        ha='center', va='bottom')
            ax4.set_title('Average TTFB by HTTP Protocol\nwith Request (R) and Visit (V) Counts')
            ax4.set_xlabel('Protocol')
            ax4.set_ylabel('TTFB (ms)')
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'performance_overview.png')

        except Exception as e:
            logger.error(f"Error creating performance overview: {str(e)}")
            plt.close('all')

    def _create_cache_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create cache analysis visualizations with request and visit metrics."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # 1. Cache Hit Ratio Over Time (Requests)
            df_time = df.set_index('timestamp')
            df_time['cache_hit'] = df_time['cache_status'].isin(['hit', 'stale', 'revalidated'])
            hit_ratio = df_time['cache_hit'].rolling('5min').mean() * 100
            
            ax1.plot(hit_ratio.index, hit_ratio.values, color='#2ecc71')
            ax1.set_title('Cache Hit Ratio Over Time\n(Based on Requests)')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Hit Ratio (%)')
            self._setup_time_axis(ax1, hit_ratio.index)

            # 2. Cache Status Distribution (Requests vs Visits)
            cache_dist = df.groupby('cache_status').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            x = np.arange(len(cache_dist))
            width = 0.35
            ax2.bar(x - width/2, cache_dist['requests_adjusted'], width, 
                   label='Requests', color='#3498db')
            ax2.bar(x + width/2, cache_dist['visits_adjusted'], width, 
                   label='Visits', color='#2ecc71')
            ax2.set_title('Cache Status Distribution\nRequests vs Visits')
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
            
            ax3.bar(content_cache.index, content_cache['cache_status'])
            for i, row in content_cache.iterrows():
                ax3.text(i, row['cache_status'], 
                        f'R:{int(row["requests_adjusted"]):,}\nV:{int(row["visits_adjusted"]):,}', 
                        ha='center', va='bottom')
            ax3.set_title('Cache Hit Ratio by Content Type\nwith Request (R) and Visit (V) Counts')
            ax3.set_xlabel('Content Type')
            ax3.set_ylabel('Hit Ratio (%)')
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 4. Bandwidth Savings by Cache Status
            cache_bandwidth = df.groupby('cache_status').agg({
                'bytes_adjusted': 'sum'
            })
            total_bytes = cache_bandwidth['bytes_adjusted'].sum()
            cache_bandwidth['percentage'] = cache_bandwidth['bytes_adjusted'] / total_bytes * 100
            
            ax4.pie(cache_bandwidth['percentage'], 
                   labels=[f"{status}\n({pct:.1f}%)" for status, pct in 
                          zip(cache_bandwidth.index, cache_bandwidth['percentage'])],
                   autopct='%1.1f%%')
            ax4.set_title('Bandwidth Distribution by Cache Status')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'cache_analysis.png')

        except Exception as e:
            logger.error(f"Error creating cache analysis: {str(e)}")
            plt.close('all')

    def _create_error_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create error analysis visualizations with request and visit metrics."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # 1. Error Rate Over Time (Requests vs Visits)
            df_time = df.set_index('timestamp')
            error_req = df_time[df_time['status'] >= 400]['requests_adjusted'].rolling('5min').sum() / \
                       df_time['requests_adjusted'].rolling('5min').sum() * 100
            error_vis = df_time[df_time['status'] >= 400]['visits_adjusted'].rolling('5min').sum() / \
                       df_time['visits_adjusted'].rolling('5min').sum() * 100
            
            ax1.plot(error_req.index, error_req.values, label='Request Errors', color='#e74c3c')
            ax1.plot(error_vis.index, error_vis.values, label='Visit Errors', color='#f1c40f')
            ax1.set_title('Error Rates Over Time\nRequests vs Visits')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Error Rate (%)')
            ax1.legend()
            self._setup_time_axis(ax1, error_req.index)

            # 2. Error Status Distribution (Requests vs Visits)
            error_status = df[df['status'] >= 400].groupby('status').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum'
            }).reset_index()
            
            x = np.arange(len(error_status))
            width = 0.35
            ax2.bar(x - width/2, error_status['requests_adjusted'], width, 
                   label='Requests', color='#3498db')
            ax2.bar(x + width/2, error_status['visits_adjusted'], width, 
                   label='Visits', color='#2ecc71')
            ax2.set_title('Error Status Distribution\nRequests vs Visits')
            ax2.set_xlabel('Status Code')
            ax2.set_ylabel('Count')
            ax2.legend()
            ax2.set_xticks(x)
            ax2.set_xticklabels(error_status['status'])

            # 3. Errors by Endpoint (Top 10 by request errors)
            endpoint_errors = df.groupby('endpoint').agg({
                'requests_adjusted': 'sum',
                'visits_adjusted': 'sum',
                'error_rate_4xx': 'mean',
                'error_rate_5xx': 'mean'
            }).nlargest(10, 'requests_adjusted')
            
            x = np.arange(len(endpoint_errors))
            ax3.bar(x - width/2, endpoint_errors['error_rate_4xx'] * 100, width, 
                   label='4xx Errors', color='#f1c40f')
            ax3.bar(x + width/2, endpoint_errors['error_rate_5xx'] * 100, width, 
                   label='5xx Errors', color='#e74c3c')
            ax3.set_title('Error Rates by Top Endpoints')
            ax3.set_ylabel('Error Rate (%)')
            ax3.set_xticks(x)
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
        """Create geographic analysis visualizations with request and visit metrics."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            # Get top 10 countries by requests
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

            # 1. Traffic Distribution (Requests vs Visits)
            total_requests = country_metrics['requests_adjusted'].sum()
            total_visits = country_metrics['visits_adjusted'].sum()
            
            request_pcts = country_metrics['requests_adjusted'] / total_requests * 100
            visit_pcts = country_metrics['visits_adjusted'] / total_visits * 100
            
            x = np.arange(len(country_metrics))
            width = 0.35
            ax1.bar(x - width/2, request_pcts, width, label='Requests', color='#3498db')
            ax1.bar(x + width/2, visit_pcts, width, label='Visits', color='#2ecc71')
            ax1.set_title('Traffic Distribution by Country\nRequests vs Visits')
            ax1.set_ylabel('Percentage (%)')
            ax1.set_xticks(x)
            ax1.set_xticklabels(country_metrics.index, rotation=45, ha='right')
            ax1.legend()

            # 2. Average TTFB by Country
            ax2.bar(country_metrics.index, country_metrics['ttfb_avg'])
            for i, (idx, row) in enumerate(country_metrics.iterrows()):
                ax2.text(i, row['ttfb_avg'], 
                        f'R:{int(row["requests_adjusted"]):,}\nV:{int(row["visits_adjusted"]):,}', 
                        ha='center', va='bottom')
            ax2.set_title('Average TTFB by Country\nwith Request (R) and Visit (V) Counts')
            ax2.set_ylabel('TTFB (ms)')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 3. Bandwidth Usage by Country
            bandwidth_gb = country_metrics['bytes_adjusted'] / (1024**3)
            ax3.bar(country_metrics.index, bandwidth_gb)
            ax3.set_title('Bandwidth Usage by Country')
            ax3.set_ylabel('Bandwidth (GB)')
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # 4. Error Rates by Country
            x = np.arange(len(country_metrics))
            ax4.bar(x - width/2, 
                   country_metrics['error_rate_4xx'] * 100, 
                   width, 
                   label='4xx')
            ax4.bar(x + width/2, 
                   country_metrics['error_rate_5xx'] * 100, 
                   width, 
                   label='5xx')
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
        """Create time series visualizations with request and visit metrics."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
            
            df_time = df.set_index('timestamp')
            
            # 1. Traffic Volume Over Time (Requests vs Visits)
            requests = df_time['requests_adjusted'].resample('5min').sum()
            visits = df_time['visits_adjusted'].resample('5min').sum()
            
            ax1.plot(requests.index, requests.values, color='#3498db', label='Requests')
            ax1.plot(visits.index, visits.values, color='#2ecc71', label='Visits')
            ax1.set_title('Traffic Volume Over Time\nRequests vs Visits')
            ax1.set_ylabel('Count')
            ax1.legend()
            self._setup_time_axis(ax1, requests.index)

            # 2. Average TTFB Over Time
            ttfb = df_time['ttfb_avg'].resample('5min').mean()
            ax2.plot(ttfb.index, ttfb.values, color='#9b59b6')
            ax2.set_title('Average TTFB Over Time')
            ax2.set_ylabel('TTFB (ms)')
            self._setup_time_axis(ax2, ttfb.index)

            # 3. Error Rates Over Time
            error_requests = df_time[df_time['status'] >= 400]['requests_adjusted'].resample('5min').sum() / \
                           df_time['requests_adjusted'].resample('5min').sum() * 100
            error_visits = df_time[df_time['status'] >= 400]['visits_adjusted'].resample('5min').sum() / \
                          df_time['visits_adjusted'].resample('5min').sum() * 100
            
            ax3.plot(error_requests.index, error_requests.values, 
                    label='Request Errors', color='#e74c3c')
            ax3.plot(error_visits.index, error_visits.values, 
                    label='Visit Errors', color='#f1c40f')
            ax3.set_title('Error Rates Over Time\nRequests vs Visits')
            ax3.set_ylabel('Error Rate (%)')
            ax3.legend()
            self._setup_time_axis(ax3, error_requests.index)

            # 4. Cache Hit Ratio Over Time
            cache_hit_ratio = df_time['cache_status'].isin(['hit', 'stale', 'revalidated']) \
                            .resample('5min').mean() * 100
            ax4.plot(cache_hit_ratio.index, cache_hit_ratio.values, color='#16a085')
            ax4.set_title('Cache Hit Ratio Over Time\n(Based on Requests)')
            ax4.set_ylabel('Hit Ratio (%)')
            self._setup_time_axis(ax4, cache_hit_ratio.index)

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'time_series.png')

        except Exception as e:
            logger.error(f"Error creating time series: {str(e)}")
            plt.close('all')

    def _create_endpoint_analysis(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create endpoint analysis visualizations with request and visit metrics."""
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

            # 1. Traffic Volume (Requests vs Visits)
            x = np.arange(len(top_endpoints))
            width = 0.35
            
            ax1.bar(x - width/2, top_endpoints['requests_adjusted'], width, 
                   label='Requests', color='#3498db')
            ax1.bar(x + width/2, top_endpoints['visits_adjusted'], width, 
                   label='Visits', color='#2ecc71')
            ax1.set_title('Traffic Volume by Top Endpoints\nRequests vs Visits')
            ax1.set_ylabel('Count')
            ax1.set_xticks(x)
            ax1.set_xticklabels(top_endpoints.index, rotation=45, ha='right')
            ax1.legend()

            # 2. Response Time by Endpoint
            ax2.bar(range(len(top_endpoints)), top_endpoints['ttfb_avg'], color='#9b59b6')
            for i, v in enumerate(top_endpoints['ttfb_avg']):
                ax2.text(i, v, f'{v:.0f}ms', ha='center', va='bottom')
            ax2.set_title('Average Response Time by Endpoint')
            ax2.set_ylabel('TTFB (ms)')
            ax2.set_xticks(range(len(top_endpoints)))
            ax2.set_xticklabels(top_endpoints.index, rotation=45, ha='right')

            # 3. Error Rates (4xx vs 5xx)
            x = np.arange(len(top_endpoints))
            ax3.bar(x - width/2, top_endpoints['error_rate_4xx'] * 100, width, 
                   label='4xx Errors', color='#f1c40f')
            ax3.bar(x + width/2, top_endpoints['error_rate_5xx'] * 100, width, 
                   label='5xx Errors', color='#e74c3c')
            ax3.set_title('Error Rates by Top Endpoints')
            ax3.set_ylabel('Error Rate (%)')
            ax3.set_xticks(x)
            ax3.set_xticklabels(top_endpoints.index, rotation=45, ha='right')
            ax3.legend()

            # 4. Requests per Visit Ratio
            req_visit_ratio = top_endpoints['requests_adjusted'] / \
                            top_endpoints['visits_adjusted'].replace(0, float('nan'))
            ax4.bar(range(len(req_visit_ratio)), req_visit_ratio.fillna(0), color='#16a085')
            ax4.set_title('Requests per Visit by Endpoint')
            ax4.set_ylabel('Requests/Visit Ratio')
            ax4.set_xticks(range(len(req_visit_ratio)))
            ax4.set_xticklabels(req_visit_ratio.index, rotation=45, ha='right')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'endpoint_analysis.png')

        except Exception as e:
            logger.error(f"Error creating endpoint analysis: {str(e)}")
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
