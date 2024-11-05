# visualizer.py
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional, Tuple
from matplotlib.dates import (DateFormatter, AutoDateLocator, 
                            HourLocator, DayLocator, MinuteLocator)
import seaborn as sns
from datetime import timedelta

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

    def _determine_time_scale(self, df: pd.DataFrame) -> Tuple[str, str]:
        """Determine appropriate time scale based on data range."""
        try:
            time_range = df['timestamp'].max() - df['timestamp'].min()
            total_minutes = time_range.total_seconds() / 60
            
            if total_minutes <= 60:  # 1 hour
                return '5min', '5 Minutes'
            elif total_minutes <= 24 * 60:  # 24 hours
                return '1h', 'Hour'
            elif total_minutes <= 7 * 24 * 60:  # 7 days
                return '6h', '6 Hours'
            elif total_minutes <= 30 * 24 * 60:  # 30 days
                return '1D', 'Day'
            elif total_minutes <= 90 * 24 * 60:  # 90 days
                return '1W', 'Week'
            else:
                return '1M', 'Month'
        except Exception as e:
            logger.error(f"Error determining time scale: {str(e)}")
            return '1h', 'Hour'  # Default

    def _setup_time_axis(self, ax, df: pd.DataFrame):
        """Configure time axis formatting based on data range."""
        try:
            time_range = df['timestamp'].max() - df['timestamp'].min()
            total_minutes = time_range.total_seconds() / 60
            
            if total_minutes <= 60:  # 1 hour
                ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
                ax.xaxis.set_major_locator(MinuteLocator(interval=5))
            elif total_minutes <= 24 * 60:  # 24 hours
                ax.xaxis.set_major_formatter(DateFormatter('%H:%M'))
                ax.xaxis.set_major_locator(HourLocator(interval=2))
            elif total_minutes <= 7 * 24 * 60:  # 7 days
                ax.xaxis.set_major_formatter(DateFormatter('%m/%d %H:%M'))
                ax.xaxis.set_major_locator(DayLocator())
            else:
                ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
                ax.xaxis.set_major_locator(AutoDateLocator())
            
            plt.xticks(rotation=45, ha='right')
            
            # Add buffer to time range
            if len(df['timestamp'].unique()) > 1:
                buffer = timedelta(minutes=total_minutes * 0.05)  # 5% buffer
                ax.set_xlim(
                    df['timestamp'].min() - buffer,
                    df['timestamp'].max() + buffer
                )
                
        except Exception as e:
            logger.error(f"Error setting up time axis: {str(e)}")

    def _safe_resample(self, df: pd.DataFrame, time_unit: str, value_column: str,
                      agg_func: str = 'mean') -> pd.Series:
        """Safely resample time series data without timestamp operations."""
        try:
            df_copy = df.copy()
            df_copy = df_copy.sort_values('timestamp')
            df_copy = df_copy.set_index('timestamp')
            
            resampled = df_copy[value_column].resample(time_unit)
            
            if agg_func == 'mean':
                result = resampled.mean()
            elif agg_func == 'sum':
                result = resampled.sum()
            else:
                result = resampled.mean()
            
            # Use modern pandas methods instead of deprecated fillna
            result = result.ffill().bfill()
            return result
            
        except Exception as e:
            logger.error(f"Error resampling data: {str(e)}")
            return pd.Series(dtype=float)

    def _plot_time_series(self, ax, df: pd.DataFrame, value_column: str,
                         time_unit: str, label: str, color: str = 'blue') -> Optional[pd.Series]:
        """Plot time series data with proper handling."""
        try:
            resampled = self._safe_resample(df, time_unit, value_column)
            
            if not resampled.empty:
                ax.plot(resampled.index, resampled.values,
                       marker='o', label=label, color=color, linewidth=2,
                       markersize=4, alpha=0.7)
                
                self._setup_time_axis(ax, df)
                return resampled
                
        except Exception as e:
            logger.error(f"Error plotting time series: {str(e)}")
            return None

    def create_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str, analysis_type: str) -> None:
        """Create visualizations with dynamic time scaling."""
        try:
            output_dir = self.config.images_dir / zone_name / analysis_type
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create a copy of the DataFrame
            df_copy = df.copy()

            if analysis_type == 'cache':
                self._create_cache_visualizations(df_copy, analysis, output_dir)
            elif analysis_type == 'performance':
                self._create_performance_visualizations(df_copy, analysis, output_dir)
            
            # Always create path-based visualizations
            self._create_path_visualizations(df_copy, output_dir)
            
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            plt.close('all')

    def _create_path_visualizations(self, df: pd.DataFrame, output_dir: Path) -> None:
        """Create path-specific visualizations."""
        try:
            # Calculate path metrics
            path_metrics = (df.groupby('path_group')
                          .agg({
                              'visits_adjusted': 'sum',
                              'ttfb_avg': 'mean',
                              'cache_status': lambda x: (x.isin(['hit', 'stale', 'revalidated']).mean() * 100)
                          })
                          .sort_values('visits_adjusted', ascending=False)
                          .head(10))

            # 1. Path Overview
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 15))
            
            # Prepare path labels
            path_labels = [path[:30] + '...' if len(path) > 30 else path 
                         for path in path_metrics.index]

            # TTFB by Path
            sns.barplot(x=range(len(path_metrics)), y=path_metrics['ttfb_avg'],
                       ax=ax1, color='blue', alpha=0.7)
            ax1.set_title('Average TTFB by Path')
            ax1.set_ylabel('TTFB (ms)')
            ax1.set_xticks(range(len(path_metrics)))
            ax1.set_xticklabels(path_labels, rotation=45, ha='right')

            # Cache Hit Ratio by Path
            sns.barplot(x=range(len(path_metrics)), y=path_metrics['cache_status'],
                       ax=ax2, color='green', alpha=0.7)
            ax2.set_title('Cache Hit Ratio by Path')
            ax2.set_ylabel('Cache Hit Ratio (%)')
            ax2.set_xticks(range(len(path_metrics)))
            ax2.set_xticklabels(path_labels, rotation=45, ha='right')

            # Request Volume by Path
            sns.barplot(x=range(len(path_metrics)), y=path_metrics['visits_adjusted'],
                       ax=ax3, color='purple', alpha=0.7)
            ax3.set_title('Request Volume by Path')
            ax3.set_ylabel('Number of Requests')
            ax3.set_xticks(range(len(path_metrics)))
            ax3.set_xticklabels(path_labels, rotation=45, ha='right')

            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'path_overview.png')

            # 2. Time Series by Path
            time_unit, time_label = self._determine_time_scale(df)
            
            for path in path_metrics.index:
                mask = df['path_group'] == path
                if mask.any():
                    path_df = df[mask].copy()
                    self._create_path_time_series(path_df, path, time_unit, output_dir)

        except Exception as e:
            logger.error(f"Error creating path visualizations: {str(e)}")
            plt.close('all')

    def _create_path_time_series(self, path_df: pd.DataFrame, path: str,
                               time_unit: str, output_dir: Path) -> None:
        """Create time series plots for a specific path."""
        try:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))
            
            # TTFB Over Time
            self._plot_time_series(
                ax1, path_df, 'ttfb_avg', time_unit,
                'TTFB', color='blue'
            )
            ax1.set_title(f'TTFB Over Time - {path[:50]}{"..." if len(path) > 50 else ""}')
            ax1.set_ylabel('TTFB (ms)')
            
            # Cache Hit Ratio Over Time
            cache_hits = path_df['cache_status'].isin(['hit', 'stale', 'revalidated'])
            path_df.loc[:, 'cache_hit_ratio'] = cache_hits.astype(float) * 100
            
            self._plot_time_series(
                ax2, path_df, 'cache_hit_ratio', time_unit,
                'Cache Hit Ratio', color='green'
            )
            ax2.set_title('Cache Hit Ratio Over Time')
            ax2.set_ylabel('Cache Hit Ratio (%)')
            
            plt.tight_layout()
            safe_path = "".join(x for x in path if x.isalnum())[:30]
            self._save_fig_safely(fig, output_dir / f'path_metrics_{safe_path}.png')
            
        except Exception as e:
            logger.error(f"Error creating path time series: {str(e)}")
            plt.close('all')

    def _create_cache_visualizations(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create cache-specific visualizations with dynamic time scaling."""
        try:
            time_unit, time_label = self._determine_time_scale(df)
            
            # 1. Cache Hit Ratio Over Time
            fig, ax = plt.subplots(figsize=(14, 7))
            
            # Calculate cache hits without timestamp operations
            df.loc[:, 'cache_hit_ratio'] = (df['cache_status']
                                          .isin(['hit', 'stale', 'revalidated'])
                                          .astype(float) * 100)
            
            resampled = self._plot_time_series(
                ax, df, 'cache_hit_ratio', time_unit,
                'Hit Ratio', color='blue'
            )
            
            if resampled is not None:
                # Add sampling rate indicators without timestamp operations
                sampling_rates = self._safe_resample(df, time_unit, 'sampling_rate')
                for i, (idx, rate) in enumerate(sampling_rates.items()):
                    if i % 2 == 0:  # Add labels every other point
                        ax.annotate(
                            f'SR: {rate:.1f}%',
                            (idx, resampled[idx]),
                            xytext=(0, 10),
                            textcoords='offset points',
                            ha='center',
                            fontsize=8
                        )
            
            ax.set_title(f'Cache Hit Ratio by {time_label}')
            ax.set_xlabel('Time')
            ax.set_ylabel('Hit Ratio (%)')
            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'cache_hit_ratio.png')

            # Create remaining visualizations...
            if analysis.get('by_cache_status'):
                self._create_cache_distribution_plot(analysis, output_dir)

            if analysis.get('by_country'):
                self._create_geo_cache_plot(analysis, output_dir)

            if analysis.get('by_content_type'):
                self._create_content_type_cache_plot(analysis, output_dir)

        except Exception as e:
            logger.error(f"Error creating cache visualizations: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')

    def _create_cache_distribution_plot(self, analysis: Dict, output_dir: Path) -> None:
        """Create cache status distribution plot."""
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            status_data = pd.DataFrame.from_dict(
                analysis['by_cache_status'],
                orient='index'
            )
            
            if not status_data.empty and 'percentage' in status_data.columns:
                sns.barplot(x=status_data.index, y=status_data['percentage'],
                          ax=ax, color='blue', alpha=0.7)
                
                # Add value labels
                for i, (_, row) in enumerate(status_data.iterrows()):
                    ax.text(i, row['percentage'],
                           f"{row['percentage']:.1f}%",
                           ha='center', va='bottom')
                    
                    # Add sampling rate if available
                    if 'avg_sampling_rate' in row:
                        ax.text(i, row['percentage'] / 2,
                               f"SR: {row['avg_sampling_rate']:.1f}%",
                               ha='center', va='center',
                               color='white', fontweight='bold')
                
                ax.set_title('Cache Status Distribution')
                ax.set_xlabel('Cache Status')
                ax.set_ylabel('Percentage of Requests (%)')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                self._save_fig_safely(fig, output_dir / 'cache_distribution.png')
                
        except Exception as e:
            logger.error(f"Error creating cache distribution plot: {str(e)}")
            plt.close('all')

    def _create_geo_cache_plot(self, analysis: Dict, output_dir: Path) -> None:
        """Create geographic cache performance plot."""
        try:
            fig, ax = plt.subplots(figsize=(15, 8))
            
            country_data = pd.DataFrame.from_dict(
                analysis['by_country'],
                orient='index'
            )
            
            # Calculate percentage from cache status instead of looking for cache_hit_ratio
            if 'requests_estimated' in country_data.columns and 'cache_status' in country_data.columns:
                total_requests = country_data['requests_estimated'].sum()
                country_data['hit_ratio'] = country_data.apply(
                    lambda x: (x['requests_estimated'] if x.get('cache_status', '').lower() in 
                             ['hit', 'stale', 'revalidated'] else 0) / total_requests * 100,
                    axis=1
                )
                
                # Sort and get top 20 countries
                country_data = country_data.sort_values('hit_ratio', ascending=False).head(20)
                
                sns.barplot(
                    x=country_data.index,
                    y=country_data['hit_ratio'],
                    ax=ax,
                    color='blue',
                    alpha=0.7
                )
                
                # Add sampling rate annotations
                for i, (_, row) in enumerate(country_data.iterrows()):
                    if 'sampling_rate' in row:
                        ax.text(i, row['hit_ratio'] / 2,
                               f"SR: {row['sampling_rate']:.1f}%",
                               ha='center', va='center',
                               color='white', fontweight='bold')
                
                ax.set_title('Cache Hit Ratio by Country (Top 20)')
                ax.set_xlabel('Country')
                ax.set_ylabel('Cache Hit Ratio (%)')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                self._save_fig_safely(fig, output_dir / 'geo_cache_performance.png')
                
        except Exception as e:
            logger.error(f"Error creating geographic cache plot: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')

    def _create_content_type_cache_plot(self, analysis: Dict, output_dir: Path) -> None:
        """Create content type cache performance plot."""
        try:
            fig, ax = plt.subplots(figsize=(14, 7))
            
            content_data = pd.DataFrame.from_dict(
                analysis['by_content_type'],
                orient='index'
            )
            
            # Calculate cache hit ratio from the data
            if 'requests_estimated' in content_data.columns and 'cache_status' in content_data.columns:
                total_requests = content_data['requests_estimated'].sum()
                content_data['hit_ratio'] = content_data.apply(
                    lambda x: (x['requests_estimated'] if x.get('cache_status', '').lower() in 
                             ['hit', 'stale', 'revalidated'] else 0) / total_requests * 100,
                    axis=1
                )
                
                # Sort and get top 10 content types
                content_data = content_data.sort_values('hit_ratio', ascending=False).head(10)
                
                sns.barplot(
                    x=content_data.index,
                    y=content_data['hit_ratio'],
                    ax=ax,
                    color='green',
                    alpha=0.7
                )
                
                # Add sampling rate annotations
                for i, (_, row) in enumerate(content_data.iterrows()):
                    if 'sampling_rate' in row:
                        ax.text(i, row['hit_ratio'] / 2,
                               f"SR: {row['sampling_rate']:.1f}%",
                               ha='center', va='center',
                               color='white', fontweight='bold')
                
                ax.set_title('Cache Hit Ratio by Content Type (Top 10)')
                ax.set_xlabel('Content Type')
                ax.set_ylabel('Cache Hit Ratio (%)')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                self._save_fig_safely(fig, output_dir / 'content_type_cache.png')
                
        except Exception as e:
            logger.error(f"Error creating content type cache plot: {str(e)}")
            logger.error(traceback.format_exc())
            plt.close('all')


    def _create_performance_visualizations(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create performance-specific visualizations."""
        try:
            time_unit, time_label = self._determine_time_scale(df)
            
            # 1. Performance Trends
            fig, ax = plt.subplots(figsize=(14, 7))
            
            # Plot TTFB and Origin Response Time
            ttfb_series = self._plot_time_series(
                ax, df, 'ttfb_avg', time_unit,
                'Edge TTFB', color='blue'
            )
            origin_series = self._plot_time_series(
                ax, df, 'origin_time_avg', time_unit,
                'Origin Response', color='orange'
            )
            
            # Add confidence bands
            if ttfb_series is not None and origin_series is not None:
                sampling_rates = self._safe_resample(df, time_unit, 'sampling_rate')
                confidence = 1 - sampling_rates / 100
                
                for series, color, label in [(ttfb_series, 'blue', 'TTFB'),
                                           (origin_series, 'orange', 'Origin')]:
                    plt.fill_between(
                        series.index,
                        series * (1 - confidence),
                        series * (1 + confidence),
                        alpha=0.2,
                        color=color,
                        label=f'{label} Confidence'
                    )
            
            ax.set_title(f'Response Time Trends by {time_label}')
            ax.set_xlabel('Time')
            ax.set_ylabel('Response Time (ms)')
            ax.legend()
            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'performance_trends.png')

            # 2. Create Error Rate Visualization
            self._create_error_rate_plot(df, time_unit, time_label, output_dir)

            # 3. Create Performance Percentiles Plot
            if 'percentiles' in analysis:
                self._create_percentile_plot(analysis, output_dir)

            # 4. Create Performance Distribution Plot
            self._create_performance_distribution_plot(df, output_dir)

            # 5. Create Performance Heatmap
            self._create_performance_heatmap(df, output_dir)

        except Exception as e:
            logger.error(f"Error creating performance visualizations: {str(e)}")
            plt.close('all')

    def _create_error_rate_plot(self, df: pd.DataFrame, time_unit: str, 
                               time_label: str, output_dir: Path) -> None:
        """Create error rate visualization."""
        try:
            fig, ax = plt.subplots(figsize=(14, 7))
            
            # Plot error rates
            error_4xx = self._plot_time_series(
                ax, df, 'error_rate_4xx', time_unit,
                '4xx Errors', color='orange'
            )
            error_5xx = self._plot_time_series(
                ax, df, 'error_rate_5xx', time_unit,
                '5xx Errors', color='red'
            )
            
            if error_4xx is not None and error_5xx is not None:
                # Add confidence bands
                sampling_rates = self._safe_resample(df, time_unit, 'sampling_rate')
                confidence = 1 - sampling_rates / 100
                
                for series, color in [(error_4xx, 'orange'), (error_5xx, 'red')]:
                    plt.fill_between(
                        series.index,
                        series * (1 - confidence),
                        series * (1 + confidence),
                        alpha=0.2,
                        color=color
                    )
            
            ax.set_title(f'Error Rates by {time_label}')
            ax.set_xlabel('Time')
            ax.set_ylabel('Error Rate (%)')
            ax.legend()
            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'error_rates.png')
            
        except Exception as e:
            logger.error(f"Error creating error rate plot: {str(e)}")
            plt.close('all')

    def _create_performance_distribution_plot(self, df: pd.DataFrame, output_dir: Path) -> None:
        """Create performance distribution plot."""
        try:
            fig, ax = plt.subplots(figsize=(14, 7))
            
            # Create violin plot for TTFB distribution
            sns.violinplot(y=df['ttfb_avg'], ax=ax, color='blue', alpha=0.7)
            
            # Add percentile lines
            percentiles = np.percentile(df['ttfb_avg'], [50, 95, 99])
            colors = ['green', 'orange', 'red']
            labels = ['P50', 'P95', 'P99']
            
            for p, c, l in zip(percentiles, colors, labels):
                ax.axhline(y=p, color=c, linestyle='--', alpha=0.7, label=f'{l}: {p:.1f}ms')
            
            ax.set_title('TTFB Distribution')
            ax.set_ylabel('TTFB (ms)')
            ax.legend()
            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'performance_distribution.png')
            
        except Exception as e:
            logger.error(f"Error creating performance distribution plot: {str(e)}")
            plt.close('all')

    def _create_performance_heatmap(self, df: pd.DataFrame, output_dir: Path) -> None:
        """Create performance heatmap by hour and day."""
        try:
            fig, ax = plt.subplots(figsize=(15, 8))
            
            # Create pivot table for heatmap
            df_copy = df.copy()
            df_copy['hour'] = df_copy['timestamp'].dt.hour
            df_copy['day'] = df_copy['timestamp'].dt.date
            
            perf_matrix = df_copy.pivot_table(
                values='ttfb_avg',
                index='day',
                columns='hour',
                aggfunc='mean'
            )
            
            # Create heatmap
            sns.heatmap(
                perf_matrix,
                cmap='YlOrRd',
                cbar_kws={'label': 'TTFB (ms)'},
                ax=ax
            )
            
            ax.set_title('Performance Heatmap (TTFB by Hour and Day)')
            ax.set_xlabel('Hour of Day')
            ax.set_ylabel('Date')
            plt.tight_layout()
            self._save_fig_safely(fig, output_dir / 'performance_heatmap.png')
            
        except Exception as e:
            logger.error(f"Error creating performance heatmap: {str(e)}")
            plt.close('all')

    def _create_percentile_plot(self, analysis: Dict, output_dir: Path) -> None:
        """Create response time percentiles plot."""
        try:
            fig, ax = plt.subplots(figsize=(14, 7))
            percentiles = analysis['percentiles'].get('ttfb', {})
            
            if percentiles:
                percentile_data = {
                    'P50': percentiles.get('p50', 0),
                    'P95': percentiles.get('p95', 0),
                    'P99': percentiles.get('p99', 0)
                }
                
                # Create bar plot
                bars = ax.bar(
                    percentile_data.keys(),
                    percentile_data.values(),
                    color=['#2ecc71', '#f1c40f', '#e74c3c'],
                    alpha=0.7
                )
                
                # Add value labels
                for bar in bars:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width()/2.,
                        height,
                        f'{height:.1f}ms',
                        ha='center',
                        va='bottom'
                    )
                
                ax.set_title('TTFB Response Time Percentiles')
                ax.set_ylabel('Response Time (ms)')
                ax.grid(True, alpha=0.3)
                plt.tight_layout()
                self._save_fig_safely(fig, output_dir / 'response_time_percentiles.png')
                
        except Exception as e:
            logger.error(f"Error creating percentile plot: {str(e)}")
            plt.close('all')

    def _save_fig_safely(self, fig: plt.Figure, filepath: Path, close: bool = True) -> None:
        """Safely save figure with error handling."""
        try:
            fig.savefig(filepath, bbox_inches='tight', dpi=300)
            if close:
                plt.close(fig)
        except Exception as e:
            logger.error(f"Error saving figure to {filepath}: {str(e)}")
            if close:
                plt.close(fig)
