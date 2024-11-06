# origin_visualizer.py
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
from pathlib import Path
from matplotlib.dates import DateFormatter
import traceback

logger = logging.getLogger(__name__)

class OriginVisualizer:
    """Visualizer for origin server performance metrics."""
    
    def __init__(self, config):
        self.config = config
        self._setup_style()
        
    def _setup_style(self):
        """Configure visualization style."""
        plt.style.use('seaborn-darkgrid')
        plt.rcParams['figure.figsize'] = [12, 6]
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 12
        plt.rcParams['axes.labelsize'] = 10
        sns.set_palette("husl")

    def create_origin_visualizations(
        self, 
        df: pd.DataFrame, 
        analysis_results: Dict, 
        zone_name: str
    ) -> None:
        """Create comprehensive origin performance visualizations."""
        try:
            if df is None or df.empty or not analysis_results:
                logger.error("No data available for visualization")
                return

            output_dir = self.config.images_dir / zone_name / 'origin'
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create visualization groups
            self._create_response_time_analysis(df, analysis_results, output_dir)
            self._create_geographic_heatmap(df, analysis_results, output_dir)
            self._create_error_analysis(df, analysis_results, output_dir)
            self._create_endpoint_performance(df, analysis_results, output_dir)
            self._create_protocol_comparison(df, analysis_results, output_dir)
            self._create_temporal_patterns(df, analysis_results, output_dir)

        except Exception as e:
            logger.error(f"Error creating origin visualizations: {str(e)}")
            plt.close('all')

    def _create_response_time_analysis(
        self, 
        df: pd.DataFrame, 
        analysis: Dict, 
        output_dir: Path
    ) -> None:
        """Create response time analysis visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Response Time Distribution
            sns.histplot(
                data=df, 
                x='origin_time_avg',
                bins=50,
                ax=ax1,
                color='blue',
                alpha=0.6
            )
            ax1.set_title('Origin Response Time Distribution')
            ax1.set_xlabel('Response Time (ms)')
            ax1.set_ylabel('Count')

            # 2. Response Time by Cache Status
            sns.boxplot(
                data=df,
                x='cache_status',
                y='origin_time_avg',
                ax=ax2
            )
            ax2.set_title('Response Time by Cache Status')
            ax2.set_xlabel('Cache Status')
            ax2.set_ylabel('Response Time (ms)')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

            # 3. Response Time Percentiles
            percentiles = df['origin_time_avg'].quantile([0.5, 0.75, 0.9, 0.95, 0.99])
            ax3.bar(
                range(len(percentiles)),
                percentiles.values,
                tick_label=[f'P{int(p*100)}' for p in percentiles.index]
            )
            ax3.set_title('Response Time Percentiles')
            ax3.set_ylabel('Response Time (ms)')

            # 4. Response Time vs Request Volume
            df_hourly = df.set_index('timestamp').resample('1H').agg({
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum'
            }).reset_index()

            ax4.scatter(
                df_hourly['visits_adjusted'],
                df_hourly['origin_time_avg'],
                alpha=0.5
            )
            ax4.set_title('Response Time vs Request Volume')
            ax4.set_xlabel('Requests per Hour')
            ax4.set_ylabel('Average Response Time (ms)')

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'response_time_analysis.png')

        except Exception as e:
            logger.error(f"Error creating response time analysis: {str(e)}")
            plt.close('all')

    def _create_geographic_heatmap(
        self, 
        df: pd.DataFrame, 
        analysis: Dict, 
        output_dir: Path
    ) -> None:
        """Create geographic performance heatmap."""
        try:
            geo_metrics = df.groupby('country').agg({
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            }).reset_index()

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 20))

            # 1. Response Time Heatmap
            response_time_data = geo_metrics.pivot_table(
                index='country',
                values='origin_time_avg',
                aggfunc='mean'
            ).sort_values(ascending=False)

            sns.heatmap(
                response_time_data.to_frame(),
                cmap='YlOrRd',
                ax=ax1,
                cbar_kws={'label': 'Response Time (ms)'}
            )
            ax1.set_title('Origin Response Time by Country')

            # 2. Error Rate Heatmap
            error_data = geo_metrics.pivot_table(
                index='country',
                values='status',
                aggfunc='mean'
            ).sort_values(ascending=False)

            sns.heatmap(
                error_data.to_frame(),
                cmap='YlOrRd',
                ax=ax2,
                cbar_kws={'label': 'Error Rate (%)'}
            )
            ax2.set_title('Origin Error Rate by Country')

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'geographic_heatmap.png')

        except Exception as e:
            logger.error(f"Error creating geographic heatmap: {str(e)}")
            plt.close('all')

    def _create_error_analysis(
        self, 
        df: pd.DataFrame, 
        analysis: Dict, 
        output_dir: Path
    ) -> None:
        """Create error analysis visualizations."""
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Error Status Distribution
            error_df = df[df['status'] >= 500]
            error_counts = error_df['status'].value_counts()
            ax1.bar(error_counts.index.astype(str), error_counts.values)
            ax1.set_title('Origin Error Status Distribution')
            ax1.set_xlabel('Status Code')
            ax1.set_ylabel('Count')

            # 2. Error Rate Over Time
            df_hourly = df.set_index('timestamp').resample('1H').agg({
                'status': lambda x: (x >= 500).mean() * 100
            }).reset_index()
            
            ax2.plot(df_hourly['timestamp'], df_hourly['status'])
            ax2.set_title('Origin Error Rate Over Time')
            ax2.set_xlabel('Time')
            ax2.set_ylabel('Error Rate (%)')
            ax2.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

            # 3. Top Error Endpoints
            endpoint_errors = df.groupby('endpoint').agg({
                'status': lambda x: (x >= 500).mean() * 100
            }).nlargest(10, 'status')
            
            ax3.barh(
                range(len(endpoint_errors)),
                endpoint_errors['status'],
                tick_label=endpoint_errors.index
            )
            ax3.set_title('Top 10 Endpoints by Error Rate')
            ax3.set_xlabel('Error Rate (%)')

            # 4. Error Response Time Distribution
            sns.boxplot(
                data=df,
                x='status',
                y='origin_time_avg',
                ax=ax4,
                whis=[5, 95]
            )
            ax4.set_title('Response Time Distribution by Status')
            ax4.set_xlabel('Status Code')
            ax4.set_ylabel('Response Time (ms)')

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'error_analysis.png')

        except Exception as e:
            logger.error(f"Error creating error analysis: {str(e)}")
            plt.close('all')

    def _create_endpoint_performance(
        self, 
        df: pd.DataFrame, 
        analysis: Dict, 
        output_dir: Path
    ) -> None:
        """Create endpoint performance visualizations."""
        try:
            # Get top 10 endpoints by request volume
            top_endpoints = df.groupby('endpoint').agg({
                'visits_adjusted': 'sum',
                'origin_time_avg': 'mean',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            }).nlargest(10, 'visits_adjusted')

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Response Time by Endpoint
            ax1.barh(
                range(len(top_endpoints)),
                top_endpoints['origin_time_avg'],
                tick_label=top_endpoints.index
            )
            ax1.set_title('Average Response Time by Top Endpoints')
            ax1.set_xlabel('Response Time (ms)')

            # 2. Request Volume by Endpoint
            ax2.barh(
                range(len(top_endpoints)),
                top_endpoints['visits_adjusted'],
                tick_label=top_endpoints.index
            )
            ax2.set_title('Request Volume by Top Endpoints')
            ax2.set_xlabel('Number of Requests')

            # 3. Error Rate by Endpoint
            ax3.barh(
                range(len(top_endpoints)),
                top_endpoints['status'],
                tick_label=top_endpoints.index
            )
            ax3.set_title('Error Rate by Top Endpoints')
            ax3.set_xlabel('Error Rate (%)')

            # 4. Response Size by Endpoint
            ax4.barh(
                range(len(top_endpoints)),
                top_endpoints['bytes_adjusted'] / (1024 * 1024),  # Convert to MB
                tick_label=top_endpoints.index
            )
            ax4.set_title('Average Response Size by Top Endpoints')
            ax4.set_xlabel('Size (MB)')

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'endpoint_performance.png')

        except Exception as e:
            logger.error(f"Error creating endpoint performance visualization: {str(e)}")
            plt.close('all')

    def _create_protocol_comparison(
        self, 
        df: pd.DataFrame, 
        analysis: Dict, 
        output_dir: Path
    ) -> None:
        """Create protocol comparison visualizations."""
        try:
            protocol_metrics = df.groupby('protocol').agg({
                'origin_time_avg': ['mean', 'std'],
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            })

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Response Time by Protocol
            ax1.bar(
                protocol_metrics.index,
                protocol_metrics['origin_time_avg']['mean'],
                yerr=protocol_metrics['origin_time_avg']['std'],
                capsize=5
            )
            ax1.set_title('Average Response Time by Protocol')
            ax1.set_ylabel('Response Time (ms)')
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

            # 2. Request Distribution by Protocol
            ax2.pie(
                protocol_metrics['visits_adjusted'],
                labels=protocol_metrics.index,
                autopct='%1.1f%%'
            )
            ax2.set_title('Request Distribution by Protocol')

            # 3. Error Rate by Protocol
            ax3.bar(protocol_metrics.index, protocol_metrics['status'])
            ax3.set_title('Error Rate by Protocol')
            ax3.set_ylabel('Error Rate (%)')
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)

            # 4. Bandwidth Usage by Protocol
            bandwidth_gb = protocol_metrics['bytes_adjusted'] / (1024**3)
            ax4.bar(bandwidth_gb.index, bandwidth_gb)
            ax4.set_title('Bandwidth Usage by Protocol')
            ax4.set_ylabel('Bandwidth (GB)')
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'protocol_comparison.png')

        except Exception as e:
            logger.error(f"Error creating protocol comparison: {str(e)}")
            plt.close('all')

    def _create_temporal_patterns(
        self, 
        df: pd.DataFrame, 
        analysis: Dict, 
        output_dir: Path
    ) -> None:
        """Create temporal pattern visualizations."""
        try:
            # Resample data to 5-minute intervals
            df_time = df.set_index('timestamp').resample('5min').agg({
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum',
                'bytes_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100
            }).reset_index()

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Response Time Over Time
            ax1.plot(df_time['timestamp'], df_time['origin_time_avg'])
            ax1.set_title('Origin Response Time Over Time')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Response Time (ms)')
            ax1.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

            # 2. Request Volume Over Time
            ax2.plot(df_time['timestamp'], df_time['visits_adjusted'])
            ax2.set_title('Request Volume Over Time')
            ax2.set_xlabel('Time')
            ax2.set_ylabel('Number of Requests')
# (Continuing _create_temporal_patterns method)
            
            # 3. Error Rate Over Time
            ax3.plot(df_time['timestamp'], df_time['status'], color='red')
            ax3.set_title('Origin Error Rate Over Time')
            ax3.set_xlabel('Time')
            ax3.set_ylabel('Error Rate (%)')
            ax3.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)

            # 4. Bandwidth Usage Over Time
            bandwidth_gb = df_time['bytes_adjusted'] / (1024**3)
            ax4.plot(df_time['timestamp'], bandwidth_gb, color='green')
            ax4.set_title('Origin Bandwidth Usage Over Time')
            ax4.set_xlabel('Time')
            ax4.set_ylabel('Bandwidth (GB)')
            ax4.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
            plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'temporal_patterns.png')

            # Create additional temporal analysis
            self._create_time_correlation_analysis(df, output_dir)
            self._create_hourly_patterns(df, output_dir)
            self._create_performance_heatmap(df, output_dir)

        except Exception as e:
            logger.error(f"Error creating temporal patterns: {str(e)}")
            plt.close('all')

    def _create_time_correlation_analysis(
        self,
        df: pd.DataFrame,
        output_dir: Path
    ) -> None:
        """Create correlation analysis between different time-based metrics."""
        try:
            # Prepare correlation data
            correlation_data = pd.DataFrame({
                'response_time': df['origin_time_avg'],
                'request_volume': df['visits_adjusted'],
                'error_rate': df['status'].apply(lambda x: 1 if x >= 500 else 0),
                'bandwidth': df['bytes_adjusted'],
                'hour': df['timestamp'].dt.hour,
                'day_of_week': df['timestamp'].dt.dayofweek
            })

            # Create correlation matrix
            correlation_matrix = correlation_data.corr()

            fig, ax = plt.subplots(figsize=(12, 10))
            sns.heatmap(
                correlation_matrix,
                annot=True,
                cmap='RdYlBu',
                center=0,
                ax=ax,
                fmt='.2f'
            )
            ax.set_title('Origin Performance Metric Correlations')
            self._save_plot(fig, output_dir / 'time_correlations.png')

        except Exception as e:
            logger.error(f"Error creating time correlation analysis: {str(e)}")
            plt.close('all')

    def _create_hourly_patterns(
        self,
        df: pd.DataFrame,
        output_dir: Path
    ) -> None:
        """Create hourly pattern analysis."""
        try:
            # Calculate hourly metrics
            hourly_metrics = df.groupby(df['timestamp'].dt.hour).agg({
                'origin_time_avg': 'mean',
                'visits_adjusted': 'sum',
                'status': lambda x: (x >= 500).mean() * 100,
                'bytes_adjusted': 'sum'
            })

            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))

            # 1. Response Time by Hour
            ax1.plot(hourly_metrics.index, hourly_metrics['origin_time_avg'], 
                    marker='o', linewidth=2)
            ax1.set_title('Average Response Time by Hour')
            ax1.set_xlabel('Hour of Day')
            ax1.set_ylabel('Response Time (ms)')
            ax1.set_xticks(range(24))

            # 2. Request Volume by Hour
            ax2.bar(hourly_metrics.index, hourly_metrics['visits_adjusted'])
            ax2.set_title('Request Volume by Hour')
            ax2.set_xlabel('Hour of Day')
            ax2.set_ylabel('Number of Requests')
            ax2.set_xticks(range(24))

            # 3. Error Rate by Hour
            ax3.plot(hourly_metrics.index, hourly_metrics['status'], 
                    color='red', marker='o', linewidth=2)
            ax3.set_title('Error Rate by Hour')
            ax3.set_xlabel('Hour of Day')
            ax3.set_ylabel('Error Rate (%)')
            ax3.set_xticks(range(24))

            # 4. Bandwidth Usage by Hour
            bandwidth_gb = hourly_metrics['bytes_adjusted'] / (1024**3)
            ax4.bar(bandwidth_gb.index, bandwidth_gb, color='green')
            ax4.set_title('Bandwidth Usage by Hour')
            ax4.set_xlabel('Hour of Day')
            ax4.set_ylabel('Bandwidth (GB)')
            ax4.set_xticks(range(24))

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'hourly_patterns.png')

        except Exception as e:
            logger.error(f"Error creating hourly patterns: {str(e)}")
            plt.close('all')

    def _create_performance_heatmap(
        self,
        df: pd.DataFrame,
        output_dir: Path
    ) -> None:
        """Create performance heatmap by hour and day of week."""
        try:
            # Prepare data for heatmap
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek

            # Create separate heatmaps for different metrics
            metrics = {
                'response_time': df.pivot_table(
                    values='origin_time_avg',
                    index='day_of_week',
                    columns='hour',
                    aggfunc='mean'
                ),
                'error_rate': df.pivot_table(
                    values='status',
                    index='day_of_week',
                    columns='hour',
                    aggfunc=lambda x: (x >= 500).mean() * 100
                ),
                'request_volume': df.pivot_table(
                    values='visits_adjusted',
                    index='day_of_week',
                    columns='hour',
                    aggfunc='sum'
                )
            }

            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 24))

            # 1. Response Time Heatmap
            sns.heatmap(
                metrics['response_time'],
                ax=ax1,
                cmap='YlOrRd',
                cbar_kws={'label': 'Response Time (ms)'}
            )
            ax1.set_title('Response Time by Hour and Day')
            ax1.set_ylabel('Day of Week')
            ax1.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

            # 2. Error Rate Heatmap
            sns.heatmap(
                metrics['error_rate'],
                ax=ax2,
                cmap='YlOrRd',
                cbar_kws={'label': 'Error Rate (%)'}
            )
            ax2.set_title('Error Rate by Hour and Day')
            ax2.set_ylabel('Day of Week')
            ax2.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

            # 3. Request Volume Heatmap
            sns.heatmap(
                metrics['request_volume'],
                ax=ax3,
                cmap='YlOrRd',
                cbar_kws={'label': 'Number of Requests'}
            )
            ax3.set_title('Request Volume by Hour and Day')
            ax3.set_ylabel('Day of Week')
            ax3.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])

            plt.tight_layout()
            self._save_plot(fig, output_dir / 'performance_heatmap.png')

        except Exception as e:
            logger.error(f"Error creating performance heatmap: {str(e)}")
            plt.close('all')

    def _save_plot(self, fig: plt.Figure, filepath: Path) -> None:
        """Safely save plot with error handling."""
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(filepath, bbox_inches='tight', dpi=300)
            plt.close(fig)
        except Exception as e:
            logger.error(f"Error saving plot to {filepath}: {str(e)}")
            plt.close(fig)
