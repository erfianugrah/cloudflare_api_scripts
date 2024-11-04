# visualizer.py
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional
from matplotlib.patches import Patch

logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self, config):
        self.config = config
        self.setup_style()
    
    def setup_style(self):
        """Configure visualization style."""
        plt.style.use('bmh')  # Using built-in matplotlib style
        plt.rcParams['figure.figsize'] = [12, 6]
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['savefig.dpi'] = 300
        plt.rcParams['axes.grid'] = True
        plt.rcParams['grid.alpha'] = 0.3
    
    def create_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str, analysis_type: str) -> None:
        """Create visualizations with confidence intervals."""
        try:
            output_dir = self.config.images_dir / zone_name / analysis_type
            output_dir.mkdir(parents=True, exist_ok=True)

            if analysis_type == 'cache':
                self._create_cache_visualizations(df, analysis, output_dir)
            elif analysis_type == 'performance':
                self._create_performance_visualizations(df, analysis, output_dir)
            
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")

    def _create_cache_visualizations(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create cache-specific visualizations with safety checks."""
        try:
            if 'overall' not in analysis or 'by_cache_status' not in analysis:
                logger.error("Missing required analysis data for cache visualizations")
                return

            # 1. Cache Hit Ratio Over Time
            plt.figure(figsize=(14, 7))
            
            if 'temporal' in analysis and 'hourly' in analysis['temporal']:
                hourly_data = pd.DataFrame.from_dict(analysis['temporal']['hourly'], orient='index')
                
                if not hourly_data.empty and 'requests_estimated' in hourly_data.columns:
                    x = hourly_data.index.astype(int)
                    
                    # Calculate hit ratio per hour
                    total_requests = hourly_data['requests_estimated'].sum()
                    if total_requests > 0:
                        hit_ratio = (hourly_data['requests_estimated'] / total_requests) * 100
                        
                        plt.plot(x, hit_ratio, 'b-', marker='o', linewidth=2, label='Hit Ratio')
                        
                        # Add sampling rate indicators
                        if 'sampling_rate' in hourly_data.columns:
                            for idx, rate in enumerate(hourly_data['sampling_rate']):
                                plt.annotate(
                                    f'SR: {rate:.1f}%',
                                    (x[idx], hit_ratio.iloc[idx]),
                                    xytext=(0, 10),
                                    textcoords='offset points',
                                    ha='center',
                                    fontsize=8
                                )
                        
                        plt.title('Cache Hit Ratio by Hour')
                        plt.xlabel('Hour of Day')
                        plt.ylabel('Hit Ratio (%)')
                        plt.legend()
                        plt.xticks(range(0, 24))
                        plt.tight_layout()
                        plt.savefig(output_dir / 'cache_hit_ratio.png')
            
            plt.close()

            # 2. Cache Status Distribution
            if analysis['by_cache_status']:
                plt.figure(figsize=(12, 6))
                status_data = pd.DataFrame.from_dict(analysis['by_cache_status'], orient='index')
                
                if not status_data.empty and 'percentage' in status_data.columns:
                    bars = plt.bar(range(len(status_data)), status_data['percentage'], alpha=0.7)
                    
                    # Add sampling rate indicators if available
                    if 'avg_sampling_rate' in status_data.columns:
                        for idx, (status, data) in enumerate(status_data.iterrows()):
                            plt.text(
                                idx,
                                data['percentage'],
                                f'SR: {data["avg_sampling_rate"]:.1f}%',
                                ha='center',
                                va='bottom'
                            )
                    
                    plt.title('Cache Status Distribution')
                    plt.xlabel('Cache Status')
                    plt.ylabel('Percentage of Requests (%)')
                    plt.xticks(range(len(status_data)), status_data.index, rotation=45)
                    plt.tight_layout()
                    plt.savefig(output_dir / 'cache_distribution.png')
                
            plt.close()

        except Exception as e:
            logger.error(f"Error creating cache visualizations: {str(e)}")
            plt.close('all')  # Ensure all figures are closed in case of error

    def _create_performance_visualizations(self, df: pd.DataFrame, analysis: Dict, output_dir: Path) -> None:
        """Create performance-specific visualizations with safety checks."""
        try:
            if 'overall' not in analysis:
                logger.error("Missing required analysis data for performance visualizations")
                return

            # 1. Response Time Distribution by Content Type
            plt.figure(figsize=(14, 7))
            
            if not df.empty and 'ttfb_avg' in df.columns and 'content_type' in df.columns:
                content_types = df['content_type'].unique()
                ttfb_by_type = [df[df['content_type'] == ct]['ttfb_avg'].dropna() for ct in content_types]
                
                # Only create boxplot if we have valid data
                if any(len(x) > 0 for x in ttfb_by_type):
                    plt.boxplot([x for x in ttfb_by_type if len(x) > 0])
                    plt.title('TTFB Distribution by Content Type')
                    plt.xlabel('Content Type')
                    plt.ylabel('Time to First Byte (ms)')
                    plt.xticks(range(1, len(content_types) + 1), content_types, rotation=45)
                    plt.tight_layout()
                    plt.savefig(output_dir / 'ttfb_distribution.png')
            
            plt.close()

            # 2. Performance Trends Over Time
            plt.figure(figsize=(14, 7))
            
            if 'trends' in analysis and 'hourly' in analysis['trends']:
                trends = pd.DataFrame.from_dict(analysis['trends']['hourly'], orient='index')
                
                if not trends.empty and 'ttfb' in trends.columns:
                    x = trends.index.astype(int)
                    plt.plot(x, trends['ttfb'], 'b-', marker='o', label='TTFB', linewidth=2)
                    
                    # Add confidence bands if sampling rate is available
                    if 'sampling_rate' in trends.columns:
                        confidence = 1 - trends['sampling_rate'].astype(float) / 100
                        plt.fill_between(
                            x,
                            trends['ttfb'] * (1 - confidence),
                            trends['ttfb'] * (1 + confidence),
                            color='blue',
                            alpha=0.2,
                            label='Confidence Interval'
                        )
                    
                    plt.title('TTFB Trends by Hour')
                    plt.xlabel('Hour of Day')
                    plt.ylabel('Time to First Byte (ms)')
                    plt.legend()
                    plt.xticks(range(0, 24))
                    plt.grid(True, alpha=0.3)
                    plt.tight_layout()
                    plt.savefig(output_dir / 'ttfb_trends.png')
            
            plt.close()

            # 3. Geographic Performance Heatmap
            plt.figure(figsize=(15, 8))
            
            if 'by_country' in analysis:
                geo_perf = pd.DataFrame.from_dict(analysis['by_country'], orient='index')
                
                if not geo_perf.empty and 'ttfb_avg' in geo_perf.columns:
                    # Sort countries by TTFB and get top 20
                    top_20_countries = geo_perf.sort_values('ttfb_avg', ascending=True).head(20)
                    
                    if not top_20_countries.empty:
                        # Create heatmap
                        plt.imshow(
                            [top_20_countries['ttfb_avg']],
                            aspect='auto',
                            cmap='YlOrRd'
                        )
                        
                        plt.colorbar(label='TTFB (ms)')
                        plt.yticks([])  # Hide y-axis ticks
                        plt.xticks(
                            range(len(top_20_countries)),
                            top_20_countries.index,
                            rotation=45,
                            ha='right'
                        )
                        
                        # Add sampling rate annotations if available
                        if 'sampling_rate' in top_20_countries.columns:
                            for i, (country, data) in enumerate(top_20_countries.iterrows()):
                                plt.text(
                                    i, 0,
                                    f'SR: {data["sampling_rate"]:.1f}%',
                                    ha='center',
                                    va='bottom'
                                )
                        
                        plt.title('Geographic Performance Heatmap (Top 20 Countries)')
                        plt.tight_layout()
                        plt.savefig(output_dir / 'geo_performance.png')
            
            plt.close()

            # 4. Error Rate Analysis
            plt.figure(figsize=(14, 7))
            
            if 'trends' in analysis and 'hourly' in analysis['trends']:
                trends = pd.DataFrame.from_dict(analysis['trends']['hourly'], orient='index')
                
                if not trends.empty and 'error_rate' in trends.columns:
                    x = trends.index.astype(int)
                    plt.plot(x, trends['error_rate'], 'r-', marker='o', linewidth=2, label='Error Rate')
                    
                    # Add sampling rate indicators if available
                    if 'sampling_rate' in trends.columns:
                        for hour, data in trends.iterrows():
                            plt.annotate(
                                f'SR: {data["sampling_rate"]:.1f}%',
                                (int(hour), data['error_rate']),
                                xytext=(0, 10),
                                textcoords='offset points',
                                ha='center',
                                fontsize=8
                            )
                    
                    plt.title('Error Rate Trends by Hour')
                    plt.xlabel('Hour of Day')
                    plt.ylabel('Error Rate (%)')
                    plt.xticks(range(0, 24))
                    plt.grid(True, alpha=0.3)
                    plt.legend()
                    plt.tight_layout()
                    plt.savefig(output_dir / 'error_trends.png')
            
            plt.close()

            # 5. Response Time Percentiles
            plt.figure(figsize=(14, 7))
            
            if 'percentiles' in analysis and 'ttfb' in analysis['percentiles']:
                percentiles = analysis['percentiles']['ttfb']
                
                if percentiles:
                    # Create percentile bars
                    percentile_data = {
                        'P50': percentiles.get('p50', 0),
                        'P95': percentiles.get('p95', 0),
                        'P99': percentiles.get('p99', 0)
                    }
                    
                    plt.bar(percentile_data.keys(), percentile_data.values(), alpha=0.7)
                    plt.title('TTFB Percentiles')
                    plt.xlabel('Percentile')
                    plt.ylabel('Time (ms)')
                    
                    # Add value labels on top of bars
                    for i, (label, value) in enumerate(percentile_data.items()):
                        plt.text(
                            i, value,
                            f'{value:.1f}ms',
                            ha='center',
                            va='bottom'
                        )
                    
                    plt.grid(True, alpha=0.3)
                    plt.tight_layout()
                    plt.savefig(output_dir / 'ttfb_percentiles.png')
            
            plt.close()

            # 6. Origin vs Edge Performance
            plt.figure(figsize=(14, 7))
            
            if ('trends' in analysis and 'hourly' in analysis['trends'] and
                all(key in trends.columns for key in ['ttfb', 'origin_time'])):
                
                x = trends.index.astype(int)
                
                # Plot both metrics
                plt.plot(x, trends['ttfb'], 'b-', marker='o', label='Edge TTFB', linewidth=2)
                plt.plot(x, trends['origin_time'], 'g-', marker='o', label='Origin Response', linewidth=2)
                
                plt.title('Edge vs Origin Performance')
                plt.xlabel('Hour of Day')
                plt.ylabel('Response Time (ms)')
                plt.legend()
                plt.xticks(range(0, 24))
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                plt.savefig(output_dir / 'edge_vs_origin.png')
            
            plt.close()

        except Exception as e:
            logger.error(f"Error creating performance visualizations: {str(e)}")
            plt.close('all')  # Ensure all figures are closed in case of error
