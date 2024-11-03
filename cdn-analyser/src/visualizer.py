import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self, config):
        self.config = config
    
    def create_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str, analysis_type: str) -> None:
        """Create visualizations based on analysis type."""
        try:
            if analysis_type == 'cache':
                self._create_cache_visualizations(df, analysis, zone_name)
            elif analysis_type == 'performance':
                self._create_performance_visualizations(df, analysis, zone_name)
            else:
                raise ValueError(f"Unknown analysis type: {analysis_type}")
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
    
    def _create_cache_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str) -> None:
        """Create cache-specific visualizations."""
        output_dir = self.config.images_dir / zone_name / 'cache'
        output_dir.mkdir(parents=True, exist_ok=True)

        # Convert string hours to numeric for plotting
        hourly_data = pd.DataFrame.from_dict(
            analysis['by_time']['hourly'],
            orient='index'
        )
        hourly_data.index = pd.to_numeric(hourly_data.index)
        hourly_data = hourly_data.sort_index()

        # 2. Hourly Cache Performance
        plt.figure(figsize=(12, 6))
        plt.plot(
            hourly_data.index,
            hourly_data['hit_ratio'],
            marker='o',
            linestyle='-',
            color='purple',
            linewidth=2
        )
        plt.title('Cache Hit Ratio by Hour')
        plt.xlabel('Hour of Day')
        plt.ylabel('Hit Ratio (%)')
        plt.xticks(range(0, 24))  # Set explicit hour ticks
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_dir / 'hourly_cache.png')
        plt.close()

        # 4. Bandwidth Usage
        plt.figure(figsize=(12, 6))
        bytes_per_hour = hourly_data['bytes'] / (1024 * 1024)  # Convert to MB
        plt.bar(
            hourly_data.index,
            bytes_per_hour,
            color='teal',
            alpha=0.7
        )
        plt.title('Bandwidth Usage by Hour')
        plt.xlabel('Hour of Day')
        plt.ylabel('Bandwidth (MB)')
        plt.xticks(range(0, 24))  # Set explicit hour ticks
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_dir / 'bandwidth_usage.png')
        plt.close()

    def _create_performance_visualizations(self, df: pd.DataFrame, analysis: Dict, zone_name: str) -> None:
        """Create performance-specific visualizations."""
        output_dir = self.config.images_dir / zone_name / 'performance'
        output_dir.mkdir(parents=True, exist_ok=True)

        # Convert string hours to numeric for plotting
        hourly_data = pd.DataFrame.from_dict(
            analysis['trends']['hourly'],
            orient='index'
        )
        hourly_data.index = pd.to_numeric(hourly_data.index)
        hourly_data = hourly_data.sort_index()

        # 2. Performance Trends
        plt.figure(figsize=(12, 6))
        plt.plot(
            hourly_data.index,
            hourly_data['ttfb'],
            marker='o',
            label='TTFB',
            color='blue'
        )
        plt.plot(
            hourly_data.index,
            hourly_data['origin_time'],
            marker='o',
            label='Origin Time',
            color='green'
        )
        plt.title('Performance Trends Over Time')
        plt.xlabel('Hour of Day')
        plt.ylabel('Response Time (ms)')
        plt.legend()
        plt.xticks(range(0, 24))  # Set explicit hour ticks
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_dir / 'performance_trends.png')
        plt.close()

        # 3. Error Rate Trends
        plt.figure(figsize=(12, 6))
        plt.plot(
            hourly_data.index,
            hourly_data['error_rate'],
            'r-',
            marker='o',
            label='Error Rate'
        )
        plt.title('Error Rate Trends')
        plt.xlabel('Hour of Day')
        plt.ylabel('Error Rate (%)')
        plt.legend()
        plt.xticks(range(0, 24))  # Set explicit hour ticks
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_dir / 'error_trends.png')
        plt.close()
