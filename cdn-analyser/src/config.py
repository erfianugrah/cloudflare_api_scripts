import os
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
from typing import Dict, Optional
from datetime import datetime, timedelta

def setup_logging() -> logging.Logger:
    """Configure logging settings."""
    # Create logs directory if it doesn't exist
    log_dir = Path('logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'cloudflare_analytics_{timestamp}.log'
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )
    
    # Create logger
    logger = logging.getLogger(__name__)
    
    # Log startup information
    logger.info(f"Logging initialized. Log file: {log_file}")
    logger.info(f"Python logger level: {logging.getLevelName(logger.getEffectiveLevel())}")
    
    return logger

def setup_plotting() -> None:
    """Configure matplotlib and seaborn settings."""
    # Set style
    plt.style.use('bmh')
    sns.set(style="darkgrid", palette="husl")
    
    # Configure plot settings
    plt.rcParams.update({
        'figure.figsize': [12, 6],
        'figure.dpi': 100,
        'savefig.dpi': 300,
        'font.size': 10,
        'axes.titlesize': 12,
        'axes.labelsize': 10,
        'xtick.labelsize': 9,
        'ytick.labelsize': 9,
        'lines.linewidth': 1.5,
        'grid.linestyle': '--',
        'grid.alpha': 0.7,
        'axes.grid': True,
        'axes.facecolor': '#f0f0f0',
        'figure.facecolor': 'white',
        'axes.spines.top': False,
        'axes.spines.right': False
    })

class Config:
    """Configuration management for Cloudflare Analytics."""
    
    def __init__(self):
        """Initialize configuration with environment variables and defaults."""
        # Load environment variables
        load_dotenv()
        
        # Auth configuration
        self.auth_type = os.getenv('CLOUDFLARE_AUTH_TYPE', 'api_key').lower()
        self.account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        self.api_key = os.getenv('CLOUDFLARE_API_KEY')
        self.email = os.getenv('CLOUDFLARE_EMAIL')
        self.bearer_token = os.getenv('CLOUDFLARE_BEARER_TOKEN')
        
        # Validate auth configuration
        self._validate_auth_config()
        
        # API configuration
        self.base_url = "https://api.cloudflare.com/client/v4"
        self.headers = self._get_auth_headers()
        
        # Directory configuration
        self.setup_directories()
        
        # Analysis configuration
        self.setup_analysis_config()
        
        # Logging configuration
        self.setup_logging_config()

    def _validate_auth_config(self) -> None:
        """Validate authentication configuration."""
        if not self.account_id:
            raise ValueError("Missing required CLOUDFLARE_ACCOUNT_ID")
            
        if self.auth_type == 'api_key':
            if not all([self.api_key, self.email]):
                raise ValueError("API Key authentication requires CLOUDFLARE_API_KEY and CLOUDFLARE_EMAIL")
        elif self.auth_type == 'bearer':
            if not self.bearer_token:
                raise ValueError("Bearer token authentication requires CLOUDFLARE_BEARER_TOKEN")
        else:
            raise ValueError("CLOUDFLARE_AUTH_TYPE must be either 'api_key' or 'bearer'")

    def _get_auth_headers(self) -> Dict[str, str]:
        """Get appropriate authentication headers based on auth type."""
        headers = {'Content-Type': 'application/json'}
        
        if self.auth_type == 'api_key':
            headers.update({
                'X-Auth-Key': self.api_key,
                'X-Auth-Email': self.email
            })
        else:  # bearer token
            headers['Authorization'] = f'Bearer {self.bearer_token}'
            
        return headers

    def setup_directories(self) -> None:
        """Create and configure necessary directories."""
        # Base directories
        self.base_dir = Path(__file__).parent.parent
        self.reports_dir = self.base_dir / 'reports'
        
        # Subdirectories
        self.json_dir = self.reports_dir / 'json'
        self.images_dir = self.reports_dir / 'images'
        self.logs_dir = self.reports_dir / 'logs'
        self.cache_dir = self.reports_dir / 'cache'
        
        # Create all directories
        for directory in [self.reports_dir, self.json_dir, self.images_dir, 
                         self.logs_dir, self.cache_dir]:
            directory.mkdir(exist_ok=True, parents=True)

    def setup_analysis_config(self) -> None:
        """Configure analysis parameters."""
        # Time windows
        self.default_time_window = timedelta(hours=24)
        self.max_time_window = timedelta(days=30)
        self.min_time_window = timedelta(minutes=5)
        
        # Sampling configuration
        self.min_sample_interval = 1
        self.max_sample_interval = 1440  # 24 hours in minutes
        self.default_sample_interval = 5
        
        # Performance thresholds
        self.performance_thresholds = {
            'ttfb': {
                'good': 100,     # ms
                'warning': 300,  # ms
                'critical': 1000 # ms
            },
            'error_rate': {
                'good': 0.1,    # %
                'warning': 1.0,  # %
                'critical': 5.0  # %
            },
            'cache_hit_ratio': {
                'good': 85,     # %
                'warning': 70,   # %
                'critical': 50   # %
            }
        }
        
        # Rate limiting
        self.rate_limit_config = {
            'max_requests_per_minute': 100,
            'max_requests_per_hour': 1000,
            'retry_after': 60,  # seconds
            'max_retries': 3
        }

    def setup_logging_config(self) -> None:
        """Configure logging settings."""
        self.logging_config = {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'date_format': '%Y-%m-%d %H:%M:%S',
            'file_rotation': {
                'when': 'midnight',
                'interval': 1,
                'backupCount': 7
            }
        }

    def get_cache_file(self, zone_id: str, date: Optional[datetime] = None) -> Path:
        """Get cache file path for a specific zone and date."""
        if date is None:
            date = datetime.now()
        
        date_str = date.strftime('%Y%m%d')
        return self.cache_dir / f"{zone_id}_{date_str}.cache"

    def get_report_file(self, zone_id: str, report_type: str) -> Path:
        """Get report file path."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return self.reports_dir / f"{zone_id}_{report_type}_{timestamp}.json"

    def get_image_dir(self, zone_name: str) -> Path:
        """Get image directory for a specific zone."""
        zone_images = self.images_dir / zone_name
        zone_images.mkdir(exist_ok=True, parents=True)
        return zone_images

    def validate_time_range(self, start_time: datetime, end_time: datetime) -> bool:
        """Validate time range against configuration limits."""
        duration = end_time - start_time
        
        if duration > self.max_time_window:
            raise ValueError(f"Time range exceeds maximum of {self.max_time_window}")
        
        if duration < self.min_time_window:
            raise ValueError(f"Time range must be at least {self.min_time_window}")
        
        return True

    def validate_sample_interval(self, interval: int) -> bool:
        """Validate sample interval against configuration limits."""
        if interval < self.min_sample_interval:
            raise ValueError(f"Sample interval must be at least {self.min_sample_interval} minute")
        
        if interval > self.max_sample_interval:
            raise ValueError(f"Sample interval cannot exceed {self.max_sample_interval} minutes")
        
        return True

    def should_use_cache(self, zone_id: str, date: datetime) -> bool:
        """Determine if cache should be used for given zone and date."""
        cache_file = self.get_cache_file(zone_id, date)
        
        if not cache_file.exists():
            return False
        
        # Check if cache is from today
        cache_date = datetime.fromtimestamp(cache_file.stat().st_mtime)
        return cache_date.date() == date.date()

    def get_performance_threshold(self, metric: str, value: float) -> str:
        """Get threshold category for a metric value."""
        thresholds = self.performance_thresholds.get(metric)
        if not thresholds:
            return 'unknown'
            
        if value <= thresholds['good']:
            return 'good'
        elif value <= thresholds['warning']:
            return 'warning'
        else:
            return 'critical'

    def __str__(self) -> str:
        """String representation of configuration."""
        return (
            f"Cloudflare Analytics Configuration:\n"
            f"- Authentication Type: {self.auth_type}\n"
            f"- Account ID: {self.account_id}\n"
            f"- Base URL: {self.base_url}\n"
            f"- Reports Directory: {self.reports_dir}\n"
            f"- Default Time Window: {self.default_time_window}\n"
            f"- Default Sample Interval: {self.default_sample_interval} minutes"
        )

    def __repr__(self) -> str:
        """Detailed string representation of configuration."""
        return (
            f"Config("
            f"auth_type='{self.auth_type}', "
            f"account_id='{self.account_id}', "
            f"base_url='{self.base_url}', "
            f"reports_dir='{self.reports_dir}'"
            f")"
        )
