import os
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

def setup_logging():
    """Configure logging settings."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('cloudflare_analytics.log')
        ]
    )
    return logging.getLogger(__name__)

def setup_plotting():
    """Configure matplotlib and seaborn settings."""
    plt.style.use('bmh')
    sns.set(style="darkgrid", palette="husl")
    plt.rcParams['figure.figsize'] = [12, 6]
    plt.rcParams['figure.dpi'] = 100
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.titlesize'] = 12
    plt.rcParams['axes.labelsize'] = 10
    plt.rcParams['xtick.labelsize'] = 9
    plt.rcParams['ytick.labelsize'] = 9

class Config:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        self.account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
        self.api_key = os.getenv('CLOUDFLARE_API_KEY')
        self.email = os.getenv('CLOUDFLARE_EMAIL')
        
        if not all([self.account_id, self.api_key, self.email]):
            raise ValueError("Missing required environment variables")
        
        self.base_url = "https://api.cloudflare.com/client/v4"
        self.headers = {
            'X-Auth-Key': self.api_key,
            'X-Auth-Email': self.email,
            'Content-Type': 'application/json'
        }
        
        self.reports_dir = Path('reports')
        self.json_dir = self.reports_dir / 'json'
        self.images_dir = self.reports_dir / 'images'
        self.logs_dir = self.reports_dir / 'logs'
        
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories."""
        for directory in [self.reports_dir, self.json_dir, self.images_dir, self.logs_dir]:
            directory.mkdir(exist_ok=True, parents=True)
