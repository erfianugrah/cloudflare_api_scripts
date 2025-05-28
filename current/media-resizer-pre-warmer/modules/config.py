"""
Configuration module for the media resizer pre-warmer.
Handles command line argument parsing and configuration settings for both images and videos.
"""
import argparse
import logging
import os
import time
from datetime import datetime

def setup_logging(verbose=False):
    """
    Configure logging with appropriate level and format.
    
    Args:
        verbose: Enable verbose (DEBUG) logging if True
        
    Returns:
        logger: Configured logger instance
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create a formatter that includes thread name for concurrent operations
    log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
    
    # Configure the root logger
    logging.basicConfig(
        level=log_level,
        format=log_format
    )
    
    # Get the logger instance
    logger = logging.getLogger(__name__)
    
    # Add a file handler for debug logs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"media_transform_{timestamp}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    
    logger.info(f"Logging initialized at {log_level} level. File logs will be saved to {log_file}")
    return logger

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        args: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Media Resizer Pre-Warmer and Optimizer (Images and Videos)')
    
    # Top-level workflow options
    workflow_group = parser.add_argument_group('Workflow Options')
    workflow_group.add_argument('--force-prewarm', action='store_true', 
                              help='Force run pre-warming even if results file exists')
    workflow_group.add_argument('--use-error-report-for-load-test', action='store_true',
                              help='Use error report for load testing to exclude problematic files')
    workflow_group.add_argument('--full-workflow', action='store_true',
                              help='Run complete workflow: pre-warming → error report → load test')
    
    # Remote storage options
    parser.add_argument('--remote', help='rclone remote name')
    parser.add_argument('--bucket', help='S3 bucket name')
    parser.add_argument('--directory', default='', help='Directory path within bucket')
    parser.add_argument('--base-url', help='Base URL to prepend to object paths')
    
    # Processing options
    parser.add_argument('--media-type', choices=['video', 'image', 'auto'], default='auto',
                      help='Type of media to process (auto detects by extension)')
    parser.add_argument('--derivatives', default='desktop,tablet,mobile', help='Comma-separated list of derivatives (for videos)')
    parser.add_argument('--image-variants', default='thumbnail,small,medium,large,webp', 
                      help='Comma-separated list of image variants')
    parser.add_argument('--use-derivatives', action='store_true', help='Include derivatives in the URL path')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers')
    parser.add_argument('--timeout', type=int, default=120, help='Request timeout in seconds')
    parser.add_argument('--connection-close-delay', type=int, default=15, help='Additional delay in seconds before closing connections (applies to both pre-warmer and load test)')
    parser.add_argument('--retry', type=int, default=2, help='Number of retry attempts for failed requests')
    parser.add_argument('--use-head-for-size', action='store_true', help='Use HEAD requests to verify content sizes (reduces bandwidth usage)')
    parser.add_argument('--generate-error-report', action='store_true', help='Generate an error report from an existing results file')
    parser.add_argument('--error-report-output', default='error_report.json', help='Output file path for error report')
    parser.add_argument('--format', choices=['markdown', 'json'], help='Format for the error report (default is based on file extension)')
    
    # Output and reporting options
    parser.add_argument('--output', default='media_transform_results.json', help='Output JSON file path')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of objects to process (0 = no limit)')
    parser.add_argument('--extension', help='File extension to filter by (e.g., .mp4, .jpg, .png)')
    parser.add_argument('--image-extensions', default='.jpg,.jpeg,.png,.webp,.gif,.bmp,.svg', 
                      help='Comma-separated list of image extensions')
    parser.add_argument('--video-extensions', default='.mp4,.webm,.mov,.avi,.mkv,.m4v', 
                      help='Comma-separated list of video extensions')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    # Comparison options
    parser.add_argument('--compare', help='Path to Cloudflare KV JSON file for comparison')
    parser.add_argument('--comparison-output', default='comparison_results.json', help='Output file for comparison results')
    parser.add_argument('--summary-output', default='comparison_summary.md', help='Output file for comparison summary')
    parser.add_argument('--summary-format', default='markdown', choices=['markdown', 'json'], help='Format for the summary output (markdown or json)')
    parser.add_argument('--only-compare', action='store_true', help='Skip processing and only compare existing results with KV data')
    
    # S3 and listing options
    parser.add_argument('--use-aws-cli', action='store_true', help='Use AWS CLI instead of rclone for listing S3 objects')
    parser.add_argument('--list-files', action='store_true', help='List all files with their sizes sorted in descending order')
    parser.add_argument('--size-threshold', type=int, default=256, help='Size threshold in MiB for file size reporting (default: 256 MiB)')
    parser.add_argument('--size-report-output', default='file_size_report.md', help='Output file for size report')
    
    # Size category thresholds for optimized processing
    parser.add_argument('--small-file-threshold', type=int, default=50, 
                      help='Threshold in MiB for small files (default: 50 MiB)')
    parser.add_argument('--medium-file-threshold', type=int, default=200, 
                      help='Threshold in MiB for medium files (default: 200 MiB)')
    
    # Worker allocation control
    parser.add_argument('--small-file-workers', type=int, default=0,
                      help='Number of workers for small files (0 = auto-calculate based on total workers)')
    parser.add_argument('--medium-file-workers', type=int, default=0,
                      help='Number of workers for medium files (0 = auto-calculate based on total workers)')
    parser.add_argument('--large-file-workers', type=int, default=0,
                      help='Number of workers for large files (0 = auto-calculate based on total workers)')
    
    # Enable performance metrics and optimized processing
    parser.add_argument('--optimize-by-size', action='store_true',
                      help='Enable size-based optimization for parallel processing')
    parser.add_argument('--performance-report', default='performance_report.md',
                      help='Output file for performance analysis report')
    
    # Video optimization options
    parser.add_argument('--optimize-videos', action='store_true',
                      help='Enable video optimization using FFmpeg')
    parser.add_argument('--optimize-in-place', action='store_true',
                      help='Re-encode large video files and replace them in-place')
    parser.add_argument('--codec', choices=['h264', 'h265', 'vp9', 'vp8', 'av1'],
                      default='h264', help='Video codec to use for optimization (h264 recommended for browser compatibility)')
    parser.add_argument('--browser-compatible', type=lambda x: (str(x).lower() == 'true'), nargs='?', const=True, default=True,
                      help='Ensure browser compatibility (forces h264 codec for mp4 files). Set to False to disable.')
    parser.add_argument('--quality', choices=['maximum', 'high', 'balanced', 'efficient', 'minimum'],
                      default='balanced', help='Encoding quality profile')
    parser.add_argument('--target-resolution', choices=['4k', '1080p', '720p', '480p', '360p'],
                      default='1080p', help='Target resolution for video optimization')
    parser.add_argument('--fit', choices=['contain', 'cover', 'pad', 'stretch', 'decrease', 'crop'],
                      default='contain', help='How to fit video to target resolution: contain (preserve aspect ratio and fit entire video within frame), cover (preserve aspect ratio and fill entire frame, may crop), pad (preserve aspect ratio and add letterbox/pillarbox), stretch (ignore aspect ratio)')
    parser.add_argument('--audio-profile', choices=['high', 'medium', 'low', 'minimum'],
                      default='medium', help='Audio encoding profile')
    parser.add_argument('--output-format', choices=['mp4', 'webm', 'mkv'],
                      default='mp4', help='Output container format')
    parser.add_argument('--create-webm', action='store_true',
                      help='Also create WebM version during optimization')
    parser.add_argument('--optimized-videos-dir', default='optimized_videos',
                      help='Directory for optimized videos')
    parser.add_argument('--hardware-acceleration', choices=['auto', 'nvidia', 'intel', 'amd', 'apple', 'none'], 
                      default='auto', help='Hardware acceleration type to use (auto will detect available hardware)')
    parser.add_argument('--disable-hardware-acceleration', action='store_true',
                      help='Disable hardware acceleration even if available')
    
    # Video validation options
    validation_group = parser.add_argument_group('Video Validation Options')
    validation_group.add_argument('--validate-videos', action='store_true',
                                help='Validate video files for corruption and integrity')
    validation_group.add_argument('--validate-directory', 
                                help='Directory containing videos to validate')
    validation_group.add_argument('--validate-results', 
                                help='Path to pre-warming results JSON file to validate videos from')
    validation_group.add_argument('--validation-workers', type=int, default=10,
                                help='Number of concurrent validation workers')
    validation_group.add_argument('--validation-report', default='validation_report.md',
                                help='Output file for validation report')
    validation_group.add_argument('--validation-format', choices=['text', 'markdown', 'json'],
                                default='markdown', help='Format for validation report')
    validation_group.add_argument('--video-pattern', default='*.mp4',
                                help='File pattern to match for validation (default: *.mp4)')
    
    # k6 load testing options
    parser.add_argument('--run-load-test', action='store_true',
                      help='Run k6 load test after pre-warming')
    parser.add_argument('--k6-script', default='video-load-test-integrated-improved.js',
                      help='Path to k6 test script')
    parser.add_argument('--url-format', choices=['imwidth', 'derivative'],
                      default='imwidth', help='URL format to use for load testing')
    parser.add_argument('--debug-mode', action='store_true',
                      help='Enable debug mode for load testing')
    parser.add_argument('--use-head-requests', action='store_true', default=True,
                      help='Use HEAD requests to get content length')
    parser.add_argument('--no-head-requests', action='store_false', dest='use_head_requests',
                      help='Disable HEAD requests')
    parser.add_argument('--skip-large-files', action='store_true', default=True,
                      help='Skip large files in load test')
    parser.add_argument('--no-skip-large-files', action='store_false', dest='skip_large_files',
                      help='Test all files regardless of size')
    parser.add_argument('--large-file-threshold-mib', type=int, default=256,
                      help='Threshold in MiB for skipping large files (default: 256 MiB)')
    parser.add_argument('--request-timeout', default='120s',
                      help='Timeout for individual requests in load test')
    parser.add_argument('--head-timeout', default='30s',
                      help='Timeout for HEAD requests in load test')
    parser.add_argument('--global-timeout', default='90s',
                      help='Global timeout for the load test')
    parser.add_argument('--failure-rate-threshold', default='0.05',
                      help='Maximum acceptable failure rate (e.g. 0.05 = 5%%)')
    parser.add_argument('--max-retries', type=int, default=2,
                      help='Maximum number of retry attempts for failed requests')

    # Stage configuration for load test
    parser.add_argument('--stage1-users', type=int, default=10,
                      help='Number of users in stage 1')
    parser.add_argument('--stage1-duration', default='30s',
                      help='Duration of stage 1')
    parser.add_argument('--stage2-users', type=int, default=20,
                      help='Number of users in stage 2')
    parser.add_argument('--stage2-duration', default='1m',
                      help='Duration of stage 2')
    parser.add_argument('--stage3-users', type=int, default=30,
                      help='Number of users in stage 3')
    parser.add_argument('--stage3-duration', default='30s',
                      help='Duration of stage 3')
    parser.add_argument('--stage4-users', type=int, default=20,
                      help='Number of users in stage 4')
    parser.add_argument('--stage4-duration', default='1m',
                      help='Duration of stage 4')
    parser.add_argument('--stage5-users', type=int, default=0,
                      help='Number of users in stage 5')
    parser.add_argument('--stage5-duration', default='30s',
                      help='Duration of stage 5')
    
    return parser.parse_args()

# File size category definitions
def get_size_category(size_bytes, small_threshold_mib=50, medium_threshold_mib=200):
    """
    Determine the size category of a file based on its size.
    
    Args:
        size_bytes: Size of the file in bytes
        small_threshold_mib: Threshold for small files in MiB (default: 50 MiB)
        medium_threshold_mib: Threshold for medium files in MiB (default: 200 MiB)
        
    Returns:
        String: 'small', 'medium', or 'large'
    """
    small_threshold = small_threshold_mib * 1024 * 1024  # Convert MiB to bytes
    medium_threshold = medium_threshold_mib * 1024 * 1024  # Convert MiB to bytes
    
    if size_bytes < small_threshold:
        return 'small'
    elif size_bytes < medium_threshold:
        return 'medium'
    else:
        return 'large'

class FileMetadata:
    """
    Class to store and manage file metadata for optimized processing.
    """
    def __init__(self, path, size_bytes, small_threshold_mib=50, medium_threshold_mib=200):
        """
        Initialize file metadata.
        
        Args:
            path: File path relative to bucket
            size_bytes: Size of the file in bytes
            small_threshold_mib: Threshold for small files in MiB
            medium_threshold_mib: Threshold for medium files in MiB
        """
        self.path = path
        self.size_bytes = size_bytes
        self.size_category = get_size_category(size_bytes, small_threshold_mib, medium_threshold_mib)
        self.processing_started = None
        self.processing_completed = None
        self.processing_duration = None
        self.derivatives = {}  # Will store derivative-specific timing data
        
    def start_processing(self):
        """Mark the start time of processing."""
        self.processing_started = time.time()
        
    def complete_processing(self):
        """Mark the completion time and calculate duration."""
        self.processing_completed = time.time()
        if self.processing_started:
            self.processing_duration = self.processing_completed - self.processing_started
    
    def start_derivative_processing(self, derivative):
        """Mark the start time of processing a specific derivative."""
        if derivative not in self.derivatives:
            self.derivatives[derivative] = {}
        self.derivatives[derivative]['started'] = time.time()
        
    def complete_derivative_processing(self, derivative):
        """Mark the completion time for a derivative and calculate duration."""
        if derivative in self.derivatives and 'started' in self.derivatives[derivative]:
            self.derivatives[derivative]['completed'] = time.time()
            self.derivatives[derivative]['duration'] = (
                self.derivatives[derivative]['completed'] - self.derivatives[derivative]['started']
            )
    
    def to_dict(self):
        """Convert metadata to dictionary for serialization."""
        return {
            'path': self.path,
            'size_bytes': self.size_bytes,
            'size_mib': self.size_bytes / (1024 * 1024),
            'size_category': self.size_category,
            'processing_started': self.processing_started,
            'processing_completed': self.processing_completed,
            'processing_duration': self.processing_duration,
            'derivatives': self.derivatives
        }