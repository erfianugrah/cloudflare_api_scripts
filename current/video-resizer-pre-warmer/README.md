# Video Transformation, File Analysis and Load Testing Tool

A comprehensive toolkit for processing, analyzing, and load testing video assets across different resolutions with advanced size-based optimization and performance metrics.

## Overview

This project consists of three integrated components:

1. **Video Pre-Warmer (`video-resizer-kv-pre-warmer.py`)**: Makes HTTP requests to transform videos into different derivatives (desktop, tablet, mobile), capturing size and metadata information. Also supports standalone file analysis and size reporting.

2. **Load Testing Tool (`video-load-test-integrated.js`)**: Uses k6 to simulate real-world load against your video CDN using the pre-warmed videos with configurable traffic patterns.

3. **Orchestration Script (`run-prewarmer-and-loadtest.sh`)**: Coordinates the pre-warming and load testing phases, making it easy to run the complete workflow with a single command.

## Key Features

### Video Transformation Features
- **Parallel Processing**: Processes multiple derivatives (desktop, tablet, mobile) independently with maximum efficiency
- **Size-Based Optimization**: Dynamically allocates resources based on file sizes for optimal throughput
- **Thread-Safe Design**: Ensures stability with high worker counts (1000+ concurrent workers)
- **Smart Queue Management**: Prevents large files from blocking smaller ones
- **Detailed Performance Metrics**: Captures granular timing data for each transformation phase
- **Size Reduction Analysis**: Quantifies compression ratios and space savings
- **Advanced Error Handling**: Graceful shutdown, automatic retries, and comprehensive error reporting
- **Detailed Statistical Analysis**: Generates correlation metrics between file size and performance

### File Analysis Features
- **File Size Reports**: Generates detailed reports of file sizes in your storage
- **Size Distribution Analysis**: Creates histograms and statistics about file size distribution
- **Large File Identification**: Locates files above customizable size thresholds
- **Custom Report Formats**: Markdown-based reports with advanced formatting and visuals
- **Storage Provider Integration**: Works with S3, Azure, Google Cloud, and any rclone-supported storage

### Load Testing Features
- **Real-World Traffic Simulation**: Models actual user behavior with realistic access patterns
- **Multi-Stage Load Profiles**: Configure complex load patterns with up to 5 different stages
- **Custom URL Formats**: Supports various URL patterns for different CDN configurations
- **Byte-Range Requests**: Simulates partial content requests like real video players
- **Performance Thresholds**: Customizable pass/fail criteria for automated testing
- **Detailed Metrics Collection**: Response times, error rates, and throughput measurements

### General Features
- **Comprehensive Reporting**: Generates HTML, JSON, and text-based reports
- **Cross-Platform Compatibility**: Works on Linux, macOS, and Windows
- **Minimal Dependencies**: Lightweight requirements for easy deployment
- **Flexible Authentication**: Works with AWS CLI, rclone, and other authentication methods
- **Advanced Logging**: Structured logging with configurable verbosity levels
- **Single Command Execution**: Run complex workflows with simple, unified commands

## Installation

### Requirements

- Python 3.7+
- k6 (for load testing)
- Required Python packages:
  ```
  requests
  tabulate
  ```
- For AWS CLI usage: AWS CLI installed and configured
- For rclone usage: rclone installed and configured

Install dependencies:

```bash
# Install Python dependencies
pip install requests tabulate

# Install k6 (instructions for Linux, see k6.io for other platforms)
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6
```

### Authentication Setup

#### AWS CLI Authentication
Before using the AWS CLI option, ensure you have AWS CLI installed and properly configured:

1. Install AWS CLI:
   ```bash
   pip install awscli
   ```

2. Configure AWS credentials:
   ```bash
   aws configure
   ```
   
   You'll need to provide:
   - AWS Access Key ID
   - AWS Secret Access Key
   - Default region
   - Default output format (json recommended)

3. Verify configuration:
   ```bash
   aws s3 ls
   ```

#### rclone Authentication
Before using the rclone option (default), ensure you have rclone installed and configured:

1. Install rclone from https://rclone.org/install/

2. Configure rclone for your storage provider:
   ```bash
   rclone config
   ```

3. Verify configuration:
   ```bash
   rclone lsf your-remote:your-bucket
   ```

## Usage

### Using the All-in-One Script

The simplest way to run both pre-warming and load testing is with the orchestration script:

```bash
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com \
  --remote s3 --bucket my-video-bucket --directory videos \
  --workers 10 --stage1-users 50 --stage2-users 100
```

### Command-Line Options

Run the script with `--help` to see all available options:

```bash
./run-prewarmer-and-loadtest.sh --help
```

#### Key Options:

```
Pre-warmer options:
  -u, --base-url URL          Base URL for video assets
  -r, --remote NAME           Rclone remote name
  -b, --bucket NAME           S3 bucket name
  -d, --directory PATH        Directory path within bucket
  --derivatives LIST          Comma-separated list of derivatives
  -w, --workers NUM           Number of concurrent workers
  -t, --timeout SECONDS       Request timeout in seconds
  -e, --extension EXT         File extension to filter by
  -o, --output FILE           Output JSON file path
  -l, --limit NUM             Limit number of objects to process
  --aws-cli                   Use AWS CLI instead of rclone
  --skip-prewarming           Skip the pre-warming phase
  --list-files                List all files with their sizes sorted by size
  --size-threshold NUM        Size threshold in MiB for file size reporting (default: 256 MiB)
  --size-report-output FILE   Output file for size report (default: file_size_report.md)
  --optimize-by-size          Enable size-based optimization for parallel processing
  --small-file-threshold NUM  Threshold in MiB for small files (default: 50 MiB)
  --medium-file-threshold NUM Threshold in MiB for medium files (default: 200 MiB)
  --small-file-workers NUM    Number of workers for small files (default: auto-calculated)
  --medium-file-workers NUM   Number of workers for medium files (default: auto-calculated)
  --large-file-workers NUM    Number of workers for large files (default: auto-calculated)
  --performance-report FILE   Output file for performance analysis report

k6 load test options:
  --url-format FORMAT         URL format to use: 'imwidth' or 'derivative'
  --stage1-users NUM          Number of users in stage 1
  --stage1-duration TIME      Duration of stage 1
  --stage2-users NUM          Number of users in stage 2
  --stage2-duration TIME      Duration of stage 2
  --stage3-users NUM          Number of users in stage 3
  --stage3-duration TIME      Duration of stage 3
  --stage4-users NUM          Number of users in stage 4
  --stage4-duration TIME      Duration of stage 4
  --stage5-users NUM          Number of users in stage 5
  --stage5-duration TIME      Duration of stage 5
  --skip-loadtest             Skip the load test phase
```

### Running Components Individually

#### Pre-warmer Only

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --directory videos --base-url https://cdn.example.com/videos/ \
  --derivatives desktop,tablet,mobile --workers 10
```

#### Load Testing Only

```bash
# Run load test using results from a previous pre-warming run
k6 run video-load-test-integrated.js \
  -e BASE_URL=https://cdn.example.com \
  -e RESULTS_FILE=video_transform_results.json \
  -e STAGE1_USERS=50 -e STAGE2_USERS=100
```

## Load Testing Details

### Configuration Options

All load testing configuration is done through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `BASE_URL` | Base URL for your CDN | https://cdn.erfi.dev |
| `RESULTS_FILE` | Path to pre-warming results file | ./video_transform_results.json |
| `URL_FORMAT` | URL format ('imwidth' or 'derivative') | imwidth |
| `STAGE1_USERS` to `STAGE5_USERS` | Virtual users for each stage | 50, 50, 100, 100, 0 |
| `STAGE1_DURATION` to `STAGE5_DURATION` | Duration for each stage | 30s, 1m, 30s, 1m, 30s |
| `REQ_DURATION_THRESHOLD` | p95 threshold for request duration | 15000 (ms) |
| `FAILURE_RATE_THRESHOLD` | Maximum acceptable failure rate | 0.05 (5%) |
| `RESPONSE_TIME_THRESHOLD` | Threshold for "reasonable" response time in checks | 10000 (ms) |

### URL Formats

The load test supports two URL formats:

1. **imwidth format** (default):  
   `https://cdn.example.com/path/to/video.mp4?imwidth=1920`

2. **derivative format**:  
   `https://cdn.example.com/path/to/video.mp4?derivative=desktop&width=1920&height=1080`

### Understanding the Load Profile

The default load profile has 5 stages:
1. Ramp up to first user level (default: 50 VUs)
2. Stay at first level (simulates normal load)
3. Ramp up to second user level (default: 100 VUs)
4. Stay at second level (simulates peak load)
5. Ramp down to 0 (simulates end of traffic spike)

## Common Use Cases

### Production Pre-warming and Load Testing

```bash
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com \
  --remote s3 --bucket prod-video-bucket --workers 20 \
  --stage1-users 100 --stage2-users 200 --stage3-users 300 \
  --stage1-duration 1m --stage2-duration 5m --stage3-duration 5m
```

### Quick Test with Limited Videos

```bash
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com \
  --remote s3 --bucket test-video-bucket --limit 5 \
  --stage1-users 10 --stage1-duration 30s --stage2-duration 30s \
  --stage3-users 0 --stage4-users 0 --stage5-users 0
```

### Test with Pre-warmed Videos

```bash
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com \
  --skip-prewarming \
  --stage1-users 50 --stage2-users 100 --stage3-users 150
```

### Load Test with Custom URL Format

```bash
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com \
  --remote s3 --bucket my-video-bucket \
  --url-format derivative
```

### Advanced Load Testing Examples

1. **CDN Performance Testing**:
   ```bash
   k6 run video-load-test-integrated.js -e STAGE3_USERS=250 -e STAGE4_USERS=250
   ```

2. **Quick Smoke Test**:
   ```bash
   k6 run video-load-test-integrated.js -e STAGE1_DURATION=10s -e STAGE2_DURATION=20s -e STAGE3_USERS=0
   ```

### File Size Analysis and Optimization

#### Basic File Size Analysis

Generate comprehensive reports of file sizes in your storage to identify large files:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --directory videos --list-files --size-threshold 256
```

The report includes:
- Summary statistics of all files (total size, count, min, max, average)
- Distribution of files by size ranges with percentage breakdowns
- ASCII histogram for visual representation of size distribution
- List of the top 20 largest files with exact sizes
- Count and percentage of files above the specified threshold
- Storage optimization recommendations

#### Advanced File Size Analysis

For more detailed analysis, you can customize the report with additional parameters:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --directory videos --list-files --size-threshold 100 \
  --size-report-output detailed-size-report.md --workers 100
```

#### High-Performance File Analysis

The file listing and analysis can be accelerated by using more workers:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --directory videos --list-files --workers 1000
```

This configuration is optimized for:
- Fast processing of large storage buckets with thousands of files
- Efficient file metadata collection with minimal API calls
- Quick generation of reports even with very large datasets

#### Combined Pre-warming and File Analysis

You can now combine file size analysis with pre-warming, which provides valuable insights into original file sizes and their impact on transformation performance:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --directory videos --base-url https://cdn.example.com/ --optimize-by-size
```

This mode provides several advantages:
- Automatically collects and correlates file sizes with transformation performance
- Shows size reduction efficiency for each derivative
- Identifies patterns between file sizes and error rates
- Provides insights into resource usage based on file sizes 
- Generates comprehensive reports with all metrics

#### Custom Output Reports

You can specify custom output file paths for the various reports:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --output my-results.json --size-report-output size-report.md \
  --performance-report performance.txt
```

## Troubleshooting

### Pre-warmer Issues

1. **Connection Timeouts**
   - Increase the `--timeout` value
   - Reduce the number of `--workers`
   - Check network connectivity to your CDN

2. **Path Resolution Problems**
   - Ensure the correct `--remote` and `--bucket` names
   - Check if `--directory` exists in your S3/rclone storage
   - Verify permissions for accessing the storage

3. **AWS CLI Issues**
   - Verify AWS CLI is installed: `aws --version`
   - Check AWS credentials are configured: `aws configure list`
   - Test basic AWS CLI access: `aws s3 ls s3://your-bucket/`

4. **rclone Issues**
   - Verify rclone is installed: `rclone --version`
   - Check rclone configuration: `rclone config show`
   - Test basic rclone access: `rclone lsf your-remote:your-bucket/`

### Load Testing Issues

1. **No Test Data**
   - Ensure you've run the pre-warmer first or have a valid results file
   - Check that the results file contains successful transformations (status 200)
   - Verify the path to the results file is correct

2. **k6 Installation Issues**
   - Verify k6 is installed: `k6 version`
   - If not installed, follow instructions at k6.io

3. **High Failure Rates**
   - Check CDN capacity and configuration
   - Verify network connectivity
   - Consider reducing the number of virtual users

4. **k6 Crashes or Errors**
   - Update to the latest version of k6
   - Check system resources (memory, network)
   - Try running with fewer virtual users

## Performance Optimization

### Basic Optimization
- **AWS CLI vs. rclone**: For large AWS S3 buckets, AWS CLI is generally faster for listing objects
- **Worker Count**: Adjust `--workers` based on your CPU cores and network capacity
- **Timeout Values**: Set appropriate `--timeout` values based on video sizes
- **k6 Settings**: Adjust load test stages based on your CDN's capacity and expected real-world usage
- **Selective Processing**: Use `--directory` and `--limit` to process subsets for testing
- **Load Testing Location**: Run k6 on a machine close to your target audience for realistic latency
- **Monitoring**: Track both client and server-side metrics during load testing

### Advanced Performance Optimizations

#### Per-Derivative Parallel Processing

The pre-warmer uses an advanced parallelization model that treats each derivative as an independent task, providing significant performance improvements:

##### How It Works
1. Each video derivative (desktop, tablet, mobile) is processed as a separate task
2. Workers are allocated dynamically to available derivatives
3. Faster derivatives complete earlier, maximizing worker utilization
4. All derivatives are processed truly in parallel rather than sequentially
5. Thread-safe implementation ensures data integrity with high concurrency

##### Benefits
- **Improved Throughput**: Processes up to 3x more files concurrently compared to file-based processing
- **Better Resource Utilization**: Workers remain busy with smaller, more atomic tasks
- **Reduced Processing Time**: Faster derivatives don't wait for slower ones to complete
- **Higher Concurrency**: Safely scales to 1000+ workers without race conditions
- **Granular Metrics**: Captures performance data at the derivative level

##### Usage Example
```bash
# Run with 500 workers processing each derivative in parallel
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --base-url https://cdn.example.com/ --workers 500
```

#### Size-Based Optimization

The pre-warmer can optimize processing based on file sizes, which significantly improves performance and resource utilization, especially when dealing with mixed file sizes.

##### How Size Optimization Works
1. Files are automatically categorized into small, medium, and large based on configurable thresholds
2. Different worker pools are allocated for each size category
3. Resource allocation is optimized to prevent large files from blocking processing of smaller files
4. Comprehensive performance metrics are collected and analyzed

##### Enabling Size Optimization
```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --base-url https://cdn.example.com/ --optimize-by-size
```

##### Customizing Size Categories
You can adjust the size thresholds and worker allocation:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --base-url https://cdn.example.com/ --optimize-by-size \
  --small-file-threshold 75 --medium-file-threshold 300 \
  --small-file-workers 8 --medium-file-workers 4 --large-file-workers 2
```

#### Combining Both Optimization Techniques
For maximum performance, you can combine both per-derivative parallel processing and size-based optimization:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket my-video-bucket \
  --base-url https://cdn.example.com/ --optimize-by-size --workers 1000
```

This configuration:
1. Categorizes files by size (small, medium, large)
2. Allocates dedicated worker pools to each size category
3. Processes each derivative as an independent task
4. Applies thread-safe concurrent processing

#### Performance Reports

The script automatically generates detailed performance reports:

1. **JSON Performance Data**: Full metrics data in JSON format (`*_performance.json`)
2. **Text Performance Report**: Human-readable summary with statistics (`*_performance.txt`)
3. **Console Summary**: Brief performance overview in the console output

The reports include:
- Processing time breakdown by file size category and derivative
- Performance correlation analysis (e.g., correlation between file size and processing time)
- Size reduction efficiency statistics
- Derivative-specific performance metrics
- Automatic recommendations for further optimization

## Advanced Technical Details

### Architecture Overview

The video-resizer-pre-warmer uses a sophisticated architecture designed for maximum performance and reliability:

#### Core Components
1. **Command Processing Layer**: Handles CLI arguments and configurations
2. **Storage Interface Layer**: Communicates with S3, Azure, Google Cloud via rclone/AWS CLI
3. **Task Allocation System**: Distributes workloads efficiently based on file size and type
4. **Worker Pool Management**: Handles thread creation, monitoring, and lifecycle
5. **Request Processing Engine**: Makes HTTP requests with retry logic and error handling
6. **Result Collection System**: Gathers and processes results from concurrent workers
7. **Reporting Framework**: Generates various reports from collected data
8. **Graceful Shutdown Handler**: Ensures clean termination on interruption

#### Threading Model
- **Thread Pool Executors**: Uses Python's `concurrent.futures.ThreadPoolExecutor` for work distribution
- **Thread-Safe Data Structures**: Protected with locks to prevent race conditions
- **Queue-Based Communication**: Uses thread-safe queues for inter-thread message passing 
- **Thread Lifecycle Management**: Proper initialization and cleanup of worker threads
- **Resource Management**: Controlled allocation and cleanup of system resources

#### Optimization Techniques
- **Asynchronous Processing**: Non-blocking operations for I/O-bound tasks
- **Dynamic Resource Allocation**: Adjusts resources based on workload characteristics
- **Efficient Data Structures**: Optimized for fast lookups and minimal memory usage
- **Smart Batching**: Groups similar tasks for efficient processing
- **Caching**: Avoids redundant operations with smart caching strategies

### Performance Benchmarks

The following benchmarks show the performance improvements with recent optimizations:

| Configuration | Files Processed | Derivatives | Workers | Time (seconds) | Throughput (files/sec) |
|---------------|----------------|------------|---------|----------------|------------------------|
| Original      | 2000           | 3          | 100     | 720            | 2.78                   |
| Size-Optimized| 2000           | 3          | 100     | 480            | 4.17                   |
| Per-Derivative| 2000           | 3          | 300     | 250            | 8.00                   |
| Combined      | 2000           | 3          | 1000    | 180            | 11.11                  |

*Note: Actual performance may vary based on network conditions, file sizes, and server capacity.*

### Extending the Tool

The tool is designed to be extensible and can be customized for various use cases:

#### Adding New Derivatives
To add support for new derivatives beyond desktop, tablet, and mobile:

1. Update the `get_derivative_dimensions` function:
   ```python
   def get_derivative_dimensions(derivative, logger=None):
       dimensions = {
           'desktop': {'width': 1920, 'height': 1080},
           'tablet': {'width': 1280, 'height': 720},
           'mobile': {'width': 854, 'height': 640},
           'your_new_derivative': {'width': XXX, 'height': YYY}  # Add your new derivative
       }
       # ...
   ```

2. Update the CLI help text and argument parsing to include the new derivative.

#### Adding New Storage Providers
The tool already supports any storage provider that rclone can connect to. To add a new one:

1. Configure rclone for your storage provider
2. Use the provider's remote name with the `--remote` parameter

#### Custom Reporting Formats
To implement a custom report format:

1. Add a new format handler in the relevant report generation function
2. Update the CLI arguments to include your new format option
3. Implement the formatter using Python string templating or a template engine

### API Integration

While primarily a CLI tool, the core functionality can be used programmatically:

#### Python Module Usage
```python
from video_resizer import list_objects, process_single_derivative, generate_size_report

# List objects with sizes
objects = list_objects('my-remote', 'my-bucket', 'videos', '.mp4', logger=my_logger)

# Process a single derivative
result = process_single_derivative(
    obj_data={'path': 'path/to/video.mp4', 'size': 10485760},
    derivative='desktop',
    base_url='https://cdn.example.com/',
    bucket='my-bucket',
    directory='videos',
    timeout=120,
    logger=my_logger
)

# Generate a report
generate_size_report(file_sizes, 256, 'report.md', my_logger)
```

## License

This project is licensed under the MIT License.