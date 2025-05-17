# Video Transformation, Analysis and Optimization Toolkit

A comprehensive toolkit for processing, analyzing, optimizing, and load testing video assets across different resolutions with advanced size-based optimization and performance metrics.

## Overview

This project consists of several integrated components:

1. **Video Pre-Warmer (`video-resizer-kv-pre-warmer.py`)**: Makes HTTP requests to transform videos into different derivatives (desktop, tablet, mobile), capturing size and metadata information.

2. **Video Optimizer**: Re-encodes large video files using FFmpeg with support for multiple codecs and quality profiles to reduce file size while maintaining acceptable quality.

3. **File Analysis Tool**: Generates detailed reports of file sizes, distribution analysis, and identifies large files for optimization.

4. **Load Testing Tool (`video-load-test-integrated.js`)**: Uses k6 to simulate real-world load against your video CDN using the pre-warmed videos with configurable traffic patterns.

5. **Orchestration Script (`run-prewarmer-and-loadtest.sh`)**: Coordinates the pre-warming and load testing phases, making it easy to run the complete workflow with a single command.

## Module Structure

The project has been refactored into a modular architecture for improved maintainability:

- `main.py`: Main entry point for the application
- `modules/`: Directory containing modular components
  - `__init__.py`: Package initialization
  - `config.py`: Configuration and argument parsing
  - `storage.py`: Remote storage access functions (rclone/S3)
  - `video_utils.py`: Video file analysis and utility functions
  - `encoding.py`: Video encoding parameters and optimization
  - `processing.py`: Core URL processing functionality
  - `reporting.py`: Report generation and statistics
  - `comparison.py`: Comparison analysis between original and optimized files

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

### Video Optimization Features
- **Re-encode Large Videos**: Automatically identifies and re-encodes large video files
- **Multi-Codec Support**: H.264, H.265/HEVC, VP9, VP8, AV1
- **Quality Profiles**: Options from maximum quality to minimum size
- **Multi-Format Support**: MP4, WebM, MKV, and MOV containers
- **Resolution Options**: 4K, 1080p, 720p, 480p, 360p
- **Audio Optimization**: Multiple audio encoding profiles
- **Parallel Encoding**: CPU-aware parallelization for batch processing
- **WebM Generation**: Optional creation of WebM versions alongside primary format

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
- **Random Request Patterns**: Simulates real user access with randomized byte range requests

## Installation

### Requirements

- Python 3.7+
- FFmpeg 4.0+ (for video optimization)
- k6 (for load testing)
- Required Python packages:
  ```
  requests
  tabulate
  numpy
  ```
- For AWS CLI usage: AWS CLI installed and configured
- For rclone usage: rclone installed and configured

### Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install FFmpeg (Ubuntu/Debian example)
sudo apt update
sudo apt install ffmpeg

# Install k6 (instructions for Linux, see k6.io for other platforms)
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6
```

### Authentication Setup

#### rclone Authentication
Before using the rclone option (default), ensure you have rclone installed and configured:

1. Install rclone from https://rclone.org/install/

2. Configure rclone for your storage provider:
   ```bash
   rclone config
   ```

3. Configure Cloudflare R2 in rclone:
   ```
   # Create a new remote with the following details
   Name: r2 (or any name you prefer)
   Type: s3 (R2 uses S3-compatible API)
   Provider: Cloudflare R2
   Access Key ID: YOUR_ACCESS_KEY
   Secret Access Key: YOUR_SECRET_KEY
   Endpoint: https://ACCOUNT_ID.r2.cloudflarestorage.com
   ```

4. Verify configuration:
   ```bash
   rclone lsf r2:videos
   ```

## Usage

### Running the Application

You can run the application using either:

```bash
# Using the main script directly
python3 main.py [options]

# Or using the run.sh wrapper
./run.sh [options]

# Or using the all-in-one script for pre-warming and load testing
./run-prewarmer-and-loadtest.sh [options]
```

### Command-Line Options

Run any script with `--help` to see all available options:

```bash
./run.sh --help
# or
./run-prewarmer-and-loadtest.sh --help
```

#### Key Options:

```
Pre-warmer options:
  --remote NAME           Rclone remote name
  --bucket NAME           S3 bucket name
  --directory PATH        Directory path within bucket
  --base-url URL          Base URL for video assets
  --derivatives LIST      Comma-separated list of derivatives
  --workers NUM           Number of concurrent workers
  --timeout SECONDS       Request timeout in seconds
  --extension EXT         File extension to filter by
  --limit NUM             Limit number of objects to process
  --list-files            List all files with their sizes sorted by size
  --size-threshold NUM    Size threshold in MiB for file size reporting (default: 256 MiB)
  --optimize-by-size      Enable size-based optimization for parallel processing
  --connection-close-delay  Additional delay before closing connections (default: 10s)
  --generate-error-report Generate a detailed error report from results file
  --error-report-output   Output file path for error report (default: error_report.md)
  --error-report-format   Format for error report (markdown or json)

Video Optimization options:
  --optimize-videos        Re-encode large video files to reduce size
  --codec                  Video codec to use (h264, h265, vp9, vp8, av1)
  --quality-profile        Encoding quality (maximum, high, balanced, efficient, minimum)
  --target-resolution      Target resolution (4k, 1080p, 720p, 480p, 360p)
  --audio-profile          Audio encoding profile (high, medium, low, minimum)
  --output-format          Container format (mp4, webm, mkv)
  --create-webm            Also create WebM version alongside primary format

k6 load test options:
  --url-format FORMAT     URL format to use: 'imwidth' or 'derivative'
  --stage1-users NUM      Number of users in stage 1
  --stage1-duration TIME  Duration of stage 1
  --stage2-users NUM      Number of users in stage 2
  --stage2-duration TIME  Duration of stage 2
  --stage3-users NUM      Number of users in stage 3
  --stage3-duration TIME  Duration of stage 3
  --stage4-users NUM      Number of users in stage 4
  --stage4-duration TIME  Duration of stage 4
  --stage5-users NUM      Number of users in stage 5
  --stage5-duration TIME  Duration of stage 5
```

## Usage Examples

### Using the All-in-One Script

The simplest way to run both pre-warming and load testing is with the orchestration script:

```bash
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com \
  --remote r2 --bucket videos --directory videos \
  --workers 10 --stage1-users 50 --stage2-users 100
```

### Video Pre-warming

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --base-url https://cdn.example.com/videos/ \
  --derivatives desktop,tablet,mobile --workers 10 --optimize-by-size
```

### Video Optimization

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --optimize-videos --codec h265 \
  --quality-profile balanced --create-webm \
  --workers 4 --limit 10
```

### File Size Analysis

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --list-files --size-threshold 256
```

### Load Testing

```bash
# Run load test using results from a previous pre-warming run
k6 run video-load-test-integrated.js \
  -e BASE_URL=https://cdn.example.com \
  -e RESULTS_FILE=video_transform_results.json \
  -e STAGE1_USERS=50 -e STAGE2_USERS=100
```

## Video Processing Architecture

### Derivative Processing Model

The tool uses the following dimensions for different derivatives:

```python
dimensions = {
    'desktop': {'width': 1920, 'height': 1080},
    'tablet': {'width': 1280, 'height': 720},
    'mobile': {'width': 854, 'height': 640}
}
```

When processing videos, each derivative results in a different resolution variant of the original video. The request URL structure for processing is:

```
{base_url}/vid/{derivative}/{path_to_video}
```

### Worker Allocation Strategy

The tool dynamically allocates workers based on file size categories:

1. **Small Files**: < 50 MiB (configurable with `--small-file-threshold`)
2. **Medium Files**: 50-200 MiB (configurable with `--medium-file-threshold`)
3. **Large Files**: > 200 MiB

When `--optimize-by-size` is enabled, the worker allocation formula prioritizes small and medium files to optimize overall throughput. The algorithm uses the following weights by default:

- Small files: 1.2x weight
- Medium files: 1.0x weight
- Large files: 0.8x weight

### Connection Management

The tool implements sophisticated connection handling with:

- Configurable timeout values (`--timeout`)
- Connection close delay (`--connection-close-delay`)
- Automatic retries for 5xx errors
- Detailed error tracking by category (timeout, connection_error, etc.)

## Video Optimization Details

### Video Codec Options

| Codec | Efficiency | Compatibility | Encoding Speed | Container | Use Case |
|-------|------------|---------------|----------------|-----------|----------|
| H.264 | Good | Excellent | Fast | MP4/MOV | General purpose, maximum compatibility |
| H.265/HEVC | Very Good | Good | Slow | MP4/MKV | Size optimization, supporting devices |
| VP9 | Very Good | Good | Very Slow | WebM/MKV | Web delivery, open format |
| VP8 | Good | Good | Fast | WebM | Legacy WebM support |
| AV1 | Excellent | Limited | Extremely Slow | WebM/MP4/MKV | Future-proofing, best compression |

### Quality Profiles

| Profile | CRF (H.264) | CRF (HEVC) | CRF (VP9) | CRF (AV1) | Preset | Target |
|---------|-------------|------------|-----------|-----------|--------|--------|
| Maximum | 18 | 22 | 31 | 25 | slower | Highest quality, larger size |
| High | 20 | 24 | 33 | 30 | slow | Visually lossless |
| Balanced | 23 | 28 | 36 | 34 | medium | Good quality, good compression |
| Efficient | 26 | 30 | 39 | 38 | fast | Priority to compression |
| Minimum | 28 | 32 | 42 | 42 | faster | Maximum compression, acceptable quality |

### FFmpeg Command Structure

The tool generates FFmpeg commands with carefully tuned parameters for each codec. For example, here's an H.265 command with balanced quality at 1080p:

```bash
ffmpeg -i input.mp4 -c:v libx265 -crf 28 -preset medium \
  -vf "scale=1920:1080:force_original_aspect_ratio=decrease" \
  -c:a aac -b:a 128k -tag:v hvc1 -movflags +faststart output.mp4
```

## Load Testing Details

### URL Formats

The load test supports two URL formats:

1. **imwidth format** (default):  
   `https://cdn.example.com/path/to/video.mp4?imwidth=1920`

2. **derivative format**:  
   `https://cdn.example.com/path/to/video.mp4?derivative=desktop&width=1920&height=1080`

### Request Patterns

The tool simulates real-world video player behavior with:

- **Byte Range Requests**: 80% of requests use Range headers to request specific portions of videos
- **Range Distribution**:
  - 50% chance: First portion (0-33% of file)
  - 40% chance: Middle portion (33-66% of file)
  - 10% chance: End portion (66-85% of file)
- **Variable Chunk Sizes**: Between 50KB and 500KB per request
- **Random Sleep Intervals**: Between requests to simulate realistic user behavior

### Load Test Thresholds

The load test uses the following thresholds by default:

```javascript
thresholds: {
  http_req_duration: [`p(95)<${__ENV.REQ_DURATION_THRESHOLD || "15000"}`], 
  http_req_failed: [`rate<${__ENV.FAILURE_RATE_THRESHOLD || "0.05"}`],
}
```

These ensure that 95% of requests complete within 15 seconds and that the failure rate is less than 5%.

## Storage Integration

### Cloudflare R2 Support

The toolkit has specialized support for Cloudflare R2 storage through the rclone interface. It uses the standard S3 API but with R2-specific endpoint formatting:

```
https://ACCOUNT_ID.r2.cloudflarestorage.com
```

### File Listing Optimization

For R2 buckets, the tool supports two listing modes:

1. **rclone mode** (default): Uses `rclone ls --recursive` for listing files
2. **AWS CLI mode**: Uses `aws s3 ls --recursive` when the `--aws-cli` flag is specified

For very large buckets (1000+ files), the AWS CLI mode may be more efficient, while rclone offers better compatibility across different storage providers.

## Report Generation

### Performance Reports

The tool generates comprehensive performance reports with:

- Overall statistics (success rate, timing percentiles)
- Category-specific metrics (by file size category)
- Error breakdown by type
- Correlation analysis between file size and processing time
- Recommendations for optimization

Example correlation metrics:
```
"correlation": {
  "size_time_pearson": 0.872,  // Strong correlation between size and time
  "regression_slope": 5.327e-7,  // Seconds per byte
  "estimated_time_100MB": 55.76  // Estimated processing time for 100MB file
}
```

### File Size Reports

The file size analysis feature generates detailed markdown reports including:

- Basic statistics (min/max/average sizes)
- Size distribution by category
- List of largest files
- Percentage of space used by files above threshold
- Potential storage savings calculations

## Common Use Cases

### Production Pre-warming and Load Testing

```bash
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com \
  --remote r2 --bucket videos --workers 20 \
  --stage1-users 100 --stage2-users 200 --stage3-users 300
```

### Video Optimization with WebM Generation

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --optimize-videos --codec h265 \
  --quality-profile balanced --create-webm --target-resolution 1080p
```

### High-Volume File Processing

For processing thousands of files with maximum efficiency:

```bash
python main.py --remote r2 --bucket videos --directory videos \
  --base-url https://cdn.example.com/ --optimize-by-size \
  --workers 500 --small-file-workers 300 --medium-file-workers 150 --large-file-workers 50
```

### Performance Benchmarking

To identify the optimal transformation parameters and worker allocation:

```bash
python main.py --remote r2 --bucket videos --directory videos \
  --base-url https://cdn.example.com/ --optimize-by-size \
  --performance-report detailed_performance.md
```

### Error Analysis and Monitoring

To analyze errors and transformation issues after a large batch run:

```bash
# Generate a detailed error report from results
python main.py --generate-error-report \
  --output video_transform_results.json \
  --error-report-output error_report.md

# Generate a JSON report for further analysis or visualization
python main.py --generate-error-report \
  --output video_transform_results.json \
  --error-report-output error_report.json \
  --error-report-format json
```

## Performance Optimization

### Per-Derivative Parallel Processing

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

### Size-Based Optimization

The pre-warmer can optimize processing based on file sizes, which significantly improves performance and resource utilization, especially when dealing with mixed file sizes.

##### How Size Optimization Works
1. Files are automatically categorized into small, medium, and large based on configurable thresholds
2. Different worker pools are allocated for each size category
3. Resource allocation is optimized to prevent large files from blocking processing of smaller files
4. Comprehensive performance metrics are collected and analyzed

Enable Size Optimization:
```bash
python main.py --remote r2 --bucket videos \
  --directory videos --base-url https://cdn.example.com/ --optimize-by-size
```

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Increase the `--timeout` value
   - Reduce the number of `--workers`
   - Increase `--connection-close-delay` to allow for proper connection cleanup

2. **Path Resolution Problems**
   - Ensure the correct `--remote` and `--bucket` names
   - Check if `--directory` exists in your storage
   - Verify permissions for accessing the storage

3. **FFmpeg Errors**
   - Verify FFmpeg is installed: `ffmpeg -version`
   - Check codec support: `ffmpeg -codecs | grep <codec>`
   - For H.265/HEVC encoding issues, ensure libx265 is installed
   - For VP9 issues, ensure libvpx-vp9 is installed

4. **HTTP 500 Errors in Pre-warming**
   - Check access permissions for your CDN/storage
   - Verify URL construction is correct
   - Reduce concurrency with fewer workers
   - Generate and analyze error reports to identify patterns:
     ```bash
     python main.py --generate-error-report --output video_transform_results.json --error-report-output error_report.md
     ```

5. **Load Test Failures**
   - Check that pre-warming completed successfully
   - Verify that the results file has successful transformations
   - Reduce the number of virtual users
   - Increase the request timeout threshold

## Error Handling and Reporting

The tool implements sophisticated error handling and reporting:

1. **Graceful Shutdown**: Captures SIGINT/SIGTERM and performs orderly shutdown
2. **Thread Pool Management**: Proper cleanup of executor threads
3. **Automatic Retries**: Configurable retry attempts for various error types
4. **Error Classification**: Categorizes errors by type for analysis
5. **Detailed Logging**: Thread-aware logging with file and console output
6. **Partial Results Saving**: Preserves results even if interrupted
7. **Comprehensive Error Reporting**: Generates detailed error reports in multiple formats

### Error Report Generation

The tool includes an advanced error report generator that provides comprehensive analysis of transformation errors:

```bash
# Generate an error report in Markdown format
python main.py --generate-error-report --output video_transform_results.json --error-report-output error_report.md

# Generate an error report in JSON format
python main.py --generate-error-report --output video_transform_results.json --error-report-output error_report.json

# Explicitly specify the format
python main.py --generate-error-report --output video_transform_results.json --error-report-output report.txt --error-report-format json
```

#### Error Report Features

- **Multiple Output Formats**: Markdown and JSON options for different use cases
- **Comprehensive Error Analysis**: Breakdown of errors by type, status code, size category, and derivative
- **Size Correlation**: Statistics comparing file sizes of successful vs. failed transformations
- **Detailed Error Examples**: Specific examples of each error type with context
- **Percentage Calculations**: Error rates and distributions with percentage metrics
- **Sortable Error Lists**: Complete inventory of all errors for further analysis

## License

This project is licensed under the MIT License.