# Video Transformation, Analysis and Optimization Toolkit

A comprehensive toolkit for processing, analyzing, optimizing, and load testing video assets across different resolutions with advanced size-based optimization and performance metrics.

## Overview

This project consists of several integrated components:

1. **Video Pre-Warmer**: Makes HTTP requests to transform videos into different derivatives (desktop, tablet, mobile), capturing size and metadata information.

2. **Video Optimizer**: Re-encodes large video files using FFmpeg with support for multiple codecs and quality profiles to reduce file size while maintaining acceptable quality.

3. **File Analysis Tool**: Generates detailed reports of file sizes, distribution analysis, and identifies large files for optimization.

4. **Load Testing Tool**: Uses k6 to simulate real-world load against your video CDN using the pre-warmed videos with configurable traffic patterns.

5. **Unified Framework (`main.py`)**: Provides a single entry point for all functionality, making it easy to run the complete workflow with a single command.

## End-to-End Workflow

The tool now provides a seamless end-to-end (e2e) workflow that includes:

1. **Pre-warming**: Cache video transformations in Cloudflare KV to ensure optimal performance
2. **Error Report Generation**: Analyze failures and produce detailed reports
3. **Load Testing**: Run k6 tests against the pre-warmed videos, excluding any problematic files

### Running the Complete E2E Workflow

You can run the entire workflow with a single command using the `--full-workflow` flag:

```bash
python3 main.py --full-workflow \
    --remote r2 \
    --bucket videos \
    --base-url https://cdn.example.com/videos/ \
    --derivatives desktop,tablet,mobile \
    --workers 500 \
    --url-format imwidth \
    --stage1-users 10 \
    --stage1-duration 1m \
    --stage2-users 20 \
    --stage2-duration 2m \
    --stage3-users 30 \
    --stage3-duration 3m \
    --stage4-users 20 \
    --stage4-duration 2m \
    --skip-large-files \
    --large-file-threshold-mib 100
```

This will:
1. Pre-warm all videos in the bucket with 500 concurrent workers
2. Generate an error report after pre-warming completes
3. Use that error report to exclude problematic files from load testing
4. Run a k6 load test with the specified stages

### Workflow Control Options

The tool offers several options for controlling the e2e workflow:

- `--full-workflow`: Enable the complete pre-warming → error report → load test sequence
- `--force-prewarm`: Force run pre-warming even if results file exists
- `--use-error-report-for-load-test`: Use error report to exclude problematic files during load testing
- `--generate-error-report`: Generate an error report from results
- `--run-load-test`: Run k6 load testing after pre-warming

You can run individual components or customize the workflow as needed.

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
  - `load_testing.py`: Integration with k6 for load testing
- `video-load-test-integrated-improved.js`: Enhanced k6 test script with better error handling

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
- **Error File Exclusion**: Automatically excludes files that failed during pre-warming
- **Connection Management**: Explicit connection delays to match Python pre-warmer behavior
- **Flexible Virtual User Configuration**: Configure the number of concurrent users per stage
- **Custom Duration Control**: Set the duration for each load testing stage independently

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

### Command-Line Options

Run with `--help` to see all available options:

```bash
python3 main.py --help
```

### Key Options

```
Workflow Options:
  --full-workflow        Run complete workflow: pre-warming → error report → load test
  --force-prewarm        Force run pre-warming even if results file exists
  --use-error-report-for-load-test
                         Use error report for load testing to exclude problematic files

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
  --connection-close-delay  Additional delay before closing connections (default: 15s)
  --generate-error-report Generate a detailed error report from results file
  --error-report-output   Output file path for error report (default: error_report.md)
  --format                Format for error report (markdown or json)

Video Optimization options:
  --optimize-videos        Re-encode large video files to reduce size (saves to new location)
  --optimize-in-place      Re-encode large videos and replace them in-place (reduces storage)
  --codec                  Video codec to use (h264, h265, vp9, vp8, av1)
                           h264 offers best browser compatibility (default)
                           h265 offers best compression but limited browser support
  --quality                Encoding quality (maximum, high, balanced, efficient, minimum)
  --target-resolution      Target resolution (4k, 1080p, 720p, 480p, 360p)
  --fit                    How to fit video to target resolution
  --audio-profile          Audio encoding profile (high, medium, low, minimum)
  --output-format          Container format (mp4, webm, mkv)

k6 load test options:
  --run-load-test         Run k6 load test after pre-warming
  --k6-script PATH        Path to k6 test script (default: video-load-test-integrated-improved.js)
  --url-format FORMAT     URL format to use: 'imwidth' or 'derivative'
  --debug-mode            Enable debug mode for load testing
  --use-head-requests     Use HEAD requests to get content length (default: true)
  --no-head-requests      Disable HEAD requests
  --skip-large-files      Skip large files in load test (default: true)
  --no-skip-large-files   Test all files regardless of size
  --large-file-threshold-mib NUM  Threshold in MiB for skipping large files (default: 256)
  --request-timeout SEC   Timeout for individual requests (default: 60s)
  --global-timeout SEC    Global timeout for the load test (default: 30s)
  --failure-rate-threshold RATE  Maximum acceptable failure rate (default: 0.05)
  --max-retries NUM       Maximum number of retry attempts for failed requests (default: 2)
  --stage1-users NUM      Number of users in stage 1 (default: 5)
  --stage1-duration TIME  Duration of stage 1 (default: 30s)
  --stage2-users NUM      Number of users in stage 2 (default: 10)
  --stage2-duration TIME  Duration of stage 2 (default: 1m)
  --stage3-users NUM      Number of users in stage 3 (default: 15)
  --stage3-duration TIME  Duration of stage 3 (default: 30s)
  --stage4-users NUM      Number of users in stage 4 (default: 10)
  --stage4-duration TIME  Duration of stage 4 (default: 1m)
  --stage5-users NUM      Number of users in stage 5 (default: 0)
  --stage5-duration TIME  Duration of stage 5 (default: 30s)
```

## Usage Examples

### End-to-End Workflow Examples

#### Complete Workflow with Default Settings

Run the entire workflow (pre-warming, error report, load test) with default settings:

```bash
python3 main.py --full-workflow \
    --remote r2 \
    --bucket videos \
    --directory videos \
    --base-url https://cdn.example.com/videos/
```

#### Complete Workflow with Custom Configuration

```bash
python3 main.py --full-workflow \
    --remote r2 \
    --bucket videos \
    --directory videos \
    --base-url https://cdn.example.com/videos/ \
    --derivatives desktop,tablet,mobile \
    --workers 1500 \
    --url-format imwidth \
    --stage1-users 20 \
    --stage1-duration 1m \
    --stage2-users 40 \
    --stage2-duration 2m \
    --stage3-users 60 \
    --stage3-duration 3m \
    --stage4-users 40 \
    --stage4-duration 2m \
    --skip-large-files \
    --large-file-threshold-mib 20 \
    --connection-close-delay 15
```

#### Customized Workflow with Explicit Steps

If you prefer to explicitly control each step of the workflow:

```bash
python3 main.py \
    --remote r2 \
    --bucket videos \
    --base-url https://cdn.example.com/videos/ \
    --workers 20 \
    --generate-error-report \
    --run-load-test \
    --use-error-report-for-load-test \
    --url-format imwidth \
    --stage1-users 10 \
    --stage1-duration 30s \
    --stage2-users 20 \
    --stage2-duration 1m \
    --stage3-users 30 \
    --stage3-duration 30s \
    --stage4-users 20 \
    --stage4-duration 1m \
    --connection-close-delay 15
```

#### Force Pre-warming with Existing Results

To force pre-warming even if results already exist:

```bash
python3 main.py \
    --force-prewarm \
    --remote r2 \
    --bucket videos \
    --base-url https://cdn.example.com/videos/ \
    --workers 20 \
    --generate-error-report \
    --run-load-test
```

### Individual Component Examples

#### Just Pre-warming

```bash
python3 main.py \
    --remote r2 \
    --bucket videos \
    --directory videos \
    --base-url https://cdn.example.com/videos/ \
    --derivatives desktop,tablet,mobile \
    --workers 500
```

#### Just Error Report Generation

```bash
python3 main.py \
    --generate-error-report \
    --output existing_results.json \
    --error-report-output detailed_errors.md
```

#### Just Load Testing

#### Basic Load Testing
```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --use-error-report-for-load-test \
    --url-format imwidth \
    --connection-close-delay 15 \
    --use-head-requests \
    --skip-large-files
```

#### Moderate Load Testing (Higher VU Count)
```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --url-format imwidth \
    --stage1-users 10 \
    --stage2-users 20 \
    --stage3-users 30 \
    --stage4-users 20 \
    --connection-close-delay 15 \
    --use-head-requests \
    --skip-large-files
```

#### High Load Testing (Extended Durations)
```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --url-format imwidth \
    --stage1-users 15 \
    --stage1-duration 1m \
    --stage2-users 30 \
    --stage2-duration 5m \
    --stage3-users 50 \
    --stage3-duration 2m \
    --stage4-users 30 \
    --stage4-duration 5m \
    --connection-close-delay 15 \
    --use-head-requests \
    --skip-large-files
```

#### Extreme Load Testing (Maximum Concurrent Users)
```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --url-format imwidth \
    --stage1-users 20 \
    --stage1-duration 2m \
    --stage2-users 50 \
    --stage2-duration 5m \
    --stage3-users 100 \
    --stage3-duration 10m \
    --stage4-users 75 \
    --stage4-duration 5m \
    --stage5-users 40 \
    --stage5-duration 3m \
    --connection-close-delay 15 \
    --request-timeout 180s \
    --global-timeout 300s \
    --use-head-requests \
    --skip-large-files
```

### Video Optimization Examples

#### Standard Optimization (Save to New Location)

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --optimize-videos --codec h264 \
  --quality balanced --create-webm \
  --hardware-acceleration auto \
  --workers 4 --limit 10
```

#### In-Place Optimization (Replace Original Files)

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h264 --quality balanced \
  --target-resolution 1080p --fit contain \
  --audio-profile medium --hardware-acceleration auto \
  --size-threshold 256 --workers 4
```

### File Analysis Example

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --list-files --size-threshold 256 \
  --size-report-output file_sizes.md
```

## Advanced Configurations

### Custom Virtual User (VU) Configuration

For fine-grained control of load test virtual users and durations:

```bash
# This example shows how to configure all 5 load testing stages
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com/videos/ \
    --output existing_results.json \
    --url-format imwidth \
    --stage1-users 5 \      # Stage 1: Warm-up phase
    --stage1-duration 30s \
    --stage2-users 15 \     # Stage 2: Ramp-up phase
    --stage2-duration 1m \
    --stage3-users 30 \     # Stage 3: Peak load phase
    --stage3-duration 2m \
    --stage4-users 20 \     # Stage 4: Sustained load phase
    --stage4-duration 3m \
    --stage5-users 10 \     # Stage 5: Cool-down phase
    --stage5-duration 1m \
    --connection-close-delay 15
```

### High-Performance Pre-warming

For maximum throughput on large video collections:

```bash
python3 main.py \
    --remote r2 \
    --bucket videos \
    --base-url https://cdn.example.com/videos/ \
    --derivatives desktop,tablet,mobile \
    --workers 2000 \
    --optimize-by-size \
    --small-file-workers 1200 \
    --medium-file-workers 600 \
    --large-file-workers 200 \
    --timeout 300 \
    --connection-close-delay 15
```

This configuration:
- Uses 2000 total workers
- Optimizes worker allocation based on file sizes
- Allocates more workers to small files (1200) for maximum throughput
- Allocates appropriate workers to medium (600) and large (200) files
- Increases timeout for large files
- Adds connection close delay to prevent connection issues

### Complex Load Testing

For thorough load testing with realistic user simulation:

```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --use-error-report-for-load-test \
    --url-format derivative \
    --use-head-requests \
    --skip-large-files \
    --large-file-threshold-mib 100 \
    --request-timeout 90s \
    --global-timeout 60s \
    --failure-rate-threshold 0.03 \
    --max-retries 3 \
    --connection-close-delay 15 \
    --stage1-users 5 \
    --stage1-duration 30s \
    --stage2-users 10 \
    --stage2-duration 1m \
    --stage3-users 15 \
    --stage3-duration 2m \
    --stage4-users 10 \
    --stage4-duration 5m \
    --stage5-users 5 \
    --stage5-duration 1m
```

This configuration:
- Uses modest concurrency (5-15 users) to prevent overwhelming the server
- Adds explicit connection delay (15s) to match the pre-warmer behavior
- Uses HEAD requests for content length determination
- Skips files larger than 100 MiB
- Uses a 5-stage load pattern: warmup → sustained load → peak load → stress test → cooldown
- Sets a strict failure rate threshold of 3%
- Configures longer timeouts for reliability
- Configures retry attempts for resilience

### Bulk Testing All Videos (Increased Coverage)

For testing ALL videos in a large collection (instead of a random sample):

```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --url-format derivative \
    --use-head-requests \
    --connection-close-delay 5 \
    --stage1-users 20 \
    --stage1-duration 5m \
    --stage2-users 50 \
    --stage2-duration 30m \
    --stage3-users 75 \
    --stage3-duration 20m \
    --stage4-users 50 \
    --stage4-duration 30m \
    --stage5-users 0 \
    --stage5-duration 1s
```

This configuration:
- Extends test duration significantly (85+ minutes total)
- Increases concurrent users (20-75 range)
- Reduces connection close delay to 5 seconds
- Uses Head requests for content determination
- Note: Consider editing the k6 script to reduce sleep times (MIN_SLEEP/MAX_SLEEP) for even more coverage

### Minimizing Sleep Time for Maximum Throughput

To increase the number of requests processed, you can modify the k6 script or use environment variables:

```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --url-format derivative \
    --connection-close-delay 2 \
    --stage2-duration 20m \
    --stage3-duration 10m \
    --stage4-duration 20m \
    --stage2-users 30 \
    --stage3-users 50 \
    --stage4-users 30 \
    --k6-script video-load-test-integrated-improved.js \
    --env MIN_SLEEP=0.5 \
    --env MAX_SLEEP=1.5 \
    --env CONNECTION_CLOSE_DELAY=2
```

This configuration:
- Drastically reduces sleep times between requests (0.5-1.5s vs the default 2-5s)
- Reduces connection close delay to 2 seconds
- Extends test duration for more coverage
- Increases concurrency for higher throughput
- Uses environment variables to override sleep times without modifying the script

### Custom Duration-free Testing to Process All Videos Once

For applications where you need to test every video exactly once (without time-based constraints):

```bash
# Modify k6 script to use iterations instead of time-based stages
cp video-load-test-integrated-improved.js video-load-test-all-videos.js

# Edit the script to use iterations mode instead of stages (add these lines)
# export const options = {
#   scenarios: {
#     per_vu_iterations: {
#       executor: 'per-vu-iterations',
#       vus: 30,
#       iterations: 1,
#       maxDuration: '24h'
#     },
#   },
# };
```

Then run with:

```bash
python3 main.py \
    --run-load-test \
    --base-url https://cdn.example.com \
    --output existing_results.json \
    --url-format derivative \
    --connection-close-delay 2 \
    --k6-script video-load-test-all-videos.js
```

This approach:
- Uses a custom k6 script with iterations instead of time-based stages
- Each virtual user processes one video per iteration
- Allows you to test all videos without time constraints
- Provides more deterministic coverage of your video library

## Workflow Details

### Data Flow Between Steps

The e2e workflow passes data between steps as follows:

1. **Pre-warming**:
   - Processes videos from the specified storage bucket
   - Saves results to the specified output file (default: `video_transform_results.json`)

2. **Error Report Generation**:
   - Reads results from the pre-warming step
   - Analyzes errors and generates a detailed report
   - Saves the report to the specified output file (default: `error_report.md` or `error_report.json`)

3. **Load Testing**:
   - Reads the pre-warming results to get the list of successfully processed videos
   - If `--use-error-report-for-load-test` is enabled, also reads the error report to exclude problematic files
   - Runs the k6 load test using only the successful videos

### Error Handling

The workflow includes robust error handling:

- If pre-warming fails for some videos, the error report will include detailed analysis
- Load testing will automatically exclude videos that failed during pre-warming
- If errors occur during any step, the workflow will continue with the remaining steps if possible
- Detailed logs are generated for troubleshooting

### Conditional Execution

The workflow is smart about determining what needs to be executed:

- If results file exists, pre-warming will be skipped unless `--force-prewarm` is used
- If error report is needed for load testing but doesn't exist, it will be generated automatically
- If load testing is requested but results file doesn't exist, pre-warming will run first

## Best Practices

### General Workflow Recommendations

1. **Start with Full Workflow for New Buckets**:
   ```bash
   python3 main.py --full-workflow \
       --remote r2 \
       --bucket your-new-bucket \
       --base-url https://your-cdn.com/ \
       --workers 500
   ```

2. **Use Force Pre-warming for Updates**:
   ```bash
   python3 main.py --full-workflow --force-prewarm \
       --remote r2 \
       --bucket your-bucket \
       --base-url https://your-cdn.com/ \
       --workers 500
   ```

3. **Increase Worker Count for Large Buckets**:
   - For buckets with thousands of videos, use 1000+ workers
   - Enable size-based optimization with `--optimize-by-size`
   - Allocate workers for different size categories with `--small-file-workers`, etc.

4. **Customize Load Test Parameters Based on Server Capacity**:
   - Use low concurrency (5-15 users) for most servers
   - Only increase user counts if your server can handle it
   - Always use connection close delays (15+ seconds)
   - Use multiple stages that gradually increase load

### Performance Optimization

For maximum performance:

1. **Worker Allocation**:
   - Use 500-2000 workers depending on available system resources
   - For mixed file sizes, enable `--optimize-by-size`
   - Allocate more workers to small files (60-70% of total)
   - Allocate fewer workers to large files (10-20% of total)

2. **Timeout Settings**:
   - Increase `--timeout` for large files (300-600 seconds)
   - Add `--connection-close-delay` (10-15 seconds) to prevent connection issues
   - For load testing, set appropriate `--request-timeout` and `--global-timeout`

3. **File Filtering**:
   - Skip very large files with `--skip-large-files --large-file-threshold-mib 100`
   - Focus on frequently accessed file types with `--extension`
   - Limit initial runs with `--limit` to test configuration

### Error Handling

For robust error handling:

1. **Generate Detailed Error Reports**:
   - Always use `--generate-error-report` for analysis
   - Use JSON format for integration with monitoring systems: `--format json`
   - Use Markdown format for human readability (default)

2. **Use Error Reports in Load Testing**:
   - Always enable `--use-error-report-for-load-test` to exclude problematic files
   - This ensures more realistic and reliable load testing results

3. **Implement Retry Mechanism**:
   - Configure `--retry` for pre-warming (default: 2)
   - Configure `--max-retries` for load testing (default: 2)
   - Increase for less stable environments

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Increase the `--timeout` value
   - Reduce the number of `--workers` for pre-warming
   - For load testing, use low concurrency (5-15 users maximum)
   - Increase `--connection-close-delay` to 15-20 seconds
   - Ensure load test sleep times are sufficient between requests

2. **Path Resolution Problems**
   - Ensure the correct `--remote` and `--bucket` names
   - Check if `--directory` exists in your storage
   - Verify permissions for accessing the storage

3. **HTTP 500 Errors in Pre-warming**
   - Check access permissions for your CDN/storage
   - Verify URL construction is correct
   - Reduce concurrency with fewer workers
   - Generate and analyze comprehensive error reports

4. **Load Test Failures**
   - Check that pre-warming completed successfully
   - Verify that the results file has successful transformations
   - Make sure `--use-error-report-for-load-test` is enabled
   - Reduce the number of virtual users (5-15 maximum is recommended)
   - Ensure `--connection-close-delay` is set to at least 15 seconds
   - Increase the request timeout threshold
   - If timeouts persist, make the load test stages more gradual
   - For HTTP 206 (Partial Content) responses, the load test has been updated to properly handle these as valid responses when range headers are used

5. **Limited Video Coverage in Load Tests**
   - Default configuration only processes ~124 videos in 3.5 minutes
   - Increase test duration with longer stage durations (e.g., `--stage2-duration 30m`)
   - Increase concurrency with more users (e.g., `--stage2-users 50`)
   - Reduce sleep times between requests by editing the script or using environment variables
   - For complete coverage, consider using iterations instead of time-based stages

### FAQ

#### Q: The workflow skips pre-warming even though I want to re-run it. What should I do?
A: Use the `--force-prewarm` flag to force pre-warming even if the results file exists.

#### Q: How can I run load tests excluding files that failed during pre-warming?
A: Use the `--use-error-report-for-load-test` flag to automatically filter out problematic files.

#### Q: How can I run the complete workflow in one command?
A: Use the `--full-workflow` flag, which enables pre-warming, error report generation, and load testing with a single command.

#### Q: Are the derivatives (desktop, tablet, mobile) required?
A: Yes, derivatives are used to process different resolutions. The default is `desktop,tablet,mobile`.

#### Q: Why does my load test fail with timeout errors when the pre-warming works fine?
A: The pre-warmer uses more conservative connection handling. Ensure your load test uses:
  - Lower concurrency (5-15 users maximum)
  - Longer connection close delays (15+ seconds)
  - Sufficient sleep times between requests
  - Matching URL format between pre-warmer and load test

#### Q: Why am I seeing HTTP 206 Partial Content responses in my load test?
A: HTTP 206 (Partial Content) responses are normal and expected when using range headers in video requests. The load test has been updated to properly handle these as valid responses. When a client requests a specific byte range of a video (which is common in video streaming), the server responds with status 206 and only the requested portion of the video.

#### Q: The optimized videos play locally but not in the browser when served from R2?
A: This is likely due to the codec. Use H.264 (`--codec h264`) for maximum browser compatibility. H.265/HEVC provides excellent compression but has limited browser support.

#### Q: Why is my load test only processing a small number of videos despite having thousands available?
A: This is due to the time-based design of k6 load testing. The default configuration (3.5 minute duration, 5-15 users, 14.5s per request) only processes ~124 videos. To test more videos:
  - Increase test duration with longer stage times (e.g., `--stage2-duration 30m`)
  - Increase concurrency with more users (e.g., `--stage2-users 50`)
  - Reduce sleep times (edit the JavaScript file or use environment variables)
  - For testing ALL videos, consider using a custom script with iterations instead of time-based stages

#### Q: Can I test every video exactly once without time constraints?
A: Yes, modify the k6 script to use iterations mode instead of time-based stages, as shown in the "Custom Duration-free Testing" example. This approach ensures each video is tested exactly once.

#### Q: How can I send parameters directly to the k6 script without modifying it?
A: Use the `--env` flag to pass environment variables to k6:
```
python3 main.py --run-load-test ... --env MIN_SLEEP=0.5 --env MAX_SLEEP=1.5
```

## License

This project is licensed under the MIT License.