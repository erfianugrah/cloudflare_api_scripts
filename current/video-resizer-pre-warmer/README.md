# Video Transformation and Load Testing Tool

A comprehensive toolkit for processing, verifying, and load testing video assets across different resolutions.

## Overview

This project consists of three integrated components:

1. **Video Pre-Warmer (`video-resizer-kv-pre-warmer.py`)**: Makes HTTP requests to transform videos into different derivatives (desktop, tablet, mobile), capturing size and metadata information.

2. **Load Testing Tool (`video-load-test-integrated.js`)**: Uses k6 to simulate real-world load against your video CDN using the pre-warmed videos.

3. **Orchestration Script (`run-prewarmer-and-loadtest.sh`)**: Coordinates the pre-warming and load testing phases, making it easy to run the complete workflow with a single command.

## Features

- **Video Transformation Processing**: Transforms videos into different derivatives (desktop, tablet, mobile)
- **Detailed Metrics**: Captures size, timing, and metadata for each video variant
- **Concurrent Processing**: Processes multiple videos simultaneously with configurable workers
- **Comprehensive Load Testing**: Simulates real-world traffic patterns against your CDN
- **Customizable Test Scenarios**: Adjust VU counts, ramp-up/down times, and test durations
- **Intelligent Range Requests**: Mimics browser behavior with realistic byte-range requests
- **Detailed Reporting**: Generates summary reports and error analysis
- **All-in-One Orchestration**: Run the complete workflow with a single command

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
# First ensure you have a results file from a previous pre-warming run
k6 run video-load-test-integrated.js \
  -e BASE_URL=https://cdn.example.com \
  -e RESULTS_FILE=video_transform_results.json \
  -e STAGE1_USERS=50 -e STAGE2_USERS=100
```

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

3. **Performance Issues**
   - Adjust virtual user counts based on your system's capabilities
   - Consider running k6 on a separate machine for high-load tests
   - Monitor system resources during tests

## Performance Optimization

- **AWS CLI vs. rclone**: For large AWS S3 buckets, AWS CLI is generally faster for listing objects
- **Worker Count**: Adjust `--workers` based on your CPU cores and network capacity
- **Timeout Values**: Set appropriate `--timeout` values based on video sizes
- **k6 Settings**: Adjust load test stages based on your CDN's capacity and expected real-world usage
- **Selective Processing**: Use `--directory` and `--limit` to process subsets for testing

## License

This project is licensed under the MIT License.