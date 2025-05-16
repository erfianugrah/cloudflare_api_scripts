# Integrated Video Pre-warming and Load Testing

This document explains how to use the integrated workflow for pre-warming video transformations and running load tests.

## Overview

The integration consists of two main components:

1. **Video Pre-Warmer (Python)**: Requests videos in different resolutions from your CDN, preloading the cache and collecting metadata.
2. **Load Testing Tool (k6)**: Uses the results from pre-warming to run realistic load tests with accurate video dimensions and sizes.

## Quick Start

```bash
# Run the entire workflow with default settings
./run-prewarmer-and-loadtest.sh --base-url https://your-cdn.example.com --bucket your-bucket

# Skip pre-warming and only run load test (using existing results)
./run-prewarmer-and-loadtest.sh --skip-prewarming --base-url https://your-cdn.example.com
```

## Workflow Details

### Step 1: Pre-warming

The Python script connects to your S3 bucket, lists video files, and requests them with different derivatives (desktop, tablet, mobile) from your CDN. This both populates the CDN cache and collects metadata about each video for accurate load testing.

### Step 2: Load Testing

The k6 script uses the metadata collected during pre-warming to run realistic load tests:

- It knows the correct dimensions for each video
- It has accurate file sizes for proper byte range requests
- It can use the same URL patterns as your production environment

## Configuration Options

### Basic Settings

- `--base-url`: The base URL of your CDN
- `--bucket`: Your S3 bucket name
- `--remote`: rclone remote name (or AWS S3 if using AWS CLI)
- `--directory`: Subdirectory within bucket to process

### Pre-warmer Settings

- `--workers`: Number of concurrent pre-warming workers
- `--derivatives`: Comma-separated list of derivatives (default: desktop,tablet,mobile)
- `--limit`: Limit the number of videos to process
- `--aws-cli`: Use AWS CLI instead of rclone for S3 access

### Load Test Settings

- `--url-format`: URL format to use ('imwidth' or 'derivative')
- `--stage1-users` through `--stage5-users`: Number of virtual users in each stage
- `--stage1-duration` through `--stage5-duration`: Duration of each stage

### Skip Options

- `--skip-prewarming`: Skip the pre-warming phase (uses existing results file)
- `--skip-loadtest`: Skip the load testing phase (only do pre-warming)

## URL Format Options

The load test supports two URL formats:

1. **imwidth format** (default): `https://cdn.example.com/path/to/video.mp4?imwidth=1920`
   
   Best for simple CDN setups where only width needs to be specified.

2. **derivative format**: `https://cdn.example.com/path/to/video.mp4?derivative=desktop&width=1920&height=1080`
   
   Matches the Python pre-warmer format with derivative and dimensions.

## Examples

```bash
# Full workflow with higher concurrency
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com --bucket videos \
  --workers 10 --stage3-users 200 --stage4-users 200

# Use AWS CLI instead of rclone
./run-prewarmer-and-loadtest.sh --aws-cli --base-url https://cdn.example.com \
  --bucket videos --directory videos

# Quick test with limited videos
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com --bucket videos \
  --limit 5 --stage1-duration 10s --stage2-duration 20s

# Use derivative format URLs
./run-prewarmer-and-loadtest.sh --base-url https://cdn.example.com --bucket videos \
  --url-format derivative
```

## Advanced Usage

### Custom URL patterns

If you need a completely custom URL pattern, edit the `generateVideoUrl` function in `video-load-test-integrated.js`.

### Adding load test metrics

K6 supports custom metrics. Add them to the `video-load-test-integrated.js` file if needed.

### Multi-phase load testing

For more complex load profiles, edit the `stages` configuration in `video-load-test-integrated.js` or use the k6 scenario API.

## Troubleshooting

- **Missing files**: Ensure your rclone or AWS CLI configuration can access the bucket
- **Pre-warming errors**: Check network connectivity and CDN configuration
- **K6 errors**: Verify k6 installation and that the results file exists
- **Byte range errors**: Some videos may have incorrect size information; adjust the safety margins in the script

## Requirements

- Python 3.7+
- k6 0.30.0+
- AWS CLI (if using `--aws-cli` option)
- rclone (if not using AWS CLI)