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
  --format                Format for error report (markdown or json)

Video Optimization options:
  --optimize-videos        Re-encode large video files to reduce size (saves to new location)
  --optimize-in-place      Re-encode large videos and replace them in-place (reduces storage)
  --codec                  Video codec to use (h264, h265, vp9, vp8, av1)
                           h264 offers best browser compatibility (default)
                           h265 offers best compression but limited browser support
  --quality                Encoding quality (maximum, high, balanced, efficient, minimum)
  --target-resolution      Target resolution (4k, 1080p, 720p, 480p, 360p)
  --fit                    How to fit video to target resolution:
                           contain: preserve aspect ratio, fit entire video (default)
                           cover: preserve aspect ratio, fill frame, may crop
                           pad: preserve aspect ratio, add letterbox/pillarbox
                           stretch: ignore aspect ratio, stretch to fill
  --audio-profile          Audio encoding profile (high, medium, low, minimum)
  --output-format          Container format (mp4, webm, mkv)
  --create-webm            Also create WebM version alongside primary format
  --optimized-videos-dir   Output directory for optimized videos (default: optimized_videos)
  --size-threshold         Only process files larger than this size in MiB (default: 256)
  --hardware-acceleration  Hardware acceleration type to use:
                           auto: detect and use best available (default)
                           nvidia: use NVIDIA NVENC
                           intel: use Intel QuickSync
                           amd: use AMD AMF
                           apple: use Apple VideoToolbox
                           none: disable hardware acceleration
  --disable-hardware-acceleration
                           Disable hardware acceleration even if available

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

#### Standard Optimization (Save to New Location)

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --optimize-videos --codec h264 \
  --quality balanced --create-webm \
  --hardware-acceleration auto \
  --workers 4 --limit 10
```

This will download videos, optimize them, and save the optimized versions to the `optimized_videos` directory (configurable with `--optimized-videos-dir`). Hardware acceleration will be automatically used if available.

#### In-Place Optimization (Replace Original Files)

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h264 --quality balanced \
  --target-resolution 1080p --fit contain \
  --audio-profile medium --hardware-acceleration auto \
  --size-threshold 256 --workers 4
```

This will download videos larger than the specified threshold (256 MiB in this example), optimize them using hardware acceleration when available, and replace the original files in the remote storage with the optimized versions.

##### In-Place Optimization Parameters

| Parameter | Description | Options |
|-----------|-------------|---------|
| `--optimize-in-place` | Enable in-place optimization | Flag (no value) |
| `--codec` | Video codec to use | `h264` (default), `h265`, `vp9`, `vp8`, `av1` |
| `--quality` | Quality profile | `maximum`, `high`, `balanced` (default), `efficient`, `minimum` |
| `--target-resolution` | Target resolution | `4k`, `1080p` (default), `720p`, `480p`, `360p` |
| `--fit` | How to handle aspect ratio | `contain` (default), `cover`, `pad`, `stretch` |
| `--audio-profile` | Audio quality profile | `high`, `medium` (default), `low`, `minimum` |
| `--size-threshold` | Only process files larger than this (MiB) | Default: 256 |
| `--workers` | Number of concurrent workers | Default: 5 |
| `--hardware-acceleration` | Hardware acceleration type | `auto` (default), `nvidia`, `intel`, `amd`, `apple`, `none` |
| `--disable-hardware-acceleration` | Disable hardware acceleration | Flag (no value) |
| `--browser-compatible` | Ensure browser compatibility | `True` (default), `False` |

##### Fit Modes for Aspect Ratio Handling

- `contain` (default): Preserves aspect ratio and fits entire video within frame
- `cover`: Preserves aspect ratio and fills entire frame (may crop)
- `pad`: Preserves aspect ratio and adds letterbox/pillarbox to fill frame
- `stretch`: Ignores aspect ratio and stretches to fill frame

##### Codec Compatibility and Selection Guide

| Codec | Compression | Browser Compatibility | Encoding Speed | Container | Best For |
|-------|-------------|----------------------|---------------|-----------|----------|
| H.264 | Good | Excellent | Fast | MP4 | Maximum browser compatibility, direct playback from R2 |
| H.265 (HEVC) | Excellent | Limited | Slow | MP4 | Maximum compression, downloaded playback |
| VP9 | Very Good | Good | Very Slow | WebM | Open formats, modern browsers |
| VP8 | Moderate | Good | Fast | WebM | Legacy WebM support |
| AV1 | Best | Limited | Extremely Slow | MP4/WebM | Future-proofing, experimental |

##### Codec Browser Support Matrix

| Browser | H.264 | H.265/HEVC | VP9 | VP8 | AV1 |
|---------|-------|------------|-----|-----|-----|
| Chrome | ✓ | Limited | ✓ | ✓ | Limited |
| Firefox | ✓ | ✗ | ✓ | ✓ | Limited |
| Safari | ✓ | ✓ (macOS/iOS) | ✗ | ✗ | ✗ |
| Edge | ✓ | Limited | ✓ | ✓ | Limited |
| Mobile Chrome | ✓ | Limited | ✓ | ✓ | Limited |
| Mobile Safari | ✓ | ✓ | ✗ | ✗ | ✗ |

> **Note about browser compatibility**: Use the `--browser-compatible` flag (enabled by default) to ensure videos can be played directly in browsers. This forces H.264 codec for MP4 files. You can disable this with `--browser-compatible=False` if you want to prioritize compression over browser compatibility.

##### Quality Settings

Video quality profiles translate to the following codec-specific settings:

| Profile | H.264 (CRF) | H.265 (CRF) | VP9 (CRF) | AV1 (CRF) | Preset | Use Case |
|---------|-------------|-------------|-----------|-----------|--------|----------|
| maximum | 18 | 22 | 31 | 25 | slower | Archival, highest quality needed |
| high | 20 | 24 | 33 | 30 | slow | Professional content, visually lossless |
| balanced | 23 | 28 | 36 | 34 | medium | Standard use, good balance |
| efficient | 26 | 30 | 39 | 38 | fast | Storage priority, acceptable quality |
| minimum | 28 | 32 | 42 | 42 | faster | Maximum storage reduction |

Lower CRF values produce higher quality but larger file sizes. Faster presets encode quicker but result in slightly lower quality or larger files at the same quality level.

##### Compression Efficiency Comparison (Approximate)

| Codec | Relative File Size | Quality Retention | Processing Speed |
|-------|-------------------|-------------------|------------------|
| H.264 | 100% (Baseline) | Baseline | 1x (Baseline) |
| H.265 | 40-60% of H.264 | Same or better | 2-4x slower than H.264 |
| VP9 | 45-65% of H.264 | Same or better | 5-7x slower than H.264 |
| AV1 | 30-50% of H.264 | Same or better | 10-20x slower than H.264 |

##### Audio Quality Profiles:

| Profile | Bitrate | Channels | Sampling Rate | Use Case |
|---------|---------|----------|---------------|----------|
| high | 192k | Original | Original | Music videos, audio-focused content |
| medium | 128k | Original | 48kHz | Standard videos, good quality |
| low | 96k | 2 | 44.1kHz | Speech-focused content |
| minimum | 64k | 2 | 44.1kHz | Maximum compression, basic audio |

### File Size Analysis and Reporting

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --list-files --size-threshold 256 \
  --size-report-output file_sizes.md
```

This generates a detailed report of all file sizes in the specified directory, sorted by size in descending order. The report includes:
- Size statistics (total, average, median)
- Size distribution by category
- List of files larger than the threshold (256 MiB by default)
- Potential storage savings if these files were optimized

You can customize the output file with `--size-report-output` and adjust the size threshold with `--size-threshold`.

#### Using AWS CLI Instead of Rclone

If you prefer using AWS CLI for listing S3 objects (which can be faster for very large buckets):

```bash
python main.py --remote r2 --bucket videos \
  --directory videos --list-files --use-aws-cli
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

### Error Reports

The tool can generate detailed error analysis reports from results files, providing:

- Multi-dimensional error breakdowns (by type, status, file size, derivative)
- Statistical analysis of file characteristics and error patterns
- Performance metric comparisons between successful and failed requests
- Identification of problematic files and common error patterns
- Data-driven recommendations for optimization

#### Scheduled Error Monitoring

For production environments, consider setting up scheduled error reporting:

```bash
# Example crontab entry for daily error reports
0 7 * * * cd /path/to/project && python main.py --generate-error-report --output /path/to/results/latest.json --error-report-output /path/to/reports/error_report_$(date +\%Y\%m\%d).md >> /var/log/error-reporting.log 2>&1

# Example for JSON reports for monitoring integration
0 */4 * * * cd /path/to/project && python main.py --generate-error-report --output /path/to/results/latest.json --error-report-output /path/to/monitoring/cdn_errors.json --format json
```

These scheduled reports can be integrated with monitoring systems or used for trend analysis over time.

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
  --directory videos --optimize-videos --codec h264 \
  --quality balanced --create-webm --target-resolution 1080p
```

### Browser Compatibility

By default, the tool now enforces browser compatibility for MP4 files by using the H.264 codec, which has the best browser support. If you want to use other codecs like H.265/HEVC (which offers better compression but limited browser support), you need to explicitly disable browser compatibility:

```bash
# Use H.265 codec for maximum compression (videos may not play directly in browser)
python main.py --remote r2 --bucket videos --optimize-in-place \
  --codec h265 --browser-compatible=False --quality balanced
```

> **IMPORTANT**: Videos encoded with H.265/HEVC and stored in R2 may not play directly in browsers when served via URL. They will play fine when downloaded locally. When browser compatibility is critical, stick with H.264 encoding (the default).

### In-Place Video Optimization Examples

#### Basic In-Place Optimization with H.264 (Maximum Browser Compatibility)

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h264 --quality balanced \
  --target-resolution 1080p --size-threshold 100 --workers 4
```

This uses H.264 encoding which offers the best browser compatibility when served directly from R2 or other storage.

#### In-Place Optimization with H.265 (Maximum Compression)

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h265 --browser-compatible=False \
  --quality efficient --target-resolution 1080p --fit contain \
  --audio-profile medium --size-threshold 100 --workers 4
```

H.265 offers superior compression (often 95-99% reduction for videos) but has limited browser compatibility. With `--browser-compatible=False`, you're explicitly acknowledging that these videos may need to be downloaded to be played in most browsers.

#### Maximum Quality Preservation

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h264 --quality maximum \
  --target-resolution 4k --audio-profile high \
  --hardware-acceleration auto \
  --size-threshold 500 --workers 2
```

This preserves maximum quality with H.264 encoding, 4K resolution, and high audio quality. Hardware acceleration ensures faster processing without quality loss. Good for archival purposes where quality is paramount.

#### Maximum Compression

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h265 --browser-compatible=False \
  --quality minimum --target-resolution 720p \
  --audio-profile minimum --hardware-acceleration auto \
  --size-threshold 50 --workers 4
```

This applies maximum compression with H.265, lower resolution, and minimum audio quality. Hardware acceleration is used when available, dramatically speeding up the encoding process. Good for drastically reducing storage costs where extreme compression is needed.

#### Hardware-Accelerated Encoding

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h265 --browser-compatible=False \
  --hardware-acceleration auto --workers 6
```

This uses automatic hardware acceleration detection to encode videos much faster than CPU encoding. The script will use the best available GPU acceleration (NVIDIA NVENC, Intel QuickSync, AMD AMF, or Apple VideoToolbox).

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec h264 --hardware-acceleration nvidia
```

This specifically uses NVIDIA GPU acceleration for encoding videos with H.264 codec.

#### WebM Format with VP9 Codec

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --codec vp9 --quality balanced \
  --target-resolution 1080p --output-format webm \
  --hardware-acceleration auto \
  --size-threshold 100 --workers 4
```

Uses VP9 codec with WebM container, good for web applications supporting modern open formats. Note that hardware acceleration for VP9 is less widely available than for H.264/H.265, but the script will use it when possible.

#### Different Fit Modes

Contain mode (default) - preserves aspect ratio, fits entire video:
```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --fit contain --hardware-acceleration auto --size-threshold 100
```

Cover mode - preserves aspect ratio, fills frame (may crop):
```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --fit cover --hardware-acceleration auto --size-threshold 100
```

Pad mode - preserves aspect ratio, adds letterbox/pillarbox:
```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --fit pad --hardware-acceleration auto --size-threshold 100
```

Stretch mode - ignores aspect ratio, stretches to fill:
```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --fit stretch --hardware-acceleration auto --size-threshold 100
```

#### Processing Files in a Specific Directory

```bash
python main.py --remote r2 --bucket videos --directory marketing \
  --optimize-in-place --codec h264 --hardware-acceleration auto \
  --size-threshold 100
```

Only processes files in the 'marketing' directory within the 'videos' bucket. Hardware acceleration is used if available.

#### Limiting to Specific File Extension

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --extension .mov \
  --codec h264 --hardware-acceleration auto \
  --size-threshold 100
```

Only processes MOV files, converting them to MP4 with H.264 encoding. Hardware acceleration significantly speeds up this conversion process.

#### Optimizing a Specific Number of Files

```bash
python main.py --remote r2 --bucket videos \
  --optimize-in-place --limit 10 --codec h264 \
  --hardware-acceleration auto --size-threshold 100
```

Only processes the 10 largest files above the size threshold, using hardware acceleration when available.

### Advanced Processing Configuration

#### Size-Based Worker Allocation

The tool can optimize processing based on file sizes to improve throughput:

```bash
python main.py --remote r2 --bucket videos --directory videos \
  --base-url https://cdn.example.com/ --optimize-by-size \
  --small-file-threshold 50 --medium-file-threshold 200 \
  --small-file-workers 300 --medium-file-workers 150 --large-file-workers 50 \
  --hardware-acceleration auto --workers 500
```

This configuration:
- Categorizes files by size:
  - Small: <50 MiB (configurable with `--small-file-threshold`)
  - Medium: 50-200 MiB (up to `--medium-file-threshold`)
  - Large: >200 MiB
- Allocates workers appropriately to each size category
- Processes small files with more parallelism (300 workers)
- Dedicates fewer but sufficient resources to large files (50 workers)
- Prevents large files from blocking the processing of smaller files

The `--optimize-by-size` flag enables intelligent worker allocation based on file distribution.

#### Per-Derivative Parallel Processing

Process each derivative (desktop, tablet, mobile) in parallel for maximum throughput:

```bash
python main.py --remote r2 --bucket videos --directory videos \
  --base-url https://cdn.example.com/ --derivatives desktop,tablet,mobile \
  --workers 30 --optimize-by-size --use-derivatives
```

With this approach:
- Each derivative is processed as a separate task
- Faster derivatives complete earlier, optimizing worker utilization
- All derivatives are processed truly in parallel
- Performance metrics are captured at the derivative level
- The `--use-derivatives` flag includes derivatives in the URL path

### Performance Benchmarking

To identify the optimal transformation parameters and worker allocation:

```bash
python main.py --remote r2 --bucket videos --directory videos \
  --base-url https://cdn.example.com/ --optimize-by-size \
  --performance-report detailed_performance.md
```

### Performance Comparison with KV Data

You can compare your transformation results with Cloudflare KV data to identify discrepancies and ensure consistency:

```bash
# First run a standard processing job and save results
python main.py --remote r2 --bucket videos --base-url https://cdn.example.com \
  --output my_results.json

# Then compare these results with Cloudflare KV data
python main.py --compare cloudflare_kv_data.json \
  --output my_results.json \
  --comparison-output comparison_results.json \
  --summary-output comparison_summary.md \
  --summary-format markdown
```

You can also skip processing and just run the comparison:

```bash
python main.py --only-compare \
  --compare cloudflare_kv_data.json \
  --output existing_results.json \
  --comparison-output comparison_results.json
```

The comparison report provides:
- Match rates between your results and KV data
- Discrepancies in file sizes, TTFB, and other metrics
- Lists of files that appear in one dataset but not the other
- Detailed performance statistics for both datasets

### Error Analysis and Monitoring

To analyze errors and transformation issues after a large batch run:

```bash
# Generate a comprehensive error report in Markdown format
python main.py --generate-error-report \
  --output video_transform_results.json \
  --error-report-output error_report.md

# Generate a JSON report for dashboards, visualizations, or programmatic analysis
python main.py --generate-error-report \
  --output video_transform_results.json \
  --error-report-output error_report.json \
  --format json

# Generate a report after filtering by specific parameters
python main.py --generate-error-report \
  --output video_transform_results.json \
  --error-report-output specific_errors.md

# Integrate with monitoring systems using JSON output
python main.py --generate-error-report \
  --output video_transform_results.json \
  --error-report-output /var/log/monitoring/cdn_errors.json \
  --format json
```

The error reports provide detailed analysis for:
- Identifying trends in error patterns across file types and sizes
- Pinpointing specific problematic files or derivatives
- Correlating performance metrics with error occurrences
- Generating actionable recommendations to optimize transformations

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

4. **Videos Not Playing in Browser After Optimization**
   - If you used H.265/HEVC encoding (`--codec h265`), most browsers can't play these videos directly
   - Switch to H.264 encoding (`--codec h264`) for maximum browser compatibility
   - Try downloading the video to play it locally if it was encoded with H.265
   - Consider using VP9 with WebM format (`--codec vp9 --output-format webm`) for modern browsers

### FAQ: Video Optimization

#### Q: Why do my optimized videos play locally but not in the browser when served from R2?
A: This is likely due to the codec you chose. H.265/HEVC provides excellent compression (95-99% reduction) but has limited browser support. By default, the tool now uses H.264 (`--codec h264`) for maximum browser compatibility. If you disabled browser compatibility and used H.265, your videos may only play properly when downloaded.

#### Q: How can I ensure my videos will play in all browsers?
A: The tool now has a `--browser-compatible` flag (enabled by default) which forces the use of H.264 codec for MP4 files. This ensures maximum browser compatibility. To prioritize compression over compatibility, use `--browser-compatible=False --codec h265`.

#### Q: What's the difference between `--optimize-videos` and `--optimize-in-place`?
A: `--optimize-videos` creates optimized copies in a local directory, while `--optimize-in-place` replaces the original files directly in the remote storage to save space.

#### Q: How do I choose between H.264 and H.265?
A: Use H.264 when you need browser compatibility for direct playback from storage (this is now the default setting). Use H.265 when you want maximum compression and files will be downloaded before playback or played in compatible applications. With H.265, you'll need to use `--browser-compatible=False` to override the default behavior.

#### Q: What compression ratio can I expect?
A: With H.264, expect about 70-90% reduction in file size. With H.265, expect 90-99% reduction for most video content. The greater compression with H.265 comes at the cost of browser compatibility.

#### Q: Can I serve H.265 videos through Cloudflare for browser playback?
A: Direct browser playback of H.265/HEVC content from R2 or other storage is challenging because of limited browser support. For web playback, you have these options:
1. Use H.264 encoding (now the default with `--browser-compatible`)
2. Use VP9 with WebM format for modern browsers 
3. Add a transcoding layer (like a media server) between your storage and the browser
4. Configure your application to download H.265 videos before playing them locally

#### Q: Will video quality be noticeably affected?
A: With the default `balanced` quality profile, most viewers won't notice quality differences. If quality is critical, use the `high` or `maximum` profiles. For maximum compression, use `efficient` or `minimum` profiles.

#### Q: How can I see the encoding parameters being used?
A: Set the logging level to debug with `--verbose` flag. This will show the exact FFmpeg commands being executed.

#### Q: Can I convert between formats (e.g., MOV to MP4)?
A: Yes, use the `--extension .mov` parameter to target specific file types, and the `--output-format mp4` to specify the output format.

#### Q: My server has limited CPU resources. How can I optimize for that?
A: Use H.264 instead of H.265/VP9, reduce the number of workers with `--workers 2`, enable hardware acceleration, and consider the `efficient` quality profile which uses faster encoding presets.

#### Q: Can I use GPU acceleration for encoding?
A: Yes! The tool now supports hardware acceleration for multiple platforms:
- NVIDIA GPUs via NVENC
- Intel GPUs via QuickSync
- AMD GPUs via AMF
- Apple Silicon/macOS via VideoToolbox

Use `--hardware-acceleration auto` (default) to automatically detect and use the best available option, or specify a platform with `--hardware-acceleration nvidia|intel|amd|apple`. You can disable hardware acceleration with `--disable-hardware-acceleration` if you prefer CPU encoding for any reason.

#### Q: How much faster is hardware acceleration compared to CPU encoding?
A: Performance varies by hardware, but you can expect:
- NVIDIA GPUs: 3-10x faster than CPU encoding
- Intel QuickSync: 2-6x faster than CPU
- AMD AMF: 2-5x faster than CPU
- Apple VideoToolbox: 3-8x faster than CPU

Hardware acceleration is particularly beneficial for H.265/HEVC encoding, which is very CPU-intensive.

4. **HTTP 500 Errors in Pre-warming**
   - Check access permissions for your CDN/storage
   - Verify URL construction is correct
   - Reduce concurrency with fewer workers
   - Generate and analyze comprehensive error reports to identify patterns:
     ```bash
     python main.py --generate-error-report --output video_transform_results.json --error-report-output error_report.md
     ```
   - Look for patterns in the error report:
     - Are errors concentrated in specific file sizes or types?
     - Do errors occur with specific derivatives (desktop, tablet, mobile)?
     - Is there a correlation between file size and error rate?
     - Are certain URL patterns more prone to errors?
   - Follow the recommendations section in the error report for specific improvement actions

5. **Load Test Failures**
   - Check that pre-warming completed successfully
   - Verify that the results file has successful transformations
   - Reduce the number of virtual users
   - Increase the request timeout threshold

## Error Handling and Reporting

The tool implements sophisticated error handling and comprehensive reporting capabilities:

1. **Graceful Shutdown**: Captures SIGINT/SIGTERM signals and performs orderly shutdown
2. **Thread Pool Management**: Proper cleanup of executor threads with resource protection
3. **Automatic Retries**: Configurable retry attempts for various error types, with exponential backoff
4. **Error Classification**: Categorizes errors by type, status code, and root cause for in-depth analysis
5. **Detailed Logging**: Thread-aware logging with file and console output, including thread IDs
6. **Partial Results Saving**: Preserves results even if processing is interrupted
7. **Comprehensive Error Reporting**: Generates detailed error reports in multiple formats with advanced analytics

### Error Report Generation

The tool includes a powerful error report generator that provides deep insights into transformation issues:

```bash
# Generate an error report in Markdown format
python main.py --generate-error-report --output video_transform_results.json --error-report-output error_report.md

# Generate an error report in JSON format
python main.py --generate-error-report --output video_transform_results.json --error-report-output error_report.json

# Explicitly specify the format
python main.py --generate-error-report --output video_transform_results.json --error-report-output report.txt --format json
```

#### Comprehensive Error Analysis Features

- **Multiple Output Formats**: 
  - Markdown for human-readable reports with formatting and tables
  - JSON for programmatic analysis, dashboards, and custom visualizations

- **Multi-dimensional Error Classification**:
  - By error type (timeout, server_error, connection_error, etc.)
  - By HTTP status code (400, 403, 404, 500, 502, etc.)
  - By size category (small, medium, large)
  - By derivative (desktop, tablet, mobile)
  - By file extension and MIME type

- **Advanced Statistical Analysis**:
  - Detailed size statistics (min, max, average, median, quartiles)
  - Performance metrics comparison (TTFB, total duration)
  - Error rate by file size range
  - Correlation between file characteristics and error rates
  - File size distribution visualization (percentiles and ranges)

- **Pattern Recognition**:
  - Common error message identification and frequency analysis
  - URL pattern analysis to identify problematic query parameters
  - Error timeframe analysis (first to last error timestamps)
  - Identification of most problematic files and patterns

- **Root Cause Indicators**:
  - Correlation between file size and error occurrence
  - Comparison of performance metrics for successful vs. failed requests
  - Analysis of error patterns by file characteristics
  - Error clustering by time, type, and file properties

- **Detailed Diagnostics**:
  - Specific examples of each error type with full context
  - Complete HTTP request information associated with errors
  - Detailed file metadata for problematic files
  - Stack traces and error details when available

- **Actionable Recommendations**:
  - Data-driven suggestions based on error patterns
  - Configuration optimization recommendations
  - Processing strategy adjustments
  - Practical next steps based on error analysis

#### Sample Error Report Sections

- **Summary Statistics**: Overview of error counts, rates, and distribution
- **Error Distribution**: Breakdown by various dimensions with percentage metrics
- **File Size Analysis**: Detailed size statistics for successful vs. failed transformations
- **Performance Metrics**: Comparison of timing data between successful and failed requests
- **Error Pattern Analysis**: Identification of common patterns and frequencies
- **Problematic Files**: Listing of files with highest error counts
- **Status Code Details**: In-depth analysis of each encountered HTTP status code
- **Detailed Error Lists**: Complete inventory of all errors with file and request details
- **Error Examples**: Representative examples of each error type with context
- **Recommendations**: Actionable suggestions based on the error analysis

## License

This project is licensed under the MIT License.