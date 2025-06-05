# Media Toolkit for Go

A high-performance, comprehensive media processing toolkit written in Go for pre-warming CDN caches, analyzing media files, and optimizing video content at scale.

## 🚀 Features

- **Cache Pre-warming**: Efficiently pre-warm CDN caches for media transformations with intelligent worker allocation
- **File Analysis**: Analyze media files and generate detailed size reports with advanced statistics
- **Native Load Testing**: Built-in load testing with staged traffic patterns and detailed metrics
- **Video Optimization**: FFmpeg-based video optimization with hardware acceleration support
- **Workflow Orchestration**: Automated 5-stage workflow with resume capability
- **Performance Monitoring**: Real-time statistics and comprehensive reporting
- **Smart Worker Allocation**: Size-based worker pools for optimal performance
- **Error Analysis**: Detailed error tracking and troubleshooting recommendations
- **Advanced File Filtering**: Comprehensive exclusion options available across all commands

## 📋 Table of Contents

- [Features](#-features)
- [Key Improvements](#key-improvements)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Commands](#commands)
  - [workflow](#workflow---full-orchestration)
  - [prewarm](#prewarm---cache-pre-warming)
  - [analyze](#analyze---file-analysis)
  - [loadtest](#loadtest---performance-testing)
  - [optimize](#optimize---video-optimization)
  - [validate](#validate---content-validation)
- [Configuration](#configuration)
- [File Filtering](#file-filtering)
- [Complete Workflow Guide](#complete-workflow-guide)
- [URL Formats](#url-formats)
- [Performance Optimization](#performance-optimization)
- [Image Variants](#image-variants)
- [Examples](#examples)
- [Storage Backends](#storage-backends)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Contributing](#contributing)

## Key Improvements

This Go implementation provides significant enhancements over traditional solutions:

- 🏃 **Faster Execution**: Compiled binary with near-instant startup
- 📉 **Lower Memory Usage**: Efficient memory management with streaming algorithms
- 🧵 **True Concurrency**: Goroutines provide superior parallelism without GIL limitations
- 📦 **Single Binary**: 15MB standalone executable vs 500MB+ runtime environments
- 🔧 **Type Safety**: Compile-time validation prevents runtime errors
- 🏗️ **Modular Architecture**: Clean interfaces with dependency injection
- ⚡ **Native Performance**: Direct system calls without interpreter overhead
- 🛡️ **Production Ready**: Structured logging, graceful shutdown, comprehensive error handling
- 📊 **Advanced Queue Management**: Batch processing, dynamic sizing, and exponential backoff prevent queue overflow issues

## Architecture

### Project Structure

```
media-toolkit-go/
├── cmd/toolkit/          # CLI application
│   ├── main.go          # Entry point with signal handling
│   └── commands/        # Command implementations
│       ├── workflow.go  # Full orchestration command
│       ├── prewarm.go   # Pre-warming functionality
│       ├── analyze.go   # File analysis and reporting
│       ├── loadtest.go  # Native load testing
│       ├── optimize.go  # Video optimization
│       └── validate.go  # Content validation
├── pkg/                 # Public packages
│   ├── config/         # Configuration management
│   ├── storage/        # Storage backends (rclone, AWS, local)
│   ├── httpclient/     # HTTP client with retries
│   ├── media/          # Media processing
│   │   ├── image/      # Image variant processing
│   │   └── video/      # Video derivative processing
│   ├── ffmpeg/         # FFmpeg integration
│   ├── loadtest/       # Native load testing engine
│   ├── stats/          # Statistics collection
│   ├── reporting/      # Report generation
│   └── utils/          # Utilities
└── internal/           # Private packages
    ├── orchestrator/   # Workflow coordination
    └── workers/        # Worker pool management
```

### Key Components

- **Storage Layer**: Unified interface supporting rclone (40+ providers), AWS SDK, and local filesystem
- **HTTP Client**: Production-ready with connection pooling, retries, and configurable delays
- **Worker Pool**: Advanced queue management with:
  - Size-based allocation for small (≤50MB), medium (50-300MB), and large (>300MB) files
  - Dynamic queue sizing with configurable multiplier
  - Batch processing to prevent queue overflow
  - Exponential backoff with jitter for retries
  - Real-time queue monitoring and diagnostics
- **Statistics**: Memory-efficient streaming algorithms with O(1) memory usage
- **Orchestrator**: Pipeline management with stage coordination and error recovery
- **Load Testing**: Native Go implementation replacing external dependencies

## Installation

### Prerequisites

- Go 1.22 or later
- FFmpeg 4.0+ (for video processing)
- rclone (optional, for cloud storage)

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/media-toolkit-go.git
cd media-toolkit-go

# Build the binary
make build

# Or install directly
go install ./cmd/toolkit
```

### Binary Releases

Download pre-built binaries from the [releases page](https://github.com/yourusername/media-toolkit-go/releases).

### Cross-Platform Builds

```bash
# Linux
GOOS=linux GOARCH=amd64 make build

# macOS (Intel)
GOOS=darwin GOARCH=amd64 make build

# macOS (Apple Silicon)
GOOS=darwin GOARCH=arm64 make build

# Windows
GOOS=windows GOARCH=amd64 make build
```

## Quick Start

### Basic Pre-warming

```bash
# Pre-warm video derivatives
media-toolkit prewarm \
  --remote r2 \
  --bucket videos \
  --base-url https://cdn.example.com/videos/ \
  --media-type video \
  --derivatives desktop,tablet,mobile \
  --workers 100

# Pre-warm image variants
media-toolkit prewarm \
  --remote r2 \
  --bucket images \
  --base-url https://cdn.example.com/images/ \
  --media-type image \
  --image-variants thumbnail,small,medium,large,webp \
  --workers 50
```

### Full Workflow

Execute the complete 5-stage workflow:

```bash
media-toolkit workflow \
  --remote ikea-mcdc \
  --bucket prod-ap-southeast-1-mcdc-media \
  --directory videos \
  --base-url https://cdn.erfi.dev/videos/ \
  --extensions .mp4,.mov \
  --workers 3000 \
  --timeout 180
```

## Commands

### `workflow` - Full Orchestration

Execute a comprehensive media processing workflow with intelligent orchestration.

```bash
media-toolkit workflow [flags]
```

The workflow includes:
1. 🔍 **File Analysis** - Analyze media files and generate size reports
2. 🚀 **Cache Pre-warming** - Pre-warm CDN cache with all derivatives/variants
3. 📊 **Error Analysis** - Analyze failures and generate detailed reports
4. ⚡ **Load Testing** - Validate performance with realistic traffic
5. 🎬 **Optimization** - Optimize large video files (optional)

**Key Features:**
- Intelligent worker allocation based on file sizes
- Comprehensive error handling and recovery
- Progress tracking and resumable execution
- Detailed reporting at each stage
- Interactive mode for step-by-step execution

**Core Flags:**
- `--remote`: rclone remote name (required)
- `--bucket`: S3 bucket name (required)
- `--base-url`: Base URL for HTTP requests (required)
- `--workers`: Number of concurrent workers (default: 500)
- `--timeout`: Request timeout in seconds (default: 240)
- `--dry-run`: Preview workflow without executing
- `--resume-from`: Resume from specific stage (analysis, prewarm, errors, loadtest, optimize)

**Stage Control:**
- `--skip-analysis`: Skip file analysis stage
- `--skip-prewarm`: Skip pre-warming stage
- `--skip-errors`: Skip error analysis stage
- `--skip-loadtest`: Skip load testing stage
- `--skip-optimize`: Skip optimization stage

**Complete Flag Reference:**

**Storage Configuration:**
- `--directory`: Directory path within bucket
- `--output-dir`: Directory for output files (default: ".")
- `--use-aws-cli`: Use AWS CLI instead of rclone

**Media Configuration:**
- `--media-type`: Type of media to process (auto, image, video) [default: "video"]
- `--extensions`: File extensions to filter by (comma-separated)
- `--derivatives`: Video derivatives (comma-separated) [default: "desktop,tablet,mobile"]
- `--image-variants`: Image variants (comma-separated) [default: "thumbnail,small,medium,large,webp"]

**Pre-warm Configuration:**
- `--connection-close-delay`: Connection close delay in seconds [default: 15]
- `--retry`: Number of retry attempts [default: 2]
- `--optimize-by-size`: Enable size-based worker optimization
- `--small-file-workers`: Workers for small files (0=auto)
- `--medium-file-workers`: Workers for medium files (0=auto)
- `--large-file-workers`: Workers for large files (0=auto)
- `--small-file-threshold`: Threshold in MiB for small files [default: 50]
- `--medium-file-threshold`: Threshold in MiB for medium files [default: 200]
- `--queue-multiplier`: Queue size multiplier (queue_size = workers * multiplier) [default: 3.0]
- `--use-head-for-size`: Use HEAD requests for size verification
- `--url-format`: URL format (imwidth, derivative, query) [default: "imwidth"]
- `--limit`: Limit number of files to process (0=no limit)

**Analysis Configuration:**
- `--size-threshold`: Size threshold in MiB for reporting [default: 100]
- `--analysis-format`: Analysis report format (markdown, json) [default: "markdown"]
- `--generate-comparison`: Generate KV comparison if data available
- `--compare-file`: KV JSON file for comparison

**Load Test Configuration:**
- `--loadtest-users`: Default number of users for load testing [default: 50]
- `--loadtest-duration`: Default duration for load testing [default: "2m"]
- `--stage1-users`: Stage 1 users (0=use default)
- `--stage1-duration`: Stage 1 duration
- `--stage2-users`: Stage 2 users
- `--stage2-duration`: Stage 2 duration
- `--stage3-users`: Stage 3 users
- `--stage3-duration`: Stage 3 duration
- `--skip-large-files`: Skip large files in load test [default: true]
- `--large-file-threshold-mb`: Threshold for large files in MB [default: 256]
- `--request-timeout`: Request timeout for load test [default: "120s"]

**Optimization Configuration:**
- `--optimize-videos`: Enable video optimization [default: true]
- `--optimize-threshold`: Size threshold in MiB for optimization [default: 256]
- `--optimize-codec`: Video codec (h264, h265, vp9, av1) [default: "h264"]
- `--optimize-quality`: Quality profile (maximum, high, balanced, efficient, minimum) [default: "balanced"]
- `--optimize-resolution`: Target resolution (4k, 1080p, 720p, 480p, 360p) [default: "1080p"]
- `--optimize-workers`: Number of optimization workers [default: 10]
- `--hardware-acceleration`: Hardware acceleration type (auto, nvidia, intel, amd, apple, none) [default: "auto"]
- `--browser-compatible`: Ensure browser compatibility [default: true]

**Workflow Control:**
- `--continue-on-error`: Continue workflow on stage errors [default: true]
- `--interactive`: Interactive mode with confirmations
- `--save-progress`: Save workflow progress for resume capability [default: true]

**Examples:**

```bash
# Basic workflow
media-toolkit workflow \
  --remote r2 \
  --bucket media \
  --base-url https://cdn.example.com/ \
  --workers 1000

# Complete workflow with ALL available flags
media-toolkit workflow \
  # Core configuration (required)
  --remote r2 \
  --bucket prod-media \
  --base-url https://cdn.example.com/ \
  \
  # Storage configuration
  --directory videos \
  --output-dir ./workflow-results \
  --use-aws-cli \
  \
  # Media configuration
  --media-type video \
  --extensions .mp4,.mov,.webm \
  --derivatives desktop,tablet,mobile \
  --image-variants thumbnail,small,medium,large,webp \
  \
  # Pre-warm configuration
  --workers 3000 \
  --timeout 300 \
  --connection-close-delay 15 \
  --retry 2 \
  --optimize-by-size \
  --small-file-workers 1000 \
  --medium-file-workers 1000 \
  --large-file-workers 1000 \
  --small-file-threshold 50 \
  --medium-file-threshold 200 \
  --use-head-for-size \
  --url-format imwidth \
  --limit 10000 \
  \
  # Analysis configuration
  --size-threshold 256 \
  --analysis-format markdown \
  --generate-comparison \
  --compare-file kv-data.json \
  \
  # Load test configuration
  --loadtest-users 100 \
  --loadtest-duration 3m \
  --stage1-users 50 \
  --stage1-duration 1m \
  --stage2-users 100 \
  --stage2-duration 3m \
  --stage3-users 50 \
  --stage3-duration 1m \
  --skip-large-files \
  --large-file-threshold-mb 256 \
  --request-timeout 180s \
  \
  # Optimization configuration
  --optimize-videos \
  --optimize-threshold 256 \
  --optimize-codec h264 \
  --optimize-quality balanced \
  --optimize-resolution 1080p \
  --optimize-workers 10 \
  --hardware-acceleration auto \
  --browser-compatible \
  \
  # Workflow control
  --dry-run \
  --continue-on-error \
  --resume-from analysis \
  --skip-analysis \
  --skip-prewarm \
  --skip-errors \
  --skip-loadtest \
  --skip-optimize \
  --interactive \
  --save-progress

# High-performance configuration
media-toolkit workflow \
  --remote r2 \
  --bucket videos \
  --base-url https://cdn.example.com/ \
  --workers 5000 \
  --optimize-by-size \
  --small-file-workers 2000 \
  --medium-file-workers 2000 \
  --large-file-workers 1000 \
  --timeout 300

# Resume from specific stage
media-toolkit workflow \
  --remote r2 \
  --bucket media \
  --base-url https://cdn.example.com/ \
  --resume-from prewarm

# Skip optimization stage
media-toolkit workflow \
  --remote r2 \
  --bucket media \
  --base-url https://cdn.example.com/ \
  --skip-optimize
```

### `prewarm` - Cache Pre-warming

Pre-warm CDN caches by making HTTP requests to all derivatives/variants.

```bash
media-toolkit prewarm [flags]
```

**Required Flags:**
- `--remote`: rclone remote name
- `--bucket`: S3 bucket name
- `--base-url`: Base URL for HTTP requests

**Key Options:**
- `--media-type`: Type of media (auto, image, video) [default: auto]
- `--derivatives`: Video derivatives [default: desktop,tablet,mobile]
- `--image-variants`: Image variants to process [default: thumbnail,small,medium,large,webp]
- `--workers`: Number of concurrent workers [default: 5]
- `--timeout`: Request timeout in seconds [default: 120]
- `--connection-close-delay`: Delay before closing connections [default: 15]
- `--retry`: Number of retry attempts [default: 2]
- `--optimize-by-size`: Enable size-based worker optimization
- `--url-format`: URL format (imwidth, derivative, query) [default: imwidth]
- `--extensions`: File extensions to filter by
- `--limit`: Limit number of objects to process (0 = no limit)
- `--output`: Output JSON file path [default: media_transform_results.json]

**Worker Allocation Options:**
- `--small-file-workers`: Workers for small files (0 = auto)
- `--medium-file-workers`: Workers for medium files (0 = auto)
- `--large-file-workers`: Workers for large files (0 = auto)
- `--small-file-threshold`: Threshold in MiB for small files [default: 50]
- `--medium-file-threshold`: Threshold in MiB for medium files [default: 200]
- `--queue-multiplier`: Queue size multiplier (queue_size = workers * multiplier) [default: 3.0]

**Examples:**

```bash
# Basic pre-warming
media-toolkit prewarm \
  --remote r2 \
  --bucket videos \
  --base-url https://cdn.example.com/

# High-performance with size optimization
media-toolkit prewarm \
  --remote r2 \
  --bucket videos \
  --base-url https://cdn.example.com/ \
  --workers 500 \
  --timeout 240 \
  --optimize-by-size \
  --small-file-workers 200 \
  --medium-file-workers 200 \
  --large-file-workers 100

# Image variants with custom transformations
media-toolkit prewarm \
  --remote r2 \
  --bucket images \
  --base-url https://cdn.example.com/ \
  --media-type image \
  --image-variants thumbnail,small,medium,large,webp,avif,og_image,twitter_card

# Pre-warm specific file types
media-toolkit prewarm \
  --remote r2 \
  --bucket media \
  --base-url https://cdn.example.com/ \
  --extensions .mp4,.mov \
  --workers 100

# High-volume pre-warming (10,000+ files)
media-toolkit prewarm \
  --remote r2 \
  --bucket large-media \
  --base-url https://cdn.example.com/ \
  --workers 3000 \
  --queue-multiplier 10.0 \
  --optimize-by-size \
  --timeout 300
```

### `analyze` - File Analysis

Analyze media files and generate comprehensive reports with multiple output formats.

```bash
media-toolkit analyze [flags]
```

**Key Options:**
- `--remote`: rclone remote name
- `--bucket`: S3 bucket name
- `--directory`: Directory path within bucket
- `--list-files`: List all files with their sizes
- `--size-threshold`: Size threshold in MiB for reporting [default: 256]
- `--size-report-output`: Size report output file [default: file_size_report.md]
- `--base-url`: Base URL to prepend to file paths in output
- `--url-encode`: URL encode file paths (spaces become %20) [default: true]
- `--format`: Report format (json, markdown, text)
- `--extensions`: File extensions to filter by
- `--media-type`: Media type preset (auto, image, video)
- `--limit`: Limit number of files to analyze
- `--generate-error-report`: Generate error report from results
- `--results-file`: Results file to analyze [default: media_transform_results.json]
- `--error-report-output`: Error report output file [default: error_report.json]
- `--compare`: Path to Cloudflare KV JSON file for comparison

**Special Features:**
- **Text Output Format**: When using `--format text` with `--list-files`, outputs one file path per line, perfect for scripts
- **URL Generation**: Use `--base-url` to generate complete URLs for each file
- **URL Encoding**: Automatically handles spaces and special characters in file names

**Examples:**

```bash
# File size analysis
media-toolkit analyze \
  --remote r2 \
  --bucket media \
  --list-files \
  --size-threshold 100 \
  --size-report-output analysis_report.md

# Generate text file list with URLs
media-toolkit analyze \
  --remote r2 \
  --bucket media \
  --list-files \
  --format text \
  --base-url https://cdn.example.com/media/ \
  --size-report-output urls.txt

# Generate list with filtering
media-toolkit analyze \
  --remote r2 \
  --bucket videos \
  --list-files \
  --format text \
  --base-url https://cdn.example.com/ \
  --extensions .mp4,.mov \
  --exclude-patterns "*test*,*backup*" \
  --exclude-max-size 5368709120 \
  --size-report-output video_urls.txt

# Generate error report
media-toolkit analyze \
  --generate-error-report \
  --results-file media_transform_results.json \
  --error-report-output error_analysis.md \
  --format markdown

# Compare with KV data
media-toolkit analyze \
  --compare kv-data.json \
  --results-file media_transform_results.json \
  --comparison-output comparison_report.md
```

### `loadtest` - Performance Testing

Run native Go load tests with staged traffic patterns.

```bash
media-toolkit loadtest [flags]
```

**Required Flags:**
- `--base-url`: Base URL for load testing

**Key Options:**
- `--results-file`: Pre-warming results file [default: media_transform_results.json]
- `--output-file`: Output file for results (default: stdout)
- `--use-error-report`: Use error report to exclude problematic files
- `--error-report-file`: Path to error report file
- `--skip-large-files`: Skip large files in load test [default: true]
- `--large-file-threshold-mb`: Threshold for large files in MB [default: 256]
- `--request-timeout`: Request timeout [default: 120s]

**Stage Configuration:**
- `--stage1-users`, `--stage1-duration`: Ramp-up stage
- `--stage2-users`, `--stage2-duration`: Sustained load stage
- `--stage3-users`, `--stage3-duration`: Ramp-down stage

**Examples:**

```bash
# Basic load test
media-toolkit loadtest \
  --base-url https://cdn.example.com/ \
  --results-file media_transform_results.json

# Advanced staged load test
media-toolkit loadtest \
  --base-url https://cdn.example.com/ \
  --results-file media_transform_results.json \
  --stage1-users 25 --stage1-duration 1m \
  --stage2-users 50 --stage2-duration 2m \
  --stage3-users 25 --stage3-duration 1m \
  --use-error-report \
  --skip-large-files

# High-performance load test
media-toolkit loadtest \
  --base-url https://cdn.example.com/ \
  --results-file media_transform_results.json \
  --stage1-users 50 --stage1-duration 2m \
  --stage2-users 100 --stage2-duration 5m \
  --stage3-users 50 --stage3-duration 2m \
  --request-timeout 180s
```

### `optimize` - Video Optimization

Optimize video files using FFmpeg with various codecs and settings.

```bash
media-toolkit optimize [flags]
```

**Required Flags:**
- `--remote`: rclone remote name
- `--bucket`: S3 bucket name

**Key Options:**
- `--optimize-videos`: Enable video optimization
- `--optimize-in-place`: Replace files in-place
- `--codec`: Video codec (h264, h265, vp9, av1) [default: h264]
- `--quality`: Quality profile (maximum, high, balanced, efficient, minimum) [default: balanced]
- `--target-resolution`: Target resolution (4k, 1080p, 720p, 480p) [default: 1080p]
- `--hardware-acceleration`: Hardware acceleration (auto, nvidia, intel, amd, apple, none) [default: auto]
- `--browser-compatible`: Ensure browser compatibility [default: true]
- `--size-threshold`: Size threshold in MiB for optimization [default: 256]
- `--workers`: Number of concurrent workers [default: 5]

**Supported Codecs:**
- **H.264**: Most compatible, good compression
- **H.265/HEVC**: Better compression, less compatible
- **VP9**: Web-optimized, good compression
- **AV1**: Cutting-edge compression, limited support

**Examples:**

```bash
# Standard optimization
media-toolkit optimize \
  --remote r2 \
  --bucket videos \
  --optimize-videos \
  --codec h264 \
  --quality balanced \
  --target-resolution 1080p

# High-quality with hardware acceleration
media-toolkit optimize \
  --remote r2 \
  --bucket videos \
  --optimize-videos \
  --codec h265 \
  --quality high \
  --hardware-acceleration nvidia

# In-place optimization for storage savings
media-toolkit optimize \
  --remote r2 \
  --bucket videos \
  --optimize-in-place \
  --codec h264 \
  --quality efficient \
  --size-threshold 100
```

### `validate` - Content Validation

Validate media files and transformations.

```bash
media-toolkit validate [flags]
```

**Options (mutually exclusive):**
- `--validate-directory`: Local directory to validate
- `--validate-results`: Pre-warming results file to validate
- `--remote` / `--bucket`: Remote storage to validate

**Key Options:**
- `--validation-workers`: Number of concurrent workers [default: 10]
- `--validation-report`: Output file for validation report [default: validation_report.md]
- `--validation-format`: Report format (text, markdown, json) [default: markdown]

**Examples:**

```bash
# Validate local directory
media-toolkit validate \
  --validate-directory /path/to/videos \
  --validation-workers 20

# Validate from results
media-toolkit validate \
  --validate-results media_transform_results.json \
  --validation-report validation_report.json

# Validate remote storage
media-toolkit validate \
  --remote r2 \
  --bucket videos \
  --validation-workers 30
```

## Configuration

### Configuration File

Create a `config.yaml` file for persistent settings:

```yaml
# Storage configuration
remote: r2
bucket: media
directory: content
base_url: https://cdn.example.com/

# Processing options
media_type: auto
workers: 500
timeout: 240
connection_close_delay: 15

# Worker allocation
optimize_by_size: true
small_file_workers: 200
medium_file_workers: 200
large_file_workers: 100
small_file_threshold: 50
medium_file_threshold: 200

# Media processing
derivatives: [desktop, tablet, mobile]
image_variants: [thumbnail, small, medium, large, webp]

# File filtering
extensions: [.mp4, .mov, .jpg, .png]
exclude_extensions: [.tmp, .temp]
exclude_patterns: ["*test*", "*backup*"]
exclude_directories: ["tmp/", "backup/", "archive/"]
exclude_min_size: 100  # bytes
exclude_max_size: 5368709120  # 5GB

# Optimization
codec: h264
quality: balanced
target_resolution: 1080p
hardware_acceleration: auto
browser_compatible: true

# Load testing
stage1_users: 25
stage1_duration: 1m
stage2_users: 50
stage2_duration: 2m
stage3_users: 25
stage3_duration: 1m
```

### Environment Variables

All configuration options can be set via environment variables:

```bash
export MEDIA_TOOLKIT_REMOTE=r2
export MEDIA_TOOLKIT_BUCKET=videos
export MEDIA_TOOLKIT_BASE_URL=https://cdn.example.com/
export MEDIA_TOOLKIT_WORKERS=500
export MEDIA_TOOLKIT_TIMEOUT=300
```

## File Filtering

All commands support comprehensive file filtering options for precise control over which files to process.

### Filtering Options

The following filtering flags are available across `analyze`, `prewarm`, `optimize`, `validate`, and `workflow` commands:

**Inclusion Filters:**
- `--extensions`: Include only files with specific extensions (e.g., `.mp4,.jpg`)
- `--media-type`: Quick preset for common media types (`image`, `video`, `auto`)

**Exclusion Filters:**
- `--exclude-extensions`: Exclude files with specific extensions
- `--exclude-patterns`: Exclude files matching wildcard patterns
- `--exclude-directories`: Exclude entire directories
- `--exclude-min-size`: Exclude files smaller than specified size (in bytes)
- `--exclude-max-size`: Exclude files larger than specified size (in bytes)

### Examples

```bash
# Process only video files, excluding temporary files
media-toolkit prewarm \
  --remote r2 \
  --bucket media \
  --base-url https://cdn.example.com/ \
  --extensions .mp4,.mov,.webm \
  --exclude-extensions .tmp,.temp

# Exclude test and backup files
media-toolkit analyze \
  --remote r2 \
  --bucket media \
  --exclude-patterns "*test*,*backup*,*_old*" \
  --exclude-directories "tmp/,backup/,archive/"

# Process only files within a specific size range
media-toolkit optimize \
  --remote r2 \
  --bucket videos \
  --exclude-min-size 1048576      # Exclude files < 1MB
  --exclude-max-size 5368709120   # Exclude files > 5GB

# Complex filtering with multiple criteria
media-toolkit workflow \
  --remote r2 \
  --bucket production \
  --base-url https://cdn.example.com/ \
  --extensions .mp4,.mov \
  --exclude-patterns "*test*,*sample*" \
  --exclude-directories "archive/,old/" \
  --exclude-max-size 10737418240  # Exclude files > 10GB
```

### Pattern Matching

The `--exclude-patterns` flag supports standard glob patterns:
- `*` matches any sequence of characters
- `?` matches any single character
- `[abc]` matches any character in the set
- `[a-z]` matches any character in the range

Examples:
- `*test*` - matches any file containing "test"
- `backup_*` - matches files starting with "backup_"
- `*.tmp.*` - matches files with .tmp. in their name
- `video_[0-9]*` - matches video_0, video_1, etc.

### Configuration File

Filtering options can also be set in the configuration file:

```yaml
# File filtering
extensions: [.mp4, .mov, .jpg, .png]
exclude_extensions: [.tmp, .temp, .bak]
exclude_patterns: ["*test*", "*backup*", "._*"]
exclude_directories: ["tmp/", "backup/", "archive/", ".git/"]
exclude_min_size: 1024  # 1KB minimum
exclude_max_size: 10737418240  # 10GB maximum
```

### Performance Impact

Filtering is applied efficiently at the file listing stage, meaning:
- Excluded files are never downloaded or processed
- No resources are wasted on unwanted files
- Significantly faster execution when processing subsets of data

## Complete Workflow Guide

### Stage 1: File Analysis

Analyze your media files to understand size distribution and identify optimization opportunities.

```bash
# Basic analysis
media-toolkit analyze \
  --remote r2 \
  --bucket media \
  --list-files \
  --size-threshold 100 \
  --size-report-output file_analysis.md

# Detailed JSON analysis
media-toolkit analyze \
  --remote r2 \
  --bucket media \
  --list-files \
  --size-threshold 50 \
  --size-report-output file_analysis.json \
  --limit 1000
```

**Output includes:**
- Size distribution statistics
- Large file identification
- Extension-based analysis
- Optimization recommendations

### Stage 2: Pre-warming

Populate CDN cache by making HTTP requests to all derivatives/variants.

```bash
# Production pre-warming
media-toolkit prewarm \
  --remote r2 \
  --bucket media \
  --base-url https://cdn.example.com/ \
  --workers 3000 \
  --timeout 300 \
  --optimize-by-size \
  --output media_transform_results.json
```

**Performance Guidelines:**
- Start with 500 workers, scale based on error rate
- Use 240s+ timeout for large files
- Enable size-based optimization for mixed datasets
- Monitor error rates (target <5%)

### Stage 3: Error Analysis

Analyze pre-warming results to identify issues and optimization opportunities.

```bash
# Generate comprehensive error report
media-toolkit analyze \
  --generate-error-report \
  --results-file media_transform_results.json \
  --error-report-output error_analysis.md \
  --format markdown
```

**Key Metrics:**
- Error rate by type and status code
- Size category distribution
- Timeout analysis
- Troubleshooting recommendations

### Stage 4: Load Testing

Validate cache effectiveness under realistic load.

```bash
# Production load test
media-toolkit loadtest \
  --base-url https://cdn.example.com/ \
  --results-file media_transform_results.json \
  --stage1-users 50 --stage1-duration 2m \
  --stage2-users 100 --stage2-duration 5m \
  --stage3-users 50 --stage3-duration 2m \
  --use-error-report \
  --skip-large-files
```

**Expected Results:**
- Cache hit rate >90%
- TTFB <500ms for cached content
- 95th percentile <2s
- Error rate <1%

### Stage 5: Optimization (Optional)

Optimize large video files for better performance.

```bash
# Video optimization
media-toolkit optimize \
  --remote r2 \
  --bucket videos \
  --optimize-videos \
  --codec h264 \
  --quality balanced \
  --target-resolution 1080p \
  --size-threshold 100 \
  --workers 10
```

### Complete Production Script

```bash
#!/bin/bash
set -e

# Configuration
REMOTE="your-remote"
BUCKET="prod-media"
BASE_URL="https://cdn.example.com/"
DATE=$(date +%Y%m%d_%H%M%S)

# Run complete workflow
media-toolkit workflow \
  --remote $REMOTE \
  --bucket $BUCKET \
  --base-url $BASE_URL \
  --workers 3000 \
  --timeout 300 \
  --optimize-by-size \
  --output-dir results_$DATE

echo "✅ Workflow completed! Check results_$DATE/ for reports."
```

## URL Formats

### Video Derivatives

The toolkit supports multiple URL formats for video derivatives:

1. **imwidth format** (default):
   ```
   https://cdn.example.com/video.mp4?imwidth=1920   # desktop
   https://cdn.example.com/video.mp4?imwidth=1280   # tablet
   https://cdn.example.com/video.mp4?imwidth=854    # mobile
   ```

2. **derivative format**:
   ```
   https://cdn.example.com/video.mp4/desktop/
   https://cdn.example.com/video.mp4/tablet/
   https://cdn.example.com/video.mp4/mobile/
   ```

3. **query format**:
   ```
   https://cdn.example.com/video.mp4?derivative=desktop
   https://cdn.example.com/video.mp4?derivative=tablet
   https://cdn.example.com/video.mp4?derivative=mobile
   ```

### Image Variants

The toolkit supports multiple transformation URL patterns:

1. **Query Parameters**:
   ```
   ?width=800&height=600&fit=cover
   ?width=400&format=webp&quality=85
   ?width=1200&height=630&fit=contain
   ```

2. **Path-Based**:
   ```
   /_width=800/_height=600/_fit=cover/image.jpg
   /_width=400/_format=webp/image.jpg
   /_width=1200/_height=630/image.jpg
   ```

3. **Akamai Image Manager**:
   ```
   ?im=resize=width:800,height:600,mode:fit
   ?im=resize=width:400&im=format=webp
   ?im=quality=80&im=resize=width:1200
   ```

4. **Compact Syntax**:
   ```
   ?r=16:9&p=0.5,0.3&f=l    # ratio, position, fit
   ?w=800&h=600&q=85        # width, height, quality
   ```

## Performance Optimization

### Worker Allocation

The toolkit uses intelligent worker allocation based on file sizes:

```bash
# Automatic allocation (recommended)
--workers 1000 --optimize-by-size

# Manual allocation for fine control
--small-file-workers 400   # Files ≤50MB (high concurrency)
--medium-file-workers 300  # Files 50-200MB (balanced)
--large-file-workers 300   # Files >200MB (resource-intensive)
```

### Performance Tuning

1. **Worker Count Guidelines**:
   - Start: 500 workers
   - Scale: +250-500 based on error rate
   - Max: 5000 workers (with adequate resources)

2. **Queue Management**:
   - Default queue size: workers × 3
   - Increase for large file sets: `--queue-multiplier 10.0`
   - Batch processing prevents queue overflow
   - Monitor queue status in error logs

3. **Timeout Configuration**:
   - Small files: 120s
   - Medium files: 240s
   - Large files: 300-600s

4. **Connection Management**:
   - `--connection-close-delay 15`: Reuse connections
   - `--retry 2`: Handle transient failures
   - Monitor connection pool metrics

### Hardware Acceleration

Video optimization supports multiple acceleration types:

```bash
# Auto-detect best available
--hardware-acceleration auto

# Specific acceleration
--hardware-acceleration nvidia   # NVENC
--hardware-acceleration intel    # QuickSync
--hardware-acceleration amd      # AMF
--hardware-acceleration apple    # VideoToolbox
```

## Image Variants

The toolkit includes 60+ predefined image variants:

### Size Variants
- `thumbnail`: 150x150 (cover fit)
- `small`: 400px width
- `medium`: 800px width
- `large`: 1200px width
- `xlarge`: 1920px width
- `xxlarge`: 2560px width

### Mobile-Optimized
- `mobile_small`: 320px width
- `mobile_medium`: 640px width
- `mobile_large`: 960px width

### Aspect Ratios
- `square`: 600x600 (1:1)
- `landscape`: 800x450 (16:9)
- `portrait`: 450x800 (9:16)
- `banner`: 1200x300 (4:1)

### Format Conversions
- `webp`: WebP format
- `avif`: AVIF format
- `jpeg`: JPEG format (85 quality)
- `png`: PNG format

### Social Media
- `og_image`: 1200x630 (Open Graph)
- `twitter_card`: 800x418 (Twitter)
- `instagram_square`: 1080x1080
- `instagram_portrait`: 1080x1350
- `facebook_cover`: 851x315

### Akamai Compatible
- `akamai_resize_small`: 400x300
- `akamai_resize_medium`: 800x600
- `akamai_resize_large`: 1200x900
- `akamai_quality`: 80% quality
- `akamai_format`: WebP conversion

## Examples

### Scenario 1: Production Video Pipeline

```bash
# Step 1: Analyze dataset
media-toolkit analyze \
  --remote r2 \
  --bucket videos \
  --directory production \
  --list-files \
  --size-threshold 100 \
  --size-report-output video_analysis.md

# Step 2: Pre-warm with optimization
media-toolkit prewarm \
  --remote r2 \
  --bucket videos \
  --directory production \
  --base-url https://cdn.example.com/videos/ \
  --media-type video \
  --derivatives desktop,tablet,mobile \
  --workers 3000 \
  --optimize-by-size \
  --timeout 300 \
  --output video_prewarm_results.json

# Step 3: Analyze errors
media-toolkit analyze \
  --generate-error-report \
  --results-file video_prewarm_results.json \
  --error-report-output video_errors.md

# Step 4: Load test
media-toolkit loadtest \
  --base-url https://cdn.example.com/videos/ \
  --results-file video_prewarm_results.json \
  --stage1-users 50 --stage1-duration 2m \
  --stage2-users 100 --stage2-duration 5m \
  --stage3-users 50 --stage3-duration 2m

# Step 5: Optimize large files
media-toolkit optimize \
  --remote r2 \
  --bucket videos \
  --directory production \
  --optimize-videos \
  --codec h264 \
  --quality balanced \
  --size-threshold 256 \
  --workers 10
```

### Scenario 2: Image CDN Pre-warming

```bash
# Pre-warm all image variants
media-toolkit prewarm \
  --remote r2 \
  --bucket images \
  --base-url https://cdn.example.com/ \
  --media-type image \
  --image-variants thumbnail,small,medium,large,webp,avif,og_image,twitter_card \
  --workers 500 \
  --timeout 180

# Pre-warm social media variants
media-toolkit prewarm \
  --remote r2 \
  --bucket images \
  --base-url https://cdn.example.com/ \
  --media-type image \
  --image-variants og_image,twitter_card,instagram_square,facebook_cover \
  --workers 200
```

### Scenario 3: Development and Testing

```bash
# Test with limited dataset
media-toolkit prewarm \
  --remote local \
  --bucket /path/to/test/media \
  --base-url https://dev-cdn.example.com/ \
  --media-type auto \
  --workers 10 \
  --limit 100 \
  --output test_results.json

# Validate test results
media-toolkit validate \
  --validate-results test_results.json \
  --validation-workers 5

# Debug load test
media-toolkit loadtest \
  --base-url https://dev-cdn.example.com/ \
  --results-file test_results.json \
  --stage1-users 2 --stage1-duration 30s \
  --stage2-users 5 --stage2-duration 1m \
  --output-file test_load_results.json
```

## Storage Backends

### rclone (Default)

Works with 40+ cloud storage providers:

```bash
# Configure rclone
rclone config

# Use with toolkit
media-toolkit prewarm --remote r2 --bucket media --base-url https://cdn.example.com/
```

### AWS CLI

Native AWS S3 support:

```bash
# Configure AWS
aws configure

# Use with toolkit
media-toolkit prewarm \
  --remote aws \
  --bucket my-bucket \
  --base-url https://cdn.example.com/ \
  --use-aws-cli
```

### Local Filesystem

For testing and development:

```bash
media-toolkit prewarm \
  --remote local \
  --bucket /path/to/media \
  --base-url https://localhost:8080/
```

## Troubleshooting

### Common Issues

1. **High Error Rate (>10%)**
   - Reduce worker count by 50%
   - Increase timeout to 300-600s
   - Enable `--optimize-by-size`
   - Check server capacity

2. **Queue Full Errors**
   - Increase `--queue-multiplier` (e.g., `--queue-multiplier 10.0`)
   - Reduce total workers to decrease submission rate
   - Adjust size-based allocation to balance queues
   - Monitor queue distribution in logs
   - The toolkit now uses batch processing to prevent overflow

3. **Memory Issues**
   - Enable size-based optimization
   - Process in smaller batches with `--limit`
   - Reduce concurrent workers

4. **Connection Timeouts**
   - Increase `--timeout`
   - Add `--connection-close-delay 20`
   - Reduce concurrent connections
   - Check network bandwidth

5. **HTTP 500 Errors**
   - Verify access permissions
   - Check URL construction
   - Reduce load on server
   - Contact CDN provider

### Performance Tuning Checklist

- [ ] Start with conservative settings (500 workers, 240s timeout)
- [ ] Monitor error rates and adjust workers accordingly
- [ ] Enable size-based optimization for mixed datasets
- [ ] Use connection pooling with appropriate delays
- [ ] Set timeouts based on largest file sizes
- [ ] Implement retry logic for transient failures
- [ ] Use error reports to exclude problematic files
- [ ] Monitor system resources during execution

## Development

### Building from Source

```bash
# Standard build
make build

# Build with version info
make build VERSION=1.0.0

# Cross-platform builds
make build-all

# Run tests
make test

# Generate coverage
make coverage

# Clean artifacts
make clean
```

### Project Guidelines

- **Code Style**: Follow standard Go conventions
- **Error Handling**: Always wrap errors with context
- **Logging**: Use structured logging with zap
- **Testing**: Aim for >80% coverage
- **Documentation**: Update README for new features

### Running Tests

```bash
# All tests
go test ./...

# Specific package
go test ./pkg/media/...

# With coverage
go test -cover ./...

# Verbose output
go test -v ./...
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes following Go conventions
4. Add tests for new functionality
5. Run tests and linting (`make test && make lint`)
6. Commit with descriptive message
7. Push to your branch
8. Open a Pull Request

### Contribution Guidelines

- Write clear commit messages
- Add tests for new features
- Update documentation
- Follow existing code style
- Keep PRs focused and small

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Cobra](https://github.com/spf13/cobra) for CLI framework
- Uses [Viper](https://github.com/spf13/viper) for configuration management
- Powered by [Zap](https://github.com/uber-go/zap) for structured logging
- FFmpeg integration for video processing
- Native Go implementation for superior performance

---

**Status**: Production Ready ✅ - Complete feature parity with enhanced performance and reliability.