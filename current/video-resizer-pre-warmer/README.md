# Video Transformation Verification Tool

A comprehensive tool for processing and verifying video assets across different resolutions and comparing them with Cloudflare KV storage. This tool helps ensure your transformed video assets match what's stored in your KV cache.

## Purpose

This script serves two main functions:

1. **Video Transformation Processing**: Makes HTTP requests to transform videos into different derivatives (desktop, tablet, mobile), capturing size and metadata information.

2. **KV Storage Verification**: Compares transformed videos with what's stored in Cloudflare KV to ensure consistency, identifying mismatches and missing assets.

## Installation

### Requirements

- Python 3.7+
- Required Python packages:
  ```
  requests
  tabulate
  ```

Install dependencies:

```bash
pip install requests tabulate
```

## Basic Usage

The script can be used in two primary modes:

### 1. Process and Compare Mode

Processes video files from S3/rclone storage and compares them with Cloudflare KV data:

```bash
python video_transform.py --remote ikea-mcdc --bucket your-bucket \
  --base-url https://example.com/videos/ --compare kv_export.json
```

### 2. Comparison-Only Mode

Only compares previously generated results with KV data (no processing):

```bash
python video_transform.py --only-compare --output previous_results.json \
  --compare kv_export.json
```

## Command-Line Options

```
--remote             rclone remote name (required)
--bucket             S3 bucket name (required)
--directory          Directory path within bucket (default: '')
--base-url           Base URL to prepend to object paths (required)
--derivatives        Comma-separated list of derivatives (default: 'desktop,tablet,mobile')
--workers            Number of concurrent workers (default: 5)
--timeout            Request timeout in seconds (default: 120)
--output             Output JSON file path (default: 'video_transform_results.json')
--limit              Limit number of objects to process (default: 0 = no limit)
--extension          File extension to filter by (default: '.mp4')
--verbose, -v        Enable verbose logging
--retry              Number of retry attempts for failed requests (default: 2)
--compare            Path to Cloudflare KV JSON file for comparison
--comparison-output  Output file for detailed comparison results (default: 'comparison_results.json')
--summary-output     Output file for summary report (default: 'comparison_summary.md')
--summary-format     Format for the summary output: 'markdown' or 'json' (default: 'markdown')
--only-compare       Skip processing and only compare existing results with KV data
```

## Common Use Cases

### Process All Videos and Generate a Report

```bash
python video_transform.py --remote ikea-mcdc --bucket video-assets \
  --base-url https://videos.example.com/ \
  --compare cloudflare_kv_export.json \
  --summary-output verification_report.md
```

### Process Specific Video Directory with Higher Concurrency

```bash
python video_transform.py --remote ikea-mcdc --bucket video-assets \
  --directory product-videos/2025 --base-url https://videos.example.com/ \
  --workers 10 --timeout 180
```

### Quick Test with Limited Videos

```bash
python video_transform.py --remote ikea-mcdc --bucket video-assets \
  --base-url https://videos.example.com/ --limit 5 --verbose
```

### Generate JSON Summary for Automated Monitoring

```bash
python video_transform.py --only-compare \
  --output previous_results.json --compare cloudflare_kv_export.json \
  --summary-output verification.json --summary-format json
```

## Output Formats

### 1. Transformation Results (JSON)

Contains detailed information about each processed video derivative:
- HTTP status
- Content length and type
- Size information
- Dimensions
- Processing duration

### 2. Comparison Results (JSON)

Detailed comparison between transformation results and KV data:
- Matching and mismatched keys
- Size differences
- Content type differences
- Missing keys in either system

### 3. Summary Report

#### Markdown Format

A human-readable report containing:
- Verification status (Success/Failure)
- Key counts and match rates
- Size verification
- Examples of mismatches (if any)
- Lists of missing keys (if any)

#### JSON Format

A machine-readable summary suitable for automated monitoring:
- Verification success status
- Summary statistics
- Size verification metrics
- Examples of mismatches or missing keys

## Key Features

- **Chunked File Handling**: Properly calculates total sizes for large files split into chunks in KV
- **Concurrent Processing**: Efficiently processes multiple videos simultaneously
- **Retry Logic**: Handles transient failures with exponential backoff
- **Detailed Logging**: Comprehensive logging with verbose mode for troubleshooting
- **Progress Updates**: Real-time progress tracking with ETA during processing
- **Clear Verification**: Explicit pass/fail indication for verification results

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Increase the `--timeout` value
   - Reduce the number of `--workers`

2. **Path Resolution Problems**
   - Ensure the correct `--remote` and `--bucket` names
   - Check if `--directory` path exists in your S3/rclone storage

3. **Missing Keys in Comparison**
   - Verify the path structure matches between KV keys and S3 paths
   - Check if path prefixes need adjustment

4. **Size Mismatches**
   - Verify content encoding (e.g., gzip compression might affect sizes)
   - Check if CDN transforms content

### Debugging

Use the `--verbose` flag to enable detailed logging:

```bash
python video_transform.py --remote ikea-mcdc --bucket video-assets \
  --base-url https://videos.example.com/ --verbose
```

## License

This tool is provided for internal use and is not published under any specific open-source license.
