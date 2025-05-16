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
- For AWS CLI usage: AWS CLI installed and configured
- For rclone usage: rclone installed and configured

Install dependencies:

```bash
pip install requests tabulate
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

## Basic Usage

The script can be used in three primary modes:

### 1. Process and Compare Mode

Processes video files from S3/rclone storage and compares them with Cloudflare KV data:

Using rclone (default):
```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket your-bucket \
  --base-url https://example.com/videos/ --compare kv_export.json
```

Using AWS CLI:
```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket your-bucket \
  --base-url https://example.com/videos/ --compare kv_export.json --use-aws-cli
```

### 2. Comparison-Only Mode

Only compares previously generated results with KV data (no processing):

```bash
python video-resizer-kv-pre-warmer.py --only-compare --output previous_results.json \
  --compare kv_export.json
```

### 3. Error Report Generation Mode

Generates detailed reports about HTTP 500 errors from existing results files:

```bash
python video-resizer-kv-pre-warmer.py --generate-error-report \
  --output video_transform_results.json
```

## Command-Line Options

### Required Parameters

These parameters are required for the main processing mode, but may be optional for certain operations like error report generation:

```
--remote             rclone remote name (required for processing mode)
--bucket             S3 bucket name (required for processing mode)
--base-url           Base URL to prepend to object paths (required for processing mode)
```

### Optional Parameters

```
--directory          Directory path within bucket (default: '')
--derivatives        Comma-separated list of derivatives (default: 'desktop,tablet,mobile')
--workers            Number of concurrent workers (default: 5)
--timeout            Request timeout in seconds (default: 120)
--connection-close-delay  Additional delay in seconds before closing connections (default: 10)
--output             Output JSON file path (default: 'video_transform_results.json')
--limit              Limit number of objects to process (default: 0 = no limit)
--extension          File extension to filter by (default: '.mp4')
--verbose, -v        Enable verbose logging
--retry              Number of retry attempts for failed requests (default: 2)
```

### Comparison Mode Parameters

```
--compare            Path to Cloudflare KV JSON file for comparison
--comparison-output  Output file for detailed comparison results (default: 'comparison_results.json')
--summary-output     Output file for summary report (default: 'comparison_summary.md')
--summary-format     Format for the summary output: 'markdown' or 'json' (default: 'markdown')
```

### Mode Selection Parameters

```
--only-compare       Skip processing and only compare existing results with KV data
--generate-error-report  Generate or regenerate the 500 error reports from an existing results file
--use-aws-cli        Use AWS CLI instead of rclone for listing S3 objects
```

## AWS CLI vs. rclone: When to Use Each

| Feature | AWS CLI | rclone |
|---------|---------|--------|
| **Best for** | Native AWS S3 operations | Multiple cloud providers (including S3) |
| **Speed** | Generally faster for AWS S3 | May be slower for AWS S3 but supports more services |
| **Authentication** | Uses AWS credentials | Supports many auth methods for various providers |
| **Configuration** | Simpler for AWS-only operations | More flexible but requires more setup |
| **Path format** | `s3://bucket/path` | `remote:bucket/path` |

Choose AWS CLI when:
- Working exclusively with AWS S3
- Need optimal performance with AWS services
- Already have AWS CLI installed and configured

Choose rclone when:
- Working with multiple cloud providers
- Need support for non-AWS storage
- Need advanced features like encryption or caching
- Have rclone already configured

## Common Use Cases

### Production Pre-warming and Verification

#### Process All Videos and Generate a Report with AWS CLI

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket video-assets \
  --base-url https://videos.example.com/ \
  --compare cloudflare_kv_export.json \
  --summary-output verification_report.md --use-aws-cli
```
Use this when you need to pre-warm an entire bucket and compare results with KV data.

#### Process Specific Video Directory with rclone and Higher Concurrency

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket video-assets \
  --directory videos/2025 --base-url https://videos.example.com/ \
  --workers 10 --timeout 180
```
Ideal for processing specific folders of content with higher performance.

#### Process Large Videos with Increased Timeout and Connection Delay

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket video-assets \
  --base-url https://videos.example.com/ --timeout 300 --connection-close-delay 20
```
Recommended for high-resolution or long-duration videos that need extended processing time.

### Testing and Development

#### Quick Test with Limited Videos Using AWS CLI

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket video-assets \
  --base-url https://videos.example.com/ --limit 5 --verbose --use-aws-cli
```
Perfect for testing configuration or CDN changes before full processing.

#### Process Videos with Custom Derivative Sizes

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket video-assets \
  --base-url https://videos.example.com/ --derivatives desktop,mobile
```
Useful when you only need to pre-warm specific derivative types.

### Monitoring and Troubleshooting

#### Generate JSON Summary for Automated Monitoring

```bash
python video-resizer-kv-pre-warmer.py --only-compare \
  --output previous_results.json --compare cloudflare_kv_export.json \
  --summary-output verification.json --summary-format json
```
Use with monitoring systems or dashboards that can ingest JSON data.

#### Generate Error Reports from Existing Results

```bash
python video-resizer-kv-pre-warmer.py --generate-error-report \
  --output video_transform_results.json
```
This mode doesn't require the standard `--remote`, `--bucket`, and `--base-url` parameters as it only processes existing results files. Use it to analyze errors from a previous processing run.

## Error Report Generation

The tool includes a specialized mode for generating detailed error reports about HTTP 500 errors encountered during video processing. This feature is useful for troubleshooting problems with specific videos or derivatives.

### What It Does

- Analyzes existing results files for HTTP 500 errors
- Generates two specialized report files:
  - A JSON report with structured data about each error (`*_500_errors.json`)
  - A human-readable text report with useful debugging information (`*_500_errors.txt`)
- Groups errors by derivative type for easier analysis
- Provides complete request URLs for easy testing and reproduction
- Includes CloudFlare-specific headers and ray IDs for troubleshooting with CDN support

### When to Use Error Report Generation

- After a large batch processing job to identify problematic videos
- When troubleshooting specific CDN or transformation issues
- For providing detailed error information to CloudFlare support
- To identify patterns in video processing failures

### Output Files

The error report generation creates two files:

1. **JSON Report** (`video_transform_results_500_errors.json`): Contains structured data about each error for programmatic processing

2. **Text Report** (`video_transform_results_500_errors.txt`): A human-readable report with:
   - Total error count
   - Error counts by derivative type
   - Complete list of errors with full request URLs for testing
   - CloudFlare ray IDs and headers for each error

## Output File Formats

### 1. Transformation Results (JSON): `video_transform_results.json`

Contains detailed information about each processed video derivative:
```json
{
  "metadata": {
    "processed": 18,
    "total": 18,
    "derivatives_requested": ["desktop", "tablet", "mobile"],
    "timestamp": "2025-05-14 15:54:04",
    "elapsed_seconds": 187.46,
    "estimated_total_seconds": 187.46,
    "estimated_remaining_seconds": 0.0
  },
  "results": {
    "video:path/to/file.mp4:derivative=desktop": {
      "status": 200,
      "contentLength": 30125892,
      "contentType": "video/mp4",
      "actualTotalVideoSize": 30125892,
      "isChunked": false,
      "duration": 10.63,
      "derivative": "desktop",
      "width": 1920,
      "height": 1080,
      "sourcePath": "/path/to/file.mp4",
      "requestDimensions": "1920",
      "etag": "abcdef123456",
      "attempt": 1
    },
    // Additional derivative entries...
  }
}
```

### 2. Comparison Results (JSON): `comparison_results.json`

Detailed comparison between transformation results and KV data:
```json
{
  "timestamp": "2025-05-14 15:54:04",
  "summary": {
    "keys_in_kv": 50,
    "keys_in_transform": 54,
    "matches": 48,
    "mismatches": 2,
    "only_in_kv": 0,
    "only_in_transform": 4
  },
  "matches": [
    {
      "key": "video:path/to/file.mp4:derivative=desktop",
      "transform_size": 30125892,
      "kv_size": 30125892,
      "size_diff": 0,
      "size_diff_percent": 0.0,
      "transform_content_type": "video/mp4",
      "kv_content_type": "video/mp4",
      "transform_is_chunked": false,
      "kv_is_chunked": false
    }
    // Additional match entries...
  ],
  "mismatches": [
    // Mismatch entries...
  ],
  "only_in_kv": [
    // Keys only in KV...
  ],
  "only_in_transform": [
    // Keys only in transform results...
  ]
}
```

### 3. KV Export JSON Format (Required for Comparison)

The expected Cloudflare KV export JSON structure:
```json
{
  "keys": [
    {
      "name": "video:path/to/file.mp4:derivative=desktop",
      "metadata": {
        "size": 30125892,
        "contentType": "video/mp4",
        "isChunked": false,
        "actualTotalVideoSize": 30125892
      }
    },
    {
      "name": "video:path/to/file.mp4:derivative=desktop_chunk_0",
      "metadata": {
        "size": 10000000,
        "contentType": "video/mp4",
        "isChunked": true
      }
    }
    // Additional KV entries...
  ]
}
```

### 4. Summary Report Formats

#### Markdown Format: `comparison_summary.md`

A human-readable report containing:
```markdown
# Video Asset Transformation Verification Report
Generated: 2025-05-14 15:54:04

## Verification Status: ✅ SUCCESSFUL

## Summary
- **Total unique keys**: 54
- **Keys in KV**: 50
- **Keys in transform results**: 54
- **Match rate**: 48/54 (88.9%)

## Size Verification
- **Total size in KV**: 1204.52 MB
- **Total size in transform results**: 1204.52 MB
- **Size difference**: 0.00 MB (0.000%)
```

#### JSON Format: `verification.json`

A machine-readable summary suitable for automated monitoring:
```json
{
  "timestamp": "2025-05-14 15:54:04",
  "verification_successful": true,
  "summary": {
    "total_unique_keys": 54,
    "keys_in_kv": 50,
    "keys_in_transform": 54,
    "matching_keys": 48,
    "match_rate_percent": 88.89,
    "mismatched_keys": 2,
    "only_in_kv": 0,
    "only_in_transform": 4
  },
  "size_verification": {
    "total_size_kv_bytes": 1262856765,
    "total_size_kv_mb": 1204.52,
    "total_size_transform_bytes": 1262856765,
    "total_size_transform_mb": 1204.52,
    "size_difference_bytes": 0,
    "size_difference_mb": 0.0,
    "size_difference_percent": 0.0
  }
}
```

## Recent Updates

### URL Parameter Format Change

The script now uses a simpler URL parameter format:
- Previous: `?derivative=X&width=Y&height=Z`
- New: `?imwidth=Y` (where Y is the desired width)

The `imwidth` parameter is used with these resolutions:
- Desktop: imwidth=1920
- Tablet: imwidth=1280
- Mobile: imwidth=854

### Improved 500 Error Reports

The 500 error reports now include the base URL in two places:
- A "Base URL: X" line at the top of the report
- A "Base URL used: X" line for each individual error entry

This makes it easier to troubleshoot errors by having complete URLs for testing.

### Flexible Parameter Requirements

The script has been updated to make certain parameters situationally required rather than globally required:

- The `--remote`, `--bucket`, and `--base-url` parameters are now only required for processing mode
- When using `--generate-error-report`, these parameters are not needed since the tool is only working with existing result files
- This allows for simpler command lines when using utility functions like error report generation

### Improved Error Handling

- Better validation of command-line parameters based on the selected operation mode
- Clear error messages when required parameters are missing for a specific operation
- Graceful handling of different operational scenarios

## Key Features

- **Chunked File Handling**: Properly calculates total sizes for large files split into chunks in KV
- **Concurrent Processing**: Efficiently processes multiple videos simultaneously
- **Retry Logic**: Handles transient failures with exponential backoff
- **Detailed Logging**: Comprehensive logging with verbose mode for troubleshooting
- **Progress Updates**: Real-time progress tracking with ETA during processing
- **Clear Verification**: Explicit pass/fail indication for verification results
- **Multiple Storage Options**: Support for both AWS CLI and rclone for file listing
- **Rich Reporting**: Customizable output formats for different use cases
- **Flexible Parameters**: Contextual parameter requirements based on operation mode
- **Graceful Shutdown**: Properly handles Ctrl+C interruption, saving partial results

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Increase the `--timeout` value
   - Reduce the number of `--workers`
   - Check network connectivity to your CDN

2. **Path Resolution Problems**
   - Ensure the correct `--remote` and `--bucket` names
   - Check if `--directory` exists in your S3/rclone storage
   - Verify permissions for accessing the storage

3. **Missing Keys in Comparison**
   - Verify the path structure matches between KV keys and S3 paths
   - Check if path prefixes need adjustment
   - Ensure the KV export JSON follows the expected format

4. **Size Mismatches**
   - Verify content encoding (e.g., gzip compression might affect sizes)
   - Check if CDN transforms content
   - Ensure chunked files are correctly identified in KV

5. **AWS CLI Issues**
   - Verify AWS CLI is installed: `aws --version`
   - Check AWS credentials are configured: `aws configure list`
   - Ensure you have correct permissions for your bucket
   - Test basic AWS CLI access: `aws s3 ls s3://your-bucket/`

6. **rclone Issues**
   - Verify rclone is installed: `rclone --version`
   - Check rclone configuration: `rclone config show`
   - Test basic rclone access: `rclone lsf your-remote:your-bucket/`
   - Check for path format issues (should be `remote:bucket/path`)

7. **Performance Issues**
   - Use the AWS CLI option for better performance with AWS S3
   - Adjust `--workers` based on available CPU and network bandwidth
   - For large files, increase `--connection-close-delay` to ensure complete transfers

8. **Command-Line Parameter Issues**
   - For general processing, `--remote`, `--bucket`, and `--base-url` are required
   - For error report generation with `--generate-error-report`, these parameters aren't needed
   - If you see "Missing required parameters" for processing mode, ensure all required parameters are present
   - Different operation modes have different parameter requirements as detailed in the Command-Line Options section

9. **Graceful Shutdown**
   - The script can be safely interrupted with Ctrl+C
   - When interrupted, the script will complete any in-progress tasks and save partial results
   - A message will indicate that shutdown is in progress
   - Results file will be updated with the processed items and marked with `early_shutdown: true`

### Debugging

Use the `--verbose` flag to enable detailed logging:

```bash
python video-resizer-kv-pre-warmer.py --remote s3 --bucket video-assets \
  --base-url https://videos.example.com/ --verbose
```

For AWS CLI troubleshooting, you can run direct AWS commands:

```bash
# Test AWS S3 access
aws s3 ls s3://your-bucket/

# Get detailed information about a specific object
aws s3api head-object --bucket your-bucket --key path/to/file.mp4
```

For rclone troubleshooting, you can run direct rclone commands:

```bash
# Test rclone access
rclone lsf your-remote:your-bucket/

# Get detailed information about a specific object
rclone lsl your-remote:your-bucket/path/to/file.mp4
```

## Performance Optimization

- **AWS CLI vs. rclone**: For large AWS S3 buckets, AWS CLI is generally faster for listing objects
- **Worker Count**: Adjust `--workers` based on your CPU cores and network capacity
- **Timeout Values**: Set appropriate `--timeout` and `--connection-close-delay` values based on video sizes
- **Selective Processing**: Use `--directory` and `--limit` to process subsets for testing
- **Memory Usage**: For very large buckets, process directories one at a time rather than the entire bucket

## License

This project is licensed under the MIT License - see the [LICENSE](../../LICENSE) file for details.
