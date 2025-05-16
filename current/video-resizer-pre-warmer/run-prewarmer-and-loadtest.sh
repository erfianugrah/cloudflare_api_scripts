#!/bin/bash
set -e

# Script to run video pre-warming followed by k6 load testing
# ------------------------------------------------------------

# Default settings - override as needed
BASE_URL="https://cdn.erfi.dev"
REMOTE="s3"
BUCKET="your-bucket"
DIRECTORY=""
DERIVATIVES="desktop,tablet,mobile"
WORKERS=5
TIMEOUT=120
EXTENSION=".mp4"
OUTPUT_FILE="video_transform_results.json"
URL_FORMAT="imwidth"  # or "derivative"
LIMIT=0  # 0 = no limit
USE_AWS_CLI=false

# k6 load test settings
STAGE1_USERS=50
STAGE1_DURATION="30s"
STAGE2_USERS=50
STAGE2_DURATION="1m"
STAGE3_USERS=100
STAGE3_DURATION="30s"
STAGE4_USERS=100
STAGE4_DURATION="1m"
STAGE5_USERS=0
STAGE5_DURATION="30s"

# Function to display help
show_help() {
  echo "Usage: $0 [options]"
  echo
  echo "This script runs the video pre-warmer to prepare CDN caches, then executes a k6 load test"
  echo "using the results from the pre-warming phase."
  echo
  echo "Options:"
  echo "  -h, --help                  Show this help message"
  echo
  echo "Pre-warmer options:"
  echo "  -u, --base-url URL          Base URL for video assets (default: $BASE_URL)"
  echo "  -r, --remote NAME           Rclone remote name (default: $REMOTE)"
  echo "  -b, --bucket NAME           S3 bucket name (default: $BUCKET)"
  echo "  -d, --directory PATH        Directory path within bucket (default: empty)"
  echo "  --derivatives LIST          Comma-separated list of derivatives (default: $DERIVATIVES)"
  echo "  -w, --workers NUM           Number of concurrent workers (default: $WORKERS)"
  echo "  -t, --timeout SECONDS       Request timeout in seconds (default: $TIMEOUT)"
  echo "  -e, --extension EXT         File extension to filter by (default: $EXTENSION)"
  echo "  -o, --output FILE           Output JSON file path (default: $OUTPUT_FILE)"
  echo "  -l, --limit NUM             Limit number of objects to process (default: $LIMIT, 0 = no limit)"
  echo "  --aws-cli                   Use AWS CLI instead of rclone"
  echo "  --skip-prewarming           Skip the pre-warming phase (use existing results file)"
  echo
  echo "k6 load test options:"
  echo "  --url-format FORMAT         URL format to use: 'imwidth' or 'derivative' (default: $URL_FORMAT)"
  echo "  --stage1-users NUM          Number of users in stage 1 (default: $STAGE1_USERS)"
  echo "  --stage1-duration TIME      Duration of stage 1 (default: $STAGE1_DURATION)"
  echo "  --stage2-users NUM          Number of users in stage 2 (default: $STAGE2_USERS)"
  echo "  --stage2-duration TIME      Duration of stage 2 (default: $STAGE2_DURATION)"
  echo "  --stage3-users NUM          Number of users in stage 3 (default: $STAGE3_USERS)"
  echo "  --stage3-duration TIME      Duration of stage 3 (default: $STAGE3_DURATION)"
  echo "  --stage4-users NUM          Number of users in stage 4 (default: $STAGE4_USERS)"
  echo "  --stage4-duration TIME      Duration of stage 4 (default: $STAGE4_DURATION)"
  echo "  --stage5-users NUM          Number of users in stage 5 (default: $STAGE5_USERS)"
  echo "  --stage5-duration TIME      Duration of stage 5 (default: $STAGE5_DURATION)"
  echo "  --skip-loadtest             Skip the load test phase"
  echo
  echo "Example usage:"
  echo "  $0 --base-url https://videos.example.com --bucket my-videos --workers 10"
  echo "  $0 --skip-prewarming --url-format derivative --stage1-users 100"
  echo
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      show_help
      exit 0
      ;;
    -u|--base-url)
      BASE_URL="$2"
      shift 2
      ;;
    -r|--remote)
      REMOTE="$2"
      shift 2
      ;;
    -b|--bucket)
      BUCKET="$2"
      shift 2
      ;;
    -d|--directory)
      DIRECTORY="$2"
      shift 2
      ;;
    --derivatives)
      DERIVATIVES="$2"
      shift 2
      ;;
    -w|--workers)
      WORKERS="$2"
      shift 2
      ;;
    -t|--timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    -e|--extension)
      EXTENSION="$2"
      shift 2
      ;;
    -o|--output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    -l|--limit)
      LIMIT="$2"
      shift 2
      ;;
    --aws-cli)
      USE_AWS_CLI=true
      shift
      ;;
    --skip-prewarming)
      SKIP_PREWARMING=true
      shift
      ;;
    --url-format)
      URL_FORMAT="$2"
      shift 2
      ;;
    --stage1-users)
      STAGE1_USERS="$2"
      shift 2
      ;;
    --stage1-duration)
      STAGE1_DURATION="$2"
      shift 2
      ;;
    --stage2-users)
      STAGE2_USERS="$2"
      shift 2
      ;;
    --stage2-duration)
      STAGE2_DURATION="$2"
      shift 2
      ;;
    --stage3-users)
      STAGE3_USERS="$2"
      shift 2
      ;;
    --stage3-duration)
      STAGE3_DURATION="$2"
      shift 2
      ;;
    --stage4-users)
      STAGE4_USERS="$2"
      shift 2
      ;;
    --stage4-duration)
      STAGE4_DURATION="$2"
      shift 2
      ;;
    --stage5-users)
      STAGE5_USERS="$2"
      shift 2
      ;;
    --stage5-duration)
      STAGE5_DURATION="$2"
      shift 2
      ;;
    --skip-loadtest)
      SKIP_LOADTEST=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      show_help
      exit 1
      ;;
  esac
done

# Check for Python and k6
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed. Aborting."; exit 1; }
command -v k6 >/dev/null 2>&1 || { echo "k6 is required but not installed. Aborting."; exit 1; }

# Check if output file exists when skipping pre-warming
if [ "$SKIP_PREWARMING" = true ] && [ ! -f "$OUTPUT_FILE" ]; then
  echo "Error: Output file $OUTPUT_FILE not found. Cannot skip pre-warming."
  exit 1
fi

# Stage 1: Pre-warming
if [ "$SKIP_PREWARMING" != true ]; then
  echo "=========================================================="
  echo "STAGE 1: PRE-WARMING VIDEO TRANSFORMATIONS"
  echo "=========================================================="
  echo "Base URL: $BASE_URL"
  echo "S3 Bucket: $BUCKET"
  echo "Directory: $DIRECTORY"
  echo "Derivatives: $DERIVATIVES"
  echo "Workers: $WORKERS"
  echo "Output file: $OUTPUT_FILE"
  echo "=========================================================="
  
  # Build the pre-warmer command
  PREWARMER_CMD="python3 video-resizer-kv-pre-warmer.py --remote $REMOTE --bucket $BUCKET --base-url $BASE_URL"
  PREWARMER_CMD+=" --derivatives $DERIVATIVES --workers $WORKERS --timeout $TIMEOUT --output $OUTPUT_FILE"
  
  if [ -n "$DIRECTORY" ]; then
    PREWARMER_CMD+=" --directory $DIRECTORY"
  fi
  
  if [ "$LIMIT" -gt 0 ]; then
    PREWARMER_CMD+=" --limit $LIMIT"
  fi
  
  if [ "$EXTENSION" != ".mp4" ]; then
    PREWARMER_CMD+=" --extension $EXTENSION"
  fi
  
  if [ "$USE_AWS_CLI" = true ]; then
    PREWARMER_CMD+=" --use-aws-cli"
  fi
  
  # Run the pre-warmer
  echo "Executing: $PREWARMER_CMD"
  eval $PREWARMER_CMD
  
  if [ $? -ne 0 ]; then
    echo "Error: Pre-warming failed. Check the logs for details."
    exit 1
  fi
  
  echo "Pre-warming completed successfully. Results saved to $OUTPUT_FILE"
  echo
fi

# Stage 2: Load testing with k6
if [ "$SKIP_LOADTEST" != true ]; then
  echo "=========================================================="
  echo "STAGE 2: K6 LOAD TESTING"
  echo "=========================================================="
  echo "Base URL: $BASE_URL"
  echo "URL Format: $URL_FORMAT"
  echo "Results file: $OUTPUT_FILE"
  echo "Users: Stage 1: $STAGE1_USERS, Stage 3: $STAGE3_USERS"
  echo "Durations: Stage 1: $STAGE1_DURATION, Stage 2: $STAGE2_DURATION, etc."
  echo "=========================================================="
  
  # Build the k6 command
  K6_CMD="k6 run video-load-test-integrated.js"
  K6_CMD+=" -e BASE_URL=$BASE_URL"
  K6_CMD+=" -e RESULTS_FILE=$OUTPUT_FILE"
  K6_CMD+=" -e URL_FORMAT=$URL_FORMAT"
  K6_CMD+=" -e STAGE1_USERS=$STAGE1_USERS"
  K6_CMD+=" -e STAGE1_DURATION=$STAGE1_DURATION"
  K6_CMD+=" -e STAGE2_USERS=$STAGE2_USERS"
  K6_CMD+=" -e STAGE2_DURATION=$STAGE2_DURATION"
  K6_CMD+=" -e STAGE3_USERS=$STAGE3_USERS"
  K6_CMD+=" -e STAGE3_DURATION=$STAGE3_DURATION"
  K6_CMD+=" -e STAGE4_USERS=$STAGE4_USERS"
  K6_CMD+=" -e STAGE4_DURATION=$STAGE4_DURATION"
  K6_CMD+=" -e STAGE5_USERS=$STAGE5_USERS"
  K6_CMD+=" -e STAGE5_DURATION=$STAGE5_DURATION"
  
  # Run k6 load test
  echo "Executing: $K6_CMD"
  eval $K6_CMD
  
  if [ $? -ne 0 ]; then
    echo "Warning: Load test completed with issues. Check the logs for details."
  else
    echo "Load test completed successfully."
  fi
fi

echo "=========================================================="
echo "WORKFLOW COMPLETED"
echo "=========================================================="