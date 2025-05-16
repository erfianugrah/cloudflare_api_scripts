# Video Load Testing

This document focuses specifically on the load testing component of the video transformation toolkit.

## Overview

The load testing component (`video-load-test-integrated.js`) is designed to:

1. Read the results from the pre-warming phase
2. Generate realistic video requests based on actual video metadata
3. Create load patterns that simulate real-world traffic
4. Use byte-range requests that mimic browser behavior

## Key Features

- **Data-driven testing**: Uses actual video sizes and dimensions from pre-warming
- **Realistic behavior**: Makes range requests similar to video player behavior
- **Configurable load patterns**: Adjustable stages with different user counts
- **Dynamic URL generation**: Supports different URL formats for flexibility

## Quick Start

```bash
# Run load test using results from a previous pre-warming run
k6 run video-load-test-integrated.js \
  -e BASE_URL=https://cdn.example.com \
  -e RESULTS_FILE=video_transform_results.json \
  -e STAGE1_USERS=50 -e STAGE2_USERS=100
```

## Configuration Options

All configuration is done through environment variables:

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

## URL Formats

The load test supports two URL formats:

1. **imwidth format** (default):  
   `https://cdn.example.com/path/to/video.mp4?imwidth=1920`

2. **derivative format**:  
   `https://cdn.example.com/path/to/video.mp4?derivative=desktop&width=1920&height=1080`

## Advanced Usage

### Understanding the Load Profile

The default load profile has 5 stages:
1. Ramp up to first user level (default: 50 VUs)
2. Stay at first level (simulates normal load)
3. Ramp up to second user level (default: 100 VUs)
4. Stay at second level (simulates peak load)
5. Ramp down to 0 (simulates end of traffic spike)

### Modifying k6 Test Settings

For more advanced load testing needs:

1. For custom thresholds, metrics, or scenarios, edit `video-load-test-integrated.js` directly
2. For integration with k6 cloud or other k6 features, refer to the [k6 documentation](https://k6.io/docs/)

### Performance Considerations

- For high load tests, run k6 on a machine with good network capacity
- Use a machine close to your target audience for realistic latency
- Monitor both client and server-side metrics during the test

## Troubleshooting

1. **No data found in results file**:
   - Ensure pre-warming completed successfully
   - Check the path to the results file
   - Verify the results contain successful (status 200) responses

2. **High failure rates**:
   - Check CDN capacity and configuration
   - Verify network connectivity
   - Consider reducing the number of virtual users

3. **k6 crashes or shows errors**:
   - Update to the latest version of k6
   - Check system resources (memory, network)
   - Try running with fewer virtual users

## Example Use Cases

1. **CDN Performance Testing**:
   ```bash
   k6 run video-load-test-integrated.js -e STAGE3_USERS=250 -e STAGE4_USERS=250
   ```

2. **Testing with Different URL Format**:
   ```bash
   k6 run video-load-test-integrated.js -e URL_FORMAT=derivative
   ```

3. **Quick Smoke Test**:
   ```bash
   k6 run video-load-test-integrated.js -e STAGE1_DURATION=10s -e STAGE2_DURATION=20s -e STAGE3_USERS=0
   ```