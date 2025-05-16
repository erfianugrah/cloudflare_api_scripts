import http from "k6/http";
import { check, sleep } from "k6";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";
import { SharedArray } from 'k6/data';

// Environment variables (can be passed via -e flag in k6 run)
const BASE_URL = __ENV.BASE_URL || "https://cdn.erfi.dev";
const RESULTS_FILE = __ENV.RESULTS_FILE || './video_transform_results.json';
const URL_PATTERN = __ENV.URL_PATTERN || '${baseUrl}/${path}?imwidth=${width}'; // Template literal style pattern
const MIN_SIZE = parseInt(__ENV.MIN_SIZE || "1000"); // Minimum video size to consider (bytes)
const WITH_DERIVATIVES = __ENV.WITH_DERIVATIVES === "true"; // Include derivative in URL logic

// Load the video transformation results from the JSON file
const videoData = new SharedArray('videos', function() {
  const f = JSON.parse(open(RESULTS_FILE));
  return Object.entries(f.results)
    .filter(([key, data]) => data.status === 200 && data.actualTotalVideoSize > MIN_SIZE)
    .map(([key, data]) => {
      // Extract derivative from the key (format: video:path/to/file.mp4:derivative=desktop)
      const keyParts = key.split(':');
      const derivativePart = keyParts.length > 2 ? keyParts[2] : "";
      const derivativeMatch = derivativePart.match(/derivative=([^_]+)/);
      const derivativeName = derivativeMatch ? derivativeMatch[1] : data.derivative;
      
      return {
        key,
        path: data.sourcePath,
        derivative: derivativeName,
        width: data.width,
        height: data.height,
        size: data.actualTotalVideoSize,
        contentType: data.contentType
      };
    });
});

// Load test configuration
export const options = {
  // These can also be overridden via command line arguments
  stages: [
    { duration: __ENV.RAMP_UP || "30s", target: parseInt(__ENV.USERS_1 || "50") },
    { duration: __ENV.PLATEAU_1 || "1m", target: parseInt(__ENV.USERS_1 || "50") },
    { duration: __ENV.RAMP_UP_2 || "30s", target: parseInt(__ENV.USERS_2 || "100") },
    { duration: __ENV.PLATEAU_2 || "1m", target: parseInt(__ENV.USERS_2 || "100") },
    { duration: __ENV.RAMP_UP_3 || "30s", target: parseInt(__ENV.USERS_3 || "150") },
    { duration: __ENV.PLATEAU_3 || "1m", target: parseInt(__ENV.USERS_3 || "150") },
    { duration: __ENV.RAMP_DOWN || "30s", target: 0 },
  ],
  thresholds: {
    http_req_duration: [`p(95)<${__ENV.REQ_DURATION || "15000"}`], 
    http_req_failed: [`rate<${__ENV.FAILURE_RATE || "0.05"}`],
  },
  timeout: __ENV.GLOBAL_TIMEOUT || "30s",
};

// Dynamically generate URL based on the URL_PATTERN template and video info
function generateVideoUrl(videoInfo) {
  // Extract path and remove leading slash if present
  const path = videoInfo.path.startsWith('/') ? videoInfo.path.slice(1) : videoInfo.path;
  
  // Generate URL by replacing placeholders in the pattern
  let url = URL_PATTERN
    .replace('${baseUrl}', BASE_URL)
    .replace('${path}', path)
    .replace('${width}', videoInfo.width)
    .replace('${height}', videoInfo.height);
  
  // Add derivative to URL if specified
  if (WITH_DERIVATIVES) {
    url = url.replace('${derivative}', videoInfo.derivative);
  }
  
  // Clean up any remaining unreplaced placeholders
  // This handles placeholders that weren't in the videoInfo object
  url = url.replace(/\${[^}]+}/g, '');
  
  return url;
}

// Generate a random byte range that's likely to be satisfiable
function generateRandomRange(videoInfo) {
  // Get size from our transformed video data
  const estimatedSize = videoInfo.size;
  
  // Safety margin: Use 80% of estimated size to stay within bounds
  const safeSize = Math.floor(estimatedSize * 0.8);

  // Divide video into regions with different request probabilities
  const rangeType = randomIntBetween(1, 10);
  let start, end, chunkSize;

  if (rangeType <= 5) {
    // 50% chance: First portion (0-33%) - common for initial loads
    start = randomIntBetween(0, Math.floor(safeSize * 0.33));
    chunkSize = randomIntBetween(100 * 1024, 500 * 1024); // 100KB-500KB chunk
  } else if (rangeType <= 9) {
    // 40% chance: Middle portion (33-66%) - common for seeking
    start = randomIntBetween(
      Math.floor(safeSize * 0.33),
      Math.floor(safeSize * 0.66)
    );
    chunkSize = randomIntBetween(100 * 1024, 500 * 1024); // 100KB-500KB chunk
  } else {
    // 10% chance: End portion (66-85%) - common for skipping ahead
    start = randomIntBetween(
      Math.floor(safeSize * 0.66),
      Math.floor(safeSize * 0.85)
    );
    chunkSize = randomIntBetween(50 * 1024, 200 * 1024); // Smaller chunk for end
  }

  // Ensure we don't exceed our safe size
  end = Math.min(start + chunkSize - 1, safeSize - 1);

  return `bytes=${start}-${end}`;
}

// Initialize and log configuration
function logConfiguration() {
  console.log(`
Configuration:
- Base URL: ${BASE_URL}
- Results File: ${RESULTS_FILE}
- URL Pattern: ${URL_PATTERN}
- Min Size: ${MIN_SIZE} bytes
- With Derivatives: ${WITH_DERIVATIVES}
- Videos available: ${videoData.length}
  `);
  
  if (videoData.length === 0) {
    console.error("WARNING: No valid videos found matching criteria!");
  } else {
    // Log a sample URL
    const sampleVideo = videoData[0];
    console.log(`Sample URL: ${generateVideoUrl(sampleVideo)}`);
  }
}

// Run once at startup
logConfiguration();

// Main test function - runs for each VU
export default function () {
  // Skip if we have no video data
  if (videoData.length === 0) {
    console.log("No valid video data found. Check your video_transform_results.json file.");
    return;
  }

  // Select a random video from our dataset
  const videoInfo = videoData[randomIntBetween(0, videoData.length - 1)];
  
  // Generate URL for this video
  const videoUrl = generateVideoUrl(videoInfo);

  // Random range header or no range header (20% chance of no range)
  const useRangeHeader = Math.random() > 0.2;

  const params = {
    headers: {
      "User-Agent": "k6-load-test/1.0",
    },
  };

  if (useRangeHeader) {
    params.headers["Range"] = generateRandomRange(videoInfo);
  }

  // Configure request parameters for proper download behavior
  params.responseType = "binary"; // Handle binary data properly
  
  // Set timeout for this individual request
  const requestTimeout = {
    timeout: __ENV.REQUEST_TIMEOUT || "60s",
  };

  // Combine the parameters
  const requestParams = { ...params, ...requestTimeout };

  // Send request and wait for completion
  const response = http.get(videoUrl, requestParams);

  // Check if successful and validate we got a response
  check(response, {
    "status is 200 or 206": (r) => r.status === 200 || r.status === 206,
    "got response body": (r) => r.body && r.body.length > 0,
    "response time reasonable": (r) => r.timings.duration < parseInt(__ENV.CHECK_DURATION || "10000"),
  });

  // Log info for debugging
  if (response.status !== 200 && response.status !== 206) {
    console.log(
      `Error fetching ${videoUrl}, status: ${response.status}${
        useRangeHeader ? ", Range: " + params.headers["Range"] : ""
      }`
    );
  }

  // Brief pause between requests
  sleep(randomIntBetween(
    parseInt(__ENV.MIN_SLEEP || "1"), 
    parseInt(__ENV.MAX_SLEEP || "3")
  ));
}