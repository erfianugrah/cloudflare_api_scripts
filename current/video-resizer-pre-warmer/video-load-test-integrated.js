import http from "k6/http";
import { check, sleep } from "k6";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";
import { SharedArray } from 'k6/data';

// Environment variables for customization
const BASE_URL = __ENV.BASE_URL || "https://cdn.erfi.dev";
const RESULTS_FILE = __ENV.RESULTS_FILE || './video_transform_results.json';
const URL_FORMAT = __ENV.URL_FORMAT || "imwidth"; // "imwidth" or "derivative" 

// Load the video transformation results from the JSON file
const videoData = new SharedArray('videos', function() {
  const f = JSON.parse(open(RESULTS_FILE));
  return Object.entries(f.results)
    .filter(([key, data]) => 
      // Only include successful transformations with meaningful size
      data.status === 200 && data.actualTotalVideoSize > 1000
    )
    .map(([key, data]) => {
      return {
        key,
        path: data.sourcePath,
        derivative: data.derivative,
        width: data.width,
        height: data.height,
        size: data.actualTotalVideoSize,
        contentType: data.contentType
      };
    });
});

// Load test configuration
export const options = {
  stages: [
    { duration: __ENV.STAGE1_DURATION || "30s", target: parseInt(__ENV.STAGE1_USERS || "50") },
    { duration: __ENV.STAGE2_DURATION || "1m", target: parseInt(__ENV.STAGE2_USERS || "50") },
    { duration: __ENV.STAGE3_DURATION || "30s", target: parseInt(__ENV.STAGE3_USERS || "100") },
    { duration: __ENV.STAGE4_DURATION || "1m", target: parseInt(__ENV.STAGE4_USERS || "100") },
    { duration: __ENV.STAGE5_DURATION || "30s", target: parseInt(__ENV.STAGE5_USERS || "0") },
  ],
  thresholds: {
    http_req_duration: [`p(95)<${__ENV.REQ_DURATION_THRESHOLD || "15000"}`], 
    http_req_failed: [`rate<${__ENV.FAILURE_RATE_THRESHOLD || "0.05"}`],
  },
  timeout: __ENV.GLOBAL_TIMEOUT || "30s",
};

// Generate the appropriate URL based on the URL_FORMAT
function generateVideoUrl(videoInfo) {
  // Extract path (remove leading slash if present)
  const path = videoInfo.path.startsWith('/') ? videoInfo.path.slice(1) : videoInfo.path;
  
  let url;
  
  if (URL_FORMAT === "derivative") {
    // Use derivative format matching the Python script's transformation style
    url = `${BASE_URL}/${path}?derivative=${videoInfo.derivative}&width=${videoInfo.width}&height=${videoInfo.height}`;
  } else {
    // Use imwidth format (default in original k6 script)
    url = `${BASE_URL}/${path}?imwidth=${videoInfo.width}`;
  }
  
  return url;
}

// Generate a random byte range based on video size
function generateRandomRange(videoInfo) {
  const estimatedSize = videoInfo.size;
  const safeSize = Math.floor(estimatedSize * 0.8);

  const rangeType = randomIntBetween(1, 10);
  let start, end, chunkSize;

  if (rangeType <= 5) {
    // 50% chance: First portion (0-33%)
    start = randomIntBetween(0, Math.floor(safeSize * 0.33));
    chunkSize = randomIntBetween(100 * 1024, 500 * 1024); // 100KB to 500KB
  } else if (rangeType <= 9) {
    // 40% chance: Middle portion (33-66%)
    start = randomIntBetween(
      Math.floor(safeSize * 0.33),
      Math.floor(safeSize * 0.66)
    );
    chunkSize = randomIntBetween(100 * 1024, 500 * 1024); // 100KB to 500KB
  } else {
    // 10% chance: End portion (66-85%)
    start = randomIntBetween(
      Math.floor(safeSize * 0.66),
      Math.floor(safeSize * 0.85)
    );
    chunkSize = randomIntBetween(50 * 1024, 200 * 1024); // Smaller chunk
  }

  // Ensure we don't exceed our safe size
  end = Math.min(start + chunkSize - 1, safeSize - 1);

  return `bytes=${start}-${end}`;
}

// Initialize and log configuration
function init() {
  console.log(`
==================================================
VIDEO LOAD TEST CONFIGURATION
==================================================
Base URL: ${BASE_URL}
Results File: ${RESULTS_FILE}
URL Format: ${URL_FORMAT}
Available Videos: ${videoData.length}
==================================================
  `);
  
  if (videoData.length === 0) {
    console.error("WARNING: No valid videos found in results file!");
  } else {
    // Log sample URLs
    const sampleVideo = videoData[0];
    console.log(`Sample URL: ${generateVideoUrl(sampleVideo)}`);
    
    if (videoData.length >= 3) {
      console.log("Video distributions by derivative:");
      const derivativeCounts = {};
      videoData.forEach(v => {
        derivativeCounts[v.derivative] = (derivativeCounts[v.derivative] || 0) + 1;
      });
      
      Object.entries(derivativeCounts).forEach(([derivative, count]) => {
        console.log(`  - ${derivative}: ${count} videos (${Math.round(count/videoData.length*100)}%)`);
      });
    }
  }
}

// Run initialization once
init();

// Main test function - runs for each VU
export default function () {
  // Skip if we have no video data
  if (videoData.length === 0) {
    console.log("No valid video data found in results file. Run the pre-warmer first.");
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
    responseType: "binary", // Handle binary data properly
    timeout: __ENV.REQUEST_TIMEOUT || "60s", // Individual request timeout
  };

  if (useRangeHeader) {
    params.headers["Range"] = generateRandomRange(videoInfo);
  }

  // Send request and wait for completion
  const response = http.get(videoUrl, params);

  // Check if successful and validate we got a response
  check(response, {
    "status is 200 or 206": (r) => r.status === 200 || r.status === 206,
    "got response body": (r) => r.body && r.body.length > 0,
    "response time reasonable": (r) => r.timings.duration < parseInt(__ENV.RESPONSE_TIME_THRESHOLD || "10000"),
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
    parseFloat(__ENV.MIN_SLEEP || "1"), 
    parseFloat(__ENV.MAX_SLEEP || "3")
  ));
}