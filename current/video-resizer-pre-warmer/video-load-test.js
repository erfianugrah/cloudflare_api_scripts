import http from "k6/http";
import { check, sleep } from "k6";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";

export const options = {
  stages: [
    { duration: "30s", target: 50 }, // Ramp up to 50 users over 30 seconds
    { duration: "1m", target: 50 }, // Stay at 50 users for 1 minute
    { duration: "30s", target: 100 }, // Ramp up to 100 users
    { duration: "1m", target: 100 }, // Stay at 100 users for 1 minute
    { duration: "30s", target: 150 }, // Ramp up to 150 users
    { duration: "1m", target: 150 }, // Stay at 150 users for 1 minute
    { duration: "30s", target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ["p(95)<15000"], // 95% of requests should be below 15s (increased for full downloads)
    http_req_failed: ["rate<0.05"], // Less than 5% of requests should fail
  },
  // Ensure requests have time to complete and prevent client disconnections
  timeout: "30s", // Global timeout for all requests
};

// Video URLs to test with different resolutions
const VIDEO_URLS = [
  // 1920 width
  "https://cdn.erfi.dev/0774521_fe000083.mp4?imwidth=1920",
  "https://cdn.erfi.dev/white-fang.mp4?imwidth=1920",
  "https://cdn.erfi.dev/0765652_fe001061.mp4?imwidth=1920",
  // 1280 width
  "https://cdn.erfi.dev/0774521_fe000083.mp4?imwidth=1280",
  "https://cdn.erfi.dev/white-fang.mp4?imwidth=1280",
  "https://cdn.erfi.dev/0765652_fe001061.mp4?imwidth=1280",
  // 854 width
  "https://cdn.erfi.dev/0774521_fe000083.mp4?imwidth=854",
  "https://cdn.erfi.dev/white-fang.mp4?imwidth=854",
  "https://cdn.erfi.dev/0765652_fe001061.mp4?imwidth=854",
];

// Dictionary of video file information: approximate sizes in bytes for different resolutions
const VIDEO_SIZES = {
  // Video 1: 0774521_fe000083.mp4
  "0774521_fe000083.mp4": {
    "1920": 6 * 1024 * 1024, // ~6MB at 1920 width
    "1280": 4 * 1024 * 1024, // ~4MB at 1280 width
    "854": 2 * 1024 * 1024, // ~2MB at 854 width
  },
  // Video 2: white-fang.mp4
  "white-fang.mp4": {
    "1920": 10 * 1024 * 1024, // ~10MB at 1920 width
    "1280": 6 * 1024 * 1024, // ~6MB at 1280 width
    "854": 3 * 1024 * 1024, // ~3MB at 854 width
  },
  // Video 3: 0765652_fe001061.mp4
  "0765652_fe001061.mp4": {
    "1920": 8 * 1024 * 1024, // ~8MB at 1920 width
    "1280": 5 * 1024 * 1024, // ~5MB at 1280 width
    "854": 2.5 * 1024 * 1024, // ~2.5MB at 854 width
  },
  // Default fallback
  "default": {
    "1920": 5 * 1024 * 1024,
    "1280": 3 * 1024 * 1024,
    "854": 1.5 * 1024 * 1024,
  },
};

// Generate a random byte range that's more likely to be satisfiable
function generateRandomRange(url) {
  // Extract video filename and resolution from URL
  const urlParts = url.split("/");
  const filenameWithQuery = urlParts[urlParts.length - 1];
  const filename = filenameWithQuery.split("?")[0];
  const resolution = url.match(/imwidth=(\d+)/)[1];

  // Get estimated file size based on filename and resolution
  let estimatedSize;
  if (VIDEO_SIZES[filename] && VIDEO_SIZES[filename][resolution]) {
    estimatedSize = VIDEO_SIZES[filename][resolution];
  } else {
    // Use default if specific video+resolution not found
    estimatedSize = VIDEO_SIZES["default"][resolution] || 3 * 1024 * 1024;
  }

  // Safety margin: Use 80% of estimated size to ensure we stay within bounds
  const safeSize = Math.floor(estimatedSize * 0.8);

  // Generate safer ranges:
  // 1. First 1/3 of video - common for initial loads
  // 2. Middle portion - common for seeking
  // 3. Small chunk near the end - common for skipping ahead
  const rangeType = randomIntBetween(1, 10);

  let start, end, chunkSize;

  if (rangeType <= 5) {
    // 50% chance: First portion (0-33%)
    start = randomIntBetween(0, Math.floor(safeSize * 0.33));
    chunkSize = randomIntBetween(100 * 1024, 500 * 1024); // 100KB to 500KB chunk
  } else if (rangeType <= 9) {
    // 40% chance: Middle portion (33-66%)
    start = randomIntBetween(
      Math.floor(safeSize * 0.33),
      Math.floor(safeSize * 0.66),
    );
    chunkSize = randomIntBetween(100 * 1024, 500 * 1024); // 100KB to 500KB chunk
  } else {
    // 10% chance: End portion (66-85%)
    start = randomIntBetween(
      Math.floor(safeSize * 0.66),
      Math.floor(safeSize * 0.85),
    );
    chunkSize = randomIntBetween(50 * 1024, 200 * 1024); // Smaller chunk for end
  }

  // Ensure we don't exceed our safe size
  end = Math.min(start + chunkSize - 1, safeSize - 1);

  return `bytes=${start}-${end}`;
}

// Main test function
export default function () {
  // Select a random URL from our list
  const videoUrl = VIDEO_URLS[randomIntBetween(0, VIDEO_URLS.length - 1)];

  // Random range header or no range header (20% chance of no range)
  const useRangeHeader = Math.random() > 0.2;

  const params = {
    headers: {
      "User-Agent": "k6-load-test/1.0",
    },
  };

  if (useRangeHeader) {
    params.headers["Range"] = generateRandomRange(videoUrl);
  }

  // Configure request parameters to ensure proper download behavior
  if (!params.responseType) {
    params.responseType = "binary"; // Handle binary data properly
  }

  // Ensure we have proper timeout settings for each request
  const requestTimeout = {
    timeout: "60s", // Individual request timeout
  };

  // Combine the parameters
  const requestParams = { ...params, ...requestTimeout };

  // Send request and wait for completion
  const response = useRangeHeader
    ? http.get(videoUrl, requestParams)
    : http.get(videoUrl, requestParams);

  // Check if successful and validate we got a response
  check(response, {
    "status is 200 or 206": (r) => r.status === 200 || r.status === 206,
    "got response body": (r) => r.body && r.body.length > 0,
    "response time reasonable": (r) => r.timings.duration < 10000,
  });

  // Log info for debugging
  if (response.status !== 200 && response.status !== 206) {
    console.log(
      `Error fetching ${videoUrl}, status: ${response.status}${
        useRangeHeader ? ", Range: " + params.headers["Range"] : ""
      }`,
    );
  }

  // Brief pause between requests
  sleep(randomIntBetween(1, 3));
}

