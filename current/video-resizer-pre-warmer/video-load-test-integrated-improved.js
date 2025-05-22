import http from "k6/http";
import { check, sleep, fail } from "k6";
import { Counter, Trend, Rate } from "k6/metrics";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";
import { SharedArray } from 'k6/data';

// Define custom metrics for tracking failures
const failureCounter = new Counter('custom_failures');
const statusErrorCounter = new Counter('status_code_errors');
const emptyBodyCounter = new Counter('empty_body_errors');
const timeoutErrorCounter = new Counter('timeout_errors');
const contentTypeErrorCounter = new Counter('content_type_errors');

// Helper function to get content size from response, handling different response types
function getResponseSize(r) {
  if (!r || !r.body) return 0;
  
  // Try different size properties based on the response type
  if (typeof r.body.byteLength === 'number') {
    return r.body.byteLength;  // ArrayBuffer or TypedArray
  } else if (typeof r.body.length === 'number') {
    return r.body.length;      // String or Array
  } else if (typeof r.body.size === 'number') {
    return r.body.size;        // Blob
  } else {
    // Last resort - try to stringify and get length
    try {
      return String(r.body).length;
    } catch (e) {
      return 0;
    }
  }
}

// Helper function to check headers for content
function hasValidHeaders(r) {
  if (!r || !r.headers) return false;
  
  // Check for valid Content-Length
  const contentLength = parseInt(r.headers["Content-Length"] || r.headers["content-length"] || "0");
  if (contentLength > 0) return true;
  
  // Check for valid Content-Range
  const contentRange = r.headers["Content-Range"] || r.headers["content-range"];
  if (contentRange && contentRange.includes("bytes")) return true;
  
  return false;
}

// Track failures by derivative
const desktopFailures = new Counter('desktop_failures');
const tabletFailures = new Counter('tablet_failures');
const mobileFailures = new Counter('mobile_failures');

// Track failures by size
const smallFailures = new Counter('small_file_failures');
const mediumFailures = new Counter('medium_file_failures');
const largeFailures = new Counter('large_file_failures');

// Environment variables for customization
const BASE_URL = __ENV.BASE_URL || "https://cdn.erfi.dev";
const RESULTS_FILE = __ENV.RESULTS_FILE || './video_transform_results.json';
const ERROR_REPORT_FILE = __ENV.ERROR_REPORT_FILE || './error_report.json'; // Optional error report file
// URL_FORMAT is no longer used - always using imwidth format
const URL_FORMAT = "imwidth";
const MAX_RETRIES = parseInt(__ENV.MAX_RETRIES || "2");
const DEBUG_MODE = __ENV.DEBUG_MODE === "true";
const SIMULATE_REAL_PLAYER = __ENV.SIMULATE_REAL_PLAYER !== "false"; // Default to true
const USE_HEAD_REQUESTS = __ENV.USE_HEAD_REQUESTS !== "false"; // Default to true
const CONTENT_LENGTH_CACHE = new Map(); // Cache for Content-Length values
const SKIP_LARGE_FILES = __ENV.SKIP_LARGE_FILES !== "false"; // Default to true
const LARGE_FILE_THRESHOLD_MIB = parseInt(__ENV.LARGE_FILE_THRESHOLD_MIB || "256"); // 256 MiB default
const LARGE_FILE_THRESHOLD_BYTES = LARGE_FILE_THRESHOLD_MIB * 1024 * 1024;

// Tracking info for skipped files
let skippedFiles = {
  errorFiles: 0,
  largeFiles: 0,
  errorFilePaths: [],
  largeFilePaths: []
};

// Load error report if available - only try this once
const errorFiles = new Set();

// Use a self-executing function to load error report only once
const errorReportLoaded = (function() {
  // Only process the error report in the first VU
  if (__VU && __VU !== 1) {
    return false;
  }
  
  try {
    // First check if file exists - read without parsing 
    let fileExists = false;
    try {
      const testOpen = open(ERROR_REPORT_FILE);
      fileExists = (testOpen && testOpen.length > 0);
    } catch (e) {
      // File probably doesn't exist, don't show a warning
      return false;
    }
    
    // Only try to parse if file exists
    if (fileExists && ERROR_REPORT_FILE) {
      try {
        // Check if it's a markdown file
        if (ERROR_REPORT_FILE.endsWith('.md')) {
          // Parse markdown error report
          const content = open(ERROR_REPORT_FILE);
          
          // Extract filenames from markdown using regex
          // Pattern looks for files inside code blocks or in error lists
          const filePattern = /(?:```|\|\s)([\w-]+\.\w+)(?:```|$|\s)/g;
          let match;
          while ((match = filePattern.exec(content)) !== null) {
            if (match[1]) {
              errorFiles.add(match[1]);
              if (DEBUG_MODE) {
                console.log(`Added error file to exclusion list: ${match[1]}`);
              }
            }
          }
        } else {
          // Assume JSON format
          const errorData = JSON.parse(open(ERROR_REPORT_FILE));
          if (errorData.detailed_errors && Array.isArray(errorData.detailed_errors)) {
            errorData.detailed_errors.forEach(err => {
              if (err.rel_path || err.file_path || err.path) {
                const path = err.rel_path || err.file_path || err.path;
                errorFiles.add(path);
                if (DEBUG_MODE) {
                  console.log(`Added error file to exclusion list: ${path}`);
                }
              }
            });
          }
        }
        console.log(`Loaded ${errorFiles.size} files to exclude from error report`);
        return true;
      } catch (err) {
        // Only log once
        console.warn(`Failed to parse error report file: ${err.message}`);
        return false;
      }
    }
  } catch (err) {
    // Only log once
    if (DEBUG_MODE) {
      console.warn(`Error report file ${ERROR_REPORT_FILE} not found or couldn't be accessed`);
    }
    return false;
  }
  return false;
})();

// Load the video transformation results from the JSON file
const videoData = new SharedArray('videos', function() {
  try {
    // Try to parse the RESULTS_FILE
    const data = JSON.parse(open(RESULTS_FILE));
    
    // Check if we have results in the expected format
    if (!data.results || Object.keys(data.results).length === 0) {
      console.error(`WARNING: No results found in ${RESULTS_FILE}`);
      return [];
    }
    
    // Log results structure for debugging
    if (DEBUG_MODE) {
      const firstKey = Object.keys(data.results)[0];
      if (firstKey) {
        console.log(`DEBUG: First result structure: ${JSON.stringify(data.results[firstKey]).substring(0, 500)}...`);
      }
    }
    
    // Filter and map the results
    const filteredResults = [];
    
    // Handle the updated data structure with derivatives as objects
    Object.entries(data.results).forEach(([key, fileData]) => {
      const path = fileData.path || "";
      
      // Skip files in error report
      if (errorFiles.has(path)) {
        skippedFiles.errorFiles++;
        skippedFiles.errorFilePaths.push(path);
        if (DEBUG_MODE) {
          console.log(`Skipping file found in error report: ${path}`);
        }
        return;
      }
      
      // Process each derivative separately
      if (fileData.derivatives) {
        Object.entries(fileData.derivatives).forEach(([derivativeName, derivativeData]) => {
          // Skip invalid derivatives
          if (!derivativeData || derivativeData.status !== "success" || derivativeData.status_code !== 200) {
            if (DEBUG_MODE) {
              console.log(`Skipping invalid derivative: ${key} (${derivativeName})`);
            }
            return;
          }
          
          const responseSize = derivativeData.response_size_bytes || 0;
          
          // Skip large files if configured
          if (SKIP_LARGE_FILES && responseSize > LARGE_FILE_THRESHOLD_BYTES) {
            skippedFiles.largeFiles++;
            skippedFiles.largeFilePaths.push(path);
            if (DEBUG_MODE) {
              console.log(`Skipping large file (${formatFileSize(responseSize)}): ${path} (${derivativeName})`);
            }
            return;
          }
          
          // Skip files with no meaningful size
          if (responseSize <= 1000) {
            if (DEBUG_MODE) {
              console.log(`Skipping file with too small response size: ${path} (${derivativeName}) - ${responseSize} bytes`);
            }
            return;
          }
          
          // Parse width from URL or use default based on derivative
          let width, height;
          const url = derivativeData.url || "";
          const widthMatch = url.match(/imwidth=(\d+)/);
          
          if (widthMatch && widthMatch[1]) {
            width = parseInt(widthMatch[1]);
            // Approximate height based on 16:9 ratio
            height = Math.round(width * 9 / 16);
          } else {
            // Use default values based on derivative name
            if (derivativeName === "desktop") {
              width = 1920;
              height = 1080;
            } else if (derivativeName === "tablet") {
              width = 1280;
              height = 720;
            } else if (derivativeName === "mobile") {
              width = 854;
              height = 480;
            } else {
              // Default fallback
              width = 1280;
              height = 720;
            }
          }
          
          // Add to filtered results
          filteredResults.push({
            key: `${key}_${derivativeName}`,
            path: path,
            derivative: derivativeName,
            width: width,
            height: height,
            size: responseSize,
            contentType: fileData.contentType || 'video/mp4' // Default to mp4 if not specified
          });
        });
      }
    });
    
    if (DEBUG_MODE) {
      console.log(`Processed ${filteredResults.length} valid videos from results file`);
    }
    
    return filteredResults;
  } catch (error) {
    console.error(`ERROR loading results file: ${error.message}`);
    return [];
  }
});

// Load test configuration with more customization options
export const options = {
  stages: [
    { duration: __ENV.STAGE1_DURATION || "30s", target: parseInt(__ENV.STAGE1_USERS || "5") },
    { duration: __ENV.STAGE2_DURATION || "1m", target: parseInt(__ENV.STAGE2_USERS || "10") },
    { duration: __ENV.STAGE3_DURATION || "30s", target: parseInt(__ENV.STAGE3_USERS || "15") },
    { duration: __ENV.STAGE4_DURATION || "1m", target: parseInt(__ENV.STAGE4_USERS || "10") },
    { duration: __ENV.STAGE5_DURATION || "30s", target: parseInt(__ENV.STAGE5_USERS || "0") },
  ],
  thresholds: {
    http_req_duration: [`p(95)<${__ENV.REQ_DURATION_THRESHOLD || "15000"}`], 
    http_req_failed: [`rate<${__ENV.FAILURE_RATE_THRESHOLD || "0.05"}`],
    'http_req_duration{status:200}': [`p(95)<${__ENV.SUCCESS_DURATION_THRESHOLD || "12000"}`],
    'http_req_duration{status:206}': [`p(95)<${__ENV.PARTIAL_DURATION_THRESHOLD || "10000"}`],
    'http_req_duration{derivative:desktop}': [`p(95)<${__ENV.DESKTOP_DURATION_THRESHOLD || "15000"}`],
    'http_req_duration{derivative:tablet}': [`p(95)<${__ENV.TABLET_DURATION_THRESHOLD || "12000"}`],
    'http_req_duration{derivative:mobile}': [`p(95)<${__ENV.MOBILE_DURATION_THRESHOLD || "10000"}`],
  },
  timeout: __ENV.GLOBAL_TIMEOUT || "90s", // Increased global timeout
};

// Generate the URL with only imwidth parameter (no derivative parameter)
function generateVideoUrl(videoInfo) {
  // Handle malformed/missing path
  if (!videoInfo || !videoInfo.path) {
    console.error("Invalid video info - missing path");
    return `${BASE_URL}/invalid-path`;
  }
  
  // Extract path (remove leading slash if present)
  const path = videoInfo.path.startsWith('/') ? videoInfo.path.slice(1) : videoInfo.path;
  
  // Remove trailing slash from BASE_URL if it exists and path doesn't start with slash
  const baseUrl = BASE_URL.endsWith('/') ? BASE_URL.slice(0, -1) : BASE_URL;
  
  // Always use the imwidth format (no derivative parameter) regardless of URL_FORMAT setting
  // This matches the updated behavior of the pre-warmer
  const url = `${baseUrl}/${path}?imwidth=${videoInfo.width}`;
  
  return url;
}

// Get content length of a video using HEAD request with caching
function getContentLength(videoUrl, headers = {}) {
  // Check cache first
  if (CONTENT_LENGTH_CACHE.has(videoUrl)) {
    return CONTENT_LENGTH_CACHE.get(videoUrl);
  }
  
  // Prepare headers for HEAD request
  const headParams = {
    headers: {
      ...headers,
      "User-Agent": generateUserAgent(),
    },
    timeout: __ENV.HEAD_TIMEOUT || "30s", // Increased timeout for HEAD requests
  };
  
  try {
    // Make HEAD request to get Content-Length without downloading the full content
    const headResponse = http.head(videoUrl, headParams);
    
    if (headResponse.status !== 200) {
      console.warn(`HEAD request failed for ${videoUrl}: status ${headResponse.status}`);
      return null;
    }
    
    // Extract Content-Length from response headers
    const contentLength = parseInt((headResponse.headers && headResponse.headers["Content-Length"]) || "0");
    
    // Only cache valid content lengths
    if (contentLength > 0) {
      CONTENT_LENGTH_CACHE.set(videoUrl, contentLength);
      return contentLength;
    } else {
      console.warn(`Invalid Content-Length (${contentLength}) for ${videoUrl}`);
      return null;
    }
  } catch (error) {
    console.warn(`Error in HEAD request for ${videoUrl}: ${error.message}`);
    return null;
  }
}

// Generate a random byte range based on actual content length or fallback to estimated size
function generateRandomRange(videoInfo, videoUrl, headers = {}) {
  let contentSize;
  
  // Try to get actual content length if HEAD requests are enabled
  if (USE_HEAD_REQUESTS) {
    contentSize = getContentLength(videoUrl, headers);
  }
  
  // Fallback to estimated size from results file if HEAD request failed or is disabled
  if (!contentSize && videoInfo && videoInfo.size && videoInfo.size > 1000) {
    contentSize = videoInfo.size;
    if (DEBUG_MODE) {
      console.log(`Using estimated size ${formatFileSize(contentSize)} for ${videoUrl}`);
    }
  } else if (!contentSize) {
    // No valid size information available
    return `bytes=0-8192`; // Default to a small range for safety
  }
  
  // Use 85% of the content size to ensure we don't exceed the actual file size
  const safeSize = Math.floor(contentSize * 0.85);
  
  // More sophisticated range generation based on real video player behavior
  if (SIMULATE_REAL_PLAYER) {
    // Video player simulation: Initial section is loaded first, then progressive chunks
    const playbackProgress = Math.random(); // 0-1 representing where in video playback we are
    
    if (playbackProgress < 0.2) {
      // Initial buffering phase - typically gets first chunk of video
      const chunkSize = randomIntBetween(256 * 1024, 1024 * 1024); // 256KB to 1MB initial chunk
      return `bytes=0-${Math.min(chunkSize - 1, safeSize - 1)}`;
    } else if (playbackProgress < 0.8) {
      // Normal playback phase - sequential chunks
      const segmentPosition = Math.floor(safeSize * (playbackProgress - 0.1));
      const chunkSize = randomIntBetween(128 * 1024, 512 * 1024); // 128KB to 512KB streaming chunks
      return `bytes=${segmentPosition}-${Math.min(segmentPosition + chunkSize - 1, safeSize - 1)}`;
    } else {
      // Near end of playback or seeking toward end
      const startPos = Math.floor(safeSize * 0.7);
      const chunkSize = randomIntBetween(128 * 1024, 768 * 1024);
      return `bytes=${startPos}-${Math.min(startPos + chunkSize - 1, safeSize - 1)}`;
    }
  } else {
    // Original distribution logic (simplified)
    const rangeType = randomIntBetween(1, 10);
    let start, chunkSize;
    
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
    const end = Math.min(start + chunkSize - 1, safeSize - 1);
    return `bytes=${start}-${end}`;
  }
}

// Enhanced user-agent generation
function generateUserAgent() {
  const agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.50 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
    "k6-load-test/2.0", // Included for transparency
  ];
  
  return agents[Math.floor(Math.random() * agents.length)];
}

// Make HTTP request with retry logic
function makeRequestWithRetry(url, params, maxRetries = MAX_RETRIES) {
  let retries = 0;
  let response;
  let lastError = "unknown error";
  let errorDetails = {};
  
  while (retries <= maxRetries) {
    try {
      // Use http.get with proper error handling
      response = http.get(url, params);
      
      // Check for success (200 OK or 206 Partial Content)
      if (response.status === 200 || response.status === 206) {
        // Get body size using our helper function
        const bodySize = getResponseSize(response);
        
        // Check if we have valid headers
        const hasValidContentLength = response.headers && 
                                    (response.headers["Content-Length"] || response.headers["content-length"]) && 
                                    parseInt(response.headers["Content-Length"] || response.headers["content-length"] || "0") > 0;
        
        const hasValidContentType = response.headers && 
                                  (response.headers["Content-Type"] || response.headers["content-type"]) &&
                                  ((response.headers["Content-Type"] || response.headers["content-type"] || "").includes("video/") ||
                                   (response.headers["Content-Type"] || response.headers["content-type"] || "").includes("application/octet-stream"));
        
        const hasValidContentRange = response.status === 206 && 
                                   response.headers && 
                                   (response.headers["Content-Range"] || response.headers["content-range"]);
        
        // Response is valid if:
        // - It has a non-zero body size, OR
        // - It has valid headers (Content-Length, Content-Type for 200, or Content-Range for 206)
        const isValid = bodySize > 0 || 
                        hasValidContentLength || 
                        hasValidContentType || 
                        hasValidContentRange;
        
        if (isValid) {
          // Response is good - add explicit delay to match Python behavior
          const connectionCloseDelay = parseInt(__ENV.CONNECTION_CLOSE_DELAY || "10");
          if (connectionCloseDelay > 0) {
            if (DEBUG_MODE) {
              console.log(`Waiting ${connectionCloseDelay}s for connection to close`);
            }
            sleep(connectionCloseDelay);
          }
          
          // Log successful response details if in debug mode
          if (DEBUG_MODE && response.status === 206) {
            console.log(`Successfully processed 206 response:`);
            console.log(`- URL: ${url}`);
            console.log(`- Body size: ${bodySize} bytes`);
            console.log(`- Content-Length: ${response.headers["Content-Length"] || response.headers["content-length"] || "not set"}`);
            console.log(`- Content-Range: ${response.headers["Content-Range"] || response.headers["content-range"] || "not set"}`);
          }
          
          return response;
        } else {
          // Response looks invalid despite having 200/206 status - track this error
          lastError = "Empty body and missing valid headers";
          errorDetails = {
            type: "empty_body",
            status: response.status,
            body_size: bodySize,
            has_content_length: hasValidContentLength,
            has_content_type: hasValidContentType,
            has_content_range: hasValidContentRange,
            headers: response.headers,
          };
          
          // For debugging, log detailed error information
          if (DEBUG_MODE) {
            console.log(`Warning: Response has ${response.status} status but appears invalid:`);
            console.log(`- URL: ${url}`);
            console.log(`- Body size: ${bodySize} bytes`);
            console.log(`- Has valid Content-Length: ${hasValidContentLength}`);
            console.log(`- Has valid Content-Type: ${hasValidContentType}`);
            console.log(`- Has valid Content-Range: ${hasValidContentRange}`);
            console.log(`- Headers: ${JSON.stringify(response.headers)}`);
          }
        }
      } else {
        // Non-success status code
        lastError = `HTTP status ${response.status}`;
        errorDetails = {
          type: "status_error",
          status: response.status,
          status_text: response.status_text || "No status text",
          body_size: getResponseSize(response),
        };
      }
      
      // Log error with more detail when retrying
      if (DEBUG_MODE) {
        console.log(`Attempt ${retries + 1}/${maxRetries + 1} failed with status ${response.status} for ${url}`);
        console.log(`Response headers: ${JSON.stringify(response.headers)}`);
      }
    } catch (error) {
      // Exception during request - network error, timeout, etc.
      lastError = error.message || "Network error";
      errorDetails = {
        type: "network_error",
        message: error.message,
        code: error.code,
        name: error.name,
      };
      
      if (DEBUG_MODE) {
        console.log(`Attempt ${retries + 1}/${maxRetries + 1} failed with error: ${error.message}`);
      }
    }
    
    retries++;
    if (retries <= maxRetries) {
      // Exponential backoff: 500ms, 1s, 2s, etc.
      const backoff = Math.min(Math.pow(2, retries - 1) * 500, 3000);
      sleep(backoff / 1000); // k6 sleep takes seconds, not ms
    }
  }
  
  // If we reach here, all retries failed
  return {
    status: 0,
    body: new Uint8Array(0),
    timings: { duration: 0 },
    headers: {},
    error: lastError,
    errorDetails: errorDetails,
    url: url
  };
}

// Initialize and log configuration
function init() {
  // Format configuration as a pretty table for better readability
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                 VIDEO LOAD TEST CONFIGURATION               ║');
  console.log('╠════════════════════════════════════╦═══════════════════════╣');
  console.log(`║ Base URL                           ║ ${padRight(BASE_URL, 21)} ║`);
  console.log(`║ Results File                       ║ ${padRight(shortenPath(RESULTS_FILE), 21)} ║`);
  console.log(`║ Error Report File                  ║ ${padRight(shortenPath(ERROR_REPORT_FILE || "Not specified"), 21)} ║`);
  console.log(`║ URL Format                         ║ ${padRight(URL_FORMAT, 21)} ║`);
  console.log(`║ Max Retries                        ║ ${padRight(MAX_RETRIES.toString(), 21)} ║`);
  console.log(`║ Available Videos                   ║ ${padRight(videoData.length.toString(), 21)} ║`);
  console.log(`║ Simulate Real Player               ║ ${padRight(SIMULATE_REAL_PLAYER.toString(), 21)} ║`);
  console.log(`║ Use HEAD Requests                  ║ ${padRight(USE_HEAD_REQUESTS.toString(), 21)} ║`);
  console.log(`║ Skip Large Files                   ║ ${padRight(SKIP_LARGE_FILES.toString(), 21)} ║`);
  console.log(`║ Large File Threshold               ║ ${padRight(LARGE_FILE_THRESHOLD_MIB + " MiB", 21)} ║`);
  console.log(`║ Debug Mode                         ║ ${padRight(DEBUG_MODE.toString(), 21)} ║`);
  console.log('╚════════════════════════════════════╩═══════════════════════╝');
  
  // Helper function to ensure consistent formatting
  function padRight(str, length) {
    str = str.toString();
    return str + ' '.repeat(Math.max(0, length - str.length));
  }
  
  // Helper to shorten paths for display
  function shortenPath(path) {
    if (path.length <= 21) return path;
    const parts = path.split('/');
    const filename = parts.pop();
    return filename.length > 18 ? filename.substring(0, 18) + '...' : filename;
  }
  
  // Report on excluded files
  if (skippedFiles.errorFiles > 0 || skippedFiles.largeFiles > 0) {
    console.log('\n╔════════════════════════════════════════════════════════════╗');
    console.log('║                  EXCLUDED FILES SUMMARY                   ║');
    console.log('╠════════════════════════════════════╦═══════════════════════╣');
    console.log(`║ Error Files Excluded               ║ ${padRight(skippedFiles.errorFiles.toString(), 21)} ║`);
    console.log(`║ Large Files Excluded              ║ ${padRight(skippedFiles.largeFiles + " (>" + LARGE_FILE_THRESHOLD_MIB + " MiB)", 21)} ║`);
    console.log(`║ Total Files Excluded              ║ ${padRight((skippedFiles.errorFiles + skippedFiles.largeFiles).toString(), 21)} ║`);
    console.log('╚════════════════════════════════════╩═══════════════════════╝');
    
    if (DEBUG_MODE) {
      if (skippedFiles.errorFiles > 0) {
        console.log("Error files excluded:");
        skippedFiles.errorFilePaths.slice(0, 10).forEach((path, i) => {
          console.log(`  ${i+1}. ${path}`);
        });
        if (skippedFiles.errorFilePaths.length > 10) {
          console.log(`  ... and ${skippedFiles.errorFilePaths.length - 10} more`);
        }
      }
      
      if (skippedFiles.largeFiles > 0) {
        console.log("Large files excluded:");
        skippedFiles.largeFilePaths.slice(0, 10).forEach((path, i) => {
          console.log(`  ${i+1}. ${path}`);
        });
        if (skippedFiles.largeFilePaths.length > 10) {
          console.log(`  ... and ${skippedFiles.largeFilePaths.length - 10} more`);
        }
      }
    }
  }
  
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
      
      // Also log size distribution
      const sizes = videoData.map(v => v.size);
      const totalSize = sizes.reduce((a, b) => a + b, 0);
      const avgSize = totalSize / sizes.length;
      const minSize = Math.min(...sizes);
      const maxSize = Math.max(...sizes);
      
      console.log(`\nVideo size statistics:`);
      console.log(`  - Average size: ${formatFileSize(avgSize)}`);
      console.log(`  - Min size: ${formatFileSize(minSize)}`);
      console.log(`  - Max size: ${formatFileSize(maxSize)}`);
      console.log(`  - Total size: ${formatFileSize(totalSize)}`);
      
      // Log size distribution breakdown
      console.log(`\nVideo size distribution:`);
      const sizeCategories = {
        'Tiny (<1 MiB)': 0,
        'Small (1-10 MiB)': 0,
        'Medium (10-50 MiB)': 0,
        'Large (50-200 MiB)': 0,
        'X-Large (>200 MiB)': 0
      };
      
      sizes.forEach(size => {
        const sizeMiB = size / (1024 * 1024);
        if (sizeMiB < 1) sizeCategories['Tiny (<1 MiB)']++;
        else if (sizeMiB < 10) sizeCategories['Small (1-10 MiB)']++;
        else if (sizeMiB < 50) sizeCategories['Medium (10-50 MiB)']++;
        else if (sizeMiB < 200) sizeCategories['Large (50-200 MiB)']++;
        else sizeCategories['X-Large (>200 MiB)']++;
      });
      
      Object.entries(sizeCategories).forEach(([category, count]) => {
        if (count > 0) {
          console.log(`  - ${category}: ${count} videos (${Math.round(count/sizes.length*100)}%)`);
        }
      });
    }
  }
}

// Format file size for human-readable output
function formatFileSize(sizeBytes) {
  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  } else if (sizeBytes < 1024 * 1024) {
    return `${(sizeBytes / 1024).toFixed(2)} KiB`;
  } else if (sizeBytes < 1024 * 1024 * 1024) {
    return `${(sizeBytes / (1024 * 1024)).toFixed(2)} MiB`;
  } else {
    return `${(sizeBytes / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
  }
}

// Make sure the init function only runs once
let initialized = false;

// Pre-load videos outside of the main function 
// This ensures it only happens once, not per VU
const preloadedVideoData = videoData;
const videoCount = preloadedVideoData.length;

// Run initialization only on the first import - not for each VU
if (!__VU || __VU === 1) {
  // Only run init for the first VU
  init();
  initialized = true;
}

// Main test function - runs for each VU
export default function() {
  // Skip if we have no video data
  if (videoCount === 0) {
    // Only log this message once from the first VU
    if (!__VU || __VU === 1) {
      console.log("No valid videos found in results file. Run the pre-warmer first.");
    }
    return;
  }

  // Select a random video from our dataset
  const videoInfo = preloadedVideoData[randomIntBetween(0, videoCount - 1)];
  
  // Generate URL for this video
  const videoUrl = generateVideoUrl(videoInfo);

  // Random range header or no range header (20% chance of no range)
  const useRangeHeader = Math.random() > 0.2;

  const params = {
    headers: {
      "User-Agent": generateUserAgent(),
      "Accept": "video/*,*/*;q=0.8", // More realistic accept header
      "Accept-Encoding": "gzip, deflate, br",
    },
    tags: {
      derivative: videoInfo.derivative,     // Tag by derivative for metrics
      size_category: getSizeCategory(videoInfo.size), // Tag by size category 
      request_type: useRangeHeader ? "range" : "full", // Tag range vs full requests
    },
    responseType: "binary", // Handle binary data properly
    timeout: __ENV.REQUEST_TIMEOUT || "120s", // Increased individual request timeout
  };

  if (useRangeHeader) {
    // Generate range based on HEAD request or fall back to estimated size
    params.headers["Range"] = generateRandomRange(videoInfo, videoUrl, params.headers);
  }

  // Make the request with retries
  const response = makeRequestWithRetry(videoUrl, params);

  // Add response status to tags for metrics segmentation
  params.tags.status = response.status.toString();
  
  // Check if successful and validate we got a response with detailed checks
  // Store information about each failure for better diagnostics
  const checks = {};
  let checksResult = true;
  
  // Track detailed check results for reporting
  checks["status is 200 or 206"] = (r) => {
    // Check for valid status codes or special case where we have a 0 status but valid error info
    // - Status 0 can happen when all retries fail, but we might have valid error info
    // - Clients can display an error to the user if needed based on this information
    // - This is especially relevant for network errors or timeouts
    const result = r.status === 200 || r.status === 206;
    
    // Track failure with counter metrics if check failed
    if (!result) {
      failureCounter.add(1);
      statusErrorCounter.add(1);
      
      // Log error details if in debug mode
      if (DEBUG_MODE) {
        // Check if this is a special status 0 case
        if (r.status === 0 && r.error) {
          console.log(`Status code error (with error info): status=${r.status}, error="${r.error}" for URL: ${videoUrl}`);
          
          // Log error details if available
          if (r.errorDetails) {
            console.log(`Error details: ${JSON.stringify(r.errorDetails)}`);
          }
        } else {
          console.log(`Status code error: ${r.status} for URL: ${videoUrl}`);
        }
      }
      
      // Track by derivative if possible
      try {
        if (videoInfo && videoInfo.derivative) {
          const derivative = videoInfo.derivative;
          if (derivative === 'desktop') desktopFailures.add(1);
          else if (derivative === 'tablet') tabletFailures.add(1);
          else if (derivative === 'mobile') mobileFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking derivative metric: ${e.message}`);
      }
      
      // Track by size category if possible
      try {
        if (videoInfo && videoInfo.size) {
          const sizeCategory = getSizeCategory(videoInfo.size);
          if (sizeCategory === 'small') smallFailures.add(1);
          else if (sizeCategory === 'medium') mediumFailures.add(1);
          else if (sizeCategory === 'large') largeFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking size metric: ${e.message}`);
      }
    }
    
    checksResult = checksResult && result;
    return result;
  };
  

  checks["got response body"] = (r) => {
    // Check response body size using the helper function
    const bodySize = getResponseSize(r);
    
    // Also check headers as a fallback
    const hasValidHeader = hasValidHeaders(r);
    
    // Response is valid if:
    // 1. It has a body with positive size, OR
    // 2. It has valid content headers (Content-Length or Content-Range)
    const result = bodySize > 0 || hasValidHeader;
    
    // Track failure with counter metrics if check failed
    if (!result) {
      failureCounter.add(1);
      emptyBodyCounter.add(1);
      
      // Log detailed error info if in debug mode
      if (DEBUG_MODE) {
        console.log(`Empty body error: body size=${bodySize} bytes, has valid headers=${hasValidHeader}, status=${r.status} for URL: ${videoUrl}`);
        if (r.headers) {
          console.log(`Headers: ${JSON.stringify(r.headers)}`);
        }
      }
      
      // Track by derivative if possible
      try {
        if (videoInfo && videoInfo.derivative) {
          const derivative = videoInfo.derivative;
          if (derivative === 'desktop') desktopFailures.add(1);
          else if (derivative === 'tablet') tabletFailures.add(1);
          else if (derivative === 'mobile') mobileFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking derivative metric: ${e.message}`);
      }
      
      // Track by size category if possible
      try {
        if (videoInfo && videoInfo.size) {
          const sizeCategory = getSizeCategory(videoInfo.size);
          if (sizeCategory === 'small') smallFailures.add(1);
          else if (sizeCategory === 'medium') mediumFailures.add(1);
          else if (sizeCategory === 'large') largeFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking size metric: ${e.message}`);
      }
    }
    
    checksResult = checksResult && result;
    return result;
  };
  
  checks["response time reasonable"] = (r) => {
    const threshold = parseInt(__ENV.RESPONSE_TIME_THRESHOLD || "10000");
    const result = r && r.timings && r.timings.duration < threshold;
    
    // Track failure with counter metrics if check failed
    if (!result) {
      failureCounter.add(1);
      timeoutErrorCounter.add(1);
      
      // Log error details if in debug mode
      if (DEBUG_MODE) {
        console.log(`Timeout error: ${r.timings.duration}ms exceeded threshold ${threshold}ms for URL: ${videoUrl}`);
      }
      
      // Track by derivative if possible
      try {
        if (videoInfo && videoInfo.derivative) {
          const derivative = videoInfo.derivative;
          if (derivative === 'desktop') desktopFailures.add(1);
          else if (derivative === 'tablet') tabletFailures.add(1);
          else if (derivative === 'mobile') mobileFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking derivative metric: ${e.message}`);
      }
      
      // Track by size category if possible
      try {
        if (videoInfo && videoInfo.size) {
          const sizeCategory = getSizeCategory(videoInfo.size);
          if (sizeCategory === 'small') smallFailures.add(1);
          else if (sizeCategory === 'medium') mediumFailures.add(1);
          else if (sizeCategory === 'large') largeFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking size metric: ${e.message}`);
      }
    }
    
    checksResult = checksResult && result;
    return result;
  };
  
  checks["content-type is video"] = (r) => {
    const contentType = (r.headers && (r.headers["Content-Type"] || r.headers["content-type"])) || "";
    
    // Get the body size using our helper function
    const bodySize = getResponseSize(r);
    
    // Accept any of the following:
    // 1. Any video/* content type (video/mp4, video/webm, etc.)
    // 2. application/octet-stream (common for partial content)
    // 3. If we have a 206 Partial Content response with valid headers or body content
    // 4. If response has Content-Range header (definitely video content)
    const isVideoContentType = contentType.includes("video/") || 
                              contentType.includes("application/octet-stream");
    
    const hasContentRange = r.headers && 
                           (r.headers["Content-Range"] || r.headers["content-range"]);
    
    const has206WithContent = r.status === 206 && 
                             (hasContentRange || bodySize > 0);
    
    // Consider valid with:
    // - Video content type, OR
    // - Valid 206 partial content response, OR
    // - Status 200 with non-empty body
    const result = isVideoContentType || 
                   has206WithContent || 
                   (r.status === 200 && bodySize > 0);
    
    // Track failure with counter metrics if check failed
    if (!result) {
      failureCounter.add(1);
      contentTypeErrorCounter.add(1);
      
      // Log detailed error info if in debug mode
      if (DEBUG_MODE) {
        console.log(`Content-type error: Expected video content, details:`);
        console.log(`- Status: ${r.status}`);
        console.log(`- Content-Type: "${contentType}"`);
        console.log(`- Has Content-Range: ${hasContentRange ? "yes" : "no"}`);
        console.log(`- Body size: ${bodySize} bytes`);
        console.log(`- URL: ${videoUrl}`);
        if (r.headers) {
          console.log(`- Headers: ${JSON.stringify(r.headers)}`);
        }
      }
      
      // Track by derivative if possible
      try {
        if (videoInfo && videoInfo.derivative) {
          const derivative = videoInfo.derivative;
          if (derivative === 'desktop') desktopFailures.add(1);
          else if (derivative === 'tablet') tabletFailures.add(1);
          else if (derivative === 'mobile') mobileFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking derivative metric: ${e.message}`);
      }
      
      // Track by size category if possible
      try {
        if (videoInfo && videoInfo.size) {
          const sizeCategory = getSizeCategory(videoInfo.size);
          if (sizeCategory === 'small') smallFailures.add(1);
          else if (sizeCategory === 'medium') mediumFailures.add(1);
          else if (sizeCategory === 'large') largeFailures.add(1);
        }
      } catch (e) {
        if (DEBUG_MODE) console.log(`Error tracking size metric: ${e.message}`);
      }
    }
    
    checksResult = checksResult && result;
    return result;
  };
  
  // Track custom detailed failure metrics
  function trackCheckFailure(failureType, details) {
    // Make sure we have access to the current scope's variables
    if (typeof videoInfo === 'undefined' || !videoInfo) {
      console.warn("videoInfo not available in current scope");
      return;
    }
    
    if (typeof videoUrl === 'undefined' || !videoUrl) {
      console.warn("videoUrl not available in current scope");
      return;
    }
    
    if (typeof response === 'undefined' || !response) {
      console.warn("response not available in current scope");
      return;
    }
    
    // Initialize or update global failure tracking
    if (!__ENV.FAILURE_TRACKING) {
      __ENV.FAILURE_TRACKING = {
        count: 0,
        by_type: {},
        by_derivative: {},
        by_status: {},
        by_size_category: {},
        samples: {}, // Store failures by type
        start_time: new Date().getTime()
      };
    }
    
    // Update counters
    const tracking = __ENV.FAILURE_TRACKING;
    tracking.count++;
    
    // Track by failure type
    if (!tracking.by_type[failureType]) tracking.by_type[failureType] = 0;
    tracking.by_type[failureType]++;
    
    // Get derivative info safely
    const derivative = videoInfo ? videoInfo.derivative || 'unknown' : 'unknown';
    if (!tracking.by_derivative[derivative]) tracking.by_derivative[derivative] = 0;
    tracking.by_derivative[derivative]++;
    
    // Track by HTTP status - safely handle status
    const status = (response && response.status !== undefined) ? response.status.toString() : 'unknown';
    if (!tracking.by_status[status]) tracking.by_status[status] = 0;
    tracking.by_status[status]++;
    
    // Track by size category - safely get size
    const sizeCategory = videoInfo && videoInfo.size ? getSizeCategory(videoInfo.size) : 'unknown';
    if (!tracking.by_size_category[sizeCategory]) tracking.by_size_category[sizeCategory] = 0;
    tracking.by_size_category[sizeCategory]++;
    
    // Store sample failures by type (max 10 samples per type)
    if (!tracking.samples[failureType]) tracking.samples[failureType] = [];
    
    if (tracking.samples[failureType].length < 10) {
      const headers = response && response.headers ? response.headers : {};
      const sample = {
        url: videoUrl,
        derivative: derivative,
        status: status,
        size_category: sizeCategory,
        details: details,
        timestamp: new Date().toISOString()
      };
      
      // Safely add headers info
      if (headers) {
        sample.content_type = (headers["Content-Type"]) || "unknown";
        sample.content_length = (headers["Content-Length"]) || "unknown";
      }
      
      // Safely add body preview
      if (response && response.body && response.body.length > 0) {
        try {
          // Make sure slice is available before calling it
          if (typeof response.body.slice === 'function') {
            sample.body_preview = response.body.slice(0, 200).toString() + "...";
          } else {
            sample.body_preview = "body not sliceable";
          }
        } catch (e) {
          sample.body_preview = "error extracting body";
        }
      } else {
        sample.body_preview = "empty";
      }
      
      tracking.samples[failureType].push(sample);
    }
    
    // Output a summary every 100 failures
    if (tracking.count % 100 === 0) {
      console.log(`\n======== FAILURE TRACKING UPDATE ========`);
      console.log(`Total failures: ${tracking.count}`);
      console.log(`Failure types: ${JSON.stringify(tracking.by_type)}`);
      console.log(`Derivatives: ${JSON.stringify(tracking.by_derivative)}`);
      console.log(`Status codes: ${JSON.stringify(tracking.by_status)}`);
      console.log(`Size categories: ${JSON.stringify(tracking.by_size_category)}`);
      console.log(`====================================`);
    }
  }
  
  // Generate a comprehensive failure report
  function generateFailureReport() {
    // Check if we have failure tracking data
    if (!__ENV.FAILURE_TRACKING || __ENV.FAILURE_TRACKING.count === 0) {
      console.log("No failures tracked during test run.");
      return;
    }
    
    const tracking = __ENV.FAILURE_TRACKING;
    const duration = (new Date().getTime() - tracking.start_time) / 1000; // in seconds
    
    // Print failure report to console
    console.log("\n\n╔════════════════════════════════════════════════════════════╗");
    console.log("║              DETAILED FAILURE REPORT                     ║");
    console.log("╠════════════════════════════════════════════════════════════╣");
    console.log(`║ Total Failures: ${tracking.count}`);
    console.log(`║ Test Duration: ${duration.toFixed(1)} seconds`);
    console.log(`║ Failure Rate: ${(tracking.count / (tracking.count + __ENV.SUCCESS_COUNT || 1) * 100).toFixed(2)}%`);
    console.log("╠════════════════════════════════════════════════════════════╣");
    
    // Failure by type
    console.log("║ FAILURE TYPES:                                           ║");
    Object.entries(tracking.by_type).sort((a, b) => b[1] - a[1]).forEach(([type, count]) => {
      const percent = (count / tracking.count * 100).toFixed(1);
      console.log(`║  - ${type.padEnd(20)}: ${count.toString().padStart(5)} (${percent.padStart(4)}%)${' '.repeat(13)}║`);
    });
    
    // Failure by derivative
    console.log("╠════════════════════════════════════════════════════════════╣");
    console.log("║ FAILURES BY DERIVATIVE:                                  ║");
    Object.entries(tracking.by_derivative).sort((a, b) => b[1] - a[1]).forEach(([derivative, count]) => {
      const percent = (count / tracking.count * 100).toFixed(1);
      console.log(`║  - ${derivative.padEnd(20)}: ${count.toString().padStart(5)} (${percent.padStart(4)}%)${' '.repeat(13)}║`);
    });
    
    // Failure by status code
    console.log("╠════════════════════════════════════════════════════════════╣");
    console.log("║ FAILURES BY STATUS CODE:                                 ║");
    Object.entries(tracking.by_status).sort((a, b) => b[1] - a[1]).forEach(([status, count]) => {
      const percent = (count / tracking.count * 100).toFixed(1);
      console.log(`║  - ${status.padEnd(20)}: ${count.toString().padStart(5)} (${percent.padStart(4)}%)${' '.repeat(13)}║`);
    });
    
    // Failure by size category
    console.log("╠════════════════════════════════════════════════════════════╣");
    console.log("║ FAILURES BY SIZE CATEGORY:                               ║");
    Object.entries(tracking.by_size_category).sort((a, b) => b[1] - a[1]).forEach(([sizeCategory, count]) => {
      const percent = (count / tracking.count * 100).toFixed(1);
      console.log(`║  - ${sizeCategory.padEnd(20)}: ${count.toString().padStart(5)} (${percent.padStart(4)}%)${' '.repeat(13)}║`);
    });
    
    // Sample failures
    console.log("╠════════════════════════════════════════════════════════════╣");
    console.log("║ SAMPLE FAILURES BY TYPE:                                 ║");
    console.log("╠════════════════════════════════════════════════════════════╣");
    
    Object.entries(tracking.samples).forEach(([failureType, samples]) => {
      if (samples.length === 0) return;
      
      console.log(`║ TYPE: ${failureType.toUpperCase().padEnd(52)}║`);
      console.log("║                                                            ║");
      
      // Show first 3 samples for each type
      samples.slice(0, 3).forEach((sample, idx) => {
        console.log(`║ Sample #${idx+1}:${' '.repeat(51)}║`);
        console.log(`║   URL: ${sample.url.substring(0, 50).padEnd(50)}║`);
        console.log(`║   Status: ${sample.status}${' '.repeat(51 - sample.status.toString().length)}║`);
        console.log(`║   Derivative: ${sample.derivative}${' '.repeat(48 - sample.derivative.length)}║`);
        console.log(`║   Size Category: ${sample.size_category}${' '.repeat(45 - sample.size_category.length)}║`);
        console.log(`║   Content-Type: ${(sample.content_type || "unknown").substring(0, 45).padEnd(45)}║`);
        console.log(`║   Content-Length: ${(sample.content_length || "unknown").substring(0, 42).padEnd(42)}║`);
        console.log(`║   Details: ${sample.details.substring(0, 50).padEnd(50)}║`);
        console.log("║                                                            ║");
      });
      
      // Show count of additional samples
      if (samples.length > 3) {
        console.log(`║ ... and ${samples.length - 3} more samples                          ║`);
      }
      console.log("║                                                            ║");
    });
    
    console.log("╚════════════════════════════════════════════════════════════╝");

    // Write detailed report to JSON file if file output is enabled
    if (__ENV.FAILURE_REPORT_FILE) {
      // Add timestamp and test metadata
      const reportData = {
        timestamp: new Date().toISOString(),
        test_duration_seconds: duration,
        total_failures: tracking.count,
        failure_tracking: tracking,
        test_params: {
          base_url: BASE_URL,
          results_file: RESULTS_FILE,
          error_report_file: ERROR_REPORT_FILE,
          url_format: URL_FORMAT,
          skip_large_files: SKIP_LARGE_FILES,
          large_file_threshold_mib: LARGE_FILE_THRESHOLD_MIB
        }
      };

      try {
        const reportPath = __ENV.FAILURE_REPORT_FILE;
        console.log(`Writing detailed failure report to ${reportPath}`);
        
        // Return this data to be saved by k6's handleSummary
        // This will ensure it's written to the file specified in FAILURE_REPORT_FILE
        __ENV.FAILURE_REPORT_DATA = reportData;
      } catch (e) {
        console.error(`Failed to prepare failure report: ${e.message}`);
      }
    }
  }
  
  // Run all checks
  const checkResult = check(response, checks);

  // Cache Content-Length for future requests if not already cached
  if (checks && response.headers && response.headers["Content-Length"] && !CONTENT_LENGTH_CACHE.has(videoUrl)) {
    const contentLength = parseInt(response.headers["Content-Length"]);
    if (contentLength > 0) {
      CONTENT_LENGTH_CACHE.set(videoUrl, contentLength);
      if (DEBUG_MODE) {
        console.log(`Cached Content-Length ${contentLength} (${formatFileSize(contentLength)}) for ${videoUrl}`);
      }
    }
  }
  
  // Success tracking disabled for now

  // Detailed error logging
  if (!checks) {
    // Use error grouping to organize output
    let byteRange = useRangeHeader ? params.headers["Range"] : "N/A";
    let errorMsg = response.error || "unknown error";
    let errorDetails = response.errorDetails || {};
    
    // Only log full details in debug mode to reduce output volume
    if (DEBUG_MODE) {
      console.log(
        `Error fetching: url=${videoUrl} | status=${response.status} | derivative=${videoInfo.derivative} | range=${byteRange}`
      );
      console.log(
        `Error details: ${errorMsg}${errorDetails ? " | " + JSON.stringify(errorDetails) : ""}`
      );
    } else {
      // In non-debug mode, just increment counters in a custom metric
      if (!__ENV.ERROR_METRICS) {
        __ENV.ERROR_METRICS = {
          count: 0,
          by_status: {},
          by_derivative: {},
          by_error_type: {}
        };
      }
      
      // Update counters
      let metrics = __ENV.ERROR_METRICS;
      metrics.count++;
      
      // Track by status
      if (!metrics.by_status[response.status]) {
        metrics.by_status[response.status] = 0;
      }
      metrics.by_status[response.status]++;
      
      // Track by derivative
      if (!metrics.by_derivative[videoInfo.derivative]) {
        metrics.by_derivative[videoInfo.derivative] = 0;
      }
      metrics.by_derivative[videoInfo.derivative]++;
      
      // Track by error type
      let errorType = errorDetails.type || "unknown";
      if (!metrics.by_error_type[errorType]) {
        metrics.by_error_type[errorType] = 0;
      }
      metrics.by_error_type[errorType]++;
      
      // Every 20 errors, output a summary to avoid overwhelming logs
      if (metrics.count % 20 === 0) {
        console.log(`Error summary (total: ${metrics.count}):`+
          ` status codes: ${JSON.stringify(metrics.by_status)},`+
          ` derivatives: ${JSON.stringify(metrics.by_derivative)},`+
          ` error types: ${JSON.stringify(metrics.by_error_type)}`);
      }
    }
  }

  // Log all 206 responses if we're in debug mode to help diagnose issues
  if (DEBUG_MODE && response.status === 206) {
    // Safe way to get response body size
    let bodySize = 0;
    if (response.body) {
      if (typeof response.body.byteLength === 'number') bodySize = response.body.byteLength;
      else if (typeof response.body.length === 'number') bodySize = response.body.length;
      else if (typeof response.body.size === 'number') bodySize = response.body.size;
    }
    
    const hasContentRange = response.headers && 
                          (response.headers["Content-Range"] || response.headers["content-range"]);
    const contentType = response.headers ? 
                       (response.headers["Content-Type"] || response.headers["content-type"] || "") : 
                       "";
    const contentLength = response.headers ? 
                         (response.headers["Content-Length"] || response.headers["content-length"] || "") : 
                         "";
    
    console.log(`206 Partial Content response details:`);
    console.log(`- URL: ${videoUrl}`);
    console.log(`- Request Range header: ${params.headers["Range"] || "not specified"}`);
    console.log(`- Response body size: ${bodySize} bytes`);
    console.log(`- Content-Range: ${response.headers["Content-Range"] || response.headers["content-range"] || "not specified"}`);
    console.log(`- Content-Type: ${contentType}`);
    console.log(`- Content-Length: ${contentLength}`);
    console.log(`- Is valid Content-Type: ${contentType.includes("video/") || contentType.includes("application/octet-stream")}`);
    console.log(`- Has valid Content-Range: ${hasContentRange ? "yes" : "no"}`);
    
    // Safely call check functions only if they exist
    try {
      console.log(`- Check results: status=${checks["status is 200 or 206"](response)}, body=${checks["got response body"](response)}, content-type=${checks["content-type is video"](response)}`);
    } catch (e) {
      console.log(`- Unable to run checks: ${e.message}`);
    }
  }
  
  // More realistic sleep between requests - sleep longer after errors
  if (response.status !== 200 && response.status !== 206) {
    sleep(randomIntBetween(
      parseFloat(__ENV.ERROR_MIN_SLEEP || "3"), 
      parseFloat(__ENV.ERROR_MAX_SLEEP || "8")
    ));
  } else {
    sleep(randomIntBetween(
      parseFloat(__ENV.MIN_SLEEP || "2"), 
      parseFloat(__ENV.MAX_SLEEP || "5")
    ));
  }
}

// Helper function to categorize video size
function getSizeCategory(sizeBytes) {
  const sizeMiB = sizeBytes / (1024 * 1024);
  
  if (sizeMiB < 50) {
    return "small";
  } else if (sizeMiB < 200) {
    return "medium";
  } else {
    return "large";
  }
}

// Handle summary function - called by k6 at the end of the test
export function handleSummary(data) {
  try {
    // Extract failure metrics from the data
    const totalFailures = data.metrics.custom_failures ? data.metrics.custom_failures.values.count : 0;
    
    // Create a detailed failure report from the k6 metrics
    let reportData = {
      timestamp: new Date().toISOString(),
      test_duration_seconds: data.state.testRunDurationMs / 1000,
      test_params: {
        base_url: BASE_URL,
        results_file: RESULTS_FILE,
        error_report_file: ERROR_REPORT_FILE,
        url_format: URL_FORMAT,
        skip_large_files: SKIP_LARGE_FILES,
        large_file_threshold_mib: LARGE_FILE_THRESHOLD_MIB
      },
      failures: {
        total: totalFailures,
        by_type: {
          status_code: data.metrics.status_code_errors ? data.metrics.status_code_errors.values.count : 0,
          empty_body: data.metrics.empty_body_errors ? data.metrics.empty_body_errors.values.count : 0,
          timeout: data.metrics.timeout_errors ? data.metrics.timeout_errors.values.count : 0,
          content_type: data.metrics.content_type_errors ? data.metrics.content_type_errors.values.count : 0
        },
        by_derivative: {
          desktop: data.metrics.desktop_failures ? data.metrics.desktop_failures.values.count : 0,
          tablet: data.metrics.tablet_failures ? data.metrics.tablet_failures.values.count : 0,
          mobile: data.metrics.mobile_failures ? data.metrics.mobile_failures.values.count : 0
        },
        by_size: {
          small: data.metrics.small_file_failures ? data.metrics.small_file_failures.values.count : 0,
          medium: data.metrics.medium_file_failures ? data.metrics.medium_file_failures.values.count : 0,
          large: data.metrics.large_file_failures ? data.metrics.large_file_failures.values.count : 0
        }
      },
      all_metrics: data.metrics
    };
      
    // Print a nice formatted summary to the console
    console.log(`\n\n╔════════════════════════════════════════════════════════════╗`);
    console.log(`║                 TEST SUMMARY REPORT                      ║`);
    console.log(`╠════════════════════════════════════════════════════════════╣`);
    console.log(`║ Test Duration: ${(data.state.testRunDurationMs / 1000).toFixed(1)} seconds`);
    
    if (data.metrics && data.metrics.checks) {
      const checks = data.metrics.checks;
      console.log(`║ Checks: ${checks.passes} passed, ${checks.fails} failed`);
    }
    
    if (data.metrics && data.metrics.http_reqs) {
      console.log(`║ Total Requests: ${data.metrics.http_reqs.count}`);
    }
    
    // Show failure information if we have failures
    if (totalFailures > 0) {
      console.log(`╠════════════════════════════════════════════════════════════╣`);
      console.log(`║ FAILURE DETAILS:                                        ║`);
      console.log(`║ Total Failures: ${totalFailures}                                    ║`);
      
      // Show failure types
      console.log(`╠════════════════════════════════════════════════════════════╣`);
      console.log(`║ FAILURE TYPES:                                          ║`);
      Object.entries(reportData.failures.by_type).forEach(([type, count]) => {
        if (count > 0) {
          const percent = (count / totalFailures * 100).toFixed(1);
          console.log(`║  - ${type.padEnd(20)}: ${count.toString().padStart(5)} (${percent.padStart(4)}%)${' '.repeat(13)}║`);
        }
      });
      
      // Show failures by derivative
      if (Object.values(reportData.failures.by_derivative).some(count => count > 0)) {
        console.log(`╠════════════════════════════════════════════════════════════╣`);
        console.log(`║ FAILURES BY DERIVATIVE:                                 ║`);
        Object.entries(reportData.failures.by_derivative).forEach(([derivative, count]) => {
          if (count > 0) {
            const percent = (count / totalFailures * 100).toFixed(1);
            console.log(`║  - ${derivative.padEnd(20)}: ${count.toString().padStart(5)} (${percent.padStart(4)}%)${' '.repeat(13)}║`);
          }
        });
      }
      
      // Show failures by size
      if (Object.values(reportData.failures.by_size).some(count => count > 0)) {
        console.log(`╠════════════════════════════════════════════════════════════╣`);
        console.log(`║ FAILURES BY SIZE:                                       ║`);
        Object.entries(reportData.failures.by_size).forEach(([size, count]) => {
          if (count > 0) {
            const percent = (count / totalFailures * 100).toFixed(1);
            console.log(`║  - ${size.padEnd(20)}: ${count.toString().padStart(5)} (${percent.padStart(4)}%)${' '.repeat(13)}║`);
          }
        });
      }
    }
    
    console.log(`╚════════════════════════════════════════════════════════════╝`);
    
    // Create the return object with standard output
    const result = {
      "stdout": JSON.stringify(data),
    };
    
    // Write detailed report to file if we have an output file path
    if (__ENV.FAILURE_REPORT_FILE) {
      result[__ENV.FAILURE_REPORT_FILE] = JSON.stringify(reportData, null, 2);
    }
    
    return result;
  } catch (e) {
    console.error(`Error generating summary: ${e.message}`);
    return {
      "stdout": JSON.stringify(data)
    };
  }
}