package k6

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
)

// TestConfig configures k6 load testing parameters
type TestConfig struct {
	// Test execution
	VirtualUsers    int           `json:"virtual_users"`
	Duration        time.Duration `json:"duration"`
	Iterations      int           `json:"iterations,omitempty"`
	
	// URLs and targeting
	BaseURL         string        `json:"base_url"`
	URLsPerVU       int           `json:"urls_per_vu"`
	TestDataFile    string        `json:"test_data_file,omitempty"`
	
	// Performance thresholds
	MaxResponseTime time.Duration `json:"max_response_time"`
	MinSuccessRate  float64       `json:"min_success_rate"`
	
	// Output configuration
	OutputFormat    string        `json:"output_format"` // json, csv, influxdb
	OutputFile      string        `json:"output_file,omitempty"`
	
	// k6 specific
	K6ScriptPath    string        `json:"k6_script_path"`
	K6Binary        string        `json:"k6_binary"`
	
	// Environment variables for k6 script
	Environment     map[string]string `json:"environment,omitempty"`
}

// DefaultTestConfig returns sensible defaults for load testing
func DefaultTestConfig() TestConfig {
	return TestConfig{
		VirtualUsers:    10,
		Duration:        30 * time.Second,
		URLsPerVU:       50,
		MaxResponseTime: 5 * time.Second,
		MinSuccessRate:  95.0,
		OutputFormat:    "json",
		K6Binary:        "k6",
		Environment:     make(map[string]string),
	}
}

// TestResult represents the results from a k6 load test
type TestResult struct {
	// Test configuration
	Config          TestConfig    `json:"config"`
	
	// Execution info
	Success         bool          `json:"success"`
	Error           string        `json:"error,omitempty"`
	StartTime       time.Time     `json:"start_time"`
	EndTime         time.Time     `json:"end_time"`
	Duration        time.Duration `json:"duration"`
	
	// Performance metrics
	Metrics         K6Metrics     `json:"metrics"`
	
	// Raw k6 output
	K6Command       string        `json:"k6_command,omitempty"`
	K6Output        string        `json:"k6_output,omitempty"`
	K6Summary       *K6Summary    `json:"k6_summary,omitempty"`
}

// K6Metrics contains parsed k6 performance metrics
type K6Metrics struct {
	// Request metrics
	HTTPRequestsTotal     int64         `json:"http_requests_total"`
	HTTPRequestsSucceeded int64         `json:"http_requests_succeeded"`
	HTTPRequestsFailed    int64         `json:"http_requests_failed"`
	HTTPRequestsRate      float64       `json:"http_requests_rate"`
	
	// Response time metrics
	HTTPReqDurationAvg    time.Duration `json:"http_req_duration_avg"`
	HTTPReqDurationMin    time.Duration `json:"http_req_duration_min"`
	HTTPReqDurationMax    time.Duration `json:"http_req_duration_max"`
	HTTPReqDurationP90    time.Duration `json:"http_req_duration_p90"`
	HTTPReqDurationP95    time.Duration `json:"http_req_duration_p95"`
	HTTPReqDurationP99    time.Duration `json:"http_req_duration_p99"`
	
	// Other metrics
	DataReceived          int64         `json:"data_received"`
	DataSent              int64         `json:"data_sent"`
	VirtualUsersMax       int           `json:"virtual_users_max"`
	IterationsTotal       int64         `json:"iterations_total"`
	IterationsRate        float64       `json:"iterations_rate"`
	
	// Success rate
	SuccessRate           float64       `json:"success_rate"`
}

// K6Summary represents k6's summary output structure
type K6Summary struct {
	Metrics map[string]interface{} `json:"metrics"`
	Root    map[string]interface{} `json:"root_group"`
}

// Runner handles k6 load test execution
type Runner struct {
	k6Binary string
	logger   *zap.Logger
}

// NewRunner creates a new k6 test runner
func NewRunner(logger *zap.Logger) *Runner {
	return &Runner{
		k6Binary: "k6",
		logger:   logger,
	}
}

// NewRunnerWithBinary creates a new k6 test runner with custom k6 binary path
func NewRunnerWithBinary(k6Binary string, logger *zap.Logger) *Runner {
	return &Runner{
		k6Binary: k6Binary,
		logger:   logger,
	}
}

// RunTest executes a k6 load test with the given configuration
func (r *Runner) RunTest(ctx context.Context, config TestConfig) (*TestResult, error) {
	startTime := time.Now()
	
	result := &TestResult{
		Config:    config,
		StartTime: startTime,
	}
	
	r.logger.Info("Starting k6 load test",
		zap.String("base_url", config.BaseURL),
		zap.Int("virtual_users", config.VirtualUsers),
		zap.Duration("duration", config.Duration),
		zap.Int("urls_per_vu", config.URLsPerVU))
	
	// Validate configuration
	if err := r.validateConfig(config); err != nil {
		result.Error = fmt.Sprintf("invalid configuration: %v", err)
		return result, err
	}
	
	// Build k6 command
	cmd, err := r.buildK6Command(ctx, config)
	if err != nil {
		result.Error = fmt.Sprintf("failed to build k6 command: %v", err)
		return result, err
	}
	result.K6Command = strings.Join(cmd.Args, " ")
	
	// Execute k6 test
	output, err := r.executeK6Test(ctx, cmd)
	if err != nil {
		result.Error = err.Error()
		result.K6Output = output
		return result, err
	}
	result.K6Output = output
	
	// Parse k6 output
	metrics, summary, err := r.parseK6Output(output)
	if err != nil {
		r.logger.Warn("Failed to parse k6 output", zap.Error(err))
	} else {
		result.Metrics = metrics
		result.K6Summary = summary
	}
	
	// Check if test passed thresholds
	if err := r.validateThresholds(config, result.Metrics); err != nil {
		result.Error = fmt.Sprintf("test failed thresholds: %v", err)
		r.logger.Error("Load test failed thresholds", zap.Error(err))
	} else {
		result.Success = true
	}
	
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	
	r.logger.Info("k6 load test completed",
		zap.Bool("success", result.Success),
		zap.Duration("duration", result.Duration),
		zap.Int64("total_requests", result.Metrics.HTTPRequestsTotal),
		zap.Float64("success_rate", result.Metrics.SuccessRate))
	
	return result, nil
}

// validateConfig validates the test configuration
func (r *Runner) validateConfig(config TestConfig) error {
	if config.BaseURL == "" {
		return fmt.Errorf("base_url is required")
	}
	
	if config.K6ScriptPath == "" {
		return fmt.Errorf("k6_script_path is required")
	}
	
	if _, err := os.Stat(config.K6ScriptPath); err != nil {
		return fmt.Errorf("k6 script not found: %w", err)
	}
	
	if config.VirtualUsers <= 0 {
		return fmt.Errorf("virtual_users must be positive")
	}
	
	if config.Duration <= 0 && config.Iterations <= 0 {
		return fmt.Errorf("either duration or iterations must be specified")
	}
	
	return nil
}

// buildK6Command constructs the k6 command with appropriate arguments
func (r *Runner) buildK6Command(ctx context.Context, config TestConfig) (*exec.Cmd, error) {
	args := []string{"run"}
	
	// Virtual users
	args = append(args, "--vus", fmt.Sprintf("%d", config.VirtualUsers))
	
	// Duration or iterations
	if config.Duration > 0 {
		args = append(args, "--duration", config.Duration.String())
	}
	if config.Iterations > 0 {
		args = append(args, "--iterations", fmt.Sprintf("%d", config.Iterations))
	}
	
	// Output format
	if config.OutputFile != "" {
		switch config.OutputFormat {
		case "json":
			args = append(args, "--out", fmt.Sprintf("json=%s", config.OutputFile))
		case "csv":
			args = append(args, "--out", fmt.Sprintf("csv=%s", config.OutputFile))
		case "influxdb":
			args = append(args, "--out", fmt.Sprintf("influxdb=%s", config.OutputFile))
		}
	}
	
	// Summary format (always JSON for parsing)
	args = append(args, "--summary-export", "/tmp/k6-summary.json")
	
	// Thresholds
	if config.MaxResponseTime > 0 {
		threshold := fmt.Sprintf("http_req_duration<=%d", config.MaxResponseTime.Milliseconds())
		args = append(args, "--threshold", threshold)
	}
	
	if config.MinSuccessRate > 0 {
		threshold := fmt.Sprintf("http_req_failed<%.2f", (100-config.MinSuccessRate)/100)
		args = append(args, "--threshold", threshold)
	}
	
	// Script path
	args = append(args, config.K6ScriptPath)
	
	cmd := exec.CommandContext(ctx, r.k6Binary, args...)
	
	// Set environment variables
	env := os.Environ()
	
	// Add BASE_URL and URLs_PER_VU for the script
	env = append(env, fmt.Sprintf("BASE_URL=%s", config.BaseURL))
	env = append(env, fmt.Sprintf("URLS_PER_VU=%d", config.URLsPerVU))
	
	// Add custom environment variables
	for key, value := range config.Environment {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}
	
	if config.TestDataFile != "" {
		env = append(env, fmt.Sprintf("TEST_DATA_FILE=%s", config.TestDataFile))
	}
	
	cmd.Env = env
	
	return cmd, nil
}

// executeK6Test executes the k6 command and captures output
func (r *Runner) executeK6Test(ctx context.Context, cmd *exec.Cmd) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	
	r.logger.Debug("Executing k6 command", zap.String("command", strings.Join(cmd.Args, " ")))
	
	err := cmd.Run()
	output := stdout.String()
	stderrOutput := stderr.String()
	
	if err != nil {
		r.logger.Error("k6 execution failed",
			zap.Error(err),
			zap.String("stdout", output),
			zap.String("stderr", stderrOutput))
		return output + "\n" + stderrOutput, fmt.Errorf("k6 failed: %w", err)
	}
	
	return output, nil
}

// parseK6Output parses k6 output to extract metrics
func (r *Runner) parseK6Output(output string) (K6Metrics, *K6Summary, error) {
	var metrics K6Metrics
	var summary *K6Summary
	
	// Try to load summary from the exported file
	summaryPath := "/tmp/k6-summary.json"
	if summaryData, err := os.ReadFile(summaryPath); err == nil {
		var s K6Summary
		if err := json.Unmarshal(summaryData, &s); err == nil {
			summary = &s
			metrics = r.extractMetricsFromSummary(&s)
		}
		// Clean up the summary file
		os.Remove(summaryPath)
	}
	
	// If summary parsing failed, try to parse from text output
	if summary == nil {
		metrics = r.parseTextOutput(output)
	}
	
	return metrics, summary, nil
}

// extractMetricsFromSummary extracts metrics from k6 JSON summary
func (r *Runner) extractMetricsFromSummary(summary *K6Summary) K6Metrics {
	var metrics K6Metrics
	
	if summary.Metrics == nil {
		return metrics
	}
	
	// Helper function to extract metric values
	getMetricValue := func(name, field string) float64 {
		if metric, ok := summary.Metrics[name].(map[string]interface{}); ok {
			if value, ok := metric[field].(float64); ok {
				return value
			}
		}
		return 0
	}
	
	// Extract HTTP request metrics
	metrics.HTTPRequestsTotal = int64(getMetricValue("http_reqs", "count"))
	metrics.HTTPRequestsFailed = int64(getMetricValue("http_req_failed", "fails"))
	metrics.HTTPRequestsSucceeded = metrics.HTTPRequestsTotal - metrics.HTTPRequestsFailed
	metrics.HTTPRequestsRate = getMetricValue("http_reqs", "rate")
	
	// Calculate success rate
	if metrics.HTTPRequestsTotal > 0 {
		metrics.SuccessRate = float64(metrics.HTTPRequestsSucceeded) / float64(metrics.HTTPRequestsTotal) * 100
	}
	
	// Extract response time metrics (convert from milliseconds to duration)
	metrics.HTTPReqDurationAvg = time.Duration(getMetricValue("http_req_duration", "avg")) * time.Millisecond
	metrics.HTTPReqDurationMin = time.Duration(getMetricValue("http_req_duration", "min")) * time.Millisecond
	metrics.HTTPReqDurationMax = time.Duration(getMetricValue("http_req_duration", "max")) * time.Millisecond
	metrics.HTTPReqDurationP90 = time.Duration(getMetricValue("http_req_duration", "p(90)")) * time.Millisecond
	metrics.HTTPReqDurationP95 = time.Duration(getMetricValue("http_req_duration", "p(95)")) * time.Millisecond
	metrics.HTTPReqDurationP99 = time.Duration(getMetricValue("http_req_duration", "p(99)")) * time.Millisecond
	
	// Extract other metrics
	metrics.DataReceived = int64(getMetricValue("data_received", "count"))
	metrics.DataSent = int64(getMetricValue("data_sent", "count"))
	metrics.VirtualUsersMax = int(getMetricValue("vus_max", "max"))
	metrics.IterationsTotal = int64(getMetricValue("iterations", "count"))
	metrics.IterationsRate = getMetricValue("iterations", "rate")
	
	return metrics
}

// parseTextOutput parses k6 text output for basic metrics (fallback)
func (r *Runner) parseTextOutput(output string) K6Metrics {
	var metrics K6Metrics
	
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		
		// Parse basic metrics from text output
		if strings.Contains(line, "http_reqs") && strings.Contains(line, "/s") {
			// Try to extract request count and rate
			// This is a simplified parser - in practice, you'd want more robust parsing
		}
		
		if strings.Contains(line, "http_req_duration") {
			// Try to extract response time metrics
		}
	}
	
	return metrics
}

// validateThresholds checks if the test results meet the configured thresholds
func (r *Runner) validateThresholds(config TestConfig, metrics K6Metrics) error {
	var errors []string
	
	// Check success rate threshold
	if config.MinSuccessRate > 0 && metrics.SuccessRate < config.MinSuccessRate {
		errors = append(errors, fmt.Sprintf("success rate %.2f%% is below threshold %.2f%%", 
			metrics.SuccessRate, config.MinSuccessRate))
	}
	
	// Check response time threshold
	if config.MaxResponseTime > 0 && metrics.HTTPReqDurationP95 > config.MaxResponseTime {
		errors = append(errors, fmt.Sprintf("95th percentile response time %v exceeds threshold %v",
			metrics.HTTPReqDurationP95, config.MaxResponseTime))
	}
	
	if len(errors) > 0 {
		return fmt.Errorf(strings.Join(errors, "; "))
	}
	
	return nil
}

// GenerateTestScript generates a basic k6 test script for media pre-warming
func (r *Runner) GenerateTestScript(outputPath string, config TestConfig) error {
	script := `import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
export let errorRate = new Rate('errors');

export let options = {
  vus: __ENV.VUS || 1,
  duration: __ENV.DURATION || '30s',
};

export default function() {
  const baseURL = __ENV.BASE_URL;
  const urlsPerVU = parseInt(__ENV.URLS_PER_VU || '50');
  
  if (!baseURL) {
    throw new Error('BASE_URL environment variable is required');
  }
  
  // Generate test URLs based on configuration
  for (let i = 0; i < urlsPerVU; i++) {
    const testPath = generateTestPath(i);
    const url = baseURL + testPath;
    
    let response = http.get(url, {
      timeout: '30s',
      headers: {
        'User-Agent': 'k6-media-prewarmer/1.0',
      },
    });
    
    const isSuccess = check(response, {
      'status is 200': (r) => r.status === 200,
      'response time < 5s': (r) => r.timings.duration < 5000,
    });
    
    errorRate.add(!isSuccess);
    
    // Small delay between requests
    sleep(0.1);
  }
}

function generateTestPath(index) {
  // Generate test paths based on your media structure
  const paths = [
    '/video1.mp4?imwidth=1920',
    '/video2.mp4?imwidth=1280',
    '/video3.mp4?imwidth=720',
    '/image1.jpg?width=800&height=600',
    '/image2.webp?width=400&height=300',
  ];
  
  return paths[index % paths.length];
}
`
	
	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	
	// Write script to file
	if err := os.WriteFile(outputPath, []byte(script), 0644); err != nil {
		return fmt.Errorf("failed to write test script: %w", err)
	}
	
	r.logger.Info("Generated k6 test script", zap.String("path", outputPath))
	return nil
}