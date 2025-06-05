package loadtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Config defines the load test configuration
type Config struct {
	BaseURL               string        `json:"base_url"`
	ResultsFile           string        `json:"results_file"`
	URLFormat             string        `json:"url_format"`
	UseHeadRequests       bool          `json:"use_head_requests"`
	SkipLargeFiles        bool          `json:"skip_large_files"`
	LargeFileThresholdMiB int           `json:"large_file_threshold_mib"`
	RequestTimeout        time.Duration `json:"request_timeout"`
	HeadTimeout           time.Duration `json:"head_timeout"`
	ConnectionCloseDelay  time.Duration `json:"connection_close_delay"`
	MaxRetries            int           `json:"max_retries"`
	RetryDelay            time.Duration `json:"retry_delay"`
	ErrorReportFile       string        `json:"error_report_file,omitempty"`
	UseErrorReport        bool          `json:"use_error_report"`

	// Stage configuration
	Stages []StageConfig `json:"stages"`
}

// StageConfig defines a load test stage
type StageConfig struct {
	Name     string        `json:"name"`
	Users    int           `json:"users"`
	Duration time.Duration `json:"duration"`
	RampUp   time.Duration `json:"ramp_up,omitempty"`
}

// Runner implements native Go load testing
type Runner struct {
	config  Config
	logger  *zap.Logger
	client  *http.Client
	urls    []URLInfo
	metrics *Metrics
}

// URLInfo contains information about a URL to test
type URLInfo struct {
	URL            string  `json:"url"`
	FilePath       string  `json:"file_path"`
	Derivative     string  `json:"derivative"`
	SizeMB         float64 `json:"size_mb"`
	SkipInLoadTest bool    `json:"skip_in_load_test,omitempty"`
}

// Metrics tracks load test metrics
type Metrics struct {
	mu            sync.RWMutex
	startTime     time.Time
	requests      int64
	successes     int64
	failures      int64
	bytesReceived int64
	durations     []time.Duration
	statusCodes   map[int]int64
	errors        map[string]int64
	urlMetrics    map[string]*URLMetrics
}

// URLMetrics tracks metrics for individual URLs
type URLMetrics struct {
	requests  int64
	successes int64
	failures  int64
	durations []time.Duration
	lastError string
}

// Result represents the load test results
type Result struct {
	StartTime          time.Time            `json:"start_time"`
	EndTime            time.Time            `json:"end_time"`
	Duration           time.Duration        `json:"duration"`
	TotalRequests      int64                `json:"total_requests"`
	SuccessfulRequests int64                `json:"successful_requests"`
	FailedRequests     int64                `json:"failed_requests"`
	BytesReceived      int64                `json:"bytes_received"`
	RequestsPerSecond  float64              `json:"requests_per_second"`
	SuccessRate        float64              `json:"success_rate"`
	AverageLatency     time.Duration        `json:"average_latency"`
	MedianLatency      time.Duration        `json:"median_latency"`
	P95Latency         time.Duration        `json:"p95_latency"`
	P99Latency         time.Duration        `json:"p99_latency"`
	StatusCodes        map[int]int64        `json:"status_codes"`
	Errors             map[string]int64     `json:"errors"`
	URLResults         map[string]URLResult `json:"url_results"`
	StageResults       []StageResult        `json:"stage_results"`
}

// URLResult represents results for a specific URL
type URLResult struct {
	URL            string        `json:"url"`
	Requests       int64         `json:"requests"`
	Successes      int64         `json:"successes"`
	Failures       int64         `json:"failures"`
	SuccessRate    float64       `json:"success_rate"`
	AverageLatency time.Duration `json:"average_latency"`
	LastError      string        `json:"last_error,omitempty"`
}

// StageResult represents results for a specific stage
type StageResult struct {
	Name              string        `json:"name"`
	Users             int           `json:"users"`
	Duration          time.Duration `json:"duration"`
	Requests          int64         `json:"requests"`
	Successes         int64         `json:"successes"`
	Failures          int64         `json:"failures"`
	RequestsPerSecond float64       `json:"requests_per_second"`
	SuccessRate       float64       `json:"success_rate"`
}

// NewRunner creates a new load test runner
func NewRunner(config Config, logger *zap.Logger) *Runner {
	// Create HTTP client with appropriate settings
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		DisableCompression:  false,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   config.RequestTimeout,
	}

	return &Runner{
		config: config,
		logger: logger,
		client: client,
		metrics: &Metrics{
			statusCodes: make(map[int]int64),
			errors:      make(map[string]int64),
			urlMetrics:  make(map[string]*URLMetrics),
		},
	}
}

// LoadURLs loads URLs from the results file
func (r *Runner) LoadURLs() error {
	// Read results file
	data, err := os.ReadFile(r.config.ResultsFile)
	if err != nil {
		return fmt.Errorf("failed to read results file: %w", err)
	}

	// Parse results - try new format first, then fall back to old format
	var results struct {
		PrewarmResults []struct {
			Path      string `json:"path"`
			SizeBytes int64  `json:"size_bytes"`
			Success   bool   `json:"success"`
			Results   *struct {
				Results map[string]struct {
					URL    string `json:"url"`
					Status string `json:"status"`
				} `json:"results"`
			} `json:"results"`
		} `json:"prewarm_results"`
		Files []struct {
			Path        string                 `json:"path"`
			SizeBytes   int64                  `json:"size_bytes"`
			Derivatives map[string]interface{} `json:"derivatives"`
		} `json:"files"`
	}

	if err := json.Unmarshal(data, &results); err != nil {
		return fmt.Errorf("failed to parse results file: %w", err)
	}

	// Load error report if specified
	errorFiles := make(map[string]bool)
	if r.config.UseErrorReport && r.config.ErrorReportFile != "" {
		errorFiles, err = r.loadErrorReport()
		if err != nil {
			r.logger.Warn("Failed to load error report", zap.Error(err))
		}
	}

	// Build URL list
	r.urls = []URLInfo{}

	// Handle new format (prewarm_results)
	if len(results.PrewarmResults) > 0 {
		for _, result := range results.PrewarmResults {
			// Skip failed results
			if !result.Success || result.Results == nil {
				continue
			}

			sizeMB := float64(result.SizeBytes) / (1024 * 1024)

			// Skip large files if configured
			if r.config.SkipLargeFiles && sizeMB > float64(r.config.LargeFileThresholdMiB) {
				r.logger.Debug("Skipping large file",
					zap.String("path", result.Path),
					zap.Float64("size_mb", sizeMB))
				continue
			}

			// Process derivatives from results
			for derivative, derivResult := range result.Results.Results {
				if derivResult.Status != "success" || derivResult.URL == "" {
					continue
				}

				// Check if URL should be skipped based on error report
				skip := errorFiles[derivResult.URL]

				r.urls = append(r.urls, URLInfo{
					URL:            derivResult.URL,
					FilePath:       result.Path,
					Derivative:     derivative,
					SizeMB:         sizeMB,
					SkipInLoadTest: skip,
				})
			}
		}
	} else if len(results.Files) > 0 {
		// Fall back to old format
		for _, file := range results.Files {
			sizeMB := float64(file.SizeBytes) / (1024 * 1024)

			// Skip large files if configured
			if r.config.SkipLargeFiles && sizeMB > float64(r.config.LargeFileThresholdMiB) {
				r.logger.Debug("Skipping large file",
					zap.String("path", file.Path),
					zap.Float64("size_mb", sizeMB))
				continue
			}

			// Process derivatives
			for derivative := range file.Derivatives {
				url := r.buildURL(file.Path, derivative)

				// Check if URL should be skipped based on error report
				skip := errorFiles[url]

				r.urls = append(r.urls, URLInfo{
					URL:            url,
					FilePath:       file.Path,
					Derivative:     derivative,
					SizeMB:         sizeMB,
					SkipInLoadTest: skip,
				})
			}
		}
	}

	// Filter out URLs to skip
	filtered := []URLInfo{}
	for _, url := range r.urls {
		if !url.SkipInLoadTest {
			filtered = append(filtered, url)
		}
	}
	r.urls = filtered

	totalPossible := 0
	if len(results.PrewarmResults) > 0 {
		// Count successful results with derivatives
		for _, result := range results.PrewarmResults {
			if result.Success && result.Results != nil {
				totalPossible += len(result.Results.Results)
			}
		}
	} else {
		totalPossible = len(results.Files) * 3 // Assume 3 derivatives per file
	}

	r.logger.Info("Loaded URLs for testing",
		zap.Int("total_urls", len(r.urls)),
		zap.Int("skipped", totalPossible-len(r.urls)))

	return nil
}

// buildURL constructs the URL based on format
func (r *Runner) buildURL(path, derivative string) string {
	switch r.config.URLFormat {
	case "derivative":
		return fmt.Sprintf("%s%s/%s/", r.config.BaseURL, path, derivative)
	case "query":
		widths := map[string]int{
			"desktop": 1920,
			"tablet":  1280,
			"mobile":  854,
		}
		if width, ok := widths[derivative]; ok {
			return fmt.Sprintf("%s%s?width=%d", r.config.BaseURL, path, width)
		}
		return fmt.Sprintf("%s%s?derivative=%s", r.config.BaseURL, path, derivative)
	default: // imwidth
		widths := map[string]int{
			"desktop": 1920,
			"tablet":  1280,
			"mobile":  854,
		}
		if width, ok := widths[derivative]; ok {
			return fmt.Sprintf("%s%s?imwidth=%d", r.config.BaseURL, path, width)
		}
		return fmt.Sprintf("%s%s", r.config.BaseURL, path)
	}
}

// loadErrorReport loads URLs to skip from error report
func (r *Runner) loadErrorReport() (map[string]bool, error) {
	data, err := os.ReadFile(r.config.ErrorReportFile)
	if err != nil {
		return nil, err
	}

	// Try to parse as JSON first
	var jsonReport struct {
		Errors []struct {
			URL string `json:"url"`
		} `json:"errors"`
	}

	errorFiles := make(map[string]bool)

	if err := json.Unmarshal(data, &jsonReport); err == nil {
		// JSON format
		for _, e := range jsonReport.Errors {
			errorFiles[e.URL] = true
		}
	} else {
		// Try markdown format - just look for URLs
		// This is a simplified parser
		content := string(data)
		lines := strings.Split(content, "\n")
		for _, line := range lines {
			if strings.Contains(line, r.config.BaseURL) {
				// Extract URL from line
				start := strings.Index(line, r.config.BaseURL)
				if start >= 0 {
					end := strings.IndexAny(line[start:], " \t\n)>")
					if end > 0 {
						url := line[start : start+end]
						errorFiles[url] = true
					}
				}
			}
		}
	}

	return errorFiles, nil
}

// Run executes the load test
func (r *Runner) Run(ctx context.Context) (*Result, error) {
	if len(r.urls) == 0 {
		return nil, fmt.Errorf("no URLs to test")
	}

	r.metrics.startTime = time.Now()

	result := &Result{
		StartTime:    r.metrics.startTime,
		StatusCodes:  make(map[int]int64),
		Errors:       make(map[string]int64),
		URLResults:   make(map[string]URLResult),
		StageResults: []StageResult{},
	}

	// Execute stages
	for i, stage := range r.config.Stages {
		r.logger.Info("Starting stage",
			zap.Int("stage", i+1),
			zap.String("name", stage.Name),
			zap.Int("users", stage.Users),
			zap.Duration("duration", stage.Duration))

		stageResult, err := r.runStage(ctx, stage)
		if err != nil {
			return nil, fmt.Errorf("stage %s failed: %w", stage.Name, err)
		}

		result.StageResults = append(result.StageResults, *stageResult)

		// Check for context cancellation
		select {
		case <-ctx.Done():
			r.logger.Info("Load test cancelled")
			break
		default:
		}
	}

	// Calculate final results
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	r.calculateFinalMetrics(result)

	return result, nil
}

// runStage executes a single load test stage
func (r *Runner) runStage(ctx context.Context, stage StageConfig) (*StageResult, error) {
	stageCtx, cancel := context.WithTimeout(ctx, stage.Duration)
	defer cancel()

	stageStart := time.Now()
	var stageRequests, stageSuccesses, stageFailures int64

	// Create worker pool
	var wg sync.WaitGroup
	userCh := make(chan int, stage.Users)

	// Start workers
	for i := 0; i < stage.Users; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			r.runUser(stageCtx, userID, &stageRequests, &stageSuccesses, &stageFailures)
		}(i)

		// Ramp up delay
		if stage.RampUp > 0 && i < stage.Users-1 {
			delay := stage.RampUp / time.Duration(stage.Users)
			time.Sleep(delay)
		}
	}

	// Wait for stage to complete
	<-stageCtx.Done()
	close(userCh)
	wg.Wait()

	stageDuration := time.Since(stageStart)

	return &StageResult{
		Name:              stage.Name,
		Users:             stage.Users,
		Duration:          stageDuration,
		Requests:          stageRequests,
		Successes:         stageSuccesses,
		Failures:          stageFailures,
		RequestsPerSecond: float64(stageRequests) / stageDuration.Seconds(),
		SuccessRate:       float64(stageSuccesses) / float64(stageRequests) * 100,
	}, nil
}

// runUser simulates a single user making requests
func (r *Runner) runUser(ctx context.Context, userID int, stageReqs, stageSucc, stageFail *int64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Pick a random URL
			url := r.urls[rand.Intn(len(r.urls))]

			// Make request
			start := time.Now()
			success, statusCode, err := r.makeRequest(ctx, url.URL)
			duration := time.Since(start)

			// Update metrics
			atomic.AddInt64(&r.metrics.requests, 1)
			atomic.AddInt64(stageReqs, 1)

			if success {
				atomic.AddInt64(&r.metrics.successes, 1)
				atomic.AddInt64(stageSucc, 1)
			} else {
				atomic.AddInt64(&r.metrics.failures, 1)
				atomic.AddInt64(stageFail, 1)
			}

			r.recordMetrics(url.URL, success, statusCode, duration, err)

			// Small delay between requests
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}
}

// makeRequest makes an HTTP request and returns success status
func (r *Runner) makeRequest(ctx context.Context, url string) (bool, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false, 0, err
	}

	// Add headers
	req.Header.Set("User-Agent", "media-toolkit-loadtest/1.0")
	req.Header.Set("Accept", "*/*")

	// Make request with retries
	var resp *http.Response
	var lastErr error

	for i := 0; i <= r.config.MaxRetries; i++ {
		resp, err = r.client.Do(req)
		if err == nil && resp.StatusCode < 500 {
			break
		}

		lastErr = err
		if i < r.config.MaxRetries {
			time.Sleep(r.config.RetryDelay * time.Duration(i+1))
		}

		if resp != nil {
			resp.Body.Close()
		}
	}

	if resp != nil {
		defer resp.Body.Close()

		// Read response body to ensure full download
		buf := make([]byte, 8192)
		var totalBytes int64
		for {
			n, err := resp.Body.Read(buf)
			totalBytes += int64(n)
			if err != nil {
				break
			}
		}

		atomic.AddInt64(&r.metrics.bytesReceived, totalBytes)

		success := resp.StatusCode >= 200 && resp.StatusCode < 400
		return success, resp.StatusCode, lastErr
	}

	return false, 0, lastErr
}

// recordMetrics records request metrics
func (r *Runner) recordMetrics(url string, success bool, statusCode int, duration time.Duration, err error) {
	r.metrics.mu.Lock()
	defer r.metrics.mu.Unlock()

	// Record duration
	r.metrics.durations = append(r.metrics.durations, duration)

	// Record status code
	if statusCode > 0 {
		r.metrics.statusCodes[statusCode]++
	}

	// Record error
	if err != nil {
		errStr := err.Error()
		r.metrics.errors[errStr]++
	}

	// Record URL-specific metrics
	if _, ok := r.metrics.urlMetrics[url]; !ok {
		r.metrics.urlMetrics[url] = &URLMetrics{}
	}

	urlMetric := r.metrics.urlMetrics[url]
	urlMetric.requests++
	if success {
		urlMetric.successes++
	} else {
		urlMetric.failures++
		if err != nil {
			urlMetric.lastError = err.Error()
		}
	}
	urlMetric.durations = append(urlMetric.durations, duration)
}

// calculateFinalMetrics calculates the final metrics
func (r *Runner) calculateFinalMetrics(result *Result) {
	r.metrics.mu.RLock()
	defer r.metrics.mu.RUnlock()

	result.TotalRequests = r.metrics.requests
	result.SuccessfulRequests = r.metrics.successes
	result.FailedRequests = r.metrics.failures
	result.BytesReceived = r.metrics.bytesReceived

	if result.Duration.Seconds() > 0 {
		result.RequestsPerSecond = float64(result.TotalRequests) / result.Duration.Seconds()
	}

	if result.TotalRequests > 0 {
		result.SuccessRate = float64(result.SuccessfulRequests) / float64(result.TotalRequests) * 100
	}

	// Calculate latency percentiles
	if len(r.metrics.durations) > 0 {
		durations := make([]time.Duration, len(r.metrics.durations))
		copy(durations, r.metrics.durations)

		// Sort durations
		for i := 0; i < len(durations); i++ {
			for j := i + 1; j < len(durations); j++ {
				if durations[i] > durations[j] {
					durations[i], durations[j] = durations[j], durations[i]
				}
			}
		}

		// Calculate percentiles
		result.MedianLatency = durations[len(durations)/2]
		result.P95Latency = durations[int(float64(len(durations))*0.95)]
		result.P99Latency = durations[int(float64(len(durations))*0.99)]

		// Calculate average
		var sum time.Duration
		for _, d := range durations {
			sum += d
		}
		result.AverageLatency = sum / time.Duration(len(durations))
	}

	// Copy status codes and errors
	result.StatusCodes = make(map[int]int64)
	for code, count := range r.metrics.statusCodes {
		result.StatusCodes[code] = count
	}

	result.Errors = make(map[string]int64)
	for err, count := range r.metrics.errors {
		result.Errors[err] = count
	}

	// Calculate URL results
	for url, metrics := range r.metrics.urlMetrics {
		urlResult := URLResult{
			URL:       url,
			Requests:  metrics.requests,
			Successes: metrics.successes,
			Failures:  metrics.failures,
			LastError: metrics.lastError,
		}

		if metrics.requests > 0 {
			urlResult.SuccessRate = float64(metrics.successes) / float64(metrics.requests) * 100
		}

		if len(metrics.durations) > 0 {
			var sum time.Duration
			for _, d := range metrics.durations {
				sum += d
			}
			urlResult.AverageLatency = sum / time.Duration(len(metrics.durations))
		}

		result.URLResults[url] = urlResult
	}
}

// Close cleans up resources
func (r *Runner) Close() error {
	if r.client != nil {
		if transport, ok := r.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	return nil
}
