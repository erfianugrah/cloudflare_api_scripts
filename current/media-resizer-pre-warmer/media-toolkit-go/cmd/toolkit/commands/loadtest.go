package commands

import (
	"context"
	"fmt"
	"time"

	"media-toolkit-go/pkg/k6"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// NewLoadTestCommand creates the load test command
func NewLoadTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "loadtest",
		Short: "Run k6 load tests against the media service",
		Long: `Run k6 load tests against the media service using pre-warming results
to generate realistic load patterns. Supports various test configurations
and failure analysis.`,
		RunE: runLoadTest,
	}

	addLoadTestFlags(cmd)
	return cmd
}

func addLoadTestFlags(cmd *cobra.Command) {
	// Required flags
	cmd.Flags().String("base-url", "", "Base URL for load testing (required)")
	cmd.Flags().String("results-file", "media_transform_results.json", "Pre-warming results file")
	
	// Load test configuration
	cmd.Flags().String("k6-script", "video-load-test-integrated-improved.js", "Path to k6 test script")
	cmd.Flags().String("url-format", "imwidth", "URL format (imwidth, derivative)")
	cmd.Flags().Bool("debug-mode", false, "Enable debug mode")
	cmd.Flags().Bool("use-head-requests", true, "Use HEAD requests")
	cmd.Flags().Bool("skip-large-files", true, "Skip large files in test")
	cmd.Flags().Int("large-file-threshold-mib", 256, "Threshold for large files in MiB")
	cmd.Flags().String("request-timeout", "120s", "Request timeout")
	cmd.Flags().String("head-timeout", "30s", "HEAD request timeout")
	cmd.Flags().String("global-timeout", "90s", "Global test timeout")
	cmd.Flags().String("failure-rate-threshold", "0.05", "Max acceptable failure rate")
	cmd.Flags().Int("max-retries", 2, "Max retry attempts")
	cmd.Flags().Int("connection-close-delay", 15, "Connection close delay")
	
	// Error report integration
	cmd.Flags().Bool("use-error-report", false, "Use error report to exclude problematic files")
	cmd.Flags().String("error-report-file", "error_report.json", "Error report file path")
	
	// Stage configuration
	cmd.Flags().Int("stage1-users", 10, "Stage 1 users")
	cmd.Flags().String("stage1-duration", "30s", "Stage 1 duration")
	cmd.Flags().Int("stage2-users", 20, "Stage 2 users")
	cmd.Flags().String("stage2-duration", "1m", "Stage 2 duration")
	cmd.Flags().Int("stage3-users", 30, "Stage 3 users")
	cmd.Flags().String("stage3-duration", "30s", "Stage 3 duration")
	cmd.Flags().Int("stage4-users", 20, "Stage 4 users")
	cmd.Flags().String("stage4-duration", "1m", "Stage 4 duration")
	cmd.Flags().Int("stage5-users", 0, "Stage 5 users")
	cmd.Flags().String("stage5-duration", "30s", "Stage 5 duration")
	
	// Mark required flags
	cmd.MarkFlagRequired("base-url")
	
	// Bind flags to viper
	viper.BindPFlags(cmd.Flags())
}

func runLoadTest(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	
	// Get logger from context
	logger, ok := ctx.Value("logger").(*zap.Logger)
	if !ok {
		return fmt.Errorf("logger not found in context")
	}

	logger.Info("Starting k6 load test process")

	// Execute load testing workflow
	return executeLoadTest(ctx, logger)
}

// executeLoadTest runs the actual load testing logic
func executeLoadTest(ctx context.Context, logger *zap.Logger) error {
	// Get configuration values from viper
	baseURL := viper.GetString("base-url")
	k6Script := viper.GetString("k6-script")
	stage1Users := viper.GetInt("stage1-users")
	stage1Duration := viper.GetString("stage1-duration")
	maxRetries := viper.GetInt("max-retries")
	failureRateThreshold := viper.GetString("failure-rate-threshold")
	
	if baseURL == "" {
		return fmt.Errorf("base-url is required for load testing")
	}
	
	// Create k6 runner
	runner := k6.NewRunner(logger)
	
	// Parse duration
	duration, err := time.ParseDuration(stage1Duration)
	if err != nil {
		duration = 30 * time.Second // Default
	}
	
	// Configure k6 test
	testConfig := k6.TestConfig{
		VirtualUsers:    stage1Users,
		Duration:        duration,
		BaseURL:         baseURL,
		URLsPerVU:       50,
		MaxResponseTime: 5 * time.Second,
		MinSuccessRate:  95.0,
		OutputFormat:    "json",
		K6ScriptPath:    k6Script,
		K6Binary:        "k6",
		Environment:     make(map[string]string),
	}
	
	// Add environment variables
	testConfig.Environment["BASE_URL"] = baseURL
	testConfig.Environment["MAX_RETRIES"] = fmt.Sprintf("%d", maxRetries)
	testConfig.Environment["FAILURE_RATE_THRESHOLD"] = failureRateThreshold
	testConfig.Environment["REQUEST_TIMEOUT"] = viper.GetString("request-timeout")
	testConfig.Environment["HEAD_TIMEOUT"] = viper.GetString("head-timeout")
	testConfig.Environment["GLOBAL_TIMEOUT"] = viper.GetString("global-timeout")
	testConfig.Environment["CONNECTION_CLOSE_DELAY"] = fmt.Sprintf("%d", viper.GetInt("connection-close-delay"))
	testConfig.Environment["USE_HEAD_REQUESTS"] = fmt.Sprintf("%t", viper.GetBool("use-head-requests"))
	testConfig.Environment["SKIP_LARGE_FILES"] = fmt.Sprintf("%t", viper.GetBool("skip-large-files"))
	testConfig.Environment["LARGE_FILE_THRESHOLD_MIB"] = fmt.Sprintf("%d", viper.GetInt("large-file-threshold-mib"))
	testConfig.Environment["URL_FORMAT"] = viper.GetString("url-format")
	testConfig.Environment["DEBUG_MODE"] = fmt.Sprintf("%t", viper.GetBool("debug-mode"))
	
	// If error report integration is enabled
	if viper.GetBool("use-error-report") {
		testConfig.Environment["ERROR_REPORT_FILE"] = viper.GetString("error-report-file")
	}
	
	// Generate test script if it doesn't exist
	if k6Script == "video-load-test-integrated-improved.js" {
		if err := runner.GenerateTestScript(k6Script, testConfig); err != nil {
			logger.Warn("Failed to generate k6 test script", zap.Error(err))
		}
	}
	
	// Run the load test
	logger.Info("Executing k6 load test",
		zap.String("base_url", baseURL),
		zap.Int("virtual_users", testConfig.VirtualUsers),
		zap.Duration("duration", testConfig.Duration),
		zap.String("script", k6Script))
	
	result, err := runner.RunTest(ctx, testConfig)
	if err != nil {
		logger.Error("Load test failed", zap.Error(err))
		return fmt.Errorf("k6 load test failed: %w", err)
	}
	
	// Log results
	logger.Info("k6 load test completed",
		zap.Bool("success", result.Success),
		zap.Duration("duration", result.Duration),
		zap.Int64("total_requests", result.Metrics.HTTPRequestsTotal),
		zap.Float64("success_rate", result.Metrics.SuccessRate),
		zap.Duration("avg_response_time", result.Metrics.HTTPReqDurationAvg),
		zap.Duration("p95_response_time", result.Metrics.HTTPReqDurationP95))
	
	if !result.Success {
		logger.Error("Load test failed thresholds", zap.String("error", result.Error))
		return fmt.Errorf("load test failed: %s", result.Error)
	}
	
	logger.Info("k6 load test process completed successfully")
	return nil
}