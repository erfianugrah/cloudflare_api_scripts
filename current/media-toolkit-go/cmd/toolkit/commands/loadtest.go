package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"media-toolkit-go/pkg/loadtest"
)

// NewLoadTestCommand creates the load test command
func NewLoadTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "loadtest",
		Short: "Run load tests against the media service",
		Long: `Run comprehensive load tests against the media service using native Go implementation.
		
This command reads the pre-warming results and performs staged load testing with configurable
user counts and durations. It provides detailed metrics including latency percentiles,
success rates, and per-URL performance data.`,
		Example: `  # Basic load test
  media-toolkit loadtest --base-url https://cdn.example.com/ --results-file results.json

  # Custom stages
  media-toolkit loadtest \
    --base-url https://cdn.example.com/ \
    --results-file results.json \
    --stage1-users 50 --stage1-duration 2m \
    --stage2-users 100 --stage2-duration 5m \
    --stage3-users 50 --stage3-duration 2m

  # With error exclusion
  media-toolkit loadtest \
    --base-url https://cdn.example.com/ \
    --results-file results.json \
    --use-error-report \
    --error-report-file error_report.md`,
		RunE: runLoadTest,
	}

	addLoadTestFlags(cmd)
	return cmd
}

func addLoadTestFlags(cmd *cobra.Command) {
	// Required flags
	cmd.Flags().String("base-url", "", "Base URL for load testing (required)")

	// Input/Output
	cmd.Flags().String("results-file", "media_transform_results.json", "Pre-warming results file")
	cmd.Flags().String("output-file", "", "Output file for load test results (default: stdout)")

	// URL configuration
	cmd.Flags().String("url-format", "imwidth", "URL format (imwidth, derivative, query)")
	cmd.Flags().Bool("use-head-requests", false, "Use HEAD requests to check content size")
	cmd.Flags().Bool("skip-large-files", true, "Skip large files in load test")
	cmd.Flags().Int("large-file-threshold-mb", 256, "Threshold for large files in MB")

	// Request configuration
	cmd.Flags().Duration("request-timeout", 120*time.Second, "Request timeout")
	cmd.Flags().Duration("head-timeout", 30*time.Second, "HEAD request timeout")
	cmd.Flags().Duration("connection-close-delay", 15*time.Second, "Connection close delay")
	cmd.Flags().Int("max-retries", 2, "Max retry attempts")
	cmd.Flags().Duration("retry-delay", time.Second, "Delay between retries")

	// Error report integration
	cmd.Flags().Bool("use-error-report", false, "Use error report to exclude problematic files")
	cmd.Flags().String("error-report-file", "error_report.json", "Error report file path")

	// Stage configuration
	cmd.Flags().Int("stage1-users", 10, "Stage 1 concurrent users")
	cmd.Flags().Duration("stage1-duration", 30*time.Second, "Stage 1 duration")
	cmd.Flags().Duration("stage1-rampup", 0, "Stage 1 ramp-up time")

	cmd.Flags().Int("stage2-users", 20, "Stage 2 concurrent users")
	cmd.Flags().Duration("stage2-duration", 60*time.Second, "Stage 2 duration")
	cmd.Flags().Duration("stage2-rampup", 0, "Stage 2 ramp-up time")

	cmd.Flags().Int("stage3-users", 30, "Stage 3 concurrent users")
	cmd.Flags().Duration("stage3-duration", 30*time.Second, "Stage 3 duration")
	cmd.Flags().Duration("stage3-rampup", 0, "Stage 3 ramp-up time")

	cmd.Flags().Int("stage4-users", 20, "Stage 4 concurrent users")
	cmd.Flags().Duration("stage4-duration", 60*time.Second, "Stage 4 duration")
	cmd.Flags().Duration("stage4-rampup", 0, "Stage 4 ramp-up time")

	cmd.Flags().Int("stage5-users", 0, "Stage 5 concurrent users (0 to disable)")
	cmd.Flags().Duration("stage5-duration", 30*time.Second, "Stage 5 duration")
	cmd.Flags().Duration("stage5-rampup", 0, "Stage 5 ramp-up time")

	// Performance thresholds
	cmd.Flags().Float64("success-rate-threshold", 95.0, "Minimum acceptable success rate (%)")
	cmd.Flags().Duration("latency-threshold-p95", 5*time.Second, "Maximum acceptable P95 latency")

	// Mark required flags
	cmd.MarkFlagRequired("base-url")
}

func runLoadTest(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Get logger from context
	logger, ok := ctx.Value("logger").(*zap.Logger)
	if !ok || logger == nil {
		return fmt.Errorf("logger not found in context")
	}

	logger.Info("Starting load test")

	// Bind flags to viper
	viper.BindPFlags(cmd.Flags())

	// Build configuration
	config := buildLoadTestConfig()

	// Validate configuration
	if err := validateLoadTestConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Create load test runner
	runner := loadtest.NewRunner(config, logger)
	defer runner.Close()

	// Load URLs from results file
	if err := runner.LoadURLs(); err != nil {
		return fmt.Errorf("failed to load URLs: %w", err)
	}

	// Run load test
	logger.Info("Executing load test",
		zap.Int("stages", len(config.Stages)),
		zap.String("base_url", config.BaseURL))

	result, err := runner.Run(ctx)
	if err != nil {
		return fmt.Errorf("load test failed: %w", err)
	}

	// Display results
	if err := displayResults(result, config, logger); err != nil {
		return fmt.Errorf("failed to display results: %w", err)
	}

	// Save results if output file specified
	if outputFile := viper.GetString("output-file"); outputFile != "" {
		if err := saveResults(result, outputFile); err != nil {
			return fmt.Errorf("failed to save results: %w", err)
		}
		logger.Info("Results saved", zap.String("file", outputFile))
	}

	// Check thresholds
	successThreshold := viper.GetFloat64("success-rate-threshold")
	latencyThreshold := viper.GetDuration("latency-threshold-p95")

	if result.SuccessRate < successThreshold {
		logger.Warn("Success rate below threshold",
			zap.Float64("actual", result.SuccessRate),
			zap.Float64("threshold", successThreshold))
	}

	if result.P95Latency > latencyThreshold {
		logger.Warn("P95 latency above threshold",
			zap.Duration("actual", result.P95Latency),
			zap.Duration("threshold", latencyThreshold))
	}

	logger.Info("Load test completed successfully")
	return nil
}

func buildLoadTestConfig() loadtest.Config {
	stages := []loadtest.StageConfig{}

	// Build stages based on configuration
	for i := 1; i <= 5; i++ {
		users := viper.GetInt(fmt.Sprintf("stage%d-users", i))
		if users == 0 {
			continue // Skip stages with 0 users
		}

		stage := loadtest.StageConfig{
			Name:     fmt.Sprintf("Stage %d", i),
			Users:    users,
			Duration: viper.GetDuration(fmt.Sprintf("stage%d-duration", i)),
			RampUp:   viper.GetDuration(fmt.Sprintf("stage%d-rampup", i)),
		}

		stages = append(stages, stage)
	}

	return loadtest.Config{
		BaseURL:               viper.GetString("base-url"),
		ResultsFile:           viper.GetString("results-file"),
		URLFormat:             viper.GetString("url-format"),
		UseHeadRequests:       viper.GetBool("use-head-requests"),
		SkipLargeFiles:        viper.GetBool("skip-large-files"),
		LargeFileThresholdMiB: viper.GetInt("large-file-threshold-mb"),
		RequestTimeout:        viper.GetDuration("request-timeout"),
		HeadTimeout:           viper.GetDuration("head-timeout"),
		ConnectionCloseDelay:  viper.GetDuration("connection-close-delay"),
		MaxRetries:            viper.GetInt("max-retries"),
		RetryDelay:            viper.GetDuration("retry-delay"),
		UseErrorReport:        viper.GetBool("use-error-report"),
		ErrorReportFile:       viper.GetString("error-report-file"),
		Stages:                stages,
	}
}

func validateLoadTestConfig(config loadtest.Config) error {
	if config.BaseURL == "" {
		return fmt.Errorf("base-url is required")
	}

	if len(config.Stages) == 0 {
		return fmt.Errorf("at least one stage must be configured")
	}

	for i, stage := range config.Stages {
		if stage.Users <= 0 {
			return fmt.Errorf("stage %d: users must be positive", i+1)
		}
		if stage.Duration <= 0 {
			return fmt.Errorf("stage %d: duration must be positive", i+1)
		}
	}

	return nil
}

func displayResults(result *loadtest.Result, config loadtest.Config, logger *zap.Logger) error {
	fmt.Println("\n🎯 Load Test Results")
	fmt.Println("═══════════════════════════════════════════════════════════════")

	// Overall summary
	fmt.Printf("\n📊 Overall Summary:\n")
	fmt.Printf("   • Total Duration: %s\n", result.Duration.Round(time.Second))
	fmt.Printf("   • Total Requests: %d\n", result.TotalRequests)
	fmt.Printf("   • Successful: %d (%.1f%%)\n", result.SuccessfulRequests, result.SuccessRate)
	fmt.Printf("   • Failed: %d\n", result.FailedRequests)
	fmt.Printf("   • Requests/sec: %.1f\n", result.RequestsPerSecond)
	fmt.Printf("   • Data Transferred: %.2f MB\n", float64(result.BytesReceived)/(1024*1024))

	// Latency metrics
	fmt.Printf("\n⏱️  Latency Metrics:\n")
	fmt.Printf("   • Average: %s\n", result.AverageLatency.Round(time.Millisecond))
	fmt.Printf("   • Median: %s\n", result.MedianLatency.Round(time.Millisecond))
	fmt.Printf("   • P95: %s\n", result.P95Latency.Round(time.Millisecond))
	fmt.Printf("   • P99: %s\n", result.P99Latency.Round(time.Millisecond))

	// Stage results
	if len(result.StageResults) > 0 {
		fmt.Printf("\n📈 Stage Results:\n")
		for _, stage := range result.StageResults {
			fmt.Printf("   %s (%d users, %s):\n", stage.Name, stage.Users, stage.Duration.Round(time.Second))
			fmt.Printf("      • Requests: %d (%.1f/sec)\n", stage.Requests, stage.RequestsPerSecond)
			fmt.Printf("      • Success Rate: %.1f%%\n", stage.SuccessRate)
		}
	}

	// Status code distribution
	if len(result.StatusCodes) > 0 {
		fmt.Printf("\n📊 Status Code Distribution:\n")
		for code, count := range result.StatusCodes {
			percentage := float64(count) / float64(result.TotalRequests) * 100
			fmt.Printf("   • %d: %d (%.1f%%)\n", code, count, percentage)
		}
	}

	// Top errors
	if len(result.Errors) > 0 {
		fmt.Printf("\n❌ Top Errors:\n")
		errorCount := 0
		for err, count := range result.Errors {
			if errorCount >= 5 {
				break
			}
			fmt.Printf("   • %s: %d occurrences\n", err, count)
			errorCount++
		}
	}

	// Problematic URLs
	problemURLs := []string{}
	for url, urlResult := range result.URLResults {
		if urlResult.SuccessRate < 90.0 && urlResult.Requests > 10 {
			problemURLs = append(problemURLs, url)
		}
	}

	if len(problemURLs) > 0 {
		fmt.Printf("\n⚠️  Problematic URLs (success rate < 90%%):\n")
		for i, url := range problemURLs {
			if i >= 10 {
				fmt.Printf("   • ... and %d more\n", len(problemURLs)-10)
				break
			}
			urlResult := result.URLResults[url]
			fmt.Printf("   • %s\n", url)
			fmt.Printf("     Success Rate: %.1f%%, Avg Latency: %s\n",
				urlResult.SuccessRate, urlResult.AverageLatency.Round(time.Millisecond))
			if urlResult.LastError != "" {
				fmt.Printf("     Last Error: %s\n", urlResult.LastError)
			}
		}
	}

	fmt.Println("\n═══════════════════════════════════════════════════════════════")

	return nil
}

func saveResults(result *loadtest.Result, outputFile string) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	if err := os.WriteFile(outputFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write results file: %w", err)
	}

	return nil
}
