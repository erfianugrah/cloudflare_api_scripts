package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"media-toolkit-go/pkg/config"
	"media-toolkit-go/pkg/httpclient"
	"media-toolkit-go/pkg/media"
	"media-toolkit-go/pkg/reporting"
	"media-toolkit-go/pkg/stats"
	"media-toolkit-go/pkg/storage"
	"media-toolkit-go/internal/orchestrator"
	"media-toolkit-go/internal/workers"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// NewPrewarmCommand creates the prewarm command
func NewPrewarmCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prewarm",
		Short: "Pre-warm media cache by making HTTP requests to derivatives/variants",
		Long: `Pre-warm Cloudflare KV cache for media transformations by making HTTP requests
to all derivatives (videos) or variants (images). This helps ensure faster
response times for end users by populating the cache.`,
		RunE: runPrewarm,
	}

	// Add flags specific to pre-warming
	addPrewarmFlags(cmd)
	
	return cmd
}

func addPrewarmFlags(cmd *cobra.Command) {
	// Required flags
	cmd.Flags().String("remote", "", "rclone remote name (required)")
	cmd.Flags().String("bucket", "", "S3 bucket name (required)")
	cmd.Flags().String("base-url", "", "Base URL to prepend to object paths (required)")
	
	// Optional flags
	cmd.Flags().String("directory", "", "Directory path within bucket")
	cmd.Flags().String("media-type", "auto", "Type of media to process (auto, image, video)")
	cmd.Flags().StringSlice("derivatives", []string{"desktop", "tablet", "mobile"}, "Video derivatives to process")
	cmd.Flags().StringSlice("image-variants", []string{"thumbnail", "small", "medium", "large", "webp"}, "Image variants to process")
	cmd.Flags().Int("workers", 5, "Number of concurrent workers")
	cmd.Flags().Int("timeout", 120, "Request timeout in seconds")
	cmd.Flags().Int("connection-close-delay", 15, "Delay before closing connections")
	cmd.Flags().Int("retry", 2, "Number of retry attempts")
	cmd.Flags().Bool("use-head-for-size", false, "Use HEAD requests to verify content sizes")
	cmd.Flags().StringSlice("extensions", []string{}, "File extensions to filter by (e.g., .mp4,.jpg,.png)")
	cmd.Flags().Int("limit", 0, "Limit number of objects to process (0 = no limit)")
	
	// Output options
	cmd.Flags().String("output", "media_transform_results.json", "Output JSON file path")
	cmd.Flags().String("performance-report", "performance_report.md", "Performance report output file")
	
	// Size thresholds
	cmd.Flags().Int("small-file-threshold", 50, "Threshold in MiB for small files")
	cmd.Flags().Int("medium-file-threshold", 200, "Threshold in MiB for medium files") 
	cmd.Flags().Int("size-threshold", 256, "Size threshold in MiB for reporting")
	
	// Worker allocation
	cmd.Flags().Int("small-file-workers", 0, "Workers for small files (0 = auto)")
	cmd.Flags().Int("medium-file-workers", 0, "Workers for medium files (0 = auto)")
	cmd.Flags().Int("large-file-workers", 0, "Workers for large files (0 = auto)")
	cmd.Flags().Bool("optimize-by-size", false, "Enable size-based worker optimization")
	
	// Storage options
	cmd.Flags().Bool("use-aws-cli", false, "Use AWS CLI instead of rclone")
	
	// Mark required flags
	cmd.MarkFlagRequired("remote")
	cmd.MarkFlagRequired("bucket")
	cmd.MarkFlagRequired("base-url")
}

func runPrewarm(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	
	// Get logger from context
	logger, ok := ctx.Value("logger").(*zap.Logger)
	if !ok || logger == nil {
		return fmt.Errorf("logger not found in context")
	}

	logger.Info("Starting pre-warming process")

	// Bind flags to viper (needs to happen after flag parsing)
	viper.BindPFlags(cmd.Flags())

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}
	
	// Validate configuration
	if err := config.ValidateConfig(cfg); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	// Log configuration summary
	logger.Info("Configuration loaded",
		zap.String("remote", cfg.Remote),
		zap.String("bucket", cfg.Bucket),
		zap.String("directory", cfg.Directory),
		zap.String("base_url", cfg.BaseURL),
		zap.String("media_type", cfg.MediaType.String()),
		zap.Int("workers", cfg.Workers),
		zap.Strings("derivatives", cfg.Derivatives),
		zap.Strings("image_variants", cfg.ImageVariants))

	// Create and run the actual pre-warming workflow
	return executePrewarmingWorkflow(ctx, cfg, logger)
}

// Helper function to check required flags
func validatePrewarmFlags(cfg *config.Config) error {
	var errors []string
	
	if cfg.Remote == "" {
		errors = append(errors, "remote is required")
	}
	if cfg.Bucket == "" {
		errors = append(errors, "bucket is required") 
	}
	if cfg.BaseURL == "" {
		errors = append(errors, "base-url is required")
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("validation failed: %s", strings.Join(errors, ", "))
	}
	
	return nil
}

// executePrewarmingWorkflow runs the actual pre-warming logic
func executePrewarmingWorkflow(ctx context.Context, cfg *config.Config, logger *zap.Logger) error {
	// 1. Create storage client
	var storageClient storage.Storage
	var err error
	
	if cfg.UseAWSCLI {
		// Use AWS storage backend
		storageConfig := storage.StorageConfig{
			Remote:     cfg.Remote,
			AWSProfile: os.Getenv("AWS_PROFILE"),
		}
		storageClient, err = storage.NewAWSStorage(storageConfig, logger)
		if err != nil {
			return fmt.Errorf("failed to create AWS storage client: %w", err)
		}
	} else {
		// Use rclone storage backend
		storageConfig := storage.StorageConfig{
			Remote:        cfg.Remote,
			RcloneBinary:  "rclone",
			Timeout:       time.Duration(cfg.Timeout) * time.Second,
			BatchSize:     1000,
			RetryAttempts: cfg.Retry,
			RetryDelay:    time.Second,
		}
		storageClient = storage.NewRcloneStorage(storageConfig, logger)
	}
	
	// 2. Create HTTP client
	httpConfig := httpclient.Config{
		Timeout:                 time.Duration(cfg.Timeout) * time.Second,
		RetryAttempts:          cfg.Retry,
		RetryDelay:             time.Second,
		ConnectionCloseDelay:   time.Duration(cfg.ConnectionCloseDelay) * time.Second,
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    10,
		IdleConnTimeout:        90 * time.Second,
		DisableKeepAlives:      false,
		UserAgent:              "media-toolkit-go/1.0",
	}
	httpClient := httpclient.NewHTTPClient(httpConfig, logger)
	defer httpClient.Close()
	
	// 3. Create media processor
	mediaProcessor := media.NewProcessor(httpClient, logger)
	defer mediaProcessor.Close()
	
	// 4. Create statistics collector
	statsCollector := stats.NewCollector()
	
	// 5. Create worker pool
	// Calculate appropriate queue size based on worker count
	totalWorkers := cfg.WorkerAllocation.SmallFileWorkers + cfg.WorkerAllocation.MediumFileWorkers + cfg.WorkerAllocation.LargeFileWorkers
	if totalWorkers == 0 {
		totalWorkers = cfg.Workers
	}
	queueSize := totalWorkers * 3 // 3x worker count for buffering
	if queueSize < 1000 {
		queueSize = 1000
	}
	
	workerConfig := workers.WorkerPoolConfig{
		SmallFileWorkers:  cfg.WorkerAllocation.SmallFileWorkers,
		MediumFileWorkers: cfg.WorkerAllocation.MediumFileWorkers,
		LargeFileWorkers:  cfg.WorkerAllocation.LargeFileWorkers,
		QueueSize:         queueSize,
		WorkerTimeout:     time.Duration(cfg.Timeout) * time.Second,
		ShutdownTimeout:   60 * time.Second,
	}
	
	// Auto-calculate worker counts if not specified
	if workerConfig.SmallFileWorkers == 0 {
		workerConfig.SmallFileWorkers = cfg.Workers * 30 / 100
		if workerConfig.SmallFileWorkers < 1 {
			workerConfig.SmallFileWorkers = 1
		}
	}
	if workerConfig.MediumFileWorkers == 0 {
		workerConfig.MediumFileWorkers = cfg.Workers * 30 / 100
		if workerConfig.MediumFileWorkers < 1 {
			workerConfig.MediumFileWorkers = 1
		}
	}
	if workerConfig.LargeFileWorkers == 0 {
		workerConfig.LargeFileWorkers = cfg.Workers * 40 / 100
		if workerConfig.LargeFileWorkers < 1 {
			workerConfig.LargeFileWorkers = 1
		}
	}
	
	workerPool := workers.NewWorkerPool(workerConfig, logger)
	
	// 6. Create workflow coordinator
	coordinator := orchestrator.NewCoordinator(
		storageClient,
		mediaProcessor,
		workerPool,
		statsCollector,
		cfg,
		logger,
	)
	defer coordinator.Shutdown()
	
	// 7. Configure and execute workflow
	workflowConfig := orchestrator.WorkflowConfig{
		Type:                orchestrator.WorkflowPrewarm,
		EnableErrorReporting: cfg.GenerateErrorReport,
		EnableReporting:     true,
		ContinueOnError:     true,
		MaxRetries:          cfg.Retry,
		RetryDelay:          5 * time.Second,
		PrewarmConfig: &orchestrator.PrewarmStageConfig{
			MediaType:        cfg.MediaType,
			ImageVariants:    cfg.ImageVariants,
			VideoDerivatives: cfg.Derivatives,
			URLFormat:        cfg.Format,
			UseHeadRequest:   cfg.UseHeadForSize,
			Timeout:          time.Duration(cfg.Timeout) * time.Second,
		},
		ReportConfig: &reporting.ReportConfig{
			OutputDir:       ".",
			ReportName:      "prewarm-report",
			Format:          []string{"markdown", "json"},
			IncludeSections: []string{"summary", "performance", "errors"},
			Timestamp:       true,
		},
	}
	
	// 8. Execute the workflow
	result, err := coordinator.ExecuteWorkflow(workflowConfig)
	if err != nil {
		logger.Error("Pre-warming workflow failed", zap.Error(err))
		return fmt.Errorf("pre-warming failed: %w", err)
	}
	
	// 9. Output results
	logger.Info("Pre-warming completed successfully",
		zap.Bool("success", result.Success),
		zap.Duration("duration", result.Duration),
		zap.Int("processed_files", len(result.PrewarmResults)),
		zap.Strings("report_paths", result.ReportPaths))
	
	// 10. Save JSON results if specified
	if cfg.Output != "" {
		if err := saveResultsToJSON(result, cfg.Output, logger); err != nil {
			logger.Warn("Failed to save results to JSON", zap.Error(err))
		}
	}
	
	return nil
}

// saveResultsToJSON saves the workflow results to a JSON file
func saveResultsToJSON(result *orchestrator.WorkflowResult, outputPath string, logger *zap.Logger) error {
	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}
	
	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write results file: %w", err)
	}
	
	logger.Info("Results saved to JSON file", zap.String("path", outputPath))
	return nil
}