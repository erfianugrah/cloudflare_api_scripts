package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"media-toolkit-go/internal/workers"
	"media-toolkit-go/pkg/ffmpeg"
	"media-toolkit-go/pkg/storage"
	"media-toolkit-go/pkg/utils"
)

// NewValidateCommand creates the validate command
func NewValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate video files for corruption and integrity",
		Long: `Validate video files for corruption and integrity using FFprobe.
Can validate from a directory, pre-warming results, or remote storage.`,
		RunE: runValidate,
	}

	addValidateFlags(cmd)
	return cmd
}

func addValidateFlags(cmd *cobra.Command) {
	// Validation source options (mutually exclusive)
	cmd.Flags().String("validate-directory", "", "Directory containing videos to validate")
	cmd.Flags().String("validate-results", "", "Path to pre-warming results JSON file")
	cmd.Flags().String("remote", "", "Remote storage for validation")
	cmd.Flags().String("bucket", "", "Bucket name for remote validation")
	cmd.Flags().String("directory", "", "Directory path within bucket")
	
	// Validation options
	cmd.Flags().Int("validation-workers", 10, "Number of concurrent validation workers")
	cmd.Flags().String("validation-report", "validation_report.md", "Output file for validation report")
	cmd.Flags().String("validation-format", "markdown", "Format for validation report (text, markdown, json)")
	cmd.Flags().String("video-pattern", "*.mp4", "File pattern to match for validation")
	cmd.Flags().StringSlice("extensions", []string{}, "File extensions to filter by (e.g., .mp4,.mkv)")
	cmd.Flags().String("media-type", "", "Media type preset: 'video' or 'all'")
	
	// Bind flags to viper
	viper.BindPFlags(cmd.Flags())
}

func runValidate(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	
	// Get logger from context
	logger, ok := ctx.Value("logger").(*zap.Logger)
	if !ok {
		return fmt.Errorf("logger not found in context")
	}

	logger.Info("Starting video validation process")

	// Execute the validation workflow
	if err := executeValidationWorkflow(ctx, logger); err != nil {
		logger.Error("Validation failed", zap.Error(err))
		return fmt.Errorf("validation failed: %w", err)
	}

	logger.Info("Video validation process completed successfully")
	return nil
}

// ValidationConfig holds configuration for validation operations
type ValidationConfig struct {
	// Validation sources (mutually exclusive)
	ValidateDirectory string
	ValidateResults   string
	Remote           string
	Bucket           string
	Directory        string
	
	// Validation options
	ValidationWorkers int
	ValidationReport  string
	ValidationFormat  string
	VideoPattern      string
	Extensions        []string
	MediaType         string
}

// ValidationResult represents the result of validating a single video
type ValidationResult struct {
	FilePath      string                `json:"file_path"`
	Valid         bool                  `json:"valid"`
	Corrupted     bool                  `json:"corrupted"`
	Errors        []string              `json:"errors,omitempty"`
	Metadata      *ffmpeg.VideoMetadata `json:"metadata,omitempty"`
	ValidationTime time.Duration        `json:"validation_time"`
	Timestamp      time.Time             `json:"timestamp"`
}

// ValidationReport represents the complete validation report
type ValidationReport struct {
	TotalFiles       int                 `json:"total_files"`
	ValidFiles       int                 `json:"valid_files"`
	CorruptedFiles   int                 `json:"corrupted_files"`
	ErrorFiles       int                 `json:"error_files"`
	TotalDuration    time.Duration       `json:"total_duration"`
	Results          []ValidationResult  `json:"results"`
	ErrorsByType     map[string]int      `json:"errors_by_type"`
	GeneratedAt      time.Time           `json:"generated_at"`
}

// executeValidationWorkflow performs the complete validation workflow
func executeValidationWorkflow(ctx context.Context, logger *zap.Logger) error {
	// Build configuration from viper settings
	cfg, err := buildValidationConfig()
	if err != nil {
		return fmt.Errorf("failed to build configuration: %w", err)
	}

	// Validate configuration
	if err := validateValidationConfig(cfg); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Get list of video files to validate
	videoFiles, err := getVideoFilesForValidation(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to get video files: %w", err)
	}

	if len(videoFiles) == 0 {
		logger.Info("No video files found to validate")
		return nil
	}

	// Run validation
	report, err := runVideoValidation(ctx, videoFiles, cfg, logger)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Generate validation report
	if err := generateValidationReport(report, cfg, logger); err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	// Log summary
	logger.Info("Validation completed",
		zap.Int("total_files", report.TotalFiles),
		zap.Int("valid_files", report.ValidFiles),
		zap.Int("corrupted_files", report.CorruptedFiles),
		zap.Int("error_files", report.ErrorFiles),
		zap.Duration("duration", report.TotalDuration))

	return nil
}

// buildValidationConfig creates validation configuration from viper settings
func buildValidationConfig() (*ValidationConfig, error) {
	return &ValidationConfig{
		ValidateDirectory: viper.GetString("validate-directory"),
		ValidateResults:   viper.GetString("validate-results"),
		Remote:           viper.GetString("remote"),
		Bucket:           viper.GetString("bucket"),
		Directory:        viper.GetString("directory"),
		ValidationWorkers: viper.GetInt("validation-workers"),
		ValidationReport:  viper.GetString("validation-report"),
		ValidationFormat:  viper.GetString("validation-format"),
		VideoPattern:     viper.GetString("video-pattern"),
		Extensions:       viper.GetStringSlice("extensions"),
		MediaType:        viper.GetString("media-type"),
	}, nil
}

// validateValidationConfig validates the validation configuration
func validateValidationConfig(cfg *ValidationConfig) error {
	// Count specified sources
	sources := 0
	if cfg.ValidateDirectory != "" {
		sources++
	}
	if cfg.ValidateResults != "" {
		sources++
	}
	if cfg.Remote != "" {
		sources++
	}

	if sources == 0 {
		return fmt.Errorf("must specify one validation source: --validate-directory, --validate-results, or --remote")
	}
	if sources > 1 {
		return fmt.Errorf("only one validation source can be specified")
	}

	// Validate workers count
	if err := utils.ValidatePositiveInt(cfg.ValidationWorkers, "validation-workers"); err != nil {
		return err
	}

	// Validate format
	validFormats := []string{"text", "markdown", "json"}
	if err := utils.ValidateOneOf(cfg.ValidationFormat, validFormats, "validation-format"); err != nil {
		return err
	}

	// Validate remote-specific options
	if cfg.Remote != "" {
		if err := utils.ValidateNonEmpty(cfg.Bucket, "bucket"); err != nil {
			return err
		}
	}

	return nil
}

// getVideoFilesForValidation retrieves video files based on validation source
func getVideoFilesForValidation(ctx context.Context, cfg *ValidationConfig, logger *zap.Logger) ([]string, error) {
	if cfg.ValidateDirectory != "" {
		return getVideoFilesFromDirectory(cfg.ValidateDirectory, cfg.VideoPattern, logger)
	}
	
	if cfg.ValidateResults != "" {
		return getVideoFilesFromResults(cfg.ValidateResults, logger)
	}
	
	if cfg.Remote != "" {
		return getVideoFilesFromRemote(ctx, cfg, logger)
	}

	return nil, fmt.Errorf("no validation source specified")
}

// getVideoFilesFromDirectory gets video files from a local directory
func getVideoFilesFromDirectory(directory, pattern string, logger *zap.Logger) ([]string, error) {
	if !utils.DirExists(directory) {
		return nil, fmt.Errorf("directory does not exist: %s", directory)
	}

	logger.Info("Scanning directory for video files", 
		zap.String("directory", directory), 
		zap.String("pattern", pattern))

	// Use filepath.Glob to find matching files
	searchPattern := filepath.Join(directory, pattern)
	matches, err := filepath.Glob(searchPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to glob pattern %s: %w", searchPattern, err)
	}

	// Filter to only include video files
	videoFiles := make([]string, 0, len(matches))
	for _, match := range matches {
		if utils.IsVideoFile(match) {
			videoFiles = append(videoFiles, match)
		}
	}

	logger.Info("Found video files", zap.Int("count", len(videoFiles)))
	return videoFiles, nil
}

// getVideoFilesFromResults gets video files from pre-warming results
func getVideoFilesFromResults(resultsFile string, logger *zap.Logger) ([]string, error) {
	if !utils.FileExists(resultsFile) {
		return nil, fmt.Errorf("results file does not exist: %s", resultsFile)
	}

	logger.Info("Loading video files from results", zap.String("file", resultsFile))

	// Read and parse results file
	data, err := os.ReadFile(resultsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read results file: %w", err)
	}

	// Parse JSON results (assuming it contains file paths)
	var results map[string]interface{}
	if err := json.Unmarshal(data, &results); err != nil {
		return nil, fmt.Errorf("failed to parse results JSON: %w", err)
	}

	// Extract file paths from results
	// This is a simplified implementation - adjust based on actual results format
	videoFiles := make([]string, 0)
	
	// Look for common result formats
	if files, ok := results["files"].([]interface{}); ok {
		for _, file := range files {
			if filePath, ok := file.(string); ok && utils.IsVideoFile(filePath) {
				videoFiles = append(videoFiles, filePath)
			}
		}
	}

	logger.Info("Loaded video files from results", zap.Int("count", len(videoFiles)))
	return videoFiles, nil
}

// getVideoFilesFromRemote gets video files from remote storage
func getVideoFilesFromRemote(ctx context.Context, cfg *ValidationConfig, logger *zap.Logger) ([]string, error) {
	logger.Info("Listing video files from remote storage",
		zap.String("remote", cfg.Remote),
		zap.String("bucket", cfg.Bucket),
		zap.String("directory", cfg.Directory))

	// Create storage client
	storageConfig := &storage.StorageConfig{
		Backend: storage.StorageBackendRclone,
		Remote:  cfg.Remote,
		Bucket:  cfg.Bucket,
	}

	storageClient, err := storage.NewStorage(storageConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	defer storageClient.Close()

	// List files
	objects, err := storageClient.ListObjects(ctx, storage.ListRequest{
		Directory: cfg.Directory,
		GetSizes:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// Filter video files
	videoFiles := make([]string, 0, len(objects))
	for _, obj := range objects {
		if utils.IsVideoFile(obj.Path) {
			videoFiles = append(videoFiles, obj.Path)
		}
	}

	logger.Info("Found video files in remote storage", zap.Int("count", len(videoFiles)))
	return videoFiles, nil
}

// runVideoValidation runs validation on all video files
func runVideoValidation(ctx context.Context, videoFiles []string, cfg *ValidationConfig, logger *zap.Logger) (*ValidationReport, error) {
	logger.Info("Starting video validation", 
		zap.Int("files", len(videoFiles)),
		zap.Int("workers", cfg.ValidationWorkers))

	startTime := time.Now()

	// Create metadata extractor
	extractor := ffmpeg.NewMetadataExtractor(logger)

	// Create worker pool for validation
	workerConfig := workers.DefaultWorkerPoolConfig()
	workerConfig.SmallFileWorkers = cfg.ValidationWorkers / 3
	workerConfig.MediumFileWorkers = cfg.ValidationWorkers / 3
	workerConfig.LargeFileWorkers = cfg.ValidationWorkers / 3
	
	pool := workers.NewWorkerPool(workerConfig, logger)
	if err := pool.Start(); err != nil {
		return nil, fmt.Errorf("failed to start worker pool: %w", err)
	}
	defer pool.Shutdown()

	// Results collection
	results := make([]ValidationResult, len(videoFiles))
	var resultsMutex sync.Mutex
	var wg sync.WaitGroup

	// Submit validation tasks
	for i, videoFile := range videoFiles {
		wg.Add(1)
		
		task := &workers.Task{
			ID:        fmt.Sprintf("validate-%d", i),
			SizeBytes: 100 * 1024 * 1024, // Assume 100MB for worker allocation
			Category:  workers.MediumFile,
			Payload:   map[string]interface{}{"file": videoFile, "index": i},
			ProcessFunc: func(ctx context.Context, payload interface{}) error {
				defer wg.Done()
				
				data := payload.(map[string]interface{})
				filePath := data["file"].(string)
				index := data["index"].(int)
				
				result := validateSingleVideo(ctx, filePath, extractor, logger)
				
				resultsMutex.Lock()
				results[index] = result
				resultsMutex.Unlock()
				
				return nil
			},
			ResultChan: make(chan workers.TaskResult, 1),
		}

		if err := pool.SubmitTask(task); err != nil {
			logger.Error("Failed to submit validation task", zap.Error(err))
			wg.Done()
		}
	}

	// Wait for all validations to complete
	wg.Wait()

	// Generate report
	report := &ValidationReport{
		TotalFiles:    len(videoFiles),
		Results:       results,
		ErrorsByType:  make(map[string]int),
		TotalDuration: time.Since(startTime),
		GeneratedAt:   time.Now(),
	}

	// Calculate statistics
	for _, result := range results {
		if result.Valid {
			report.ValidFiles++
		} else if result.Corrupted {
			report.CorruptedFiles++
		} else {
			report.ErrorFiles++
		}

		// Count errors by type
		for _, errMsg := range result.Errors {
			errorType := categorizeError(errMsg)
			report.ErrorsByType[errorType]++
		}
	}

	logger.Info("Validation completed",
		zap.Duration("duration", report.TotalDuration),
		zap.Int("valid", report.ValidFiles),
		zap.Int("corrupted", report.CorruptedFiles),
		zap.Int("errors", report.ErrorFiles))

	return report, nil
}

// validateSingleVideo validates a single video file
func validateSingleVideo(ctx context.Context, filePath string, extractor *ffmpeg.MetadataExtractor, logger *zap.Logger) ValidationResult {
	startTime := time.Now()
	
	result := ValidationResult{
		FilePath:  filePath,
		Timestamp: startTime,
	}

	// Extract metadata and validate
	metadata, err := extractor.ValidateVideo(ctx, filePath)
	if err != nil {
		result.Valid = false
		result.Errors = append(result.Errors, err.Error())
		result.ValidationTime = time.Since(startTime)
		return result
	}

	result.Metadata = metadata
	result.Corrupted = metadata.IsCorrupted
	result.Valid = !metadata.HasErrors && !metadata.IsCorrupted

	if metadata.HasErrors {
		result.Errors = append(result.Errors, metadata.ErrorMessages...)
	}

	result.ValidationTime = time.Since(startTime)
	return result
}

// categorizeError categorizes an error message for statistics
func categorizeError(errMsg string) string {
	errLower := strings.ToLower(errMsg)
	
	switch {
	case strings.Contains(errLower, "corrupt"):
		return "corruption"
	case strings.Contains(errLower, "timeout"):
		return "timeout"
	case strings.Contains(errLower, "permission"):
		return "permission"
	case strings.Contains(errLower, "not found"):
		return "file_not_found"
	case strings.Contains(errLower, "invalid"):
		return "invalid_format"
	case strings.Contains(errLower, "codec"):
		return "codec_error"
	default:
		return "other"
	}
}

// generateValidationReport generates the validation report
func generateValidationReport(report *ValidationReport, cfg *ValidationConfig, logger *zap.Logger) error {
	logger.Info("Generating validation report", zap.String("output", cfg.ValidationReport))

	// Create output directory if needed
	if err := utils.EnsureDir(filepath.Dir(cfg.ValidationReport)); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate report content based on format
	var content string
	var err error

	switch cfg.ValidationFormat {
	case "json":
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal report to JSON: %w", err)
		}
		content = string(data)
	case "markdown":
		content = generateMarkdownValidationReport(report)
	case "text":
		content = generateTextValidationReport(report)
	default:
		return fmt.Errorf("unsupported format: %s", cfg.ValidationFormat)
	}

	// Write report to file
	if err = os.WriteFile(cfg.ValidationReport, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write report: %w", err)
	}

	logger.Info("Validation report generated successfully", zap.String("file", cfg.ValidationReport))
	return nil
}

// generateMarkdownValidationReport generates a markdown format validation report
func generateMarkdownValidationReport(report *ValidationReport) string {
	var sb strings.Builder

	sb.WriteString("# Video Validation Report\n\n")
	sb.WriteString(fmt.Sprintf("**Generated:** %s\n", report.GeneratedAt.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("**Duration:** %s\n\n", report.TotalDuration.String()))

	// Summary
	sb.WriteString("## Summary\n\n")
	sb.WriteString(fmt.Sprintf("- **Total Files:** %d\n", report.TotalFiles))
	sb.WriteString(fmt.Sprintf("- **Valid Files:** %d (%.1f%%)\n", 
		report.ValidFiles, float64(report.ValidFiles)/float64(report.TotalFiles)*100))
	sb.WriteString(fmt.Sprintf("- **Corrupted Files:** %d (%.1f%%)\n", 
		report.CorruptedFiles, float64(report.CorruptedFiles)/float64(report.TotalFiles)*100))
	sb.WriteString(fmt.Sprintf("- **Error Files:** %d (%.1f%%)\n\n", 
		report.ErrorFiles, float64(report.ErrorFiles)/float64(report.TotalFiles)*100))

	// Error breakdown
	if len(report.ErrorsByType) > 0 {
		sb.WriteString("## Error Breakdown\n\n")
		sb.WriteString("| Error Type | Count |\n")
		sb.WriteString("|------------|-------|\n")
		for errType, count := range report.ErrorsByType {
			sb.WriteString(fmt.Sprintf("| %s | %d |\n", errType, count))
		}
		sb.WriteString("\n")
	}

	// Failed files
	failedFiles := make([]ValidationResult, 0)
	for _, result := range report.Results {
		if !result.Valid {
			failedFiles = append(failedFiles, result)
		}
	}

	if len(failedFiles) > 0 {
		sb.WriteString("## Failed Files\n\n")
		sb.WriteString("| File | Status | Errors |\n")
		sb.WriteString("|------|--------|--------|\n")
		for _, result := range failedFiles {
			status := "Error"
			if result.Corrupted {
				status = "Corrupted"
			}
			errors := strings.Join(result.Errors, "; ")
			sb.WriteString(fmt.Sprintf("| %s | %s | %s |\n", 
				result.FilePath, status, utils.TruncateString(errors, 100)))
		}
	}

	return sb.String()
}

// generateTextValidationReport generates a plain text validation report
func generateTextValidationReport(report *ValidationReport) string {
	var sb strings.Builder

	sb.WriteString("Video Validation Report\n")
	sb.WriteString("=======================\n\n")
	sb.WriteString(fmt.Sprintf("Generated: %s\n", report.GeneratedAt.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Duration: %s\n\n", report.TotalDuration.String()))

	sb.WriteString("Summary:\n")
	sb.WriteString(fmt.Sprintf("  Total Files: %d\n", report.TotalFiles))
	sb.WriteString(fmt.Sprintf("  Valid Files: %d (%.1f%%)\n", 
		report.ValidFiles, float64(report.ValidFiles)/float64(report.TotalFiles)*100))
	sb.WriteString(fmt.Sprintf("  Corrupted Files: %d (%.1f%%)\n", 
		report.CorruptedFiles, float64(report.CorruptedFiles)/float64(report.TotalFiles)*100))
	sb.WriteString(fmt.Sprintf("  Error Files: %d (%.1f%%)\n\n", 
		report.ErrorFiles, float64(report.ErrorFiles)/float64(report.TotalFiles)*100))

	// List failed files
	for _, result := range report.Results {
		if !result.Valid {
			status := "ERROR"
			if result.Corrupted {
				status = "CORRUPTED"
			}
			sb.WriteString(fmt.Sprintf("[%s] %s\n", status, result.FilePath))
			for _, err := range result.Errors {
				sb.WriteString(fmt.Sprintf("  - %s\n", err))
			}
			sb.WriteString("\n")
		}
	}

	return sb.String()
}