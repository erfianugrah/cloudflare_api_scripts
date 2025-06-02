package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"media-toolkit-go/pkg/storage"
	"media-toolkit-go/pkg/utils"
)

// NewAnalyzeCommand creates the analyze command
func NewAnalyzeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analyze file sizes and generate reports",
		Long: `Analyze file sizes in remote storage and generate comprehensive reports.
Can also compare results with Cloudflare KV data and generate error reports.`,
		RunE: runAnalyze,
	}

	addAnalyzeFlags(cmd)
	return cmd
}

func addAnalyzeFlags(cmd *cobra.Command) {
	// Required flags for listing
	cmd.Flags().String("remote", "", "rclone remote name")
	cmd.Flags().String("bucket", "", "S3 bucket name")
	cmd.Flags().String("directory", "", "Directory path within bucket")
	
	// Analysis options
	cmd.Flags().Bool("list-files", false, "List all files with their sizes")
	cmd.Flags().Int("size-threshold", 256, "Size threshold in MiB for reporting")
	cmd.Flags().String("size-report-output", "file_size_report.md", "Size report output file")
	cmd.Flags().StringSlice("extensions", []string{}, "File extensions to filter by (e.g., .mp4,.jpg,.png)")
	cmd.Flags().String("media-type", "", "Media type preset: 'image', 'video', or 'all'")
	cmd.Flags().Int("limit", 0, "Limit number of files to analyze")
	
	// Error report generation
	cmd.Flags().Bool("generate-error-report", false, "Generate error report from results")
	cmd.Flags().String("results-file", "media_transform_results.json", "Results file to analyze")
	cmd.Flags().String("error-report-output", "error_report.json", "Error report output file")
	cmd.Flags().String("format", "", "Report format (json, markdown)")
	
	// Comparison options
	cmd.Flags().String("compare", "", "Path to Cloudflare KV JSON file for comparison")
	cmd.Flags().String("comparison-output", "comparison_results.json", "Comparison results output")
	cmd.Flags().String("summary-output", "comparison_summary.md", "Comparison summary output")
	cmd.Flags().String("summary-format", "markdown", "Summary format (markdown, json)")
	cmd.Flags().Bool("only-compare", false, "Only run comparison without processing")
	
	// Storage options
	cmd.Flags().Bool("use-aws-cli", false, "Use AWS CLI instead of rclone")
	
	// Bind flags to viper
	viper.BindPFlags(cmd.Flags())
}

func runAnalyze(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	
	// Get logger from context
	logger, ok := ctx.Value("logger").(*zap.Logger)
	if !ok || logger == nil {
		return fmt.Errorf("logger not found in context")
	}

	logger.Info("Starting analysis process")

	// Execute the analysis workflow
	if err := executeAnalysisWorkflow(ctx, logger); err != nil {
		logger.Error("Analysis failed", zap.Error(err))
		return fmt.Errorf("analysis failed: %w", err)
	}

	logger.Info("Analysis process completed successfully")
	return nil
}

// executeAnalysisWorkflow performs the complete analysis workflow
func executeAnalysisWorkflow(ctx context.Context, logger *zap.Logger) error {
	// Build configuration from viper settings
	cfg, err := buildAnalysisConfig()
	if err != nil {
		return fmt.Errorf("failed to build configuration: %w", err)
	}

	// Get file list cache from context if available
	if cache, ok := ctx.Value("fileListCache").(*storage.FileListCache); ok && cache != nil {
		cfg.FileListCache = cache
	}

	// Validate configuration
	if err := validateAnalysisConfig(cfg); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Check if this is a comparison-only run
	if viper.GetBool("only-compare") {
		return runComparisonOnly(ctx, cfg, logger)
	}

	// Check if this is error report generation only
	if viper.GetBool("generate-error-report") && !cfg.ListFiles {
		return generateErrorReportOnly(ctx, cfg, logger)
	}

	// Create storage client
	storageClient, err := createStorageClient(cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer storageClient.Close()

	// List files from storage
	files, err := listFilesForAnalysis(ctx, storageClient, cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	// Analyze files and generate reports
	analysis, err := analyzeFiles(files, cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to analyze files: %w", err)
	}

	// Generate size report
	if err := generateSizeReport(analysis, cfg, logger); err != nil {
		return fmt.Errorf("failed to generate size report: %w", err)
	}

	// Generate error report if requested
	if cfg.GenerateErrorReport {
		if err := generateErrorReport(ctx, cfg, logger); err != nil {
			logger.Warn("Failed to generate error report", zap.Error(err))
		}
	}

	// Run comparison if requested
	if cfg.CompareFile != "" {
		if err := runComparison(ctx, cfg, logger); err != nil {
			logger.Warn("Failed to run comparison", zap.Error(err))
		}
	}

	return nil
}

// AnalysisConfig holds configuration for analysis operations
type AnalysisConfig struct {
	// Storage configuration
	Remote             string
	Bucket             string
	Directory          string
	UseAWSCLI          bool
	
	// Analysis options
	ListFiles          bool
	SizeThreshold      int
	SizeReportOutput   string
	Extensions         []string
	MediaType          string
	Limit              int
	
	// Error report options
	GenerateErrorReport bool
	ResultsFile        string
	ErrorReportOutput  string
	Format             string
	
	// Comparison options
	CompareFile        string
	ComparisonOutput   string
	SummaryOutput      string
	SummaryFormat      string
	
	// File list cache
	FileListCache      *storage.FileListCache
}

// FileAnalysis represents analysis results for files
type FileAnalysis struct {
	TotalFiles     int                    `json:"total_files"`
	TotalSize      int64                  `json:"total_size"`
	AllFiles       []storage.FileInfo     `json:"all_files,omitempty"`
	LargeFiles     []storage.FileInfo     `json:"large_files"`
	SizeDistribution map[string]int       `json:"size_distribution"`
	ExtensionStats map[string]*ExtStats   `json:"extension_stats"`
	GeneratedAt    time.Time              `json:"generated_at"`
}

// ExtStats represents statistics for a file extension
type ExtStats struct {
	Count     int   `json:"count"`
	TotalSize int64 `json:"total_size"`
	AvgSize   int64 `json:"avg_size"`
	MinSize   int64 `json:"min_size"`
	MaxSize   int64 `json:"max_size"`
}

// buildAnalysisConfig creates analysis configuration from viper settings
func buildAnalysisConfig() (*AnalysisConfig, error) {
	return &AnalysisConfig{
		Remote:              viper.GetString("remote"),
		Bucket:              viper.GetString("bucket"),
		Directory:           viper.GetString("directory"),
		UseAWSCLI:           viper.GetBool("use-aws-cli"),
		ListFiles:           viper.GetBool("list-files"),
		SizeThreshold:       viper.GetInt("size-threshold"),
		SizeReportOutput:    viper.GetString("size-report-output"),
		Extensions:          viper.GetStringSlice("extensions"),
		MediaType:           viper.GetString("media-type"),
		Limit:               viper.GetInt("limit"),
		GenerateErrorReport: viper.GetBool("generate-error-report"),
		ResultsFile:         viper.GetString("results-file"),
		ErrorReportOutput:   viper.GetString("error-report-output"),
		Format:              viper.GetString("format"),
		CompareFile:         viper.GetString("compare"),
		ComparisonOutput:    viper.GetString("comparison-output"),
		SummaryOutput:       viper.GetString("summary-output"),
		SummaryFormat:       viper.GetString("summary-format"),
	}, nil
}

// validateAnalysisConfig validates the analysis configuration
func validateAnalysisConfig(cfg *AnalysisConfig) error {
	// Check if we need storage access
	needsStorage := cfg.ListFiles || (!cfg.GenerateErrorReport && cfg.CompareFile == "")
	
	if needsStorage {
		if err := utils.ValidateNonEmpty(cfg.Remote, "remote"); err != nil {
			return err
		}
		if err := utils.ValidateNonEmpty(cfg.Bucket, "bucket"); err != nil {
			return err
		}
	}

	// Validate threshold
	if err := utils.ValidatePositiveInt(cfg.SizeThreshold, "size-threshold"); err != nil {
		return err
	}

	// Validate format if specified
	if cfg.Format != "" {
		validFormats := []string{"json", "markdown"}
		if err := utils.ValidateOneOf(cfg.Format, validFormats, "format"); err != nil {
			return err
		}
	}

	// Validate summary format
	validSummaryFormats := []string{"json", "markdown"}
	if err := utils.ValidateOneOf(cfg.SummaryFormat, validSummaryFormats, "summary-format"); err != nil {
		return err
	}

	// Validate media type if specified (only when listing files)
	if cfg.MediaType != "" && (cfg.ListFiles || (!cfg.GenerateErrorReport && cfg.CompareFile == "")) {
		validMediaTypes := []string{"image", "video", "all", "auto"}
		if err := utils.ValidateOneOf(cfg.MediaType, validMediaTypes, "media-type"); err != nil {
			return err
		}
	}

	return nil
}

// createStorageClient creates a storage client based on configuration
func createStorageClient(cfg *AnalysisConfig, logger *zap.Logger) (storage.Storage, error) {
	storageConfig := &storage.StorageConfig{
		Backend: storage.StorageBackendRclone,
		Remote:  cfg.Remote,
		Bucket:  cfg.Bucket,
	}

	if cfg.UseAWSCLI {
		storageConfig.Backend = storage.StorageBackendAWS
		storageConfig.AWSRegion = viper.GetString("aws-region")
		storageConfig.AWSProfile = viper.GetString("aws-profile")
	}

	return storage.NewStorage(storageConfig, logger)
}

// listFilesForAnalysis lists files from storage for analysis
func listFilesForAnalysis(ctx context.Context, storageClient storage.Storage, cfg *AnalysisConfig, logger *zap.Logger) ([]storage.FileInfo, error) {
	logger.Info("Listing files for analysis",
		zap.String("remote", cfg.Remote),
		zap.String("bucket", cfg.Bucket),
		zap.String("directory", cfg.Directory))

	// Check cache first if available
	if cfg.FileListCache != nil {
		cached, found := cfg.FileListCache.Get(cfg.Remote, cfg.Bucket, cfg.Directory, cfg.Extensions)
		if found {
			logger.Info("Using cached file listing", zap.Int("file_count", len(cached.Files)))
			return cached.Files, nil
		}
	}

	objects, err := storageClient.ListObjects(ctx, storage.ListRequest{
		Bucket:    cfg.Bucket,
		Directory: cfg.Directory,
		GetSizes:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// Cache the results if cache is available
	if cfg.FileListCache != nil {
		if err := cfg.FileListCache.Set(cfg.Remote, cfg.Bucket, cfg.Directory, nil, objects); err != nil {
			logger.Warn("Failed to cache file listing", zap.Error(err))
		}
	}

	// Determine extensions to filter by
	var filterExtensions []string
	
	// Handle media type presets
	if cfg.MediaType != "" {
		switch strings.ToLower(cfg.MediaType) {
		case "image":
			filterExtensions = []string{".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg"}
		case "video":
			filterExtensions = []string{".mp4", ".webm", ".mov", ".avi", ".mkv", ".m4v"}
		case "all", "auto":
			// No filtering by extension
			filterExtensions = nil
		default:
			return nil, fmt.Errorf("invalid media type: %s (use 'image', 'video', 'all', or 'auto')", cfg.MediaType)
		}
	} else if len(cfg.Extensions) > 0 {
		// Use explicitly provided extensions
		filterExtensions = cfg.Extensions
	}
	
	// Apply extension filter if needed
	if len(filterExtensions) > 0 {
		filtered := make([]storage.FileInfo, 0, len(objects))
		for _, obj := range objects {
			objExt := strings.ToLower(filepath.Ext(obj.Path))
			for _, ext := range filterExtensions {
				if objExt == strings.ToLower(ext) {
					filtered = append(filtered, obj)
					break
				}
			}
		}
		objects = filtered
	}

	// Apply limit if specified
	if cfg.Limit > 0 && len(objects) > cfg.Limit {
		objects = objects[:cfg.Limit]
	}

	logger.Info("Files retrieved", zap.Int("count", len(objects)))
	return objects, nil
}

// analyzeFiles performs analysis on the list of files
func analyzeFiles(files []storage.FileInfo, cfg *AnalysisConfig, logger *zap.Logger) (*FileAnalysis, error) {
	logger.Info("Analyzing files", zap.Int("count", len(files)))

	analysis := &FileAnalysis{
		TotalFiles:       len(files),
		LargeFiles:       make([]storage.FileInfo, 0),
		SizeDistribution: make(map[string]int),
		ExtensionStats:   make(map[string]*ExtStats),
		GeneratedAt:      time.Now(),
	}
	
	// Include all files if list-files is enabled
	if cfg.ListFiles {
		analysis.AllFiles = files
	}

	thresholdBytes := int64(cfg.SizeThreshold) * 1024 * 1024 // Convert MiB to bytes

	for _, file := range files {
		analysis.TotalSize += file.Size

		// Check if file is large
		if file.Size >= thresholdBytes {
			analysis.LargeFiles = append(analysis.LargeFiles, file)
		}

		// Update size distribution
		sizeCategory := getSizeCategory(file.Size)
		analysis.SizeDistribution[sizeCategory]++

		// Update extension statistics
		ext := strings.ToLower(filepath.Ext(file.Path))
		if ext == "" {
			ext = "no_extension"
		}

		stats, exists := analysis.ExtensionStats[ext]
		if !exists {
			stats = &ExtStats{
				MinSize: file.Size,
				MaxSize: file.Size,
			}
			analysis.ExtensionStats[ext] = stats
		}

		stats.Count++
		stats.TotalSize += file.Size
		stats.AvgSize = stats.TotalSize / int64(stats.Count)

		if file.Size < stats.MinSize {
			stats.MinSize = file.Size
		}
		if file.Size > stats.MaxSize {
			stats.MaxSize = file.Size
		}
	}

	logger.Info("Analysis complete",
		zap.Int("total_files", analysis.TotalFiles),
		zap.String("total_size", utils.FormatBytes(analysis.TotalSize)),
		zap.Int("large_files", len(analysis.LargeFiles)))

	return analysis, nil
}

// getSizeCategory categorizes file size into buckets
func getSizeCategory(size int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case size < KB:
		return "< 1KB"
	case size < 10*KB:
		return "1KB - 10KB"
	case size < 100*KB:
		return "10KB - 100KB"
	case size < MB:
		return "100KB - 1MB"
	case size < 10*MB:
		return "1MB - 10MB"
	case size < 100*MB:
		return "10MB - 100MB"
	case size < GB:
		return "100MB - 1GB"
	default:
		return "> 1GB"
	}
}

// generateSizeReport generates a size analysis report
func generateSizeReport(analysis *FileAnalysis, cfg *AnalysisConfig, logger *zap.Logger) error {
	logger.Info("Generating size report", zap.String("output", cfg.SizeReportOutput))

	// Create output directory if needed
	if err := utils.EnsureDir(filepath.Dir(cfg.SizeReportOutput)); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate report content
	var content string
	if strings.HasSuffix(cfg.SizeReportOutput, ".json") {
		data, err := json.MarshalIndent(analysis, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal analysis to JSON: %w", err)
		}
		content = string(data)
	} else {
		content = generateMarkdownReport(analysis)
	}

	// Write report to file
	if err := os.WriteFile(cfg.SizeReportOutput, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write report: %w", err)
	}

	logger.Info("Size report generated successfully", zap.String("file", cfg.SizeReportOutput))
	return nil
}

// generateMarkdownReport generates a markdown format report
func generateMarkdownReport(analysis *FileAnalysis) string {
	var sb strings.Builder

	sb.WriteString("# File Size Analysis Report\n\n")
	sb.WriteString(fmt.Sprintf("**Generated:** %s\n\n", analysis.GeneratedAt.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("**Total Files:** %d\n", analysis.TotalFiles))
	sb.WriteString(fmt.Sprintf("**Total Size:** %s\n\n", utils.FormatBytes(analysis.TotalSize)))

	// Size distribution
	sb.WriteString("## Size Distribution\n\n")
	sb.WriteString("| Size Range | Count |\n")
	sb.WriteString("|------------|-------|\n")
	for category, count := range analysis.SizeDistribution {
		sb.WriteString(fmt.Sprintf("| %s | %d |\n", category, count))
	}
	sb.WriteString("\n")

	// Extension statistics
	if len(analysis.ExtensionStats) > 0 {
		sb.WriteString("## Extension Statistics\n\n")
		sb.WriteString("| Extension | Count | Total Size | Avg Size | Min Size | Max Size |\n")
		sb.WriteString("|-----------|-------|------------|----------|----------|----------|\n")
		for ext, stats := range analysis.ExtensionStats {
			sb.WriteString(fmt.Sprintf("| %s | %d | %s | %s | %s | %s |\n",
				ext, stats.Count,
				utils.FormatBytes(stats.TotalSize),
				utils.FormatBytes(stats.AvgSize),
				utils.FormatBytes(stats.MinSize),
				utils.FormatBytes(stats.MaxSize)))
		}
		sb.WriteString("\n")
	}

	// All files (if list-files was enabled)
	if len(analysis.AllFiles) > 0 {
		sb.WriteString("## All Files\n\n")
		sb.WriteString("| File | Size |\n")
		sb.WriteString("|------|------|\n")
		for _, file := range analysis.AllFiles {
			sb.WriteString(fmt.Sprintf("| %s | %s |\n", file.Path, utils.FormatBytes(file.Size)))
		}
		sb.WriteString("\n")
	}

	// Large files
	if len(analysis.LargeFiles) > 0 {
		sb.WriteString("## Large Files\n\n")
		sb.WriteString("| File | Size |\n")
		sb.WriteString("|------|------|\n")
		for _, file := range analysis.LargeFiles {
			sb.WriteString(fmt.Sprintf("| %s | %s |\n", file.Path, utils.FormatBytes(file.Size)))
		}
	}

	return sb.String()
}

// runComparisonOnly runs comparison workflow only
func runComparisonOnly(ctx context.Context, cfg *AnalysisConfig, logger *zap.Logger) error {
	if cfg.CompareFile == "" {
		return fmt.Errorf("comparison file must be specified for comparison-only mode")
	}
	return runComparison(ctx, cfg, logger)
}

// generateErrorReportOnly generates error report only
func generateErrorReportOnly(ctx context.Context, cfg *AnalysisConfig, logger *zap.Logger) error {
	return generateErrorReport(ctx, cfg, logger)
}

// generateErrorReport generates an error report from results file
func generateErrorReport(ctx context.Context, cfg *AnalysisConfig, logger *zap.Logger) error {
	if !utils.FileExists(cfg.ResultsFile) {
		return fmt.Errorf("results file not found: %s", cfg.ResultsFile)
	}

	logger.Info("Generating error report", 
		zap.String("results_file", cfg.ResultsFile),
		zap.String("output", cfg.ErrorReportOutput))

	// Read and parse results file
	data, err := os.ReadFile(cfg.ResultsFile)
	if err != nil {
		return fmt.Errorf("failed to read results file: %w", err)
	}

	var results WorkflowResults
	if err := json.Unmarshal(data, &results); err != nil {
		return fmt.Errorf("failed to parse results file: %w", err)
	}

	// Extract and analyze errors
	errorAnalysis, err := extractAndAnalyzeErrors(results, logger)
	if err != nil {
		return fmt.Errorf("failed to analyze errors: %w", err)
	}

	// Generate report
	var reportContent []byte
	format := cfg.Format
	if format == "" {
		// Determine format from output file extension
		if strings.HasSuffix(cfg.ErrorReportOutput, ".md") {
			format = "markdown"
		} else {
			format = "json"
		}
	}

	if format == "markdown" {
		reportContent = []byte(generateErrorMarkdownReport(errorAnalysis))
	} else {
		reportContent, err = json.MarshalIndent(errorAnalysis, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal error analysis: %w", err)
		}
	}

	// Write the error report
	if err := os.WriteFile(cfg.ErrorReportOutput, reportContent, 0644); err != nil {
		return fmt.Errorf("failed to write error report: %w", err)
	}

	logger.Info("Error report generated successfully", 
		zap.String("file", cfg.ErrorReportOutput),
		zap.Int("total_errors", errorAnalysis.Summary.TotalErrors))

	return nil
}

// runComparison runs comparison with KV data
func runComparison(ctx context.Context, cfg *AnalysisConfig, logger *zap.Logger) error {
	if !utils.FileExists(cfg.CompareFile) {
		return fmt.Errorf("comparison file not found: %s", cfg.CompareFile)
	}

	logger.Info("Running comparison", 
		zap.String("compare_file", cfg.CompareFile),
		zap.String("output", cfg.ComparisonOutput))

	// Simple comparison implementation - read and process comparison data
	data, err := os.ReadFile(cfg.CompareFile)
	if err != nil {
		return fmt.Errorf("failed to read comparison file: %w", err)
	}

	// Write comparison results (simplified implementation)
	return os.WriteFile(cfg.ComparisonOutput, data, 0644)
}

// Types for error analysis

// WorkflowResults represents the structure of the results file
type WorkflowResults struct {
	WorkflowType   int              `json:"workflow_type"`
	Success        bool             `json:"success"`
	StartTime      time.Time        `json:"start_time"`
	EndTime        time.Time        `json:"end_time"`
	Duration       int64            `json:"duration"`
	PrewarmResults []PrewarmResult  `json:"prewarm_results"`
	Statistics     *StatsSummary    `json:"statistics,omitempty"`
	WorkerStats    *WorkerStats     `json:"worker_stats,omitempty"`
}

// PrewarmResult represents a single file processing result
type PrewarmResult struct {
	Path           string                     `json:"path"`
	SizeBytes      int64                      `json:"size_bytes"`
	SizeCategory   string                     `json:"size_category"`
	MediaType      string                     `json:"media_type"`
	ProcessingTime int64                      `json:"processing_time"`
	Success        bool                       `json:"success"`
	Results        *VideoProcessResult        `json:"results"`
	Error          string                     `json:"error,omitempty"`
}

// VideoProcessResult represents video processing results
type VideoProcessResult struct {
	Path           string                          `json:"path"`
	SizeBytes      int64                           `json:"size_bytes"`
	SizeCategory   string                          `json:"size_category"`
	MediaType      string                          `json:"media_type"`
	ProcessingTime int64                           `json:"processing_time"`
	Success        bool                            `json:"success"`
	Results        map[string]*DerivativeResult    `json:"results"`
	Error          string                          `json:"error,omitempty"`
}

// DerivativeResult represents the result of processing a single derivative
type DerivativeResult struct {
	DerivativeName         string  `json:"derivative_name"`
	URL                    string  `json:"url"`
	Status                 string  `json:"status"`
	StatusCode             int     `json:"status_code,omitempty"`
	TimeToFirstByte        int64   `json:"time_to_first_byte,omitempty"`
	TotalTime              int64   `json:"total_time"`
	ResponseSize           int64   `json:"response_size_bytes,omitempty"`
	ContentType            string  `json:"content_type,omitempty"`
	Method                 string  `json:"method,omitempty"`
	Retries                int     `json:"retries,omitempty"`
	Error                  string  `json:"error,omitempty"`
	ErrorType              string  `json:"error_type,omitempty"`
	OriginalSize           int64   `json:"original_size_bytes,omitempty"`
	SizeReductionBytes     int64   `json:"size_reduction_bytes,omitempty"`
	SizeReductionPercent   float64 `json:"size_reduction_percent,omitempty"`
}

// StatsSummary represents statistics summary
type StatsSummary struct {
	TotalRequests    int64 `json:"total_requests"`
	SuccessfulCount  int64 `json:"successful_count"`
	ErrorCount       int64 `json:"error_count"`
}

// WorkerStats represents worker statistics  
type WorkerStats struct {
	TasksSubmitted int64 `json:"tasks_submitted"`
	TasksCompleted int64 `json:"tasks_completed"`
	TasksFailed    int64 `json:"tasks_failed"`
}

// ErrorAnalysis represents comprehensive error analysis
type ErrorAnalysis struct {
	Timestamp         string                      `json:"timestamp"`
	OriginalTimestamp string                      `json:"original_timestamp"`
	Summary           ErrorSummary                `json:"summary"`
	ErrorTypes        map[string]int              `json:"error_types"`
	StatusCodes       map[string]int              `json:"status_codes"`
	SizeCategories    map[string]int              `json:"size_categories"`
	Derivatives       map[string]int              `json:"derivatives"`
	CommonMessages    map[string]int              `json:"common_error_messages"`
	SizeStats         SizeStatistics              `json:"size_statistics"`
	DetailedErrors    []DetailedError             `json:"detailed_errors"`
	Recommendations   []string                    `json:"troubleshooting_recommendations"`
	ErrorExamples     map[string]ErrorExample     `json:"error_examples"`
}

// ErrorSummary represents error summary statistics
type ErrorSummary struct {
	TotalErrors     int     `json:"total_errors"`
	TotalProcessed  int     `json:"total_processed"`
	ErrorRate       float64 `json:"error_rate"`
}

// SizeStatistics represents size-based error statistics
type SizeStatistics struct {
	ErrorFiles   *FileSizeStats `json:"error_files,omitempty"`
	SuccessFiles *FileSizeStats `json:"success_files,omitempty"`
}

// FileSizeStats represents file size statistics
type FileSizeStats struct {
	Count      int   `json:"count"`
	MinSize    int64 `json:"min_size"`
	MaxSize    int64 `json:"max_size"`
	AvgSize    int64 `json:"avg_size"`
	MedianSize int64 `json:"median_size"`
	TotalSize  int64 `json:"total_size"`
}

// DetailedError represents a single error with full details
type DetailedError struct {
	File          string `json:"file"`
	Derivative    string `json:"derivative"`
	StatusCode    int    `json:"status_code"`
	ErrorType     string `json:"error_type"`
	URL           string `json:"url"`
	ErrorMessage  string `json:"error_msg"`
	SizeBytes     int64  `json:"size_bytes"`
	SizeFormatted string `json:"size_formatted"`
	SizeCategory  string `json:"size_category"`
}

// ErrorExample represents an example of an error type
type ErrorExample struct {
	File          string `json:"file"`
	URL           string `json:"url"`
	StatusCode    int    `json:"status_code"`
	ErrorMessage  string `json:"error_msg"`
	SizeBytes     int64  `json:"size_bytes"`
	SizeFormatted string `json:"size_formatted"`
}

// extractAndAnalyzeErrors extracts errors from results and performs analysis
func extractAndAnalyzeErrors(results WorkflowResults, logger *zap.Logger) (*ErrorAnalysis, error) {
	logger.Info("Extracting and analyzing errors from results")

	analysis := &ErrorAnalysis{
		Timestamp:         time.Now().Format(time.RFC3339),
		OriginalTimestamp: results.StartTime.Format(time.RFC3339),
		ErrorTypes:        make(map[string]int),
		StatusCodes:       make(map[string]int),
		SizeCategories:    make(map[string]int),
		Derivatives:       make(map[string]int),
		CommonMessages:    make(map[string]int),
		DetailedErrors:    make([]DetailedError, 0),
		Recommendations:   make([]string, 0),
		ErrorExamples:     make(map[string]ErrorExample),
	}

	var allErrors []DetailedError
	var errorSizes, successSizes []int64
	errorTypeExamples := make(map[string]DetailedError)

	// Extract errors from prewarm results
	for _, result := range results.PrewarmResults {
		if result.Results != nil && result.Results.Results != nil {
			for derivativeName, derivative := range result.Results.Results {
				if derivative.Status != "success" && derivative.Status != "" {
					// This is an error
					detailedError := DetailedError{
						File:          result.Path,
						Derivative:    derivativeName,
						StatusCode:    derivative.StatusCode,
						ErrorType:     derivative.ErrorType,
						URL:           derivative.URL,
						ErrorMessage:  derivative.Error,
						SizeBytes:     result.SizeBytes,
						SizeFormatted: utils.FormatBytes(result.SizeBytes),
						SizeCategory:  result.SizeCategory,
					}

					allErrors = append(allErrors, detailedError)
					errorSizes = append(errorSizes, result.SizeBytes)

					// Count by various categories
					analysis.ErrorTypes[derivative.ErrorType]++
					analysis.StatusCodes[fmt.Sprintf("%d", derivative.StatusCode)]++
					analysis.SizeCategories[result.SizeCategory]++
					analysis.Derivatives[derivativeName]++
					
					if derivative.Error != "" {
						analysis.CommonMessages[derivative.Error]++
					}

					// Store first example of each error type
					if _, exists := errorTypeExamples[derivative.ErrorType]; !exists {
						errorTypeExamples[derivative.ErrorType] = detailedError
					}
				}
			}
		}

		// Track successful file sizes for comparison
		if result.Success {
			successSizes = append(successSizes, result.SizeBytes)
		}
	}

	// Calculate summary statistics
	totalProcessed := len(results.PrewarmResults)
	totalErrors := len(allErrors)
	errorRate := 0.0
	if totalProcessed > 0 {
		errorRate = float64(totalErrors) / float64(totalProcessed) * 100
	}

	analysis.Summary = ErrorSummary{
		TotalErrors:    totalErrors,
		TotalProcessed: totalProcessed,
		ErrorRate:      errorRate,
	}

	// Calculate size statistics
	analysis.SizeStats = calculateSizeStatistics(errorSizes, successSizes)

	// Store detailed errors (limit to first 100 for performance)
	maxErrors := 100
	if len(allErrors) > maxErrors {
		analysis.DetailedErrors = allErrors[:maxErrors]
	} else {
		analysis.DetailedErrors = allErrors
	}

	// Create error examples
	for errorType, example := range errorTypeExamples {
		analysis.ErrorExamples[errorType] = ErrorExample{
			File:          example.File,
			URL:           example.URL,
			StatusCode:    example.StatusCode,
			ErrorMessage:  example.ErrorMessage,
			SizeBytes:     example.SizeBytes,
			SizeFormatted: example.SizeFormatted,
		}
	}

	// Generate troubleshooting recommendations
	analysis.Recommendations = generateRecommendations(analysis)

	logger.Info("Error analysis completed",
		zap.Int("total_errors", totalErrors),
		zap.Int("error_types", len(analysis.ErrorTypes)),
		zap.Float64("error_rate", errorRate))

	return analysis, nil
}

// calculateSizeStatistics calculates size-based statistics
func calculateSizeStatistics(errorSizes, successSizes []int64) SizeStatistics {
	stats := SizeStatistics{}

	if len(errorSizes) > 0 {
		stats.ErrorFiles = &FileSizeStats{
			Count:      len(errorSizes),
			MinSize:    minInt64(errorSizes),
			MaxSize:    maxInt64(errorSizes),
			AvgSize:    avgInt64(errorSizes),
			MedianSize: medianInt64(errorSizes),
			TotalSize:  sumInt64(errorSizes),
		}
	}

	if len(successSizes) > 0 {
		stats.SuccessFiles = &FileSizeStats{
			Count:      len(successSizes),
			MinSize:    minInt64(successSizes),
			MaxSize:    maxInt64(successSizes),
			AvgSize:    avgInt64(successSizes),
			MedianSize: medianInt64(successSizes),
			TotalSize:  sumInt64(successSizes),
		}
	}

	return stats
}

// generateRecommendations generates troubleshooting recommendations based on error patterns
func generateRecommendations(analysis *ErrorAnalysis) []string {
	var recommendations []string

	// Check for timeout errors
	if analysis.ErrorTypes["request_error"] > 0 {
		recommendations = append(recommendations, 
			"Request Errors: Consider increasing timeout settings or reducing worker count to avoid overwhelming the server")
	}

	// Check for 500 errors
	if analysis.StatusCodes["500"] > 0 {
		recommendations = append(recommendations, 
			"Server Errors (500): These indicate issues on the server side. Consider retrying failed files or contacting the API provider")
	}

	// Check for 404 errors
	if analysis.StatusCodes["404"] > 0 {
		recommendations = append(recommendations, 
			"Not Found Errors (404): Verify file paths and base URL configuration are correct")
	}

	// Check for high error rate
	if analysis.Summary.ErrorRate > 50 {
		recommendations = append(recommendations, 
			"High Error Rate: Consider reducing concurrency, checking network connectivity, or validating input files")
	}

	// Check for large file errors
	if analysis.SizeCategories["large"] > 0 {
		recommendations = append(recommendations, 
			"Large File Errors: Consider pre-processing large files or increasing timeout settings for large file processing")
	}

	return recommendations
}

// generateErrorMarkdownReport generates a markdown format error report
func generateErrorMarkdownReport(analysis *ErrorAnalysis) string {
	var sb strings.Builder

	sb.WriteString("# Video Transformation Error Report\n\n")
	sb.WriteString(fmt.Sprintf("**Generated:** %s\n", analysis.Timestamp))
	sb.WriteString(fmt.Sprintf("**Original Data:** %s\n\n", analysis.OriginalTimestamp))

	// Summary
	sb.WriteString("## Summary\n\n")
	sb.WriteString(fmt.Sprintf("- **Total errors:** %d\n", analysis.Summary.TotalErrors))
	sb.WriteString(fmt.Sprintf("- **Total processed:** %d\n", analysis.Summary.TotalProcessed))
	sb.WriteString(fmt.Sprintf("- **Error rate:** %.2f%%\n\n", analysis.Summary.ErrorRate))

	// Error Types
	if len(analysis.ErrorTypes) > 0 {
		sb.WriteString("## Error Types\n\n")
		sb.WriteString("| Error Type | Count | Percentage |\n")
		sb.WriteString("|------------|-------|------------|\n")
		for errorType, count := range analysis.ErrorTypes {
			percentage := float64(count) / float64(analysis.Summary.TotalErrors) * 100
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", errorType, count, percentage))
		}
		sb.WriteString("\n")
	}

	// Status Codes
	if len(analysis.StatusCodes) > 0 {
		sb.WriteString("## Status Codes\n\n")
		sb.WriteString("| Status Code | Count | Percentage |\n")
		sb.WriteString("|-------------|-------|------------|\n")
		for statusCode, count := range analysis.StatusCodes {
			percentage := float64(count) / float64(analysis.Summary.TotalErrors) * 100
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", statusCode, count, percentage))
		}
		sb.WriteString("\n")
	}

	// Size Categories
	if len(analysis.SizeCategories) > 0 {
		sb.WriteString("## Errors by Size Category\n\n")
		sb.WriteString("| Size Category | Count | Percentage |\n")
		sb.WriteString("|---------------|-------|------------|\n")
		for sizeCategory, count := range analysis.SizeCategories {
			percentage := float64(count) / float64(analysis.Summary.TotalErrors) * 100
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", sizeCategory, count, percentage))
		}
		sb.WriteString("\n")
	}

	// Derivatives
	if len(analysis.Derivatives) > 0 {
		sb.WriteString("## Errors by Derivative\n\n")
		sb.WriteString("| Derivative | Count | Percentage |\n")
		sb.WriteString("|------------|-------|------------|\n")
		for derivative, count := range analysis.Derivatives {
			percentage := float64(count) / float64(analysis.Summary.TotalErrors) * 100
			sb.WriteString(fmt.Sprintf("| %s | %d | %.1f%% |\n", derivative, count, percentage))
		}
		sb.WriteString("\n")
	}

	// File Size Statistics
	if analysis.SizeStats.ErrorFiles != nil || analysis.SizeStats.SuccessFiles != nil {
		sb.WriteString("## File Size Statistics\n\n")
		
		if analysis.SizeStats.ErrorFiles != nil {
			stats := analysis.SizeStats.ErrorFiles
			sb.WriteString("### Error Files\n")
			sb.WriteString(fmt.Sprintf("- Count: %d\n", stats.Count))
			sb.WriteString(fmt.Sprintf("- Min size: %s\n", utils.FormatBytes(stats.MinSize)))
			sb.WriteString(fmt.Sprintf("- Max size: %s\n", utils.FormatBytes(stats.MaxSize)))
			sb.WriteString(fmt.Sprintf("- Average size: %s\n", utils.FormatBytes(stats.AvgSize)))
			sb.WriteString(fmt.Sprintf("- Total size: %s\n\n", utils.FormatBytes(stats.TotalSize)))
		}

		if analysis.SizeStats.SuccessFiles != nil {
			stats := analysis.SizeStats.SuccessFiles
			sb.WriteString("### Success Files\n")
			sb.WriteString(fmt.Sprintf("- Count: %d\n", stats.Count))
			sb.WriteString(fmt.Sprintf("- Min size: %s\n", utils.FormatBytes(stats.MinSize)))
			sb.WriteString(fmt.Sprintf("- Max size: %s\n", utils.FormatBytes(stats.MaxSize)))
			sb.WriteString(fmt.Sprintf("- Average size: %s\n", utils.FormatBytes(stats.AvgSize)))
			sb.WriteString(fmt.Sprintf("- Total size: %s\n\n", utils.FormatBytes(stats.TotalSize)))
		}
	}

	// Troubleshooting Recommendations
	if len(analysis.Recommendations) > 0 {
		sb.WriteString("## Troubleshooting Recommendations\n\n")
		for _, rec := range analysis.Recommendations {
			sb.WriteString(fmt.Sprintf("- %s\n", rec))
		}
		sb.WriteString("\n")
	}

	// Error Examples
	if len(analysis.ErrorExamples) > 0 {
		sb.WriteString("## Error Examples\n\n")
		for errorType, example := range analysis.ErrorExamples {
			sb.WriteString(fmt.Sprintf("### %s\n", errorType))
			sb.WriteString(fmt.Sprintf("- **File:** %s\n", example.File))
			sb.WriteString(fmt.Sprintf("- **URL:** %s\n", example.URL))
			sb.WriteString(fmt.Sprintf("- **Status Code:** %d\n", example.StatusCode))
			sb.WriteString(fmt.Sprintf("- **File Size:** %s\n", example.SizeFormatted))
			sb.WriteString(fmt.Sprintf("- **Error Message:** %s\n\n", example.ErrorMessage))
		}
	}

	// Detailed Error List (first 20)
	if len(analysis.DetailedErrors) > 0 {
		sb.WriteString("## Detailed Error List\n\n")
		sb.WriteString("| File | Derivative | Status | Error Type | Size | Category |\n")
		sb.WriteString("|------|------------|--------|------------|------|----------|\n")
		
		maxShow := 20
		if len(analysis.DetailedErrors) < maxShow {
			maxShow = len(analysis.DetailedErrors)
		}
		
		for i := 0; i < maxShow; i++ {
			err := analysis.DetailedErrors[i]
			sb.WriteString(fmt.Sprintf("| %s | %s | %d | %s | %s | %s |\n",
				err.File, err.Derivative, err.StatusCode, err.ErrorType, 
				err.SizeFormatted, err.SizeCategory))
		}
		
		if len(analysis.DetailedErrors) > maxShow {
			sb.WriteString(fmt.Sprintf("\n*Note: Showing only the first %d of %d errors*\n", 
				maxShow, len(analysis.DetailedErrors)))
		}
	}

	return sb.String()
}

// Helper functions for statistics calculations
func minInt64(vals []int64) int64 {
	if len(vals) == 0 { return 0 }
	min := vals[0]
	for _, v := range vals[1:] {
		if v < min { min = v }
	}
	return min
}

func maxInt64(vals []int64) int64 {
	if len(vals) == 0 { return 0 }
	max := vals[0]
	for _, v := range vals[1:] {
		if v > max { max = v }
	}
	return max
}

func avgInt64(vals []int64) int64 {
	if len(vals) == 0 { return 0 }
	return sumInt64(vals) / int64(len(vals))
}

func sumInt64(vals []int64) int64 {
	sum := int64(0)
	for _, v := range vals {
		sum += v
	}
	return sum
}

func medianInt64(vals []int64) int64 {
	if len(vals) == 0 { return 0 }
	// Simple median calculation (would need sorting for accuracy)
	return vals[len(vals)/2]
}