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
	cmd.Flags().String("extension", "", "File extension to filter by")
	cmd.Flags().StringSlice("image-extensions", []string{".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg"}, "Image extensions")
	cmd.Flags().StringSlice("video-extensions", []string{".mp4", ".webm", ".mov", ".avi", ".mkv", ".m4v"}, "Video extensions")
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
	if !ok {
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
	Extension          string
	ImageExtensions    []string
	VideoExtensions    []string
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
}

// FileAnalysis represents analysis results for files
type FileAnalysis struct {
	TotalFiles     int                    `json:"total_files"`
	TotalSize      int64                  `json:"total_size"`
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
		Extension:           viper.GetString("extension"),
		ImageExtensions:     viper.GetStringSlice("image-extensions"),
		VideoExtensions:     viper.GetStringSlice("video-extensions"),
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

	objects, err := storageClient.ListObjects(ctx, storage.ListRequest{
		Directory: cfg.Directory,
		GetSizes:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// Filter by extension if specified
	if cfg.Extension != "" {
		filtered := make([]storage.FileInfo, 0, len(objects))
		for _, obj := range objects {
			if strings.HasSuffix(strings.ToLower(obj.Path), strings.ToLower(cfg.Extension)) {
				filtered = append(filtered, obj)
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

	// Simple error report generation - read results and write summary
	data, err := os.ReadFile(cfg.ResultsFile)
	if err != nil {
		return fmt.Errorf("failed to read results file: %w", err)
	}

	// Write the error report (simplified implementation)
	return os.WriteFile(cfg.ErrorReportOutput, data, 0644)
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