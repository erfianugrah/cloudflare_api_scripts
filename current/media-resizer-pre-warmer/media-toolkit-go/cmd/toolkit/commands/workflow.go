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
)

// WorkflowStage represents a stage in the workflow
type WorkflowStage struct {
	ID          int
	Name        string
	Description string
	Icon        string
	Enabled     bool
	Execute     func(ctx context.Context, cfg *EnhancedWorkflowConfig, logger *zap.Logger) error
}

// EnhancedWorkflowConfig holds comprehensive configuration for the full workflow
type EnhancedWorkflowConfig struct {
	// Core configuration
	Remote     string
	Bucket     string
	Directory  string
	BaseURL    string
	OutputDir  string
	DateSuffix string

	// Media configuration
	MediaType     string
	Extensions    []string
	Derivatives   []string
	ImageVariants []string

	// Pre-warm configuration
	Workers                int
	Timeout                int
	ConnectionCloseDelay   int
	Retry                  int
	OptimizeBySize        bool
	SmallFileWorkers      int
	MediumFileWorkers     int
	LargeFileWorkers      int
	SmallFileThreshold    int
	MediumFileThreshold   int
	UseHeadForSize        bool
	URLFormat             string
	Limit                 int

	// Analysis configuration
	SizeThreshold      int
	AnalysisFormat     string
	GenerateComparison bool
	CompareFile        string

	// Load test configuration
	LoadTestUsers        int
	LoadTestDuration     string
	Stage1Users          int
	Stage1Duration       string
	Stage2Users          int
	Stage2Duration       string
	Stage3Users          int
	Stage3Duration       string
	UseErrorReport       bool
	SkipLargeFiles       bool
	LargeFileThresholdMB int
	RequestTimeout       string

	// Optimization configuration
	OptimizeVideos          bool
	OptimizeThreshold       int
	OptimizeCodec           string
	OptimizeQuality         string
	OptimizeResolution      string
	OptimizeWorkers         int
	HardwareAcceleration    string
	BrowserCompatible       bool

	// Workflow control
	DryRun         bool
	ContinueOnError bool
	ResumeFrom     string
	SkipAnalysis   bool
	SkipPrewarm    bool
	SkipErrors     bool
	SkipLoadTest   bool
	SkipOptimize   bool
	Interactive    bool
	SaveProgress   bool
}

// WorkflowProgress tracks workflow execution progress
type WorkflowProgress struct {
	StartTime      time.Time              `json:"start_time"`
	LastUpdate     time.Time              `json:"last_update"`
	CompletedStages []string              `json:"completed_stages"`
	CurrentStage    string                `json:"current_stage"`
	OutputFiles     map[string]string     `json:"output_files"`
	Configuration   map[string]interface{} `json:"configuration"`
}

// NewEnhancedWorkflowCommand creates the enhanced workflow command
func NewEnhancedWorkflowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Execute the complete media processing workflow",
		Long: `Execute a comprehensive media processing workflow with intelligent orchestration.

The workflow includes:
  1. 🔍 File Analysis     - Analyze media files and generate size reports
  2. 🚀 Cache Pre-warming - Pre-warm CDN cache with all derivatives/variants
  3. 📊 Error Analysis    - Analyze failures and generate detailed reports
  4. ⚡ Load Testing      - Validate performance with realistic traffic
  5. 🎬 Optimization      - Optimize large video files (optional)

Features:
  • Intelligent worker allocation based on file sizes
  • Comprehensive error handling and recovery
  • Progress tracking and resumable execution
  • Detailed reporting at each stage
  • Interactive mode for step-by-step execution`,
		Example: `  # Basic workflow with your configuration
  media-toolkit workflow \
    --remote ikea-mcdc \
    --bucket prod-ap-southeast-1-mcdc-media \
    --directory videos \
    --base-url https://cdn.erfi.dev/videos/ \
    --extensions .mp4,.mov \
    --workers 3000 \
    --timeout 180

  # Dry run to preview actions
  media-toolkit workflow \
    --remote r2 \
    --bucket media \
    --base-url https://cdn.example.com/ \
    --dry-run

  # Resume from a specific stage
  media-toolkit workflow \
    --remote r2 \
    --bucket media \
    --base-url https://cdn.example.com/ \
    --resume-from prewarm

  # High-performance configuration
  media-toolkit workflow \
    --remote r2 \
    --bucket videos \
    --base-url https://cdn.example.com/ \
    --workers 5000 \
    --optimize-by-size \
    --small-file-workers 2000 \
    --medium-file-workers 2000 \
    --large-file-workers 1000 \
    --timeout 300`,
		RunE: runEnhancedWorkflow,
	}

	// Add comprehensive flags
	addEnhancedWorkflowFlags(cmd)

	return cmd
}

func addEnhancedWorkflowFlags(cmd *cobra.Command) {
	// Core flags (required)
	cmd.Flags().String("remote", "", "rclone remote name (required)")
	cmd.Flags().String("bucket", "", "S3 bucket name (required)")
	cmd.Flags().String("base-url", "", "Base URL for HTTP requests (required)")

	// Storage configuration
	cmd.Flags().String("directory", "", "Directory path within bucket")
	cmd.Flags().String("output-dir", ".", "Directory for output files")
	cmd.Flags().Bool("use-aws-cli", false, "Use AWS CLI instead of rclone")

	// Media configuration
	cmd.Flags().String("media-type", "video", "Type of media to process (auto, image, video)")
	cmd.Flags().StringSlice("extensions", []string{}, "File extensions to filter by")
	cmd.Flags().StringSlice("derivatives", []string{"desktop", "tablet", "mobile"}, "Video derivatives")
	cmd.Flags().StringSlice("image-variants", []string{"thumbnail", "small", "medium", "large", "webp"}, "Image variants")

	// Pre-warm configuration
	cmd.Flags().Int("workers", 500, "Number of concurrent workers")
	cmd.Flags().Int("timeout", 240, "Request timeout in seconds")
	cmd.Flags().Int("connection-close-delay", 15, "Connection close delay in seconds")
	cmd.Flags().Int("retry", 2, "Number of retry attempts")
	cmd.Flags().Bool("optimize-by-size", false, "Enable size-based worker optimization")
	cmd.Flags().Int("small-file-workers", 0, "Workers for small files (0=auto)")
	cmd.Flags().Int("medium-file-workers", 0, "Workers for medium files (0=auto)")
	cmd.Flags().Int("large-file-workers", 0, "Workers for large files (0=auto)")
	cmd.Flags().Int("small-file-threshold", 50, "Threshold in MiB for small files")
	cmd.Flags().Int("medium-file-threshold", 200, "Threshold in MiB for medium files")
	cmd.Flags().Bool("use-head-for-size", false, "Use HEAD requests for size verification")
	cmd.Flags().String("url-format", "imwidth", "URL format (imwidth, derivative, query)")
	cmd.Flags().Int("limit", 0, "Limit number of files to process (0=no limit)")

	// Analysis configuration
	cmd.Flags().Int("size-threshold", 100, "Size threshold in MiB for reporting")
	cmd.Flags().String("analysis-format", "markdown", "Analysis report format (markdown, json)")
	cmd.Flags().Bool("generate-comparison", false, "Generate KV comparison if data available")
	cmd.Flags().String("compare-file", "", "KV JSON file for comparison")

	// Load test configuration
	cmd.Flags().Int("loadtest-users", 50, "Default number of users for load testing")
	cmd.Flags().String("loadtest-duration", "2m", "Default duration for load testing")
	cmd.Flags().Int("stage1-users", 0, "Stage 1 users (0=use default)")
	cmd.Flags().String("stage1-duration", "", "Stage 1 duration")
	cmd.Flags().Int("stage2-users", 0, "Stage 2 users")
	cmd.Flags().String("stage2-duration", "", "Stage 2 duration")
	cmd.Flags().Int("stage3-users", 0, "Stage 3 users")
	cmd.Flags().String("stage3-duration", "", "Stage 3 duration")
	cmd.Flags().Bool("skip-large-files", true, "Skip large files in load test")
	cmd.Flags().Int("large-file-threshold-mb", 256, "Threshold for large files in MB")
	cmd.Flags().String("request-timeout", "120s", "Request timeout for load test")

	// Optimization configuration
	cmd.Flags().Bool("optimize-videos", true, "Enable video optimization")
	cmd.Flags().Int("optimize-threshold", 256, "Size threshold in MiB for optimization")
	cmd.Flags().String("optimize-codec", "h264", "Video codec (h264, h265, vp9, av1)")
	cmd.Flags().String("optimize-quality", "balanced", "Quality profile")
	cmd.Flags().String("optimize-resolution", "1080p", "Target resolution")
	cmd.Flags().Int("optimize-workers", 10, "Number of optimization workers")
	cmd.Flags().String("hardware-acceleration", "auto", "Hardware acceleration type")
	cmd.Flags().Bool("browser-compatible", true, "Ensure browser compatibility")

	// Workflow control
	cmd.Flags().Bool("dry-run", false, "Preview workflow without executing")
	cmd.Flags().Bool("continue-on-error", true, "Continue workflow on stage errors")
	cmd.Flags().String("resume-from", "", "Resume from specific stage (analysis, prewarm, errors, loadtest, optimize)")
	cmd.Flags().Bool("skip-analysis", false, "Skip file analysis stage")
	cmd.Flags().Bool("skip-prewarm", false, "Skip pre-warming stage")
	cmd.Flags().Bool("skip-errors", false, "Skip error analysis stage")
	cmd.Flags().Bool("skip-loadtest", false, "Skip load testing stage")
	cmd.Flags().Bool("skip-optimize", false, "Skip optimization stage")
	cmd.Flags().Bool("interactive", false, "Interactive mode with confirmations")
	cmd.Flags().Bool("save-progress", true, "Save workflow progress for resume capability")

	// Mark required flags
	cmd.MarkFlagRequired("remote")
	cmd.MarkFlagRequired("bucket")
	cmd.MarkFlagRequired("base-url")
}

func runEnhancedWorkflow(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Get logger from context
	logger, ok := ctx.Value("logger").(*zap.Logger)
	if !ok || logger == nil {
		return fmt.Errorf("logger not found in context")
	}

	// Bind flags to viper
	viper.BindPFlags(cmd.Flags())

	// Build comprehensive configuration
	cfg := buildEnhancedConfig()

	// Validate configuration
	if err := validateEnhancedConfig(cfg); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Show workflow preview
	if err := showWorkflowPreview(cfg, logger); err != nil {
		return err
	}

	// Check for dry run
	if cfg.DryRun {
		fmt.Println("\n✅ Dry run complete. No actions were performed.")
		return nil
	}

	// Interactive confirmation
	if cfg.Interactive && !confirmWorkflow() {
		fmt.Println("\n❌ Workflow cancelled by user.")
		return nil
	}

	// Load or create progress tracking
	progress, err := loadOrCreateProgress(cfg)
	if err != nil {
		logger.Warn("Failed to load progress", zap.Error(err))
		progress = &WorkflowProgress{
			StartTime:       time.Now(),
			CompletedStages: []string{},
			OutputFiles:     make(map[string]string),
			Configuration:   make(map[string]interface{}),
		}
	}

	// Define workflow stages
	stages := defineWorkflowStages(cfg)

	// Execute workflow
	return executeEnhancedWorkflow(ctx, cfg, stages, progress, logger)
}

func buildEnhancedConfig() *EnhancedWorkflowConfig {
	cfg := &EnhancedWorkflowConfig{
		// Core
		Remote:     viper.GetString("remote"),
		Bucket:     viper.GetString("bucket"),
		Directory:  viper.GetString("directory"),
		BaseURL:    viper.GetString("base-url"),
		OutputDir:  viper.GetString("output-dir"),
		DateSuffix: time.Now().Format("20060102_150405"),

		// Media
		MediaType:     viper.GetString("media-type"),
		Extensions:    viper.GetStringSlice("extensions"),
		Derivatives:   viper.GetStringSlice("derivatives"),
		ImageVariants: viper.GetStringSlice("image-variants"),

		// Pre-warm
		Workers:              viper.GetInt("workers"),
		Timeout:              viper.GetInt("timeout"),
		ConnectionCloseDelay: viper.GetInt("connection-close-delay"),
		Retry:                viper.GetInt("retry"),
		OptimizeBySize:       viper.GetBool("optimize-by-size"),
		SmallFileWorkers:     viper.GetInt("small-file-workers"),
		MediumFileWorkers:    viper.GetInt("medium-file-workers"),
		LargeFileWorkers:     viper.GetInt("large-file-workers"),
		SmallFileThreshold:   viper.GetInt("small-file-threshold"),
		MediumFileThreshold:  viper.GetInt("medium-file-threshold"),
		UseHeadForSize:       viper.GetBool("use-head-for-size"),
		URLFormat:            viper.GetString("url-format"),
		Limit:                viper.GetInt("limit"),

		// Analysis
		SizeThreshold:      viper.GetInt("size-threshold"),
		AnalysisFormat:     viper.GetString("analysis-format"),
		GenerateComparison: viper.GetBool("generate-comparison"),
		CompareFile:        viper.GetString("compare-file"),

		// Load test
		LoadTestUsers:        viper.GetInt("loadtest-users"),
		LoadTestDuration:     viper.GetString("loadtest-duration"),
		Stage1Users:          viper.GetInt("stage1-users"),
		Stage1Duration:       viper.GetString("stage1-duration"),
		Stage2Users:          viper.GetInt("stage2-users"),
		Stage2Duration:       viper.GetString("stage2-duration"),
		Stage3Users:          viper.GetInt("stage3-users"),
		Stage3Duration:       viper.GetString("stage3-duration"),
		UseErrorReport:       true,
		SkipLargeFiles:       viper.GetBool("skip-large-files"),
		LargeFileThresholdMB: viper.GetInt("large-file-threshold-mb"),
		RequestTimeout:       viper.GetString("request-timeout"),

		// Optimization
		OptimizeVideos:       viper.GetBool("optimize-videos"),
		OptimizeThreshold:    viper.GetInt("optimize-threshold"),
		OptimizeCodec:        viper.GetString("optimize-codec"),
		OptimizeQuality:      viper.GetString("optimize-quality"),
		OptimizeResolution:   viper.GetString("optimize-resolution"),
		OptimizeWorkers:      viper.GetInt("optimize-workers"),
		HardwareAcceleration: viper.GetString("hardware-acceleration"),
		BrowserCompatible:    viper.GetBool("browser-compatible"),

		// Workflow control
		DryRun:          viper.GetBool("dry-run"),
		ContinueOnError: viper.GetBool("continue-on-error"),
		ResumeFrom:      viper.GetString("resume-from"),
		SkipAnalysis:    viper.GetBool("skip-analysis"),
		SkipPrewarm:     viper.GetBool("skip-prewarm"),
		SkipErrors:      viper.GetBool("skip-errors"),
		SkipLoadTest:    viper.GetBool("skip-loadtest"),
		SkipOptimize:    viper.GetBool("skip-optimize"),
		Interactive:     viper.GetBool("interactive"),
		SaveProgress:    viper.GetBool("save-progress"),
	}

	// Set default extensions based on media type if not specified
	if len(cfg.Extensions) == 0 {
		switch cfg.MediaType {
		case "video":
			cfg.Extensions = []string{".mp4", ".mov", ".webm", ".avi", ".mkv"}
		case "image":
			cfg.Extensions = []string{".jpg", ".jpeg", ".png", ".webp", ".gif"}
		case "auto", "all":
			cfg.Extensions = []string{".mp4", ".mov", ".webm", ".jpg", ".jpeg", ".png", ".webp"}
		}
	}

	// Set load test stage defaults
	if cfg.Stage1Users == 0 {
		cfg.Stage1Users = cfg.LoadTestUsers / 2
	}
	if cfg.Stage1Duration == "" {
		cfg.Stage1Duration = "1m"
	}
	if cfg.Stage2Users == 0 {
		cfg.Stage2Users = cfg.LoadTestUsers
	}
	if cfg.Stage2Duration == "" {
		cfg.Stage2Duration = cfg.LoadTestDuration
	}
	if cfg.Stage3Users == 0 {
		cfg.Stage3Users = cfg.LoadTestUsers / 2
	}
	if cfg.Stage3Duration == "" {
		cfg.Stage3Duration = "1m"
	}

	return cfg
}

func validateEnhancedConfig(cfg *EnhancedWorkflowConfig) error {
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
	if cfg.Workers < 1 {
		errors = append(errors, "workers must be at least 1")
	}
	if cfg.Timeout < 1 {
		errors = append(errors, "timeout must be at least 1 second")
	}

	if len(errors) > 0 {
		return fmt.Errorf("validation errors:\n  • %s", strings.Join(errors, "\n  • "))
	}

	return nil
}

func showWorkflowPreview(cfg *EnhancedWorkflowConfig, logger *zap.Logger) error {
	fmt.Println("\n🔷 Media Processing Workflow Preview")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	
	// Configuration summary
	fmt.Printf("\n📋 Configuration:\n")
	fmt.Printf("   • Remote: %s\n", cfg.Remote)
	fmt.Printf("   • Bucket: %s\n", cfg.Bucket)
	if cfg.Directory != "" {
		fmt.Printf("   • Directory: %s\n", cfg.Directory)
	}
	fmt.Printf("   • Base URL: %s\n", cfg.BaseURL)
	fmt.Printf("   • Media Type: %s\n", cfg.MediaType)
	fmt.Printf("   • Extensions: %s\n", strings.Join(cfg.Extensions, ", "))
	fmt.Printf("   • Workers: %d\n", cfg.Workers)
	fmt.Printf("   • Timeout: %ds\n", cfg.Timeout)
	
	if cfg.OptimizeBySize {
		fmt.Printf("\n📊 Size-based Worker Allocation:\n")
		fmt.Printf("   • Small files (≤%dMB): %d workers\n", cfg.SmallFileThreshold, cfg.SmallFileWorkers)
		fmt.Printf("   • Medium files (%d-%dMB): %d workers\n", cfg.SmallFileThreshold, cfg.MediumFileThreshold, cfg.MediumFileWorkers)
		fmt.Printf("   • Large files (>%dMB): %d workers\n", cfg.MediumFileThreshold, cfg.LargeFileWorkers)
	}

	// Stages preview
	fmt.Printf("\n📍 Workflow Stages:\n")
	stages := []struct {
		enabled bool
		icon    string
		name    string
		desc    string
	}{
		{!cfg.SkipAnalysis, "🔍", "File Analysis", fmt.Sprintf("Analyze files >%dMB", cfg.SizeThreshold)},
		{!cfg.SkipPrewarm, "🚀", "Cache Pre-warming", fmt.Sprintf("Process %s derivatives", strings.Join(cfg.Derivatives, ", "))},
		{!cfg.SkipErrors && !cfg.SkipPrewarm, "📊", "Error Analysis", "Generate error reports"},
		{!cfg.SkipLoadTest && !cfg.SkipPrewarm, "⚡", "Load Testing", fmt.Sprintf("%d users for %s", cfg.LoadTestUsers, cfg.LoadTestDuration)},
		{!cfg.SkipOptimize, "🎬", "Video Optimization", fmt.Sprintf("Files >%dMB with %s codec", cfg.OptimizeThreshold, cfg.OptimizeCodec)},
	}

	for i, stage := range stages {
		status := "✅ Enabled"
		if !stage.enabled {
			status = "⏭️  Skipped"
		}
		fmt.Printf("   %d. %s %s - %s [%s]\n", i+1, stage.icon, stage.name, stage.desc, status)
	}

	// Output files preview
	fmt.Printf("\n📁 Output Files (in %s):\n", cfg.OutputDir)
	fmt.Printf("   • file_analysis_%s.%s\n", cfg.DateSuffix, cfg.AnalysisFormat)
	fmt.Printf("   • media_transform_results_%s.json\n", cfg.DateSuffix)
	fmt.Printf("   • error_analysis_%s.md\n", cfg.DateSuffix)
	fmt.Printf("   • load_test_results_%s.json\n", cfg.DateSuffix)
	if !cfg.SkipOptimize {
		fmt.Printf("   • optimization_report_%s.json\n", cfg.DateSuffix)
	}

	// Resume information
	if cfg.ResumeFrom != "" {
		fmt.Printf("\n⏩ Resuming from: %s stage\n", cfg.ResumeFrom)
	}

	fmt.Println("\n═══════════════════════════════════════════════════════════════")

	return nil
}

func confirmWorkflow() bool {
	fmt.Print("\n❓ Do you want to proceed with this workflow? (y/N): ")
	var response string
	fmt.Scanln(&response)
	return strings.ToLower(response) == "y" || strings.ToLower(response) == "yes"
}

func defineWorkflowStages(cfg *EnhancedWorkflowConfig) []WorkflowStage {
	return []WorkflowStage{
		{
			ID:          1,
			Name:        "analysis",
			Description: "Analyzing dataset",
			Icon:        "🔍",
			Enabled:     !cfg.SkipAnalysis,
			Execute:     executeEnhancedAnalysisStage,
		},
		{
			ID:          2,
			Name:        "prewarm",
			Description: fmt.Sprintf("Pre-warming cache (%d workers)", cfg.Workers),
			Icon:        "🚀",
			Enabled:     !cfg.SkipPrewarm,
			Execute:     executeEnhancedPrewarmStage,
		},
		{
			ID:          3,
			Name:        "errors",
			Description: "Analyzing results and errors",
			Icon:        "📊",
			Enabled:     !cfg.SkipErrors && !cfg.SkipPrewarm,
			Execute:     executeEnhancedErrorAnalysisStage,
		},
		{
			ID:          4,
			Name:        "loadtest",
			Description: fmt.Sprintf("Load testing (%d users)", cfg.LoadTestUsers),
			Icon:        "⚡",
			Enabled:     !cfg.SkipLoadTest && !cfg.SkipPrewarm,
			Execute:     executeEnhancedLoadTestStage,
		},
		{
			ID:          5,
			Name:        "optimize",
			Description: "Optimizing large videos",
			Icon:        "🎬",
			Enabled:     !cfg.SkipOptimize && cfg.OptimizeVideos,
			Execute:     executeEnhancedOptimizationStage,
		},
	}
}

func executeEnhancedWorkflow(ctx context.Context, cfg *EnhancedWorkflowConfig, stages []WorkflowStage, progress *WorkflowProgress, logger *zap.Logger) error {
	startTime := time.Now()
	var stageResults []string
	skipUntil := cfg.ResumeFrom

	fmt.Println("\n🚀 Starting Media Processing Workflow")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	for _, stage := range stages {
		// Handle resume logic
		if skipUntil != "" && stage.Name != skipUntil {
			if isStageCompleted(stage.Name, progress) {
				fmt.Printf("\n%s Stage %d: %s - Already completed ✓\n", stage.Icon, stage.ID, stage.Description)
				continue
			}
			fmt.Printf("\n%s Stage %d: %s - Skipping (resuming from %s)\n", stage.Icon, stage.ID, stage.Description, skipUntil)
			continue
		}
		skipUntil = "" // Found resume point

		if !stage.Enabled {
			fmt.Printf("\n⏭️  Stage %d: %s - Skipped by configuration\n", stage.ID, stage.Description)
			continue
		}

		// Stage header
		fmt.Printf("\n%s Stage %d: %s\n", stage.Icon, stage.ID, stage.Description)
		fmt.Println("───────────────────────────────────────────────────────────────")

		// Update progress
		progress.CurrentStage = stage.Name
		progress.LastUpdate = time.Now()
		if cfg.SaveProgress {
			saveProgress(progress, cfg.OutputDir)
		}

		// Execute stage
		stageStart := time.Now()
		if err := stage.Execute(ctx, cfg, logger); err != nil {
			stageResults = append(stageResults, fmt.Sprintf("❌ %s: Failed - %v", stage.Name, err))
			
			logger.Error("Stage failed",
				zap.String("stage", stage.Name),
				zap.Error(err))
			
			if !cfg.ContinueOnError {
				fmt.Printf("\n❌ Stage failed: %v\n", err)
				fmt.Println("💡 Tip: Use --continue-on-error to proceed despite failures")
				return fmt.Errorf("stage %s failed: %w", stage.Name, err)
			}
			
			fmt.Printf("\n⚠️  Stage failed but continuing: %v\n", err)
		} else {
			stageDuration := time.Since(stageStart)
			stageResults = append(stageResults, fmt.Sprintf("✅ %s: Completed in %s", stage.Name, formatDuration(stageDuration)))
			
			// Mark stage as completed
			progress.CompletedStages = append(progress.CompletedStages, stage.Name)
			if cfg.SaveProgress {
				saveProgress(progress, cfg.OutputDir)
			}
			
			fmt.Printf("\n✅ Stage completed in %s\n", formatDuration(stageDuration))
		}
	}

	// Final summary
	totalDuration := time.Since(startTime)
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 Workflow Summary")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("\nTotal Duration: %s\n", formatDuration(totalDuration))
	fmt.Println("\nStage Results:")
	for i, result := range stageResults {
		fmt.Printf("  %d. %s\n", i+1, result)
	}

	// Output files summary
	fmt.Printf("\n📁 Output Files in: %s\n", cfg.OutputDir)
	fmt.Println("───────────────────────────────────────────────────────────────")
	outputFiles := []struct {
		condition bool
		file      string
		desc      string
	}{
		{!cfg.SkipAnalysis, fmt.Sprintf("file_analysis_%s.%s", cfg.DateSuffix, cfg.AnalysisFormat), "File size analysis"},
		{!cfg.SkipPrewarm, fmt.Sprintf("media_transform_results_%s.json", cfg.DateSuffix), "Pre-warming results"},
		{!cfg.SkipErrors && !cfg.SkipPrewarm, fmt.Sprintf("error_analysis_%s.md", cfg.DateSuffix), "Error analysis report"},
		{!cfg.SkipLoadTest && !cfg.SkipPrewarm, fmt.Sprintf("load_test_results_%s.json", cfg.DateSuffix), "Load test results"},
		{!cfg.SkipOptimize && cfg.OptimizeVideos, fmt.Sprintf("optimization_report_%s.json", cfg.DateSuffix), "Optimization report"},
	}

	for _, output := range outputFiles {
		if output.condition {
			fmt.Printf("  • %-40s - %s\n", output.file, output.desc)
		}
	}

	fmt.Println("\n✅ Workflow completed successfully!")
	
	// Clean up progress file on success
	if cfg.SaveProgress {
		cleanupProgress(cfg.OutputDir)
	}

	return nil
}

// Stage execution functions
func executeEnhancedAnalysisStage(ctx context.Context, cfg *EnhancedWorkflowConfig, logger *zap.Logger) error {
	cmd := NewAnalyzeCommand()
	cmd.SetContext(ctx)

	args := buildAnalysisArgs(cfg)
	if err := cmd.ParseFlags(args); err != nil {
		return fmt.Errorf("failed to parse analysis flags: %w", err)
	}

	return cmd.RunE(cmd, []string{})
}

func executeEnhancedPrewarmStage(ctx context.Context, cfg *EnhancedWorkflowConfig, logger *zap.Logger) error {
	cmd := NewPrewarmCommand()
	cmd.SetContext(ctx)

	args := buildPrewarmArgs(cfg)
	if err := cmd.ParseFlags(args); err != nil {
		return fmt.Errorf("failed to parse prewarm flags: %w", err)
	}

	return cmd.RunE(cmd, []string{})
}

func executeEnhancedErrorAnalysisStage(ctx context.Context, cfg *EnhancedWorkflowConfig, logger *zap.Logger) error {
	cmd := NewAnalyzeCommand()
	cmd.SetContext(ctx)

	args := []string{
		"--generate-error-report",
		"--results-file", filepath.Join(cfg.OutputDir, fmt.Sprintf("media_transform_results_%s.json", cfg.DateSuffix)),
		"--error-report-output", filepath.Join(cfg.OutputDir, fmt.Sprintf("error_analysis_%s.md", cfg.DateSuffix)),
		"--format", "markdown",
	}

	if err := cmd.ParseFlags(args); err != nil {
		return fmt.Errorf("failed to parse error analysis flags: %w", err)
	}

	return cmd.RunE(cmd, []string{})
}

func executeEnhancedLoadTestStage(ctx context.Context, cfg *EnhancedWorkflowConfig, logger *zap.Logger) error {
	cmd := NewLoadTestCommand()
	cmd.SetContext(ctx)

	args := buildLoadTestArgs(cfg)
	if err := cmd.ParseFlags(args); err != nil {
		return fmt.Errorf("failed to parse loadtest flags: %w", err)
	}

	return cmd.RunE(cmd, []string{})
}

func executeEnhancedOptimizationStage(ctx context.Context, cfg *EnhancedWorkflowConfig, logger *zap.Logger) error {
	cmd := NewOptimizeCommand()
	cmd.SetContext(ctx)

	args := buildOptimizationArgs(cfg)
	if err := cmd.ParseFlags(args); err != nil {
		return fmt.Errorf("failed to parse optimize flags: %w", err)
	}

	return cmd.RunE(cmd, []string{})
}

// Helper functions
func buildAnalysisArgs(cfg *EnhancedWorkflowConfig) []string {
	args := []string{
		"--remote", cfg.Remote,
		"--bucket", cfg.Bucket,
		"--list-files",
		"--size-threshold", fmt.Sprintf("%d", cfg.SizeThreshold),
		"--size-report-output", filepath.Join(cfg.OutputDir, fmt.Sprintf("file_analysis_%s.%s", cfg.DateSuffix, cfg.AnalysisFormat)),
	}

	if cfg.Directory != "" {
		args = append(args, "--directory", cfg.Directory)
	}

	for _, ext := range cfg.Extensions {
		args = append(args, "--extensions", ext)
	}

	if cfg.Limit > 0 {
		args = append(args, "--limit", fmt.Sprintf("%d", cfg.Limit))
	}

	return args
}

func buildPrewarmArgs(cfg *EnhancedWorkflowConfig) []string {
	args := []string{
		"--remote", cfg.Remote,
		"--bucket", cfg.Bucket,
		"--base-url", cfg.BaseURL,
		"--media-type", cfg.MediaType,
		"--workers", fmt.Sprintf("%d", cfg.Workers),
		"--timeout", fmt.Sprintf("%d", cfg.Timeout),
		"--connection-close-delay", fmt.Sprintf("%d", cfg.ConnectionCloseDelay),
		"--retry", fmt.Sprintf("%d", cfg.Retry),
		"--output", filepath.Join(cfg.OutputDir, fmt.Sprintf("media_transform_results_%s.json", cfg.DateSuffix)),
		"--performance-report", filepath.Join(cfg.OutputDir, fmt.Sprintf("performance_report_%s.md", cfg.DateSuffix)),
	}

	if cfg.Directory != "" {
		args = append(args, "--directory", cfg.Directory)
	}

	for _, ext := range cfg.Extensions {
		args = append(args, "--extensions", ext)
	}

	for _, deriv := range cfg.Derivatives {
		args = append(args, "--derivatives", deriv)
	}

	for _, variant := range cfg.ImageVariants {
		args = append(args, "--image-variants", variant)
	}

	if cfg.OptimizeBySize {
		args = append(args, "--optimize-by-size")
		if cfg.SmallFileWorkers > 0 {
			args = append(args, "--small-file-workers", fmt.Sprintf("%d", cfg.SmallFileWorkers))
		}
		if cfg.MediumFileWorkers > 0 {
			args = append(args, "--medium-file-workers", fmt.Sprintf("%d", cfg.MediumFileWorkers))
		}
		if cfg.LargeFileWorkers > 0 {
			args = append(args, "--large-file-workers", fmt.Sprintf("%d", cfg.LargeFileWorkers))
		}
		args = append(args, 
			"--small-file-threshold", fmt.Sprintf("%d", cfg.SmallFileThreshold),
			"--medium-file-threshold", fmt.Sprintf("%d", cfg.MediumFileThreshold))
	}

	if cfg.UseHeadForSize {
		args = append(args, "--use-head-for-size")
	}

	if cfg.URLFormat != "" {
		args = append(args, "--url-format", cfg.URLFormat)
	}

	if cfg.Limit > 0 {
		args = append(args, "--limit", fmt.Sprintf("%d", cfg.Limit))
	}

	return args
}

func buildLoadTestArgs(cfg *EnhancedWorkflowConfig) []string {
	args := []string{
		"--base-url", cfg.BaseURL,
		"--results-file", filepath.Join(cfg.OutputDir, fmt.Sprintf("media_transform_results_%s.json", cfg.DateSuffix)),
		"--output-file", filepath.Join(cfg.OutputDir, fmt.Sprintf("load_test_results_%s.json", cfg.DateSuffix)),
		"--stage1-users", fmt.Sprintf("%d", cfg.Stage1Users),
		"--stage1-duration", cfg.Stage1Duration,
		"--stage2-users", fmt.Sprintf("%d", cfg.Stage2Users),
		"--stage2-duration", cfg.Stage2Duration,
		"--stage3-users", fmt.Sprintf("%d", cfg.Stage3Users),
		"--stage3-duration", cfg.Stage3Duration,
		"--request-timeout", cfg.RequestTimeout,
	}

	if cfg.UseErrorReport {
		args = append(args, 
			"--use-error-report",
			"--error-report-file", filepath.Join(cfg.OutputDir, fmt.Sprintf("error_analysis_%s.md", cfg.DateSuffix)))
	}

	if cfg.SkipLargeFiles {
		args = append(args, 
			"--skip-large-files",
			"--large-file-threshold-mb", fmt.Sprintf("%d", cfg.LargeFileThresholdMB))
	}

	return args
}

func buildOptimizationArgs(cfg *EnhancedWorkflowConfig) []string {
	args := []string{
		"--remote", cfg.Remote,
		"--bucket", cfg.Bucket,
		"--optimize-videos",
		"--codec", cfg.OptimizeCodec,
		"--quality", cfg.OptimizeQuality,
		"--target-resolution", cfg.OptimizeResolution,
		"--size-threshold", fmt.Sprintf("%d", cfg.OptimizeThreshold),
		"--workers", fmt.Sprintf("%d", cfg.OptimizeWorkers),
		"--hardware-acceleration", cfg.HardwareAcceleration,
	}

	if cfg.Directory != "" {
		args = append(args, "--directory", cfg.Directory)
	}

	if cfg.BrowserCompatible {
		args = append(args, "--browser-compatible")
	}

	return args
}

// Progress tracking functions
func loadOrCreateProgress(cfg *EnhancedWorkflowConfig) (*WorkflowProgress, error) {
	progressFile := filepath.Join(cfg.OutputDir, ".workflow_progress.json")
	
	data, err := os.ReadFile(progressFile)
	if err != nil {
		return nil, err
	}

	var progress WorkflowProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, err
	}

	return &progress, nil
}

func saveProgress(progress *WorkflowProgress, outputDir string) error {
	progressFile := filepath.Join(outputDir, ".workflow_progress.json")
	
	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(progressFile, data, 0644)
}

func cleanupProgress(outputDir string) {
	progressFile := filepath.Join(outputDir, ".workflow_progress.json")
	os.Remove(progressFile)
}

func isStageCompleted(stageName string, progress *WorkflowProgress) bool {
	for _, completed := range progress.CompletedStages {
		if completed == stageName {
			return true
		}
	}
	return false
}

// Utility functions
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh%dm", hours, minutes)
}