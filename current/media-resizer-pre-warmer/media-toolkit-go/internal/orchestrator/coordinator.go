package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"media-toolkit-go/pkg/config"
	"media-toolkit-go/pkg/ffmpeg"
	"media-toolkit-go/pkg/k6"
	"media-toolkit-go/pkg/media"
	"media-toolkit-go/pkg/reporting"
	"media-toolkit-go/pkg/stats"
	"media-toolkit-go/pkg/storage"
	"media-toolkit-go/internal/workers"
	"go.uber.org/zap"
)

// WorkflowType defines the type of workflow to execute
type WorkflowType int

const (
	WorkflowPrewarm   WorkflowType = iota // Pre-warming workflow
	WorkflowOptimize                      // Optimization workflow
	WorkflowValidate                      // Validation workflow
	WorkflowLoadTest                      // Load testing workflow
	WorkflowAnalyze                       // Analysis workflow
	WorkflowFull                          // Full workflow (all stages)
)

// String returns the string representation of a workflow type
func (wt WorkflowType) String() string {
	switch wt {
	case WorkflowPrewarm:
		return "prewarm"
	case WorkflowOptimize:
		return "optimize"
	case WorkflowValidate:
		return "validate"
	case WorkflowLoadTest:
		return "loadtest"
	case WorkflowAnalyze:
		return "analyze"
	case WorkflowFull:
		return "full"
	default:
		return "unknown"
	}
}

// WorkflowConfig configures workflow execution
type WorkflowConfig struct {
	Type                WorkflowType      `json:"type"`
	EnableErrorReporting bool             `json:"enable_error_reporting"`
	EnableLoadTesting   bool              `json:"enable_load_testing"`
	EnableOptimization  bool              `json:"enable_optimization"`
	EnableValidation    bool              `json:"enable_validation"`
	EnableReporting     bool              `json:"enable_reporting"`
	
	// Stage-specific configs
	PrewarmConfig       *PrewarmStageConfig       `json:"prewarm_config,omitempty"`
	OptimizationConfig  *OptimizationStageConfig  `json:"optimization_config,omitempty"`
	ValidationConfig    *ValidationStageConfig    `json:"validation_config,omitempty"`
	AnalysisConfig      *AnalysisStageConfig      `json:"analysis_config,omitempty"`
	LoadTestConfig      *k6.TestConfig            `json:"load_test_config,omitempty"`
	ReportConfig        *reporting.ReportConfig   `json:"report_config,omitempty"`
	StorageConfig       *storage.StorageConfig    `json:"storage_config,omitempty"`
	
	// Global settings
	ContinueOnError     bool              `json:"continue_on_error"`
	MaxRetries          int               `json:"max_retries"`
	RetryDelay          time.Duration     `json:"retry_delay"`
}

// PrewarmStageConfig configures the pre-warming stage
type PrewarmStageConfig struct {
	MediaType        config.MediaType `json:"media_type"`
	ImageVariants    []string         `json:"image_variants,omitempty"`
	VideoDerivatives []string         `json:"video_derivatives,omitempty"`
	URLFormat        string           `json:"url_format,omitempty"`
	UseHeadRequest   bool             `json:"use_head_request"`
	Timeout          time.Duration    `json:"timeout"`
}

// OptimizationStageConfig configures the optimization stage
type OptimizationStageConfig struct {
	InputPath    string                      `json:"input_path"`
	OutputPath   string                      `json:"output_path"`
	FFmpegConfig ffmpeg.OptimizationConfig   `json:"ffmpeg_config"`
	Concurrent   int                         `json:"concurrent"`
}

// ValidationStageConfig configures the validation stage
type ValidationStageConfig struct {
	ValidationPath   string `json:"validation_path"`
	RemoteValidation bool   `json:"remote_validation"`
	Concurrent       int    `json:"concurrent"`
}

// AnalysisStageConfig configures the analysis stage
type AnalysisStageConfig struct {
	AnalysisPath string `json:"analysis_path"`
}

// DefaultWorkflowConfig returns sensible defaults
func DefaultWorkflowConfig() WorkflowConfig {
	return WorkflowConfig{
		Type:                WorkflowPrewarm,
		EnableErrorReporting: true,
		EnableLoadTesting:   false,
		EnableOptimization:  false,
		EnableValidation:    false,
		EnableReporting:     true,
		ContinueOnError:     true,
		MaxRetries:          3,
		RetryDelay:          5 * time.Second,
		PrewarmConfig: &PrewarmStageConfig{
			MediaType:      config.MediaTypeAuto,
			UseHeadRequest: false,
			Timeout:        30 * time.Second,
		},
		ReportConfig: &reporting.ReportConfig{
			OutputDir:       "./reports",
			ReportName:      "workflow-report",
			Format:          []string{"markdown", "json"},
			IncludeSections: []string{"summary", "performance", "errors"},
			Timestamp:       true,
		},
	}
}

// WorkflowResult contains the results of workflow execution
type WorkflowResult struct {
	WorkflowType        WorkflowType                  `json:"workflow_type"`
	Success             bool                          `json:"success"`
	Error               string                        `json:"error,omitempty"`
	StartTime           time.Time                     `json:"start_time"`
	EndTime             time.Time                     `json:"end_time"`
	Duration            time.Duration                 `json:"duration"`
	
	// Stage results
	PrewarmResults      []media.ProcessResult         `json:"prewarm_results,omitempty"`
	OptimizationResults []ffmpeg.OptimizationResult   `json:"optimization_results,omitempty"`
	LoadTestResults     []k6.TestResult               `json:"load_test_results,omitempty"`
	ValidationResults   []ValidationResult            `json:"validation_results,omitempty"`
	
	// Counters
	SuccessCount        int                           `json:"success_count"`
	ErrorCount          int                           `json:"error_count"`
	
	// Analysis data
	AnalysisData        map[string]interface{}        `json:"analysis_data,omitempty"`
	
	// Statistics
	Statistics          *stats.CollectorSummary       `json:"statistics,omitempty"`
	WorkerStats         *workers.WorkerPoolStats      `json:"worker_stats,omitempty"`
	
	// Report paths
	ReportPaths         []string                      `json:"report_paths,omitempty"`
}

// ValidationResult represents validation results
type ValidationResult struct {
	FilePath  string                `json:"file_path"`
	Success   bool                  `json:"success"`
	Error     string                `json:"error,omitempty"`
	Metadata  *ffmpeg.VideoMetadata `json:"metadata,omitempty"`
}

// FileInfo represents file information for processing
type FileInfo struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// Coordinator orchestrates the complete workflow
type Coordinator struct {
	// Core components
	storage       storage.Storage
	mediaProcessor *media.Processor
	workerPool    *workers.WorkerPool
	statsCollector *stats.Collector
	
	// Optional components
	videoOptimizer *ffmpeg.VideoOptimizer
	metadataExtractor *ffmpeg.MetadataExtractor
	k6Runner       *k6.Runner
	reportGenerator *reporting.Generator
	
	// Configuration
	appConfig     *config.Config
	logger        *zap.Logger
	
	// State
	ctx           context.Context
	cancel        context.CancelFunc
	running       bool
	runningMutex  sync.RWMutex
}

// NewCoordinator creates a new workflow coordinator
func NewCoordinator(
	storage storage.Storage,
	mediaProcessor *media.Processor,
	workerPool *workers.WorkerPool,
	statsCollector *stats.Collector,
	appConfig *config.Config,
	logger *zap.Logger,
) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &Coordinator{
		storage:        storage,
		mediaProcessor: mediaProcessor,
		workerPool:     workerPool,
		statsCollector: statsCollector,
		appConfig:      appConfig,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
		
		// Initialize optional components
		videoOptimizer:    ffmpeg.NewVideoOptimizer(logger),
		metadataExtractor: ffmpeg.NewMetadataExtractor(logger),
		k6Runner:          k6.NewRunner(logger),
		reportGenerator:   reporting.NewGenerator(logger),
	}
}

// ExecuteWorkflow executes the specified workflow
func (c *Coordinator) ExecuteWorkflow(workflowConfig WorkflowConfig) (*WorkflowResult, error) {
	c.runningMutex.Lock()
	if c.running {
		c.runningMutex.Unlock()
		return nil, fmt.Errorf("workflow is already running")
	}
	c.running = true
	c.runningMutex.Unlock()
	
	defer func() {
		c.runningMutex.Lock()
		c.running = false
		c.runningMutex.Unlock()
	}()
	
	startTime := time.Now()
	result := &WorkflowResult{
		WorkflowType: workflowConfig.Type,
		StartTime:    startTime,
	}
	
	c.logger.Info("Starting workflow execution",
		zap.String("type", workflowConfig.Type.String()),
		zap.Bool("continue_on_error", workflowConfig.ContinueOnError))
	
	// Start worker pool
	if err := c.workerPool.Start(); err != nil {
		result.Error = fmt.Sprintf("failed to start worker pool: %v", err)
		return result, err
	}
	defer c.workerPool.Shutdown()
	
	// Execute workflow stages based on type
	var err error
	switch workflowConfig.Type {
	case WorkflowPrewarm:
		err = c.executePrewarmWorkflow(workflowConfig, result)
	case WorkflowOptimize:
		err = c.executeOptimizationWorkflow(workflowConfig, result)
	case WorkflowValidate:
		err = c.executeValidationWorkflow(workflowConfig, result)
	case WorkflowLoadTest:
		err = c.executeLoadTestWorkflow(workflowConfig, result)
	case WorkflowAnalyze:
		err = c.executeAnalysisWorkflow(workflowConfig, result)
	case WorkflowFull:
		err = c.executeFullWorkflow(workflowConfig, result)
	default:
		err = fmt.Errorf("unknown workflow type: %v", workflowConfig.Type)
	}
	
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.Success = err == nil
	
	if err != nil {
		result.Error = err.Error()
		c.logger.Error("Workflow execution failed",
			zap.String("type", workflowConfig.Type.String()),
			zap.Error(err),
			zap.Duration("duration", result.Duration))
	} else {
		c.logger.Info("Workflow execution completed successfully",
			zap.String("type", workflowConfig.Type.String()),
			zap.Duration("duration", result.Duration))
	}
	
	// Collect final statistics
	stats := c.statsCollector.GetSummary()
	result.Statistics = &stats
	workerStats := c.workerPool.Statistics()
	result.WorkerStats = &workerStats
	
	// Generate reports if enabled
	if workflowConfig.EnableReporting && workflowConfig.ReportConfig != nil {
		if reportPaths, err := c.generateReports(*workflowConfig.ReportConfig, result); err != nil {
			c.logger.Warn("Failed to generate reports", zap.Error(err))
		} else {
			result.ReportPaths = reportPaths
		}
	}
	
	return result, err
}

// executePrewarmWorkflow executes the pre-warming workflow
func (c *Coordinator) executePrewarmWorkflow(workflowConfig WorkflowConfig, result *WorkflowResult) error {
	c.logger.Info("Executing pre-warm workflow")
	
	// Get list of objects from storage
	listReq := storage.ListRequest{
		Bucket:    c.appConfig.Bucket,
		Directory: c.appConfig.Directory,
		GetSizes:  true,
	}
	
	// Get extensions based on media type and custom extensions
	listReq.Extensions = config.GetExtensionsForMediaType(c.appConfig.MediaTypeString, c.appConfig.Extensions)
	
	if len(listReq.Extensions) > 0 {
		c.logger.Info("Filtering files by extensions", 
			zap.String("media_type", c.appConfig.MediaTypeString),
			zap.Strings("extensions", listReq.Extensions))
	} else {
		c.logger.Info("No extension filtering applied - processing all files")
	}
	
	c.logger.Info("Starting file discovery", 
		zap.String("bucket", listReq.Bucket),
		zap.String("directory", listReq.Directory),
		zap.Bool("get_sizes", listReq.GetSizes),
		zap.Strings("extensions", listReq.Extensions))
	
	objects, err := c.storage.ListObjects(c.ctx, listReq)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}
	
	c.logger.Info("Retrieved objects for processing", zap.Int("count", len(objects)))
	
	// Convert to file metadata
	var fileMetadata []FileInfo
	for _, obj := range objects {
		metadata := FileInfo{
			Path: obj.Path,
			Size: obj.Size,
		}
		fileMetadata = append(fileMetadata, metadata)
	}
	
	// Process files concurrently using worker pool
	processConfig := media.ProcessConfig{
		BaseURL:         c.appConfig.BaseURL,
		MediaType:       workflowConfig.PrewarmConfig.MediaType,
		ImageVariants:   workflowConfig.PrewarmConfig.ImageVariants,
		VideoDerivatives: workflowConfig.PrewarmConfig.VideoDerivatives,
		URLFormat:       workflowConfig.PrewarmConfig.URLFormat,
		Timeout:         workflowConfig.PrewarmConfig.Timeout,
		UseHeadRequest:  workflowConfig.PrewarmConfig.UseHeadRequest,
	}
	
	// Process files in batches
	resultsChan := make(chan *media.ProcessResult, len(fileMetadata))
	errorsChan := make(chan error, len(fileMetadata))
	
	var wg sync.WaitGroup
	for i := range fileMetadata {
		wg.Add(1)
		metadata := &fileMetadata[i]
		
		// Create task for worker pool
		task := &workers.Task{
			ID:         fmt.Sprintf("prewarm-%s", metadata.Path),
			SizeBytes:  metadata.Size,
			Category:   workers.GetSizeCategory(metadata.Size),
			Payload:    metadata,
			ResultChan: make(chan workers.TaskResult, 1),
			ProcessFunc: func(ctx context.Context, payload interface{}) error {
				// Don't use defer in the closure - call wg.Done() directly
				fileInfo := payload.(*FileInfo)
				
				// Create file metadata using the helper function
				configMetadata := config.NewFileMetadata(fileInfo.Path, fileInfo.Size, 50, 300)
				processResult, err := c.mediaProcessor.ProcessMedia(ctx, configMetadata, processConfig)
				
				if err != nil {
					c.statsCollector.RecordError(err.Error())
					if workflowConfig.ContinueOnError {
						// Use non-blocking send to avoid deadlock
						select {
						case errorsChan <- err:
						case <-ctx.Done():
							// Context cancelled, stop processing
						default:
							// Channel full, skip this error
						}
						wg.Done() // Call directly instead of defer
						return nil // Don't fail the task, just record the error
					}
					wg.Done() // Call directly instead of defer
					return err
				}
				
				// Record successful processing
				c.statsCollector.RecordRequest(
					processResult.ProcessingTime,
					processResult.SizeBytes,
					processResult.Success,
				)
				
				// Use non-blocking send to avoid deadlock
				select {
				case resultsChan <- processResult:
				case <-ctx.Done():
					// Context cancelled, stop processing
				default:
					// Channel full, skip this result
				}
				
				wg.Done() // Call directly instead of defer
				return nil
			},
		}
		
		// Submit task to worker pool
		if err := c.workerPool.SubmitTask(task); err != nil {
			wg.Done()
			c.logger.Error("Failed to submit task", zap.Error(err))
			if !workflowConfig.ContinueOnError {
				return fmt.Errorf("failed to submit task: %w", err)
			}
		}
	}
	
	// Wait for all tasks to complete or context cancellation
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	
	select {
	case <-done:
		c.logger.Info("All pre-warm tasks completed")
	case <-c.ctx.Done():
		c.logger.Info("Pre-warm workflow interrupted by context cancellation")
		// Let remaining tasks finish naturally
		wg.Wait()
	}
	
	close(resultsChan)
	close(errorsChan)
	
	// Collect results
	for processResult := range resultsChan {
		result.PrewarmResults = append(result.PrewarmResults, *processResult)
	}
	
	// Log any errors that occurred
	for err := range errorsChan {
		c.logger.Warn("Pre-warm task failed", zap.Error(err))
	}
	
	c.logger.Info("Pre-warm workflow completed",
		zap.Int("processed", len(result.PrewarmResults)),
		zap.Int("total", len(fileMetadata)))
	
	return nil
}

// executeOptimizationWorkflow executes the optimization workflow
func (c *Coordinator) executeOptimizationWorkflow(workflowConfig WorkflowConfig, result *WorkflowResult) error {
	c.logger.Info("Executing optimization workflow")
	
	if workflowConfig.OptimizationConfig == nil {
		return fmt.Errorf("optimization configuration is required")
	}
	
	cfg := *workflowConfig.OptimizationConfig
	
	// Create FFmpeg optimizer
	optimizer := ffmpeg.NewVideoOptimizer(c.logger)
	
	// Create storage client for downloading/uploading
	storageClient, err := storage.NewStorage(workflowConfig.StorageConfig, c.logger)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer storageClient.Close()
	
	// List files to optimize
	objects, err := storageClient.ListObjects(c.ctx, storage.ListRequest{
		Directory: cfg.InputPath,
		GetSizes:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}
	
	// Filter video files
	videoFiles := make([]storage.Object, 0, len(objects))
	for _, obj := range objects {
		if isVideoFile(obj.Path) {
			videoFiles = append(videoFiles, obj)
		}
	}
	
	c.logger.Info("Found video files to optimize", zap.Int("count", len(videoFiles)))
	
	// Process each video file
	for _, file := range videoFiles {
		if err := c.optimizeSingleFile(file, storageClient, optimizer, cfg.FFmpegConfig); err != nil {
			c.logger.Error("Failed to optimize file", 
				zap.String("file", file.Path), 
				zap.Error(err))
			result.ErrorCount++
		} else {
			result.SuccessCount++
		}
	}
	
	return nil
}

// executeValidationWorkflow executes the validation workflow
func (c *Coordinator) executeValidationWorkflow(workflowConfig WorkflowConfig, result *WorkflowResult) error {
	c.logger.Info("Executing validation workflow")
	
	if workflowConfig.ValidationConfig == nil {
		return fmt.Errorf("validation configuration is required")
	}
	
	cfg := *workflowConfig.ValidationConfig
	
	// Create metadata extractor for validation
	extractor := ffmpeg.NewMetadataExtractor(c.logger)
	
	// Create storage client if needed
	var objects []storage.Object
	if cfg.RemoteValidation {
		storageClient, err := storage.NewStorage(workflowConfig.StorageConfig, c.logger)
		if err != nil {
			return fmt.Errorf("failed to create storage client: %w", err)
		}
		defer storageClient.Close()
		
		objects, err = storageClient.ListObjects(c.ctx, storage.ListRequest{
			Directory: cfg.ValidationPath,
			GetSizes:  true,
		})
		if err != nil {
			return fmt.Errorf("failed to list files: %w", err)
		}
	} else {
		// For local validation, we'd scan the local directory
		// This is a simplified implementation
		return fmt.Errorf("local validation not implemented in orchestrator")
	}
	
	// Filter video files
	videoFiles := make([]storage.Object, 0, len(objects))
	for _, obj := range objects {
		if isVideoFile(obj.Path) {
			videoFiles = append(videoFiles, obj)
		}
	}
	
	c.logger.Info("Found video files to validate", zap.Int("count", len(videoFiles)))
	
	// Validate each video file
	for _, file := range videoFiles {
		if err := c.validateSingleFile(file, extractor, cfg); err != nil {
			c.logger.Error("Failed to validate file", 
				zap.String("file", file.Path), 
				zap.Error(err))
			result.ErrorCount++
		} else {
			result.SuccessCount++
		}
	}
	
	return nil
}

// executeLoadTestWorkflow executes the load testing workflow
func (c *Coordinator) executeLoadTestWorkflow(workflowConfig WorkflowConfig, result *WorkflowResult) error {
	c.logger.Info("Executing load test workflow")
	
	if workflowConfig.LoadTestConfig == nil {
		return fmt.Errorf("load test configuration is required")
	}
	
	testResult, err := c.k6Runner.RunTest(c.ctx, *workflowConfig.LoadTestConfig)
	if err != nil {
		return fmt.Errorf("load test failed: %w", err)
	}
	
	result.LoadTestResults = append(result.LoadTestResults, *testResult)
	return nil
}

// executeAnalysisWorkflow executes the analysis workflow
func (c *Coordinator) executeAnalysisWorkflow(workflowConfig WorkflowConfig, result *WorkflowResult) error {
	c.logger.Info("Executing analysis workflow")
	
	if workflowConfig.AnalysisConfig == nil {
		return fmt.Errorf("analysis configuration is required")
	}
	
	cfg := *workflowConfig.AnalysisConfig
	
	// Create storage client
	storageClient, err := storage.NewStorage(workflowConfig.StorageConfig, c.logger)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}
	defer storageClient.Close()
	
	// List files for analysis
	objects, err := storageClient.ListObjects(c.ctx, storage.ListRequest{
		Directory: cfg.AnalysisPath,
		GetSizes:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}
	
	c.logger.Info("Found files for analysis", zap.Int("count", len(objects)))
	
	// Analyze file sizes and distributions
	var totalSize int64
	extensionStats := make(map[string]int)
	
	for _, obj := range objects {
		totalSize += obj.Size
		ext := getFileExtension(obj.Path)
		extensionStats[ext]++
	}
	
	// Store analysis results
	result.AnalysisData = map[string]interface{}{
		"total_files":      len(objects),
		"total_size":       totalSize,
		"extension_stats":  extensionStats,
		"analysis_path":    cfg.AnalysisPath,
	}
	
	result.SuccessCount = len(objects)
	
	c.logger.Info("Analysis completed", 
		zap.Int("files", len(objects)),
		zap.Int64("total_size", totalSize))
	
	return nil
}

// executeFullWorkflow executes the complete workflow with all stages
func (c *Coordinator) executeFullWorkflow(workflowConfig WorkflowConfig, result *WorkflowResult) error {
	c.logger.Info("Executing full workflow")
	
	// Execute pre-warming first
	if err := c.executePrewarmWorkflow(workflowConfig, result); err != nil {
		if !workflowConfig.ContinueOnError {
			return fmt.Errorf("pre-warm stage failed: %w", err)
		}
		c.logger.Error("Pre-warm stage failed, continuing", zap.Error(err))
	}
	
	// Execute optimization if enabled
	if workflowConfig.EnableOptimization && workflowConfig.OptimizationConfig != nil {
		if err := c.executeOptimizationWorkflow(workflowConfig, result); err != nil {
			if !workflowConfig.ContinueOnError {
				return fmt.Errorf("optimization stage failed: %w", err)
			}
			c.logger.Error("Optimization stage failed, continuing", zap.Error(err))
		}
	}
	
	// Execute validation if enabled
	if workflowConfig.EnableValidation {
		if err := c.executeValidationWorkflow(workflowConfig, result); err != nil {
			if !workflowConfig.ContinueOnError {
				return fmt.Errorf("validation stage failed: %w", err)
			}
			c.logger.Error("Validation stage failed, continuing", zap.Error(err))
		}
	}
	
	// Execute load testing if enabled
	if workflowConfig.EnableLoadTesting && workflowConfig.LoadTestConfig != nil {
		if err := c.executeLoadTestWorkflow(workflowConfig, result); err != nil {
			if !workflowConfig.ContinueOnError {
				return fmt.Errorf("load test stage failed: %w", err)
			}
			c.logger.Error("Load test stage failed, continuing", zap.Error(err))
		}
	}
	
	return nil
}

// generateReports generates reports for the workflow results
func (c *Coordinator) generateReports(reportConfig reporting.ReportConfig, result *WorkflowResult) ([]string, error) {
	reportData := reporting.ReportData{
		GeneratedAt:         time.Now(),
		ReportName:          fmt.Sprintf("%s-workflow", result.WorkflowType.String()),
		ProcessingStats:     result.Statistics,
		WorkerStats:         result.WorkerStats,
		OptimizationResults: result.OptimizationResults,
		LoadTestResults:     result.LoadTestResults,
		ErrorAnalysis:       c.reportGenerator.AnalyzeErrors(c.statsCollector),
	}
	
	if err := c.reportGenerator.GenerateReport(reportData, reportConfig); err != nil {
		return nil, err
	}
	
	// Return generated report paths
	var reportPaths []string
	for _, format := range reportConfig.Format {
		var ext string
		switch format {
		case "markdown", "md":
			ext = ".md"
		case "json":
			ext = ".json"
		case "html":
			ext = ".html"
		}
		
		reportName := reportConfig.ReportName
		if reportConfig.Timestamp {
			timestamp := reportData.GeneratedAt.Format("20060102-150405")
			reportName = fmt.Sprintf("%s-%s", reportConfig.ReportName, timestamp)
		}
		
		reportPath := fmt.Sprintf("%s/%s%s", reportConfig.OutputDir, reportName, ext)
		reportPaths = append(reportPaths, reportPath)
	}
	
	return reportPaths, nil
}

// GenerateErrorReport generates an error report from results file
func (c *Coordinator) GenerateErrorReport(ctx context.Context, resultsFile, outputFile string) error {
	c.logger.Info("Generating error report", 
		zap.String("results_file", resultsFile),
		zap.String("output_file", outputFile))
	
	// Use the reporting generator
	generator := reporting.NewGenerator(c.logger)
	
	reportConfig := reporting.ReportConfig{
		Format:          []string{"json"},
		OutputDir:       filepath.Dir(outputFile),
		ReportName:      strings.TrimSuffix(filepath.Base(outputFile), filepath.Ext(outputFile)),
		Timestamp:       false,
		IncludeSections: []string{"errors"},
	}
	
	// Load results data
	data, err := os.ReadFile(resultsFile)
	if err != nil {
		return fmt.Errorf("failed to read results file: %w", err)
	}
	
	var resultsData map[string]interface{}
	if err := json.Unmarshal(data, &resultsData); err != nil {
		return fmt.Errorf("failed to parse results: %w", err)
	}
	
	// Create report data with error analysis
	errorAnalysis := &reporting.ErrorAnalysis{
		TotalErrors:       1, // Simplified - would parse from resultsData
		ErrorsByType:      make(map[string]int64),
		ErrorsByCategory:  make(map[string]int64),
		MostCommonErrors:  []reporting.ErrorFrequency{},
	}
	
	reportData := reporting.ReportData{
		GeneratedAt:   time.Now(),
		ReportName:    reportConfig.ReportName,
		ErrorAnalysis: errorAnalysis,
		Metadata:      map[string]string{"source": resultsFile},
	}
	
	// Generate error report
	return generator.GenerateReport(reportData, reportConfig)
}

// RunComparison runs comparison with KV data
func (c *Coordinator) RunComparison(ctx context.Context, compareFile, outputFile, summaryFile string) error {
	c.logger.Info("Running comparison", 
		zap.String("compare_file", compareFile),
		zap.String("output_file", outputFile),
		zap.String("summary_file", summaryFile))
	
	// Load comparison data
	data, err := os.ReadFile(compareFile)
	if err != nil {
		return fmt.Errorf("failed to read comparison file: %w", err)
	}
	
	var compareData map[string]interface{}
	if err := json.Unmarshal(data, &compareData); err != nil {
		return fmt.Errorf("failed to parse comparison data: %w", err)
	}
	
	// Use the reporting generator for comparison
	generator := reporting.NewGenerator(c.logger)
	
	// Generate comparison report
	reportConfig := reporting.ReportConfig{
		Format:          []string{"json"},
		OutputDir:       filepath.Dir(outputFile),
		ReportName:      strings.TrimSuffix(filepath.Base(outputFile), filepath.Ext(outputFile)),
		Timestamp:       false,
		IncludeSections: []string{"comparison"},
	}
	
	// Create report data
	reportData := reporting.ReportData{
		GeneratedAt: time.Now(),
		ReportName:  reportConfig.ReportName,
		Metadata:    map[string]string{"type": "comparison"},
	}
	
	err = generator.GenerateReport(reportData, reportConfig)
	if err != nil {
		return fmt.Errorf("failed to generate comparison report: %w", err)
	}
	
	// Generate summary if requested
	if summaryFile != "" {
		summaryConfig := reporting.ReportConfig{
			Format:          []string{"markdown"},
			OutputDir:       filepath.Dir(summaryFile),
			ReportName:      strings.TrimSuffix(filepath.Base(summaryFile), filepath.Ext(summaryFile)),
			Timestamp:       false,
			IncludeSections: []string{"summary"},
		}
		
		summaryData := reporting.ReportData{
			GeneratedAt: time.Now(),
			ReportName:  summaryConfig.ReportName,
			Metadata:    map[string]string{"type": "summary"},
		}
		
		err = generator.GenerateReport(summaryData, summaryConfig)
		if err != nil {
			c.logger.Warn("Failed to generate summary report", zap.Error(err))
		}
	}
	
	return nil
}

// optimizeSingleFile optimizes a single video file
func (c *Coordinator) optimizeSingleFile(file storage.Object, storageClient storage.Storage, optimizer *ffmpeg.VideoOptimizer, cfg ffmpeg.OptimizationConfig) error {
	c.logger.Debug("Optimizing file", zap.String("file", file.Path))
	
	// Download file to temporary location
	tempDir, err := os.MkdirTemp("", "optimize-")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)
	
	inputPath := filepath.Join(tempDir, "input"+filepath.Ext(file.Path))
	outputPath := filepath.Join(tempDir, "output"+filepath.Ext(file.Path))
	
	// Download file
	if err := storageClient.DownloadObject(c.ctx, file.Path, inputPath); err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	
	// Optimize file
	result, err := optimizer.OptimizeVideo(c.ctx, inputPath, outputPath, cfg)
	if err != nil {
		return fmt.Errorf("optimization failed: %w", err)
	}
	
	// Upload optimized file
	outputKey := file.Path // Use same name in output location
	if err := storageClient.UploadObject(c.ctx, outputPath, outputKey); err != nil {
		return fmt.Errorf("failed to upload optimized file: %w", err)
	}
	
	c.logger.Debug("File optimized successfully", 
		zap.String("file", file.Path),
		zap.Int64("original_size", result.OriginalSize),
		zap.Int64("optimized_size", result.OptimizedSize),
		zap.Float64("reduction_percent", result.SizeReductionPct))
	
	return nil
}

// validateSingleFile validates a single video file  
func (c *Coordinator) validateSingleFile(file storage.Object, extractor *ffmpeg.MetadataExtractor, cfg ValidationStageConfig) error {
	c.logger.Debug("Validating file", zap.String("file", file.Path))
	
	// For remote files, we'd need to download first or use remote validation
	// This is a simplified implementation
	tempDir, err := os.MkdirTemp("", "validate-")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)
	
	// For now, just validate metadata without downloading the full file
	// In a complete implementation, you'd download the file first
	metadata, err := extractor.ExtractMetadata(c.ctx, file.Path)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	
	if metadata.HasErrors || metadata.IsCorrupted {
		return fmt.Errorf("file validation failed: %v", metadata.ErrorMessages)
	}
	
	c.logger.Debug("File validated successfully", zap.String("file", file.Path))
	return nil
}

// Helper functions
func isVideoFile(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	videoExts := []string{".mp4", ".avi", ".mov", ".mkv", ".wmv", ".flv", ".webm", ".m4v"}
	for _, videoExt := range videoExts {
		if ext == videoExt {
			return true
		}
	}
	return false
}

func getFileExtension(filename string) string {
	ext := filepath.Ext(filename)
	if ext == "" {
		return "no_extension"
	}
	return strings.ToLower(ext)
}

// Shutdown gracefully shuts down the coordinator
func (c *Coordinator) Shutdown() error {
	c.logger.Info("Shutting down workflow coordinator")
	
	c.cancel() // Cancel context
	
	// Shutdown worker pool
	if err := c.workerPool.Shutdown(); err != nil {
		c.logger.Error("Failed to shutdown worker pool", zap.Error(err))
		return err
	}
	
	c.logger.Info("Workflow coordinator shutdown complete")
	return nil
}

// IsRunning returns whether a workflow is currently running
func (c *Coordinator) IsRunning() bool {
	c.runningMutex.RLock()
	defer c.runningMutex.RUnlock()
	return c.running
}