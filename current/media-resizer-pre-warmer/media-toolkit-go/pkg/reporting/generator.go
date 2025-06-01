package reporting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"media-toolkit-go/pkg/ffmpeg"
	"media-toolkit-go/pkg/loadtest"
	"media-toolkit-go/pkg/stats"
	"media-toolkit-go/internal/workers"
	"go.uber.org/zap"
)

// ReportConfig configures report generation
type ReportConfig struct {
	OutputDir       string            `json:"output_dir"`
	ReportName      string            `json:"report_name"`
	Format          []string          `json:"format"` // markdown, json, html
	IncludeSections []string          `json:"include_sections"`
	Timestamp       bool              `json:"timestamp"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// DefaultReportConfig returns sensible defaults
func DefaultReportConfig() ReportConfig {
	return ReportConfig{
		OutputDir:       "./reports",
		ReportName:      "media-processing-report",
		Format:          []string{"markdown", "json"},
		IncludeSections: []string{"summary", "performance", "errors", "optimization", "load-test"},
		Timestamp:       true,
		Metadata:        make(map[string]string),
	}
}

// ReportData contains all data for report generation
type ReportData struct {
	// Report metadata
	GeneratedAt     time.Time         `json:"generated_at"`
	ReportName      string            `json:"report_name"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	
	// Processing statistics
	ProcessingStats *stats.CollectorSummary `json:"processing_stats,omitempty"`
	WorkerStats     *workers.WorkerPoolStats `json:"worker_stats,omitempty"`
	
	// Optimization results
	OptimizationResults []ffmpeg.OptimizationResult `json:"optimization_results,omitempty"`
	
	// Load test results
	LoadTestResults []loadtest.Result `json:"load_test_results,omitempty"`
	
	// Error analysis
	ErrorAnalysis   *ErrorAnalysis   `json:"error_analysis,omitempty"`
	
	// Performance summary
	PerformanceSummary *PerformanceSummary `json:"performance_summary,omitempty"`
}

// ErrorAnalysis contains error analysis data
type ErrorAnalysis struct {
	TotalErrors       int64                    `json:"total_errors"`
	ErrorsByType      map[string]int64         `json:"errors_by_type"`
	ErrorsByCategory  map[string]int64         `json:"errors_by_category"`
	MostCommonErrors  []ErrorFrequency         `json:"most_common_errors"`
	ErrorRate         float64                  `json:"error_rate"`
	CriticalErrors    []string                 `json:"critical_errors,omitempty"`
}

// ErrorFrequency represents error frequency data
type ErrorFrequency struct {
	Error     string `json:"error"`
	Count     int64  `json:"count"`
	Frequency float64 `json:"frequency"`
}

// PerformanceSummary contains performance analysis
type PerformanceSummary struct {
	// Processing performance
	TotalProcessingTime   time.Duration `json:"total_processing_time"`
	AverageProcessingTime time.Duration `json:"average_processing_time"`
	ProcessingThroughput  float64       `json:"processing_throughput"` // items per second
	
	// Network performance
	AverageResponseTime   time.Duration `json:"average_response_time"`
	TotalDataTransferred  int64         `json:"total_data_transferred"`
	NetworkThroughput     float64       `json:"network_throughput"` // bytes per second
	
	// Resource utilization
	WorkerUtilization     float64       `json:"worker_utilization"`
	QueueUtilization      float64       `json:"queue_utilization"`
	
	// Optimization performance
	TotalSizeSaved        int64         `json:"total_size_saved"`
	AverageCompressionRatio float64     `json:"average_compression_ratio"`
	OptimizationEfficiency float64      `json:"optimization_efficiency"`
}

// Generator handles report generation
type Generator struct {
	logger *zap.Logger
}

// NewGenerator creates a new report generator
func NewGenerator(logger *zap.Logger) *Generator {
	return &Generator{
		logger: logger,
	}
}

// GenerateReport generates a comprehensive report from the provided data
func (g *Generator) GenerateReport(data ReportData, config ReportConfig) error {
	g.logger.Info("Generating report",
		zap.String("name", config.ReportName),
		zap.Strings("formats", config.Format),
		zap.String("output_dir", config.OutputDir))
	
	// Ensure output directory exists
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	
	// Add timestamp to report name if requested
	reportName := config.ReportName
	if config.Timestamp {
		timestamp := data.GeneratedAt.Format("20060102-150405")
		reportName = fmt.Sprintf("%s-%s", config.ReportName, timestamp)
	}
	
	// Generate each requested format
	for _, format := range config.Format {
		var err error
		switch strings.ToLower(format) {
		case "markdown", "md":
			err = g.generateMarkdown(data, config, reportName)
		case "json":
			err = g.generateJSON(data, config, reportName)
		case "html":
			err = g.generateHTML(data, config, reportName)
		default:
			g.logger.Warn("Unknown report format", zap.String("format", format))
			continue
		}
		
		if err != nil {
			g.logger.Error("Failed to generate report format",
				zap.String("format", format),
				zap.Error(err))
			return err
		}
	}
	
	g.logger.Info("Report generation completed",
		zap.String("name", reportName),
		zap.String("output_dir", config.OutputDir))
	
	return nil
}

// generateMarkdown generates a Markdown report
func (g *Generator) generateMarkdown(data ReportData, config ReportConfig, reportName string) error {
	tmpl := `# {{.ReportName}} Report

**Generated:** {{.GeneratedAt.Format "2006-01-02 15:04:05 UTC"}}

{{if .Metadata}}
## Metadata
{{range $key, $value := .Metadata}}
- **{{$key}}:** {{$value}}
{{end}}
{{end}}

{{if and .ProcessingStats (contains .IncludeSections "summary")}}
## Summary

### Processing Overview
- **Total Requests:** {{.ProcessingStats.TotalRequests}}
- **Successful Requests:** {{.ProcessingStats.SuccessfulReqs}}
- **Failed Requests:** {{.ProcessingStats.FailedRequests}}
- **Success Rate:** {{printf "%.2f%%" .ProcessingStats.SuccessRate}}
- **Requests per Second:** {{printf "%.2f" .ProcessingStats.RequestsPerSecond}}
- **Elapsed Time:** {{.ProcessingStats.ElapsedTime}}

{{if .ProcessingStats.SizeReduction}}
### Size Reduction Summary
- **Files Processed:** {{.ProcessingStats.SizeReduction.FilesProcessed}}
- **Bytes Saved:** {{humanizeBytes .ProcessingStats.SizeReduction.BytesSaved}}
- **Average Reduction:** {{printf "%.2f%%" .ProcessingStats.SizeReduction.AverageReductionPercent}}
- **Overall Reduction:** {{printf "%.2f%%" .ProcessingStats.SizeReduction.OverallReductionPercent}}
{{end}}
{{end}}

{{if and .ProcessingStats (contains .IncludeSections "performance")}}
## Performance Metrics

### Response Time Statistics
- **Average:** {{.ProcessingStats.RequestTimeStats.Mean | printf "%.2f"}}ms
- **Minimum:** {{.ProcessingStats.RequestTimeStats.Min | printf "%.2f"}}ms
- **Maximum:** {{.ProcessingStats.RequestTimeStats.Max | printf "%.2f"}}ms
- **Standard Deviation:** {{.ProcessingStats.RequestTimeStats.StdDev | printf "%.2f"}}ms

### Data Transfer
- **Response Size (avg):** {{humanizeBytes (int64 .ProcessingStats.ResponseSizeStats.Mean)}}
- **Response Size (total):** {{humanizeBytes (int64 .ProcessingStats.ResponseSizeStats.Sum)}}

{{if .WorkerStats}}
### Worker Pool Statistics
- **Tasks Submitted:** {{.WorkerStats.TasksSubmitted}}
- **Tasks Completed:** {{.WorkerStats.TasksCompleted}}
- **Tasks Failed:** {{.WorkerStats.TasksFailed}}

#### Small Files Pool
- **Workers:** {{.WorkerStats.SmallPool.WorkerCount}}
- **Active:** {{.WorkerStats.SmallPool.ActiveWorkers}}
- **Queue Size:** {{.WorkerStats.SmallPool.QueueSize}}/{{.WorkerStats.SmallPool.QueueCapacity}}

#### Medium Files Pool
- **Workers:** {{.WorkerStats.MediumPool.WorkerCount}}
- **Active:** {{.WorkerStats.MediumPool.ActiveWorkers}}
- **Queue Size:** {{.WorkerStats.MediumPool.QueueSize}}/{{.WorkerStats.MediumPool.QueueCapacity}}

#### Large Files Pool
- **Workers:** {{.WorkerStats.LargePool.WorkerCount}}
- **Active:** {{.WorkerStats.LargePool.ActiveWorkers}}
- **Queue Size:** {{.WorkerStats.LargePool.QueueSize}}/{{.WorkerStats.LargePool.QueueCapacity}}
{{end}}
{{end}}

{{if and .ErrorAnalysis (contains .IncludeSections "errors")}}
## Error Analysis

### Error Summary
- **Total Errors:** {{.ErrorAnalysis.TotalErrors}}
- **Error Rate:** {{printf "%.2f%%" .ErrorAnalysis.ErrorRate}}

{{if .ErrorAnalysis.MostCommonErrors}}
### Most Common Errors
{{range .ErrorAnalysis.MostCommonErrors}}
- **{{.Error}}** ({{.Count}} occurrences, {{printf "%.2f%%" .Frequency}})
{{end}}
{{end}}

{{if .ErrorAnalysis.ErrorsByCategory}}
### Errors by Category
{{range $category, $count := .ErrorAnalysis.ErrorsByCategory}}
- **{{$category}}:** {{$count}}
{{end}}
{{end}}

{{if .ErrorAnalysis.CriticalErrors}}
### Critical Errors
{{range .ErrorAnalysis.CriticalErrors}}
- {{.}}
{{end}}
{{end}}
{{end}}

{{if and .OptimizationResults (contains .IncludeSections "optimization")}}
## Optimization Results

{{if .OptimizationResults}}
### Optimization Summary
{{range .OptimizationResults}}
#### {{.InputFile}}
- **Success:** {{.Success}}
- **Original Size:** {{humanizeBytes .OriginalSize}}
- **Optimized Size:** {{humanizeBytes .OptimizedSize}}
- **Size Reduction:** {{printf "%.2f%%" .SizeReductionPct}}
- **Encoding Time:** {{.EncodingTime}}
- **Encoding Speed:** {{printf "%.2fx" .EncodingSpeed}}
{{if .Error}}- **Error:** {{.Error}}{{end}}

{{end}}
{{end}}
{{end}}

{{if and .LoadTestResults (contains .IncludeSections "load-test")}}
## Load Test Results

{{range .LoadTestResults}}
### Load Test: {{.Config.BaseURL}}
- **Virtual Users:** {{.Config.VirtualUsers}}
- **Duration:** {{.Config.Duration}}
- **Success:** {{.Success}}
{{if .Error}}- **Error:** {{.Error}}{{end}}

#### Performance Metrics
- **Total Requests:** {{.Metrics.HTTPRequestsTotal}}
- **Success Rate:** {{printf "%.2f%%" .Metrics.SuccessRate}}
- **Requests/sec:** {{printf "%.2f" .Metrics.HTTPRequestsRate}}
- **Avg Response Time:** {{.Metrics.HTTPReqDurationAvg}}
- **95th Percentile:** {{.Metrics.HTTPReqDurationP95}}
- **Data Received:** {{humanizeBytes .Metrics.DataReceived}}

{{end}}
{{end}}

---
*Report generated by Media Toolkit Go at {{.GeneratedAt.Format "2006-01-02 15:04:05 UTC"}}*
`

	// Parse template
	t, err := template.New("markdown").Funcs(template.FuncMap{
		"contains":      contains,
		"humanizeBytes": humanizeBytes,
		"int64": func(f float64) int64 {
			return int64(f)
		},
	}).Parse(tmpl)
	if err != nil {
		return fmt.Errorf("failed to parse markdown template: %w", err)
	}

	// Prepare template data
	templateData := struct {
		ReportData
		IncludeSections []string
	}{
		ReportData:      data,
		IncludeSections: config.IncludeSections,
	}
	
	// Execute template
	var buf bytes.Buffer
	if err := t.Execute(&buf, templateData); err != nil {
		return fmt.Errorf("failed to execute markdown template: %w", err)
	}
	
	// Write to file
	outputPath := filepath.Join(config.OutputDir, reportName+".md")
	if err := os.WriteFile(outputPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write markdown report: %w", err)
	}
	
	g.logger.Debug("Generated markdown report", zap.String("path", outputPath))
	return nil
}

// generateJSON generates a JSON report
func (g *Generator) generateJSON(data ReportData, config ReportConfig, reportName string) error {
	// Marshal data to JSON with indentation
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	
	// Write to file
	outputPath := filepath.Join(config.OutputDir, reportName+".json")
	if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write JSON report: %w", err)
	}
	
	g.logger.Debug("Generated JSON report", zap.String("path", outputPath))
	return nil
}

// generateHTML generates an HTML report
func (g *Generator) generateHTML(data ReportData, config ReportConfig, reportName string) error {
	htmlTemplate := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{.ReportName}} Report</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; margin: 40px; line-height: 1.6; }
        .header { border-bottom: 2px solid #333; padding-bottom: 20px; margin-bottom: 30px; }
        .section { margin-bottom: 30px; }
        .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0; }
        .metric-card { background: #f5f5f5; padding: 15px; border-radius: 8px; border-left: 4px solid #007acc; }
        .metric-label { font-weight: bold; color: #666; }
        .metric-value { font-size: 1.2em; color: #333; }
        .error { color: #d32f2f; }
        .success { color: #388e3c; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f5f5f5; font-weight: bold; }
        .progress-bar { width: 100%; height: 20px; background: #f0f0f0; border-radius: 10px; overflow: hidden; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #4caf50, #81c784); }
    </style>
</head>
<body>
    <div class="header">
        <h1>{{.ReportName}} Report</h1>
        <p><strong>Generated:</strong> {{.GeneratedAt.Format "2006-01-02 15:04:05 UTC"}}</p>
    </div>

    {{if .ProcessingStats}}
    <div class="section">
        <h2>Processing Summary</h2>
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-label">Total Requests</div>
                <div class="metric-value">{{.ProcessingStats.TotalRequests}}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Success Rate</div>
                <div class="metric-value {{if gt .ProcessingStats.SuccessRate 95.0}}success{{else}}error{{end}}">
                    {{printf "%.2f%%" .ProcessingStats.SuccessRate}}
                </div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Requests/Second</div>
                <div class="metric-value">{{printf "%.2f" .ProcessingStats.RequestsPerSecond}}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Elapsed Time</div>
                <div class="metric-value">{{.ProcessingStats.ElapsedTime}}</div>
            </div>
        </div>
    </div>
    {{end}}

    {{if .ErrorAnalysis}}
    <div class="section">
        <h2>Error Analysis</h2>
        <div class="metrics">
            <div class="metric-card">
                <div class="metric-label">Total Errors</div>
                <div class="metric-value error">{{.ErrorAnalysis.TotalErrors}}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Error Rate</div>
                <div class="metric-value {{if lt .ErrorAnalysis.ErrorRate 5.0}}success{{else}}error{{end}}">
                    {{printf "%.2f%%" .ErrorAnalysis.ErrorRate}}
                </div>
            </div>
        </div>
        
        {{if .ErrorAnalysis.MostCommonErrors}}
        <h3>Most Common Errors</h3>
        <table>
            <thead>
                <tr><th>Error</th><th>Count</th><th>Frequency</th></tr>
            </thead>
            <tbody>
                {{range .ErrorAnalysis.MostCommonErrors}}
                <tr>
                    <td>{{.Error}}</td>
                    <td>{{.Count}}</td>
                    <td>{{printf "%.2f%%" .Frequency}}</td>
                </tr>
                {{end}}
            </tbody>
        </table>
        {{end}}
    </div>
    {{end}}

    <div class="section">
        <p><em>Report generated by Media Toolkit Go</em></p>
    </div>
</body>
</html>`

	// Parse template
	t, err := template.New("html").Parse(htmlTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse HTML template: %w", err)
	}
	
	// Execute template
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute HTML template: %w", err)
	}
	
	// Write to file
	outputPath := filepath.Join(config.OutputDir, reportName+".html")
	if err := os.WriteFile(outputPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write HTML report: %w", err)
	}
	
	g.logger.Debug("Generated HTML report", zap.String("path", outputPath))
	return nil
}

// AnalyzeErrors analyzes errors from statistics
func (g *Generator) AnalyzeErrors(statsCollector *stats.Collector) *ErrorAnalysis {
	errors := statsCollector.GetErrors()
	totalRequests := statsCollector.TotalRequests()
	totalErrors := statsCollector.FailedRequests()
	
	analysis := &ErrorAnalysis{
		TotalErrors:      totalErrors,
		ErrorsByType:     make(map[string]int64),
		ErrorsByCategory: make(map[string]int64),
		MostCommonErrors: make([]ErrorFrequency, 0),
	}
	
	if totalRequests > 0 {
		analysis.ErrorRate = float64(totalErrors) / float64(totalRequests) * 100
	}
	
	// Analyze error types and create frequency list
	for errorMsg, count := range errors {
		frequency := float64(count) / float64(totalErrors) * 100
		analysis.MostCommonErrors = append(analysis.MostCommonErrors, ErrorFrequency{
			Error:     errorMsg,
			Count:     count,
			Frequency: frequency,
		})
		
		// Categorize errors
		category := categorizeError(errorMsg)
		analysis.ErrorsByCategory[category] += count
	}
	
	return analysis
}

// Helper functions

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func humanizeBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}

func categorizeError(errorMsg string) string {
	errorMsg = strings.ToLower(errorMsg)
	
	if strings.Contains(errorMsg, "timeout") {
		return "timeout"
	}
	if strings.Contains(errorMsg, "connection") || strings.Contains(errorMsg, "network") {
		return "network"
	}
	if strings.Contains(errorMsg, "401") || strings.Contains(errorMsg, "403") {
		return "authentication"
	}
	if strings.Contains(errorMsg, "404") {
		return "not_found"
	}
	if strings.Contains(errorMsg, "500") || strings.Contains(errorMsg, "502") || strings.Contains(errorMsg, "503") {
		return "server_error"
	}
	if strings.Contains(errorMsg, "400") {
		return "client_error"
	}
	
	return "other"
}