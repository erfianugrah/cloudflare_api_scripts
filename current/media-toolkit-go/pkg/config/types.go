package config

import (
	"time"
)

// MediaType represents the type of media to process
type MediaType int

const (
	MediaTypeAuto MediaType = iota
	MediaTypeImage
	MediaTypeVideo
)

// String returns the string representation of MediaType
func (m MediaType) String() string {
	switch m {
	case MediaTypeAuto:
		return "auto"
	case MediaTypeImage:
		return "image"
	case MediaTypeVideo:
		return "video"
	default:
		return "unknown"
	}
}

// UnmarshalText implements the encoding.TextUnmarshaler interface
func (m *MediaType) UnmarshalText(text []byte) error {
	switch string(text) {
	case "auto":
		*m = MediaTypeAuto
	case "image":
		*m = MediaTypeImage
	case "video":
		*m = MediaTypeVideo
	default:
		*m = MediaTypeAuto
	}
	return nil
}

// MarshalText implements the encoding.TextMarshaler interface
func (m MediaType) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

// SizeCategory represents file size categories for worker allocation
type SizeCategory int

const (
	SizeCategorySmall SizeCategory = iota
	SizeCategoryMedium
	SizeCategoryLarge
)

// String returns the string representation of SizeCategory
func (s SizeCategory) String() string {
	switch s {
	case SizeCategorySmall:
		return "small"
	case SizeCategoryMedium:
		return "medium"
	case SizeCategoryLarge:
		return "large"
	default:
		return "unknown"
	}
}

// WorkerAllocationConfig defines worker allocation settings
type WorkerAllocationConfig struct {
	SmallFileWorkers  int  `mapstructure:"small-file-workers"`
	MediumFileWorkers int  `mapstructure:"medium-file-workers"`
	LargeFileWorkers  int  `mapstructure:"large-file-workers"`
	OptimizeBySize    bool `mapstructure:"optimize-by-size"`
}

// ThresholdConfig defines size thresholds for file categorization
type ThresholdConfig struct {
	SmallFileMiB     int `mapstructure:"small-file-threshold"`
	MediumFileMiB    int `mapstructure:"medium-file-threshold"`
	SizeThresholdMiB int `mapstructure:"size-threshold"`
}

// OptimizationConfig defines video optimization settings
type OptimizationConfig struct {
	Codec                       string `mapstructure:"codec"`
	Quality                     string `mapstructure:"quality"`
	TargetResolution            string `mapstructure:"target-resolution"`
	Fit                         string `mapstructure:"fit"`
	AudioProfile                string `mapstructure:"audio-profile"`
	OutputFormat                string `mapstructure:"output-format"`
	CreateWebM                  bool   `mapstructure:"create-webm"`
	HardwareAcceleration        string `mapstructure:"hardware-acceleration"`
	DisableHardwareAcceleration bool   `mapstructure:"disable-hardware-acceleration"`
	BrowserCompatible           bool   `mapstructure:"browser-compatible"`
	OptimizeVideos              bool   `mapstructure:"optimize-videos"`
	OptimizeInPlace             bool   `mapstructure:"optimize-in-place"`
	OptimizedVideosDir          string `mapstructure:"optimized-videos-dir"`
}

// ValidationConfig defines video validation settings
type ValidationConfig struct {
	ValidateVideos    bool   `mapstructure:"validate-videos"`
	ValidateDirectory string `mapstructure:"validate-directory"`
	ValidateResults   string `mapstructure:"validate-results"`
	ValidationWorkers int    `mapstructure:"validation-workers"`
	ValidationReport  string `mapstructure:"validation-report"`
	ValidationFormat  string `mapstructure:"validation-format"`
	VideoPattern      string `mapstructure:"video-pattern"`
}

// LoadTestConfig defines load testing settings
type LoadTestConfig struct {
	RunLoadTest               bool   `mapstructure:"run-load-test"`
	URLFormat                 string `mapstructure:"url-format"`
	DebugMode                 bool   `mapstructure:"debug-mode"`
	UseHeadRequests           bool   `mapstructure:"use-head-requests"`
	SkipLargeFiles            bool   `mapstructure:"skip-large-files"`
	LargeFileThresholdMiB     int    `mapstructure:"large-file-threshold-mib"`
	RequestTimeout            string `mapstructure:"request-timeout"`
	HeadTimeout               string `mapstructure:"head-timeout"`
	GlobalTimeout             string `mapstructure:"global-timeout"`
	FailureRateThreshold      string `mapstructure:"failure-rate-threshold"`
	MaxRetries                int    `mapstructure:"max-retries"`
	UseErrorReportForLoadTest bool   `mapstructure:"use-error-report-for-load-test"`

	// Stage configuration
	Stage1Users    int    `mapstructure:"stage1-users"`
	Stage1Duration string `mapstructure:"stage1-duration"`
	Stage2Users    int    `mapstructure:"stage2-users"`
	Stage2Duration string `mapstructure:"stage2-duration"`
	Stage3Users    int    `mapstructure:"stage3-users"`
	Stage3Duration string `mapstructure:"stage3-duration"`
	Stage4Users    int    `mapstructure:"stage4-users"`
	Stage4Duration string `mapstructure:"stage4-duration"`
	Stage5Users    int    `mapstructure:"stage5-users"`
	Stage5Duration string `mapstructure:"stage5-duration"`
}

// Config represents the complete application configuration
type Config struct {
	// Global options
	DryRun bool `mapstructure:"dry-run"`

	// Workflow options
	ForcePrewarm              bool `mapstructure:"force-prewarm"`
	UseErrorReportForLoadTest bool `mapstructure:"use-error-report-for-load-test"`
	FullWorkflow              bool `mapstructure:"full-workflow"`

	// Remote storage options
	Remote    string `mapstructure:"remote"`
	Bucket    string `mapstructure:"bucket"`
	Directory string `mapstructure:"directory"`
	BaseURL   string `mapstructure:"base-url"`

	// Processing options
	MediaType            MediaType `mapstructure:"-"`
	MediaTypeString      string    `mapstructure:"media-type"`
	Derivatives          []string  `mapstructure:"derivatives"`
	ImageVariants        []string  `mapstructure:"image-variants"`
	UseDerivatives       bool      `mapstructure:"use-derivatives"`
	Workers              int       `mapstructure:"workers"`
	Timeout              int       `mapstructure:"timeout"`
	ConnectionCloseDelay int       `mapstructure:"connection-close-delay"`
	Retry                int       `mapstructure:"retry"`
	QueueMultiplier      float64   `mapstructure:"queue-multiplier"`
	UseHeadForSize       bool      `mapstructure:"use-head-for-size"`
	GenerateErrorReport  bool      `mapstructure:"generate-error-report"`
	ErrorReportOutput    string    `mapstructure:"error-report-output"`
	Format               string    `mapstructure:"format"`

	// Output and reporting options
	Output     string   `mapstructure:"output"`
	Limit      int      `mapstructure:"limit"`
	Extensions []string `mapstructure:"extensions"`
	Verbose    bool     `mapstructure:"verbose"`

	// Comparison options
	Compare          string `mapstructure:"compare"`
	ComparisonOutput string `mapstructure:"comparison-output"`
	SummaryOutput    string `mapstructure:"summary-output"`
	SummaryFormat    string `mapstructure:"summary-format"`
	OnlyCompare      bool   `mapstructure:"only-compare"`

	// S3 and listing options
	UseAWSCLI        bool   `mapstructure:"use-aws-cli"`
	ListFiles        bool   `mapstructure:"list-files"`
	SizeReportOutput string `mapstructure:"size-report-output"`

	// Performance and optimization
	PerformanceReport string `mapstructure:"performance-report"`

	// Thresholds
	Thresholds ThresholdConfig `mapstructure:"thresholds"`

	// Worker allocation
	WorkerAllocation WorkerAllocationConfig `mapstructure:"worker-allocation"`

	// Optimization settings
	Optimization OptimizationConfig `mapstructure:"optimization"`

	// Validation settings
	Validation ValidationConfig `mapstructure:"validation"`

	// Load testing settings
	LoadTest LoadTestConfig `mapstructure:"load-test"`

	// Exclusion filters
	ExcludeExtensions  []string `mapstructure:"exclude-extensions"`
	ExcludePatterns    []string `mapstructure:"exclude-patterns"`
	ExcludeDirectories []string `mapstructure:"exclude-directories"`
	ExcludeMinSize     int      `mapstructure:"exclude-min-size"`
	ExcludeMaxSize     int      `mapstructure:"exclude-max-size"`
}

// FileMetadata represents metadata for a file being processed
type FileMetadata struct {
	Path                string                        `json:"path"`
	Size                int64                         `json:"size_bytes"`
	SizeCategory        SizeCategory                  `json:"size_category"`
	MediaType           MediaType                     `json:"media_type,omitempty"`
	ProcessingStarted   *time.Time                    `json:"processing_started,omitempty"`
	ProcessingCompleted *time.Time                    `json:"processing_completed,omitempty"`
	ProcessingDuration  *time.Duration                `json:"processing_duration,omitempty"`
	Derivatives         map[string]DerivativeMetadata `json:"derivatives,omitempty"`
}

// DerivativeMetadata represents processing metadata for a specific derivative/variant
type DerivativeMetadata struct {
	Started   *time.Time     `json:"started,omitempty"`
	Completed *time.Time     `json:"completed,omitempty"`
	Duration  *time.Duration `json:"duration,omitempty"`
}

// StartProcessing marks the start time of processing
func (f *FileMetadata) StartProcessing() {
	now := time.Now()
	f.ProcessingStarted = &now
}

// CompleteProcessing marks the completion time and calculates duration
func (f *FileMetadata) CompleteProcessing() {
	now := time.Now()
	f.ProcessingCompleted = &now
	if f.ProcessingStarted != nil {
		duration := now.Sub(*f.ProcessingStarted)
		f.ProcessingDuration = &duration
	}
}

// StartDerivativeProcessing marks the start time for a specific derivative
func (f *FileMetadata) StartDerivativeProcessing(derivative string) {
	if f.Derivatives == nil {
		f.Derivatives = make(map[string]DerivativeMetadata)
	}
	now := time.Now()
	meta := f.Derivatives[derivative]
	meta.Started = &now
	f.Derivatives[derivative] = meta
}

// CompleteDerivativeProcessing marks the completion time for a derivative
func (f *FileMetadata) CompleteDerivativeProcessing(derivative string) {
	if f.Derivatives == nil {
		return
	}

	meta, exists := f.Derivatives[derivative]
	if !exists || meta.Started == nil {
		return
	}

	now := time.Now()
	meta.Completed = &now
	duration := now.Sub(*meta.Started)
	meta.Duration = &duration
	f.Derivatives[derivative] = meta
}

// ToDict converts FileMetadata to a map for serialization
func (f *FileMetadata) ToDict() map[string]interface{} {
	result := map[string]interface{}{
		"path":          f.Path,
		"size_bytes":    f.Size,
		"size_mib":      float64(f.Size) / (1024 * 1024),
		"size_category": f.SizeCategory.String(),
	}

	if f.ProcessingStarted != nil {
		result["processing_started"] = f.ProcessingStarted.Unix()
	}
	if f.ProcessingCompleted != nil {
		result["processing_completed"] = f.ProcessingCompleted.Unix()
	}
	if f.ProcessingDuration != nil {
		result["processing_duration"] = f.ProcessingDuration.Seconds()
	}
	if f.Derivatives != nil {
		derivatives := make(map[string]interface{})
		for name, meta := range f.Derivatives {
			derivativeMap := make(map[string]interface{})
			if meta.Started != nil {
				derivativeMap["started"] = meta.Started.Unix()
			}
			if meta.Completed != nil {
				derivativeMap["completed"] = meta.Completed.Unix()
			}
			if meta.Duration != nil {
				derivativeMap["duration"] = meta.Duration.Seconds()
			}
			derivatives[name] = derivativeMap
		}
		result["derivatives"] = derivatives
	}

	return result
}

// GetSizeCategory determines the size category based on thresholds
func GetSizeCategory(sizeBytes int64, smallThresholdMiB, mediumThresholdMiB int) SizeCategory {
	smallThreshold := int64(smallThresholdMiB) * 1024 * 1024
	mediumThreshold := int64(mediumThresholdMiB) * 1024 * 1024

	if sizeBytes < smallThreshold {
		return SizeCategorySmall
	} else if sizeBytes < mediumThreshold {
		return SizeCategoryMedium
	} else {
		return SizeCategoryLarge
	}
}

// NewFileMetadata creates a new FileMetadata instance
func NewFileMetadata(path string, sizeBytes int64, smallThresholdMiB, mediumThresholdMiB int) *FileMetadata {
	return &FileMetadata{
		Path:         path,
		Size:         sizeBytes,
		SizeCategory: GetSizeCategory(sizeBytes, smallThresholdMiB, mediumThresholdMiB),
		Derivatives:  make(map[string]DerivativeMetadata),
	}
}
