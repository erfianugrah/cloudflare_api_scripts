package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Default values
const (
	DefaultWorkers               = 5
	DefaultTimeout               = 120
	DefaultConnectionCloseDelay  = 15
	DefaultRetry                 = 2
	DefaultSmallFileThreshold    = 50
	DefaultMediumFileThreshold   = 200
	DefaultSizeThreshold         = 256
	DefaultValidationWorkers     = 10
	DefaultOutputFile            = "media_transform_results.json"
	DefaultErrorReportOutput     = "error_report.json"
	DefaultPerformanceReport     = "performance_report.md"
	DefaultSizeReportOutput      = "file_size_report.md"
	DefaultComparisonOutput      = "comparison_results.json"
	DefaultSummaryOutput         = "comparison_summary.md"
	DefaultValidationReport      = "validation_report.md"
	DefaultOptimizedVideosDir    = "optimized_videos"
	DefaultURLFormat             = "imwidth"
)

// SetDefaults sets default values for the configuration
func SetDefaults() {
	// Processing options
	viper.SetDefault("media-type", "auto")
	viper.SetDefault("derivatives", []string{"desktop", "tablet", "mobile"})
	viper.SetDefault("image-variants", []string{"thumbnail", "small", "medium", "large", "webp"})
	viper.SetDefault("workers", DefaultWorkers)
	viper.SetDefault("timeout", DefaultTimeout)
	viper.SetDefault("connection-close-delay", DefaultConnectionCloseDelay)
	viper.SetDefault("retry", DefaultRetry)

	// File extensions
	viper.SetDefault("extensions", []string{})

	// Output options
	viper.SetDefault("output", DefaultOutputFile)
	viper.SetDefault("error-report-output", DefaultErrorReportOutput)
	viper.SetDefault("performance-report", DefaultPerformanceReport)
	viper.SetDefault("size-report-output", DefaultSizeReportOutput)
	viper.SetDefault("comparison-output", DefaultComparisonOutput)
	viper.SetDefault("summary-output", DefaultSummaryOutput)
	viper.SetDefault("summary-format", "markdown")

	// Thresholds
	viper.SetDefault("thresholds.small-file-threshold", DefaultSmallFileThreshold)
	viper.SetDefault("thresholds.medium-file-threshold", DefaultMediumFileThreshold)
	viper.SetDefault("thresholds.size-threshold", DefaultSizeThreshold)

	// Optimization defaults
	viper.SetDefault("optimization.codec", "h264")
	viper.SetDefault("optimization.quality", "balanced")
	viper.SetDefault("optimization.target-resolution", "1080p")
	viper.SetDefault("optimization.fit", "contain")
	viper.SetDefault("optimization.audio-profile", "medium")
	viper.SetDefault("optimization.output-format", "mp4")
	viper.SetDefault("optimization.browser-compatible", true)
	viper.SetDefault("optimization.hardware-acceleration", "auto")
	viper.SetDefault("optimization.optimized-videos-dir", DefaultOptimizedVideosDir)

	// Validation defaults
	viper.SetDefault("validation.validation-workers", DefaultValidationWorkers)
	viper.SetDefault("validation.validation-report", DefaultValidationReport)
	viper.SetDefault("validation.validation-format", "markdown")
	viper.SetDefault("validation.video-pattern", "*.mp4")

	// Load test defaults
	viper.SetDefault("load-test.url-format", DefaultURLFormat)
	viper.SetDefault("load-test.url-format", "imwidth")
	viper.SetDefault("load-test.use-head-requests", true)
	viper.SetDefault("load-test.skip-large-files", true)
	viper.SetDefault("load-test.large-file-threshold-mib", DefaultSizeThreshold)
	viper.SetDefault("load-test.request-timeout", "120s")
	viper.SetDefault("load-test.head-timeout", "30s")
	viper.SetDefault("load-test.global-timeout", "90s")
	viper.SetDefault("load-test.failure-rate-threshold", "0.05")
	viper.SetDefault("load-test.max-retries", 2)

	// Load test stage defaults
	viper.SetDefault("load-test.stage1-users", 10)
	viper.SetDefault("load-test.stage1-duration", "30s")
	viper.SetDefault("load-test.stage2-users", 20)
	viper.SetDefault("load-test.stage2-duration", "1m")
	viper.SetDefault("load-test.stage3-users", 30)
	viper.SetDefault("load-test.stage3-duration", "30s")
	viper.SetDefault("load-test.stage4-users", 20)
	viper.SetDefault("load-test.stage4-duration", "1m")
	viper.SetDefault("load-test.stage5-users", 0)
	viper.SetDefault("load-test.stage5-duration", "30s")
}

// LoadConfig loads configuration from various sources
func LoadConfig() (*Config, error) {
	SetDefaults()

	// Set config name and paths
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.media-toolkit")
	viper.AddConfigPath("/etc/media-toolkit/")

	// Enable environment variable support
	viper.SetEnvPrefix("MEDIA_TOOLKIT")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv()

	// Try to read config file (optional)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found is okay, we'll use defaults and CLI flags
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Post-process configuration
	if err := postProcessConfig(&config); err != nil {
		return nil, fmt.Errorf("error post-processing config: %w", err)
	}

	return &config, nil
}

// postProcessConfig performs validation and adjustments to the configuration
func postProcessConfig(config *Config) error {
	// Convert media type string to enum
	switch strings.ToLower(config.MediaTypeString) {
	case "auto":
		config.MediaType = MediaTypeAuto
	case "image":
		config.MediaType = MediaTypeImage
	case "video":
		config.MediaType = MediaTypeVideo
	default:
		config.MediaType = MediaTypeAuto
	}

	// Ensure derivatives and image variants are properly set
	if len(config.Derivatives) == 0 {
		derivativesStr := viper.GetString("derivatives")
		if derivativesStr != "" {
			parts := strings.Split(derivativesStr, ",")
			config.Derivatives = make([]string, 0, len(parts))
			for _, part := range parts {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					config.Derivatives = append(config.Derivatives, trimmed)
				}
			}
		}
	}

	if len(config.ImageVariants) == 0 {
		variantsStr := viper.GetString("image-variants")
		if variantsStr != "" {
			parts := strings.Split(variantsStr, ",")
			config.ImageVariants = make([]string, 0, len(parts))
			for _, part := range parts {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					config.ImageVariants = append(config.ImageVariants, trimmed)
				}
			}
		}
	}

	// Validate required fields for certain operations
	if config.FullWorkflow || (!config.OnlyCompare && !config.ListFiles && !config.Validation.ValidateVideos) {
		if config.Remote == "" {
			return fmt.Errorf("remote is required for processing operations")
		}
		if config.Bucket == "" {
			return fmt.Errorf("bucket is required for processing operations")
		}
		if config.BaseURL == "" && !config.Optimization.OptimizeInPlace {
			return fmt.Errorf("base-url is required for pre-warming operations")
		}
	}

	// Validate worker allocation
	if config.Workers <= 0 {
		config.Workers = DefaultWorkers
	}

	// Ensure timeout values are reasonable
	if config.Timeout <= 0 {
		config.Timeout = DefaultTimeout
	}

	// Validate optimization settings
	if config.Optimization.OptimizeVideos || config.Optimization.OptimizeInPlace {
		validCodecs := []string{"h264", "h265", "vp9", "vp8", "av1"}
		if !contains(validCodecs, config.Optimization.Codec) {
			return fmt.Errorf("invalid codec: %s, must be one of: %s", 
				config.Optimization.Codec, strings.Join(validCodecs, ", "))
		}

		validQualities := []string{"maximum", "high", "balanced", "efficient", "minimum"}
		if !contains(validQualities, config.Optimization.Quality) {
			return fmt.Errorf("invalid quality: %s, must be one of: %s", 
				config.Optimization.Quality, strings.Join(validQualities, ", "))
		}
	}

	return nil
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// SetupLogging configures logging based on the configuration
func SetupLogging(verbose bool) (*zap.Logger, error) {
	var config zap.Config

	if verbose {
		config = zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		config = zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	// Customize output format
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.LevelKey = "level"
	config.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	config.EncoderConfig.CallerKey = "caller"
	config.EncoderConfig.MessageKey = "message"

	// Add file output
	timestamp := time.Now().Format("20060102_150405")
	logFile := fmt.Sprintf("media_transform_%s.log", timestamp)

	config.OutputPaths = []string{
		"stdout",
		logFile,
	}
	config.ErrorOutputPaths = []string{
		"stderr",
		logFile,
	}

	logger, err := config.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	// Log initialization
	logger.Info("Logging initialized",
		zap.String("level", config.Level.String()),
		zap.String("log_file", logFile),
	)

	return logger, nil
}

// ValidateConfig performs comprehensive validation of the configuration
func ValidateConfig(config *Config) error {
	var errors []string

	// Validate paths if provided
	if config.Validation.ValidateDirectory != "" {
		if _, err := os.Stat(config.Validation.ValidateDirectory); os.IsNotExist(err) {
			errors = append(errors, fmt.Sprintf("validation directory does not exist: %s", config.Validation.ValidateDirectory))
		}
	}

	if config.Validation.ValidateResults != "" {
		if _, err := os.Stat(config.Validation.ValidateResults); os.IsNotExist(err) {
			errors = append(errors, fmt.Sprintf("validation results file does not exist: %s", config.Validation.ValidateResults))
		}
	}

	if config.Compare != "" {
		if _, err := os.Stat(config.Compare); os.IsNotExist(err) {
			errors = append(errors, fmt.Sprintf("comparison file does not exist: %s", config.Compare))
		}
	}

	// Validate load test configuration if enabled
	if config.LoadTest.RunLoadTest {
		// Validate URL format
		validFormats := []string{"imwidth", "derivative", "query"}
		if !contains(validFormats, config.LoadTest.URLFormat) {
			errors = append(errors, fmt.Sprintf("invalid URL format: %s (must be one of: %s)", config.LoadTest.URLFormat, strings.Join(validFormats, ", ")))
		}
	}

	// Validate output directories
	for _, path := range []string{
		filepath.Dir(config.Output),
		filepath.Dir(config.ErrorReportOutput),
		filepath.Dir(config.PerformanceReport),
		filepath.Dir(config.SizeReportOutput),
		filepath.Dir(config.Validation.ValidationReport),
	} {
		if path != "." && path != "" {
			if err := os.MkdirAll(path, 0755); err != nil {
				errors = append(errors, fmt.Sprintf("cannot create output directory %s: %v", path, err))
			}
		}
	}

	// Create optimized videos directory if needed
	if config.Optimization.OptimizeVideos || config.Optimization.OptimizeInPlace {
		if err := os.MkdirAll(config.Optimization.OptimizedVideosDir, 0755); err != nil {
			errors = append(errors, fmt.Sprintf("cannot create optimized videos directory %s: %v", 
				config.Optimization.OptimizedVideosDir, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("configuration validation failed:\n%s", strings.Join(errors, "\n"))
	}

	return nil
}

// GetExtensionsForMediaType returns the appropriate file extensions based on media type
func GetExtensionsForMediaType(mediaType string, customExtensions []string) []string {
	// If custom extensions are provided, use them
	if len(customExtensions) > 0 {
		return customExtensions
	}
	
	// Otherwise, use media type presets
	switch strings.ToLower(mediaType) {
	case "image":
		return []string{".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg"}
	case "video":
		return []string{".mp4", ".webm", ".mov", ".avi", ".mkv", ".m4v"}
	case "all", "auto":
		// Return empty to indicate no filtering
		return []string{}
	default:
		// Default to no filtering
		return []string{}
	}
}