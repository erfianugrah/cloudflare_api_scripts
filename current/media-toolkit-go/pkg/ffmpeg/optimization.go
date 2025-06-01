package ffmpeg

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// OptimizationConfig configures video optimization parameters
type OptimizationConfig struct {
	// Quality settings
	CRF               int     `json:"crf"`                // Constant Rate Factor (0-51, lower = better quality)
	Preset            string  `json:"preset"`             // Encoding preset (ultrafast, fast, medium, slow, etc.)
	Profile           string  `json:"profile"`            // H.264 profile (baseline, main, high)
	Level             string  `json:"level"`              // H.264 level
	MaxBitrate        int64   `json:"max_bitrate"`        // Maximum bitrate in kbps
	BufferSize        int64   `json:"buffer_size"`        // Buffer size in kbps
	
	// Resolution settings
	MaxWidth          int     `json:"max_width"`          // Maximum width
	MaxHeight         int     `json:"max_height"`         // Maximum height
	ScaleAlgorithm    string  `json:"scale_algorithm"`    // Scaling algorithm (lanczos, bicubic, etc.)
	
	// Audio settings
	AudioCodec        string  `json:"audio_codec"`        // Audio codec (aac, mp3, etc.)
	AudioBitrate      int     `json:"audio_bitrate"`      // Audio bitrate in kbps
	AudioSampleRate   int     `json:"audio_sample_rate"`  // Audio sample rate
	AudioChannels     int     `json:"audio_channels"`     // Number of audio channels
	
	// Output settings
	OutputFormat      string  `json:"output_format"`      // Output container format
	OutputCodec       string  `json:"output_codec"`       // Video codec (libx264, libx265, etc.)
	
	// Hardware acceleration
	HardwareAccel     string  `json:"hardware_accel"`     // Hardware acceleration type
	HardwareDevice    string  `json:"hardware_device"`    // Hardware device
	
	// Performance settings
	Threads           int     `json:"threads"`            // Number of encoding threads
	TileColumns       int     `json:"tile_columns"`       // AV1 tile columns
	TileRows          int     `json:"tile_rows"`          // AV1 tile rows
	
	// Optimization flags
	TwoPass           bool    `json:"two_pass"`           // Enable two-pass encoding
	Overwrite         bool    `json:"overwrite"`          // Overwrite existing files
	PreserveMetadata  bool    `json:"preserve_metadata"`  // Preserve original metadata
}

// DefaultOptimizationConfig returns sensible defaults for video optimization
func DefaultOptimizationConfig() OptimizationConfig {
	return OptimizationConfig{
		CRF:              23,
		Preset:           "medium",
		Profile:          "high",
		Level:            "4.0",
		ScaleAlgorithm:   "lanczos",
		AudioCodec:       "aac",
		AudioBitrate:     128,
		AudioSampleRate:  44100,
		AudioChannels:    2,
		OutputFormat:     "mp4",
		OutputCodec:      "libx264",
		Threads:          0, // Auto-detect
		Overwrite:        false,
		PreserveMetadata: true,
	}
}

// OptimizationResult contains the results of video optimization
type OptimizationResult struct {
	InputFile         string        `json:"input_file"`
	OutputFile        string        `json:"output_file"`
	Success           bool          `json:"success"`
	Error             string        `json:"error,omitempty"`
	
	// Size metrics
	OriginalSize      int64         `json:"original_size"`
	OptimizedSize     int64         `json:"optimized_size"`
	SizeReduction     int64         `json:"size_reduction"`
	SizeReductionPct  float64       `json:"size_reduction_percent"`
	
	// Quality metrics
	OriginalMetadata  *VideoMetadata `json:"original_metadata,omitempty"`
	OptimizedMetadata *VideoMetadata `json:"optimized_metadata,omitempty"`
	
	// Performance metrics
	EncodingTime      time.Duration `json:"encoding_time"`
	EncodingSpeed     float64       `json:"encoding_speed"` // Speed relative to playback time
	
	// Process info
	FFmpegCommand     string        `json:"ffmpeg_command,omitempty"`
	StartTime         time.Time     `json:"start_time"`
	EndTime           time.Time     `json:"end_time"`
}

// VideoOptimizer handles video optimization using FFmpeg
type VideoOptimizer struct {
	ffmpegPath string
	extractor  *MetadataExtractor
	logger     *zap.Logger
}

// NewVideoOptimizer creates a new video optimizer
func NewVideoOptimizer(logger *zap.Logger) *VideoOptimizer {
	return &VideoOptimizer{
		ffmpegPath: "ffmpeg", // Assume ffmpeg is in PATH
		extractor:  NewMetadataExtractor(logger),
		logger:     logger,
	}
}

// NewVideoOptimizerWithPath creates a new video optimizer with custom paths
func NewVideoOptimizerWithPath(ffmpegPath, ffprobePath string, logger *zap.Logger) *VideoOptimizer {
	return &VideoOptimizer{
		ffmpegPath: ffmpegPath,
		extractor:  NewMetadataExtractorWithPath(ffprobePath, 30*time.Second, logger),
		logger:     logger,
	}
}

// OptimizeVideo optimizes a video file according to the provided configuration
func (vo *VideoOptimizer) OptimizeVideo(ctx context.Context, inputPath, outputPath string, config OptimizationConfig) (*OptimizationResult, error) {
	startTime := time.Now()
	
	result := &OptimizationResult{
		InputFile:  inputPath,
		OutputFile: outputPath,
		StartTime:  startTime,
	}
	
	vo.logger.Info("Starting video optimization",
		zap.String("input", inputPath),
		zap.String("output", outputPath))
	
	// Extract original metadata
	originalMetadata, err := vo.extractor.ExtractMetadata(ctx, inputPath)
	if err != nil {
		result.Error = fmt.Sprintf("failed to extract original metadata: %v", err)
		return result, err
	}
	result.OriginalMetadata = originalMetadata
	
	// Get original file size
	if stat, err := os.Stat(inputPath); err == nil {
		result.OriginalSize = stat.Size()
	}
	
	// Build FFmpeg command
	cmd, err := vo.buildFFmpegCommand(ctx, inputPath, outputPath, config, originalMetadata)
	if err != nil {
		result.Error = fmt.Sprintf("failed to build ffmpeg command: %v", err)
		return result, err
	}
	result.FFmpegCommand = strings.Join(cmd.Args, " ")
	
	// Execute optimization
	if err := vo.executeOptimization(ctx, cmd, result); err != nil {
		result.Error = err.Error()
		return result, err
	}
	
	// Extract optimized metadata
	optimizedMetadata, err := vo.extractor.ExtractMetadata(ctx, outputPath)
	if err != nil {
		vo.logger.Warn("Failed to extract optimized metadata", zap.Error(err))
	} else {
		result.OptimizedMetadata = optimizedMetadata
	}
	
	// Get optimized file size and calculate metrics
	if stat, err := os.Stat(outputPath); err == nil {
		result.OptimizedSize = stat.Size()
		result.SizeReduction = result.OriginalSize - result.OptimizedSize
		if result.OriginalSize > 0 {
			result.SizeReductionPct = float64(result.SizeReduction) / float64(result.OriginalSize) * 100
		}
	}
	
	// Calculate encoding speed
	result.EndTime = time.Now()
	result.EncodingTime = result.EndTime.Sub(result.StartTime)
	if originalMetadata.Duration > 0 {
		result.EncodingSpeed = originalMetadata.Duration.Seconds() / result.EncodingTime.Seconds()
	}
	
	result.Success = true
	
	vo.logger.Info("Video optimization completed",
		zap.String("input", inputPath),
		zap.String("output", outputPath),
		zap.Int64("original_size", result.OriginalSize),
		zap.Int64("optimized_size", result.OptimizedSize),
		zap.Float64("size_reduction_pct", result.SizeReductionPct),
		zap.Duration("encoding_time", result.EncodingTime),
		zap.Float64("encoding_speed", result.EncodingSpeed))
	
	return result, nil
}

// buildFFmpegCommand constructs the FFmpeg command based on configuration
func (vo *VideoOptimizer) buildFFmpegCommand(ctx context.Context, inputPath, outputPath string, config OptimizationConfig, metadata *VideoMetadata) (*exec.Cmd, error) {
	args := []string{"-y"} // Always overwrite for now
	
	// Hardware acceleration
	if config.HardwareAccel != "" {
		args = append(args, "-hwaccel", config.HardwareAccel)
		if config.HardwareDevice != "" {
			args = append(args, "-hwaccel_device", config.HardwareDevice)
		}
	}
	
	// Input file
	args = append(args, "-i", inputPath)
	
	// Video codec and settings
	args = append(args, "-c:v", config.OutputCodec)
	
	if config.CRF > 0 {
		args = append(args, "-crf", strconv.Itoa(config.CRF))
	}
	
	if config.Preset != "" {
		args = append(args, "-preset", config.Preset)
	}
	
	if config.Profile != "" {
		args = append(args, "-profile:v", config.Profile)
	}
	
	if config.Level != "" {
		args = append(args, "-level", config.Level)
	}
	
	// Bitrate settings
	if config.MaxBitrate > 0 {
		args = append(args, "-maxrate", fmt.Sprintf("%dk", config.MaxBitrate))
		if config.BufferSize > 0 {
			args = append(args, "-bufsize", fmt.Sprintf("%dk", config.BufferSize))
		}
	}
	
	// Resolution scaling
	if config.MaxWidth > 0 || config.MaxHeight > 0 {
		scaleFilter := vo.buildScaleFilter(config, metadata)
		if scaleFilter != "" {
			args = append(args, "-vf", scaleFilter)
		}
	}
	
	// Audio settings
	if config.AudioCodec != "" {
		args = append(args, "-c:a", config.AudioCodec)
	}
	
	if config.AudioBitrate > 0 {
		args = append(args, "-b:a", fmt.Sprintf("%dk", config.AudioBitrate))
	}
	
	if config.AudioSampleRate > 0 {
		args = append(args, "-ar", strconv.Itoa(config.AudioSampleRate))
	}
	
	if config.AudioChannels > 0 {
		args = append(args, "-ac", strconv.Itoa(config.AudioChannels))
	}
	
	// Threading
	if config.Threads > 0 {
		args = append(args, "-threads", strconv.Itoa(config.Threads))
	}
	
	// Codec-specific optimizations
	args = append(args, vo.getCodecSpecificArgs(config)...)
	
	// Metadata
	if !config.PreserveMetadata {
		args = append(args, "-map_metadata", "-1")
	}
	
	// Output format
	if config.OutputFormat != "" {
		args = append(args, "-f", config.OutputFormat)
	}
	
	// Output file
	args = append(args, outputPath)
	
	cmd := exec.CommandContext(ctx, vo.ffmpegPath, args...)
	return cmd, nil
}

// buildScaleFilter builds the video scaling filter
func (vo *VideoOptimizer) buildScaleFilter(config OptimizationConfig, metadata *VideoMetadata) string {
	if config.MaxWidth == 0 && config.MaxHeight == 0 {
		return ""
	}
	
	// Determine target dimensions while preserving aspect ratio
	targetWidth, targetHeight := vo.calculateTargetDimensions(
		metadata.Width, metadata.Height,
		config.MaxWidth, config.MaxHeight)
	
	if targetWidth == metadata.Width && targetHeight == metadata.Height {
		return "" // No scaling needed
	}
	
	algorithm := config.ScaleAlgorithm
	if algorithm == "" {
		algorithm = "lanczos"
	}
	
	return fmt.Sprintf("scale=%d:%d:flags=%s", targetWidth, targetHeight, algorithm)
}

// calculateTargetDimensions calculates target dimensions while preserving aspect ratio
func (vo *VideoOptimizer) calculateTargetDimensions(currentWidth, currentHeight, maxWidth, maxHeight int) (int, int) {
	if maxWidth == 0 && maxHeight == 0 {
		return currentWidth, currentHeight
	}
	
	// If only one dimension is specified, calculate the other
	if maxWidth == 0 {
		ratio := float64(maxHeight) / float64(currentHeight)
		return int(float64(currentWidth) * ratio), maxHeight
	}
	
	if maxHeight == 0 {
		ratio := float64(maxWidth) / float64(currentWidth)
		return maxWidth, int(float64(currentHeight) * ratio)
	}
	
	// Both dimensions specified - fit within bounds
	widthRatio := float64(maxWidth) / float64(currentWidth)
	heightRatio := float64(maxHeight) / float64(currentHeight)
	
	// Use the smaller ratio to ensure we fit within both bounds
	ratio := widthRatio
	if heightRatio < widthRatio {
		ratio = heightRatio
	}
	
	targetWidth := int(float64(currentWidth) * ratio)
	targetHeight := int(float64(currentHeight) * ratio)
	
	// Ensure even dimensions (required by many codecs)
	if targetWidth%2 != 0 {
		targetWidth--
	}
	if targetHeight%2 != 0 {
		targetHeight--
	}
	
	return targetWidth, targetHeight
}

// getCodecSpecificArgs returns codec-specific optimization arguments
func (vo *VideoOptimizer) getCodecSpecificArgs(config OptimizationConfig) []string {
	var args []string
	
	switch config.OutputCodec {
	case "libx264":
		args = append(args, "-movflags", "+faststart") // Enable fast start for web
		if config.TwoPass {
			// Two-pass encoding would require separate implementation
		}
		
	case "libx265":
		args = append(args, "-movflags", "+faststart")
		args = append(args, "-tag:v", "hvc1") // Better compatibility
		
	case "libvpx-vp9":
		args = append(args, "-deadline", "good")
		args = append(args, "-cpu-used", "2")
		if config.TileColumns > 0 {
			args = append(args, "-tile-columns", strconv.Itoa(config.TileColumns))
		}
		if config.TileRows > 0 {
			args = append(args, "-tile-rows", strconv.Itoa(config.TileRows))
		}
		
	case "libaom-av1":
		args = append(args, "-cpu-used", "4")
		if config.TileColumns > 0 {
			args = append(args, "-tile-columns", strconv.Itoa(config.TileColumns))
		}
		if config.TileRows > 0 {
			args = append(args, "-tile-rows", strconv.Itoa(config.TileRows))
		}
	}
	
	return args
}

// executeOptimization executes the FFmpeg command and monitors progress
func (vo *VideoOptimizer) executeOptimization(ctx context.Context, cmd *exec.Cmd, result *OptimizationResult) error {
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	
	vo.logger.Debug("Executing FFmpeg command", zap.String("command", strings.Join(cmd.Args, " ")))
	
	// Execute the command
	err := cmd.Run()
	if err != nil {
		stderrStr := stderr.String()
		vo.logger.Error("FFmpeg execution failed",
			zap.Error(err),
			zap.String("stderr", stderrStr))
		return fmt.Errorf("ffmpeg failed: %w, stderr: %s", err, stderrStr)
	}
	
	// Check if output file exists
	if _, err := os.Stat(result.OutputFile); err != nil {
		return fmt.Errorf("output file not created: %w", err)
	}
	
	return nil
}

// DetectHardwareAcceleration detects available hardware acceleration methods
func (vo *VideoOptimizer) DetectHardwareAcceleration(ctx context.Context) ([]string, error) {
	cmd := exec.CommandContext(ctx, vo.ffmpegPath, "-hwaccels")
	
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to detect hardware acceleration: %w", err)
	}
	
	lines := strings.Split(stdout.String(), "\n")
	var hwaccels []string
	
	// Skip the header line and parse hardware acceleration methods
	for i, line := range lines {
		if i == 0 || strings.TrimSpace(line) == "" {
			continue
		}
		hwaccels = append(hwaccels, strings.TrimSpace(line))
	}
	
	vo.logger.Info("Detected hardware acceleration methods", zap.Strings("hwaccels", hwaccels))
	return hwaccels, nil
}

// OptimizeInPlace optimizes a video file and replaces the original
func (vo *VideoOptimizer) OptimizeInPlace(ctx context.Context, filePath string, config OptimizationConfig) (*OptimizationResult, error) {
	// Create temporary output file
	tempDir := filepath.Dir(filePath)
	tempFile := filepath.Join(tempDir, ".tmp_"+filepath.Base(filePath))
	
	// Ensure cleanup
	defer func() {
		if _, err := os.Stat(tempFile); err == nil {
			os.Remove(tempFile)
		}
	}()
	
	// Optimize to temporary file
	result, err := vo.OptimizeVideo(ctx, filePath, tempFile, config)
	if err != nil {
		return result, err
	}
	
	// Replace original with optimized version
	if err := os.Rename(tempFile, filePath); err != nil {
		return result, fmt.Errorf("failed to replace original file: %w", err)
	}
	
	result.OutputFile = filePath
	return result, nil
}