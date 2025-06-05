package ffmpeg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// VideoMetadata represents comprehensive video file metadata
type VideoMetadata struct {
	// Basic properties
	Filename string        `json:"filename"`
	Format   string        `json:"format"`
	Duration time.Duration `json:"duration"`
	Size     int64         `json:"size"`
	Bitrate  int64         `json:"bitrate"`

	// Video stream properties
	Width        int     `json:"width"`
	Height       int     `json:"height"`
	AspectRatio  string  `json:"aspect_ratio"`
	FrameRate    float64 `json:"frame_rate"`
	VideoCodec   string  `json:"video_codec"`
	VideoBitrate int64   `json:"video_bitrate"`
	PixelFormat  string  `json:"pixel_format"`

	// Audio stream properties
	AudioCodec    string `json:"audio_codec"`
	AudioBitrate  int64  `json:"audio_bitrate"`
	SampleRate    int    `json:"sample_rate"`
	AudioChannels int    `json:"audio_channels"`

	// Quality assessment
	HasErrors     bool     `json:"has_errors"`
	ErrorMessages []string `json:"error_messages,omitempty"`
	IsCorrupted   bool     `json:"is_corrupted"`

	// Analysis metadata
	AnalysisTime time.Time `json:"analysis_time"`
}

// FFProbeStream represents a stream from FFProbe output
type FFProbeStream struct {
	Index              int                    `json:"index"`
	CodecName          string                 `json:"codec_name"`
	CodecType          string                 `json:"codec_type"`
	Width              int                    `json:"width,omitempty"`
	Height             int                    `json:"height,omitempty"`
	SampleRate         string                 `json:"sample_rate,omitempty"`
	Channels           int                    `json:"channels,omitempty"`
	Duration           string                 `json:"duration,omitempty"`
	BitRate            string                 `json:"bit_rate,omitempty"`
	AvgFrameRate       string                 `json:"avg_frame_rate,omitempty"`
	RFrameRate         string                 `json:"r_frame_rate,omitempty"`
	PixFmt             string                 `json:"pix_fmt,omitempty"`
	DisplayAspectRatio string                 `json:"display_aspect_ratio,omitempty"`
	Tags               map[string]interface{} `json:"tags,omitempty"`
}

// FFProbeFormat represents format information from FFProbe
type FFProbeFormat struct {
	Filename       string                 `json:"filename"`
	FormatName     string                 `json:"format_name"`
	FormatLongName string                 `json:"format_long_name"`
	Duration       string                 `json:"duration"`
	Size           string                 `json:"size"`
	BitRate        string                 `json:"bit_rate"`
	Tags           map[string]interface{} `json:"tags,omitempty"`
}

// FFProbeOutput represents the complete FFProbe JSON output
type FFProbeOutput struct {
	Streams []FFProbeStream `json:"streams"`
	Format  FFProbeFormat   `json:"format"`
}

// MetadataExtractor handles video metadata extraction using FFProbe
type MetadataExtractor struct {
	ffprobePath string
	timeout     time.Duration
	logger      *zap.Logger
}

// NewMetadataExtractor creates a new metadata extractor
func NewMetadataExtractor(logger *zap.Logger) *MetadataExtractor {
	return &MetadataExtractor{
		ffprobePath: "ffprobe", // Assume ffprobe is in PATH
		timeout:     30 * time.Second,
		logger:      logger,
	}
}

// NewMetadataExtractorWithPath creates a new metadata extractor with custom ffprobe path
func NewMetadataExtractorWithPath(ffprobePath string, timeout time.Duration, logger *zap.Logger) *MetadataExtractor {
	return &MetadataExtractor{
		ffprobePath: ffprobePath,
		timeout:     timeout,
		logger:      logger,
	}
}

// ExtractMetadata extracts comprehensive metadata from a video file
func (me *MetadataExtractor) ExtractMetadata(ctx context.Context, filePath string) (*VideoMetadata, error) {
	me.logger.Debug("Extracting video metadata", zap.String("file", filePath))

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, me.timeout)
	defer cancel()

	// Build FFProbe command
	cmd := exec.CommandContext(timeoutCtx, me.ffprobePath,
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		"-show_error",
		filePath)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Execute command
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w, stderr: %s", err, stderr.String())
	}

	// Parse FFProbe output
	var probeOutput FFProbeOutput
	if err := json.Unmarshal(stdout.Bytes(), &probeOutput); err != nil {
		return nil, fmt.Errorf("failed to parse ffprobe output: %w", err)
	}

	// Convert to our metadata format
	metadata, err := me.convertToMetadata(filePath, &probeOutput)
	if err != nil {
		return nil, fmt.Errorf("failed to convert metadata: %w", err)
	}

	// Check for corruption
	metadata.IsCorrupted = me.detectCorruption(ctx, filePath)

	me.logger.Debug("Metadata extraction complete",
		zap.String("file", filePath),
		zap.Duration("duration", metadata.Duration),
		zap.Int("width", metadata.Width),
		zap.Int("height", metadata.Height),
		zap.Bool("corrupted", metadata.IsCorrupted))

	return metadata, nil
}

// convertToMetadata converts FFProbe output to our VideoMetadata structure
func (me *MetadataExtractor) convertToMetadata(filePath string, probe *FFProbeOutput) (*VideoMetadata, error) {
	metadata := &VideoMetadata{
		Filename:     filePath,
		AnalysisTime: time.Now(),
	}

	// Parse format information
	if err := me.parseFormat(metadata, &probe.Format); err != nil {
		return nil, fmt.Errorf("failed to parse format: %w", err)
	}

	// Parse streams
	for _, stream := range probe.Streams {
		switch stream.CodecType {
		case "video":
			if err := me.parseVideoStream(metadata, &stream); err != nil {
				me.logger.Warn("Failed to parse video stream", zap.Error(err))
			}
		case "audio":
			if err := me.parseAudioStream(metadata, &stream); err != nil {
				me.logger.Warn("Failed to parse audio stream", zap.Error(err))
			}
		}
	}

	return metadata, nil
}

// parseFormat parses format information from FFProbe output
func (me *MetadataExtractor) parseFormat(metadata *VideoMetadata, format *FFProbeFormat) error {
	metadata.Format = format.FormatName

	// Parse duration
	if format.Duration != "" {
		if duration, err := strconv.ParseFloat(format.Duration, 64); err == nil {
			metadata.Duration = time.Duration(duration * float64(time.Second))
		}
	}

	// Parse size
	if format.Size != "" {
		if size, err := strconv.ParseInt(format.Size, 10, 64); err == nil {
			metadata.Size = size
		}
	}

	// Parse bitrate
	if format.BitRate != "" {
		if bitrate, err := strconv.ParseInt(format.BitRate, 10, 64); err == nil {
			metadata.Bitrate = bitrate
		}
	}

	return nil
}

// parseVideoStream parses video stream information
func (me *MetadataExtractor) parseVideoStream(metadata *VideoMetadata, stream *FFProbeStream) error {
	metadata.Width = stream.Width
	metadata.Height = stream.Height
	metadata.VideoCodec = stream.CodecName
	metadata.PixelFormat = stream.PixFmt
	metadata.AspectRatio = stream.DisplayAspectRatio

	// Parse frame rate
	if stream.AvgFrameRate != "" {
		if frameRate := me.parseFrameRate(stream.AvgFrameRate); frameRate > 0 {
			metadata.FrameRate = frameRate
		}
	}

	// Parse video bitrate
	if stream.BitRate != "" {
		if bitrate, err := strconv.ParseInt(stream.BitRate, 10, 64); err == nil {
			metadata.VideoBitrate = bitrate
		}
	}

	return nil
}

// parseAudioStream parses audio stream information
func (me *MetadataExtractor) parseAudioStream(metadata *VideoMetadata, stream *FFProbeStream) error {
	metadata.AudioCodec = stream.CodecName
	metadata.AudioChannels = stream.Channels

	// Parse sample rate
	if stream.SampleRate != "" {
		if sampleRate, err := strconv.Atoi(stream.SampleRate); err == nil {
			metadata.SampleRate = sampleRate
		}
	}

	// Parse audio bitrate
	if stream.BitRate != "" {
		if bitrate, err := strconv.ParseInt(stream.BitRate, 10, 64); err == nil {
			metadata.AudioBitrate = bitrate
		}
	}

	return nil
}

// parseFrameRate parses frame rate from FFProbe format (e.g., "30/1", "29.97")
func (me *MetadataExtractor) parseFrameRate(frameRateStr string) float64 {
	if strings.Contains(frameRateStr, "/") {
		parts := strings.Split(frameRateStr, "/")
		if len(parts) == 2 {
			numerator, err1 := strconv.ParseFloat(parts[0], 64)
			denominator, err2 := strconv.ParseFloat(parts[1], 64)
			if err1 == nil && err2 == nil && denominator != 0 {
				return numerator / denominator
			}
		}
	} else {
		if frameRate, err := strconv.ParseFloat(frameRateStr, 64); err == nil {
			return frameRate
		}
	}
	return 0
}

// detectCorruption performs a quick corruption check using FFProbe
func (me *MetadataExtractor) detectCorruption(ctx context.Context, filePath string) bool {
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Use FFProbe to verify file integrity
	cmd := exec.CommandContext(timeoutCtx, me.ffprobePath,
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "frame=pkt_pts",
		"-of", "csv=p=0",
		"-count_frames",
		filePath)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		me.logger.Debug("Corruption check failed",
			zap.String("file", filePath),
			zap.Error(err),
			zap.String("stderr", stderr.String()))
		return true
	}

	// Check for error messages in stderr
	stderrStr := stderr.String()
	if strings.Contains(stderrStr, "error") ||
		strings.Contains(stderrStr, "corrupt") ||
		strings.Contains(stderrStr, "invalid") {
		return true
	}

	return false
}

// ValidateVideo performs comprehensive video validation
func (me *MetadataExtractor) ValidateVideo(ctx context.Context, filePath string) (*VideoMetadata, error) {
	// Extract metadata first
	metadata, err := me.ExtractMetadata(ctx, filePath)
	if err != nil {
		return nil, err
	}

	// Perform additional validation checks
	errors := me.performValidationChecks(metadata)
	if len(errors) > 0 {
		metadata.HasErrors = true
		metadata.ErrorMessages = errors
	}

	return metadata, nil
}

// performValidationChecks runs additional validation checks on metadata
func (me *MetadataExtractor) performValidationChecks(metadata *VideoMetadata) []string {
	var errors []string

	// Check for missing essential properties
	if metadata.Width == 0 || metadata.Height == 0 {
		errors = append(errors, "Invalid video dimensions")
	}

	if metadata.Duration == 0 {
		errors = append(errors, "Invalid or missing duration")
	}

	if metadata.VideoCodec == "" {
		errors = append(errors, "Missing video codec information")
	}

	// Check for suspicious values
	if metadata.FrameRate > 120 || metadata.FrameRate < 1 {
		errors = append(errors, fmt.Sprintf("Suspicious frame rate: %.2f", metadata.FrameRate))
	}

	if metadata.Width > 8192 || metadata.Height > 8192 {
		errors = append(errors, "Unusually high resolution")
	}

	return errors
}
