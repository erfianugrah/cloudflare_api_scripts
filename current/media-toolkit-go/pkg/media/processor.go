package media

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"media-toolkit-go/pkg/config"
	"media-toolkit-go/pkg/httpclient"
	"media-toolkit-go/pkg/media/image"
	"media-toolkit-go/pkg/media/video"
	"go.uber.org/zap"
)

// Processor handles media processing for both images and videos
type Processor struct {
	imageProcessor *image.Processor
	videoProcessor *video.Processor
	logger         *zap.Logger
}

// NewProcessor creates a new media processor
func NewProcessor(httpClient httpclient.Client, logger *zap.Logger) *Processor {
	return &Processor{
		imageProcessor: image.NewProcessor(httpClient, logger),
		videoProcessor: video.NewProcessor(httpClient, logger),
		logger:         logger,
	}
}

// ProcessConfig contains configuration for processing
type ProcessConfig struct {
	BaseURL         string
	MediaType       config.MediaType
	ImageVariants   []string
	VideoDerivatives []string
	URLFormat       string
	Timeout         time.Duration
	UseHeadRequest  bool
}

// ProcessResult represents the result of processing media
type ProcessResult struct {
	Path           string        `json:"path"`
	SizeBytes      int64         `json:"size_bytes"`
	SizeCategory   string        `json:"size_category"`
	MediaType      string        `json:"media_type"`
	ProcessingTime time.Duration `json:"processing_time"`
	Success        bool          `json:"success"`
	Results        interface{}   `json:"results"` // Can be image.ProcessResult or video.ProcessResult
	Error          string        `json:"error,omitempty"`
}

// ProcessMedia processes a media file based on its type
func (p *Processor) ProcessMedia(ctx context.Context, metadata *config.FileMetadata, cfg ProcessConfig) (*ProcessResult, error) {
	// Detect media type if auto
	mediaType := cfg.MediaType
	if mediaType == config.MediaTypeAuto {
		mediaType = p.detectMediaType(metadata.Path, cfg.ImageVariants, cfg.VideoDerivatives)
		metadata.MediaType = mediaType
	}

	p.logger.Debug("Processing media file", 
		zap.String("path", metadata.Path),
		zap.String("media_type", mediaType.String()),
		zap.Int64("size", metadata.Size))

	var result *ProcessResult
	var err error

	switch mediaType {
	case config.MediaTypeImage:
		result, err = p.processImage(ctx, metadata, cfg)
	case config.MediaTypeVideo:
		result, err = p.processVideo(ctx, metadata, cfg)
	default:
		return nil, fmt.Errorf("unsupported media type: %s", mediaType.String())
	}

	if err != nil {
		p.logger.Error("Failed to process media", 
			zap.String("path", metadata.Path),
			zap.String("media_type", mediaType.String()),
			zap.Error(err))
		return nil, err
	}

	return result, nil
}

// processImage processes an image file
func (p *Processor) processImage(ctx context.Context, metadata *config.FileMetadata, cfg ProcessConfig) (*ProcessResult, error) {
	imageResult, err := p.imageProcessor.ProcessImage(ctx, metadata, cfg.BaseURL, cfg.ImageVariants, cfg.Timeout, cfg.UseHeadRequest)
	if err != nil {
		return nil, err
	}

	return &ProcessResult{
		Path:           imageResult.Path,
		SizeBytes:      imageResult.SizeBytes,
		SizeCategory:   imageResult.SizeCategory,
		MediaType:      imageResult.MediaType,
		ProcessingTime: imageResult.ProcessingTime,
		Success:        imageResult.Success,
		Results:        imageResult,
		Error:          imageResult.Error,
	}, nil
}

// processVideo processes a video file
func (p *Processor) processVideo(ctx context.Context, metadata *config.FileMetadata, cfg ProcessConfig) (*ProcessResult, error) {
	var videoResult *video.ProcessResult
	var err error

	if len(cfg.VideoDerivatives) == 0 {
		// Process without derivatives
		videoResult, err = p.videoProcessor.ProcessVideoWithoutDerivatives(ctx, metadata, cfg.BaseURL, cfg.Timeout, cfg.UseHeadRequest)
	} else {
		// Process with derivatives
		videoResult, err = p.videoProcessor.ProcessVideo(ctx, metadata, cfg.BaseURL, cfg.VideoDerivatives, cfg.URLFormat, cfg.Timeout, cfg.UseHeadRequest)
	}

	if err != nil {
		return nil, err
	}

	return &ProcessResult{
		Path:           videoResult.Path,
		SizeBytes:      videoResult.SizeBytes,
		SizeCategory:   videoResult.SizeCategory,
		MediaType:      videoResult.MediaType,
		ProcessingTime: videoResult.ProcessingTime,
		Success:        videoResult.Success,
		Results:        videoResult,
		Error:          videoResult.Error,
	}, nil
}

// detectMediaType detects the media type based on file extension
func (p *Processor) detectMediaType(filePath string, imageVariants, videoDerivatives []string) config.MediaType {
	ext := strings.ToLower(filepath.Ext(filePath))
	
	// Define common extensions
	imageExts := []string{".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg", ".tiff", ".ico"}
	videoExts := []string{".mp4", ".webm", ".mov", ".avi", ".mkv", ".m4v", ".flv", ".wmv", ".mpg", ".mpeg"}
	
	// Check image extensions
	for _, imgExt := range imageExts {
		if ext == imgExt {
			return config.MediaTypeImage
		}
	}
	
	// Check video extensions
	for _, vidExt := range videoExts {
		if ext == vidExt {
			return config.MediaTypeVideo
		}
	}
	
	// If we have image variants specified and no video derivatives, assume image
	if len(imageVariants) > 0 && len(videoDerivatives) == 0 {
		return config.MediaTypeImage
	}
	
	// If we have video derivatives specified and no image variants, assume video
	if len(videoDerivatives) > 0 && len(imageVariants) == 0 {
		return config.MediaTypeVideo
	}
	
	// Default to video for backward compatibility
	return config.MediaTypeVideo
}

// Close closes the media processor and its resources
func (p *Processor) Close() error {
	var err1, err2 error
	
	if p.imageProcessor != nil {
		err1 = p.imageProcessor.Close()
	}
	
	if p.videoProcessor != nil {
		err2 = p.videoProcessor.Close()
	}
	
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	
	return nil
}

// GetSupportedImageVariants returns all supported image variant names
func GetSupportedImageVariants() []string {
	return image.GetVariantNames()
}

// GetSupportedVideoDerivatives returns all supported video derivative names
func GetSupportedVideoDerivatives() []string {
	return video.GetDerivativeNames()
}