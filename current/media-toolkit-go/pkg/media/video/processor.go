package video

import (
	"context"
	"fmt"
	"strings"
	"time"

	"media-toolkit-go/pkg/config"
	"media-toolkit-go/pkg/httpclient"
	"go.uber.org/zap"
)

// Processor handles video derivative processing
type Processor struct {
	httpClient httpclient.Client
	logger     *zap.Logger
}

// NewProcessor creates a new video processor
func NewProcessor(httpClient httpclient.Client, logger *zap.Logger) *Processor {
	return &Processor{
		httpClient: httpClient,
		logger:     logger,
	}
}

// RequestResult represents the result of processing a single video derivative
type RequestResult struct {
	DerivativeName   string        `json:"derivative_name"`
	URL              string        `json:"url"`
	Status           string        `json:"status"`
	StatusCode       int           `json:"status_code,omitempty"`
	TimeToFirstByte  time.Duration `json:"time_to_first_byte,omitempty"`
	TotalTime        time.Duration `json:"total_time"`
	ResponseSize     int64         `json:"response_size_bytes,omitempty"`
	ContentType      string        `json:"content_type,omitempty"`
	Method           string        `json:"method,omitempty"`
	Retries          int           `json:"retries,omitempty"`
	Error            string        `json:"error,omitempty"`
	ErrorType        string        `json:"error_type,omitempty"`
	
	// Size reduction metrics
	OriginalSize         int64   `json:"original_size_bytes,omitempty"`
	SizeReductionBytes   int64   `json:"size_reduction_bytes,omitempty"`
	SizeReductionPercent float64 `json:"size_reduction_percent,omitempty"`
}

// ProcessResult represents the complete processing result for a video
type ProcessResult struct {
	Path           string                    `json:"path"`
	SizeBytes      int64                     `json:"size_bytes"`
	SizeCategory   string                    `json:"size_category"`
	MediaType      string                    `json:"media_type"`
	ProcessingTime time.Duration             `json:"processing_time"`
	Success        bool                      `json:"success"`
	Results        map[string]*RequestResult `json:"results"`
	Error          string                    `json:"error,omitempty"`
}

// ProcessVideo processes a single video with all specified derivatives
func (p *Processor) ProcessVideo(ctx context.Context, metadata *config.FileMetadata, baseURL string, derivatives []string, urlFormat string, timeout time.Duration, useHeadRequest bool) (*ProcessResult, error) {
	metadata.StartProcessing()
	defer metadata.CompleteProcessing()

	p.logger.Debug("Processing video", 
		zap.String("path", metadata.Path),
		zap.Int64("size", metadata.Size),
		zap.Strings("derivatives", derivatives),
		zap.String("url_format", urlFormat))

	result := &ProcessResult{
		Path:         metadata.Path,
		SizeBytes:    metadata.Size,
		SizeCategory: metadata.SizeCategory.String(),
		MediaType:    "video",
		Results:      make(map[string]*RequestResult),
	}

	// Get derivatives to process
	derivativeMap := GetDerivatives(derivatives)
	if len(derivativeMap) == 0 {
		return result, fmt.Errorf("no valid derivatives found")
	}

	// Process each derivative
	for derivativeName, derivative := range derivativeMap {
		metadata.StartDerivativeProcessing(derivativeName)
		
		requestResult, err := p.processDerivative(ctx, metadata, derivative, baseURL, urlFormat, timeout, useHeadRequest)
		if err != nil {
			p.logger.Warn("Failed to process derivative", 
				zap.String("derivative", derivativeName), 
				zap.String("path", metadata.Path),
				zap.Error(err))
			
			requestResult = &RequestResult{
				DerivativeName: derivativeName,
				Status:         "error",
				Error:          err.Error(),
				ErrorType:      "processing_error",
				TotalTime:      0,
			}
		}
		
		result.Results[derivativeName] = requestResult
		metadata.CompleteDerivativeProcessing(derivativeName)
	}

	// Calculate overall success
	successCount := 0
	for _, res := range result.Results {
		if res.Status == "success" {
			successCount++
		}
	}
	
	result.Success = successCount > 0
	if metadata.ProcessingDuration != nil {
		result.ProcessingTime = *metadata.ProcessingDuration
	}

	p.logger.Info("Completed video processing", 
		zap.String("path", metadata.Path),
		zap.Int("derivatives_processed", len(result.Results)),
		zap.Int("successful", successCount),
		zap.Duration("processing_time", result.ProcessingTime))

	return result, nil
}

// ProcessVideoWithoutDerivatives processes a video without derivatives (simple URL request)
func (p *Processor) ProcessVideoWithoutDerivatives(ctx context.Context, metadata *config.FileMetadata, baseURL string, timeout time.Duration, useHeadRequest bool) (*ProcessResult, error) {
	metadata.StartProcessing()
	defer metadata.CompleteProcessing()

	p.logger.Debug("Processing video without derivatives", 
		zap.String("path", metadata.Path),
		zap.Int64("size", metadata.Size))

	result := &ProcessResult{
		Path:         metadata.Path,
		SizeBytes:    metadata.Size,
		SizeCategory: metadata.SizeCategory.String(),
		MediaType:    "video",
		Results:      make(map[string]*RequestResult),
	}

	// Create a simple derivative for processing
	defaultDerivative := &Derivative{
		Name:   "default",
		Width:  1280,
		Height: 720,
	}

	requestResult, err := p.processDerivative(ctx, metadata, defaultDerivative, baseURL, "simple", timeout, useHeadRequest)
	if err != nil {
		p.logger.Warn("Failed to process video", 
			zap.String("path", metadata.Path),
			zap.Error(err))
		
		requestResult = &RequestResult{
			DerivativeName: "default",
			Status:         "error",
			Error:          err.Error(),
			ErrorType:      "processing_error",
			TotalTime:      0,
		}
	}
	
	result.Results["default"] = requestResult
	result.Success = requestResult.Status == "success"
	
	if metadata.ProcessingDuration != nil {
		result.ProcessingTime = *metadata.ProcessingDuration
	}

	p.logger.Info("Completed video processing", 
		zap.String("path", metadata.Path),
		zap.Bool("successful", result.Success),
		zap.Duration("processing_time", result.ProcessingTime))

	return result, nil
}

// processDerivative processes a single video derivative
func (p *Processor) processDerivative(ctx context.Context, metadata *config.FileMetadata, derivative *Derivative, baseURL string, urlFormat string, timeout time.Duration, useHeadRequest bool) (*RequestResult, error) {
	var url string
	var err error
	
	if urlFormat == "simple" {
		// Simple URL format: just base_url/file_path
		baseURL = strings.TrimSuffix(baseURL, "/")
		url = fmt.Sprintf("%s/%s", baseURL, metadata.Path)
	} else {
		// Generate URL for this derivative
		url, err = derivative.GenerateURL(baseURL, metadata.Path, urlFormat)
		if err != nil {
			return nil, fmt.Errorf("failed to generate URL: %w", err)
		}
	}

	result := &RequestResult{
		DerivativeName: derivative.Name,
		URL:            url,
	}

	p.logger.Debug("Processing video derivative", 
		zap.String("derivative", derivative.Name),
		zap.String("url", url))

	// Prepare request options
	var opts []httpclient.RequestOption
	if timeout > 0 {
		opts = append(opts, httpclient.WithTimeout(timeout))
	}

	// Try HEAD request first if enabled
	if useHeadRequest {
		response, err := p.httpClient.Head(ctx, url, opts...)
		if err == nil && response.StatusCode < 400 {
			result.Status = "success"
			result.StatusCode = response.StatusCode
			result.TimeToFirstByte = response.TTFB
			result.TotalTime = response.TotalTime
			result.Method = "HEAD"
			result.Retries = response.Retries
			
			if response.ContentLength > 0 {
				result.ResponseSize = response.ContentLength
			}
			
			if contentType := response.Headers.Get("Content-Type"); contentType != "" {
				result.ContentType = contentType
			}
			
			// Calculate size reduction if we have original size
			if metadata.Size > 0 && result.ResponseSize > 0 {
				result.OriginalSize = metadata.Size
				result.SizeReductionBytes = metadata.Size - result.ResponseSize
				result.SizeReductionPercent = float64(result.SizeReductionBytes) / float64(metadata.Size) * 100
			}
			
			p.logger.Debug("HEAD request successful", 
				zap.String("derivative", derivative.Name),
				zap.Int("status_code", response.StatusCode),
				zap.Duration("ttfb", response.TTFB))
			
			return result, nil
		}
		
		p.logger.Debug("HEAD request failed, falling back to GET", 
			zap.String("derivative", derivative.Name),
			zap.Error(err))
	}

	// GET request
	response, err := p.httpClient.Get(ctx, url, opts...)
	if err != nil {
		result.Status = "error"
		result.Error = err.Error()
		result.ErrorType = "request_error"
		if response != nil {
			result.TotalTime = response.TotalTime
			result.Retries = response.Retries
		}
		return result, nil
	}

	// Populate result
	result.StatusCode = response.StatusCode
	result.TimeToFirstByte = response.TTFB
	result.TotalTime = response.TotalTime
	result.Method = "GET"
	result.Retries = response.Retries
	result.ResponseSize = int64(len(response.Body))
	
	if contentType := response.Headers.Get("Content-Type"); contentType != "" {
		result.ContentType = contentType
	}

	// Determine status
	if response.StatusCode >= 400 {
		result.Status = "error"
		result.Error = fmt.Sprintf("HTTP %d: %s", response.StatusCode, response.Status)
		
		// Categorize error types
		switch {
		case response.StatusCode == 404:
			result.ErrorType = "not_found"
		case response.StatusCode == 403:
			result.ErrorType = "forbidden"
		case response.StatusCode == 429:
			result.ErrorType = "rate_limited"
		case response.StatusCode >= 500:
			result.ErrorType = "server_error"
		default:
			result.ErrorType = "client_error"
		}
	} else {
		result.Status = "success"
		
		// Calculate size reduction if we have original size
		if metadata.Size > 0 && result.ResponseSize > 0 {
			result.OriginalSize = metadata.Size
			result.SizeReductionBytes = metadata.Size - result.ResponseSize
			result.SizeReductionPercent = float64(result.SizeReductionBytes) / float64(metadata.Size) * 100
		}
	}

	p.logger.Debug("GET request completed", 
		zap.String("derivative", derivative.Name),
		zap.String("status", result.Status),
		zap.Int("status_code", response.StatusCode),
		zap.Duration("ttfb", response.TTFB),
		zap.Duration("total_time", response.TotalTime))

	return result, nil
}

// Close closes the video processor and its resources
func (p *Processor) Close() error {
	if p.httpClient != nil {
		return p.httpClient.Close()
	}
	return nil
}