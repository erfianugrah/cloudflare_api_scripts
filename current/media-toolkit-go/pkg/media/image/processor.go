package image

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"media-toolkit-go/pkg/config"
	"media-toolkit-go/pkg/httpclient"
)

// Processor handles image variant processing
type Processor struct {
	httpClient httpclient.Client
	logger     *zap.Logger
}

// NewProcessor creates a new image processor
func NewProcessor(httpClient httpclient.Client, logger *zap.Logger) *Processor {
	return &Processor{
		httpClient: httpClient,
		logger:     logger,
	}
}

// RequestResult represents the result of processing a single image variant
type RequestResult struct {
	VariantName     string        `json:"variant_name"`
	URL             string        `json:"url"`
	Status          string        `json:"status"`
	StatusCode      int           `json:"status_code,omitempty"`
	TimeToFirstByte time.Duration `json:"time_to_first_byte,omitempty"`
	TotalTime       time.Duration `json:"total_time"`
	ResponseSize    int64         `json:"response_size_bytes,omitempty"`
	ContentType     string        `json:"content_type,omitempty"`
	Method          string        `json:"method,omitempty"`
	Retries         int           `json:"retries,omitempty"`
	Error           string        `json:"error,omitempty"`
	ErrorType       string        `json:"error_type,omitempty"`

	// Size reduction metrics
	OriginalSize         int64   `json:"original_size_bytes,omitempty"`
	SizeReductionBytes   int64   `json:"size_reduction_bytes,omitempty"`
	SizeReductionPercent float64 `json:"size_reduction_percent,omitempty"`
}

// ProcessResult represents the complete processing result for an image
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

// ProcessImage processes a single image with all specified variants
func (p *Processor) ProcessImage(ctx context.Context, metadata *config.FileMetadata, baseURL string, variants []string, timeout time.Duration, useHeadRequest bool, dryRun bool) (*ProcessResult, error) {
	metadata.StartProcessing()
	defer metadata.CompleteProcessing()

	p.logger.Debug("Processing image",
		zap.String("path", metadata.Path),
		zap.Int64("size", metadata.Size),
		zap.Strings("variants", variants))

	result := &ProcessResult{
		Path:         metadata.Path,
		SizeBytes:    metadata.Size,
		SizeCategory: metadata.SizeCategory.String(),
		MediaType:    "image",
		Results:      make(map[string]*RequestResult),
	}

	// Get variants to process
	variantMap := GetVariants(variants)
	if len(variantMap) == 0 {
		return result, fmt.Errorf("no valid variants found")
	}

	// Process each variant
	for variantName, variant := range variantMap {
		metadata.StartDerivativeProcessing(variantName)

		requestResult, err := p.processVariant(ctx, metadata, variant, baseURL, timeout, useHeadRequest, dryRun)
		if err != nil {
			p.logger.Warn("Failed to process variant",
				zap.String("variant", variantName),
				zap.String("path", metadata.Path),
				zap.Error(err))

			requestResult = &RequestResult{
				VariantName: variantName,
				Status:      "error",
				Error:       err.Error(),
				ErrorType:   "processing_error",
				TotalTime:   0,
			}
		}

		result.Results[variantName] = requestResult
		metadata.CompleteDerivativeProcessing(variantName)
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

	p.logger.Info("Completed image processing",
		zap.String("path", metadata.Path),
		zap.Int("variants_processed", len(result.Results)),
		zap.Int("successful", successCount),
		zap.Duration("processing_time", result.ProcessingTime))

	return result, nil
}

// processVariant processes a single image variant
func (p *Processor) processVariant(ctx context.Context, metadata *config.FileMetadata, variant *Variant, baseURL string, timeout time.Duration, useHeadRequest bool, dryRun bool) (*RequestResult, error) {
	// Generate URL for this variant
	url, err := variant.GenerateURL(baseURL, metadata.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to generate URL: %w", err)
	}

	result := &RequestResult{
		VariantName: variant.Name,
		URL:         url,
	}

	p.logger.Debug("Processing image variant",
		zap.String("variant", variant.Name),
		zap.String("url", url))

	// Prepare request options
	var opts []httpclient.RequestOption
	if timeout > 0 {
		opts = append(opts, httpclient.WithTimeout(timeout))
	}
	if dryRun {
		opts = append(opts, httpclient.WithDryRun(true))
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
				zap.String("variant", variant.Name),
				zap.Int("status_code", response.StatusCode),
				zap.Duration("ttfb", response.TTFB))

			return result, nil
		}

		p.logger.Debug("HEAD request failed, falling back to GET",
			zap.String("variant", variant.Name),
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
		zap.String("variant", variant.Name),
		zap.String("status", result.Status),
		zap.Int("status_code", response.StatusCode),
		zap.Duration("ttfb", response.TTFB),
		zap.Duration("total_time", response.TotalTime))

	return result, nil
}

// Close closes the image processor and its resources
func (p *Processor) Close() error {
	if p.httpClient != nil {
		return p.httpClient.Close()
	}
	return nil
}
