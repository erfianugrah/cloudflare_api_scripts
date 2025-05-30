package httpclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Client defines the HTTP client interface
type Client interface {
	Get(ctx context.Context, url string, opts ...RequestOption) (*Response, error)
	Head(ctx context.Context, url string, opts ...RequestOption) (*Response, error)
	Close() error
}

// HTTPClient implements the Client interface
type HTTPClient struct {
	client *http.Client
	config Config
	logger *zap.Logger
}

// Config holds HTTP client configuration
type Config struct {
	Timeout                 time.Duration
	RetryAttempts          int
	RetryDelay             time.Duration
	ConnectionCloseDelay   time.Duration
	MaxIdleConns           int
	MaxIdleConnsPerHost    int
	IdleConnTimeout        time.Duration
	DisableKeepAlives      bool
	UserAgent              string
}

// Response represents an HTTP response
type Response struct {
	StatusCode    int
	Status        string
	Headers       http.Header
	Body          []byte
	ContentLength int64
	TTFB          time.Duration
	TotalTime     time.Duration
	Method        string
	URL           string
	Retries       int
}

// RequestOption allows customization of individual requests
type RequestOption func(*requestConfig)

type requestConfig struct {
	headers map[string]string
	timeout time.Duration
}

// WithHeader adds a header to the request
func WithHeader(key, value string) RequestOption {
	return func(cfg *requestConfig) {
		if cfg.headers == nil {
			cfg.headers = make(map[string]string)
		}
		cfg.headers[key] = value
	}
}

// WithTimeout sets a custom timeout for the request
func WithTimeout(timeout time.Duration) RequestOption {
	return func(cfg *requestConfig) {
		cfg.timeout = timeout
	}
}

// NewHTTPClient creates a new HTTP client
func NewHTTPClient(config Config, logger *zap.Logger) *HTTPClient {
	// Set defaults
	if config.Timeout == 0 {
		config.Timeout = 120 * time.Second
	}
	if config.RetryAttempts == 0 {
		config.RetryAttempts = 2
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = time.Second
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 100
	}
	if config.MaxIdleConnsPerHost == 0 {
		config.MaxIdleConnsPerHost = 10
	}
	if config.IdleConnTimeout == 0 {
		config.IdleConnTimeout = 90 * time.Second
	}
	if config.UserAgent == "" {
		config.UserAgent = "media-toolkit-go/1.0"
	}

	// Create HTTP transport with optimized settings
	transport := &http.Transport{
		MaxIdleConns:        config.MaxIdleConns,
		MaxIdleConnsPerHost: config.MaxIdleConnsPerHost,
		IdleConnTimeout:     config.IdleConnTimeout,
		DisableKeepAlives:   config.DisableKeepAlives,
	}

	// Create HTTP client
	client := &http.Client{
		Transport: transport,
		Timeout:   config.Timeout,
	}

	logger.Info("HTTP client initialized",
		zap.Duration("timeout", config.Timeout),
		zap.Int("retry_attempts", config.RetryAttempts),
		zap.Int("max_idle_conns", config.MaxIdleConns),
		zap.Bool("disable_keep_alives", config.DisableKeepAlives))

	return &HTTPClient{
		client: client,
		config: config,
		logger: logger,
	}
}

// Get performs a GET request with retries
func (c *HTTPClient) Get(ctx context.Context, url string, opts ...RequestOption) (*Response, error) {
	return c.doRequest(ctx, "GET", url, opts...)
}

// Head performs a HEAD request with retries
func (c *HTTPClient) Head(ctx context.Context, url string, opts ...RequestOption) (*Response, error) {
	return c.doRequest(ctx, "HEAD", url, opts...)
}

// doRequest performs the actual HTTP request with retry logic
func (c *HTTPClient) doRequest(ctx context.Context, method, url string, opts ...RequestOption) (*Response, error) {
	// Apply request options
	reqConfig := &requestConfig{}
	for _, opt := range opts {
		opt(reqConfig)
	}

	response := &Response{
		Method: method,
		URL:    url,
	}

	var lastErr error
	startTime := time.Now()

	// Retry loop
	for attempt := 0; attempt <= c.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			response.Retries = attempt
			
			// Wait before retry
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.config.RetryDelay * time.Duration(attempt)):
			}

			c.logger.Debug("Retrying request",
				zap.String("method", method),
				zap.String("url", url),
				zap.Int("attempt", attempt))
		}

		// Create request
		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			continue
		}

		// Set headers
		req.Header.Set("User-Agent", c.config.UserAgent)
		for key, value := range reqConfig.headers {
			req.Header.Set(key, value)
		}

		// Override timeout if specified in options
		client := c.client
		if reqConfig.timeout > 0 {
			client = &http.Client{
				Transport: c.client.Transport,
				Timeout:   reqConfig.timeout,
			}
		}

		// Perform request
		requestStart := time.Now()
		resp, err := client.Do(req)
		ttfb := time.Since(requestStart)

		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			
			// Don't retry on context cancellation
			if ctx.Err() != nil {
				break
			}
			
			continue
		}

		// Read response body
		var body []byte
		if resp.Body != nil {
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				lastErr = fmt.Errorf("failed to read response body: %w", err)
				continue
			}
		}

		totalTime := time.Since(requestStart)

		// Populate response
		response.StatusCode = resp.StatusCode
		response.Status = resp.Status
		response.Headers = resp.Header
		response.Body = body
		response.ContentLength = resp.ContentLength
		response.TTFB = ttfb
		response.TotalTime = totalTime

		c.logger.Debug("Request completed",
			zap.String("method", method),
			zap.String("url", url),
			zap.Int("status_code", resp.StatusCode),
			zap.Duration("ttfb", ttfb),
			zap.Duration("total_time", totalTime),
			zap.Int("attempt", attempt))

		// Check if we should retry based on status code
		if resp.StatusCode >= 500 && attempt < c.config.RetryAttempts {
			lastErr = fmt.Errorf("server error: %s", resp.Status)
			continue
		}

		// Add connection close delay if configured
		if c.config.ConnectionCloseDelay > 0 {
			time.Sleep(c.config.ConnectionCloseDelay)
		}

		response.TotalTime = time.Since(startTime)
		return response, nil
	}

	// All retries exhausted
	if lastErr == nil {
		lastErr = fmt.Errorf("all retry attempts exhausted")
	}

	response.TotalTime = time.Since(startTime)
	return response, lastErr
}

// Close closes the HTTP client and its underlying transport
func (c *HTTPClient) Close() error {
	c.logger.Debug("Closing HTTP client")
	
	if transport, ok := c.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	
	return nil
}

// GetStats returns client statistics (could be extended with metrics)
func (c *HTTPClient) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"timeout":                  c.config.Timeout,
		"retry_attempts":          c.config.RetryAttempts,
		"max_idle_conns":          c.config.MaxIdleConns,
		"max_idle_conns_per_host": c.config.MaxIdleConnsPerHost,
		"idle_conn_timeout":       c.config.IdleConnTimeout,
		"disable_keep_alives":     c.config.DisableKeepAlives,
	}
}