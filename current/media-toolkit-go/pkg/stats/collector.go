package stats

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// Collector aggregates various statistics during media processing
type Collector struct {
	// Processing statistics
	RequestTimes     *StreamingStats
	ResponseSizes    *StreamingStats
	SizeReduction    *SizeReductionStats
	
	// Counters
	totalRequests    int64
	successfulReqs   int64
	failedRequests   int64
	timeouts         int64
	
	// Error tracking
	errorsMu         sync.RWMutex
	errors           map[string]int64
	
	// Performance metrics
	startTime        time.Time
	lastUpdate       int64
	
	// Worker pool statistics
	workersActive    int64
	workersIdle      int64
	queueSize        int64
}

// NewCollector creates a new statistics collector
func NewCollector() *Collector {
	return &Collector{
		RequestTimes:   NewStreamingStats(),
		ResponseSizes:  NewStreamingStats(),
		SizeReduction:  NewSizeReductionStats(),
		totalRequests:  0,
		successfulReqs: 0,
		failedRequests: 0,
		timeouts:       0,
		errors:         make(map[string]int64),
		startTime:      time.Now(),
		lastUpdate:     time.Now().Unix(),
		workersActive:  0,
		workersIdle:    0,
		queueSize:      0,
	}
}

// RecordRequest records a completed request
func (c *Collector) RecordRequest(duration time.Duration, responseSize int64, success bool) {
	atomic.AddInt64(&c.totalRequests, 1)
	
	if success {
		atomic.AddInt64(&c.successfulReqs, 1)
		c.RequestTimes.Update(float64(duration.Milliseconds()))
		if responseSize > 0 {
			c.ResponseSizes.Update(float64(responseSize))
		}
	} else {
		atomic.AddInt64(&c.failedRequests, 1)
	}
	
	atomic.StoreInt64(&c.lastUpdate, time.Now().Unix())
}

// RecordTimeout records a request timeout
func (c *Collector) RecordTimeout() {
	atomic.AddInt64(&c.timeouts, 1)
	atomic.AddInt64(&c.failedRequests, 1)
	atomic.AddInt64(&c.totalRequests, 1)
	atomic.StoreInt64(&c.lastUpdate, time.Now().Unix())
}

// RecordError records an error with a specific message
func (c *Collector) RecordError(errorMsg string) {
	c.errorsMu.Lock()
	c.errors[errorMsg]++
	c.errorsMu.Unlock()
	
	atomic.AddInt64(&c.failedRequests, 1)
	atomic.AddInt64(&c.totalRequests, 1)
	atomic.StoreInt64(&c.lastUpdate, time.Now().Unix())
}

// RecordSizeReduction records file size optimization
func (c *Collector) RecordSizeReduction(originalSize, optimizedSize int64) {
	c.SizeReduction.UpdateSizeReduction(originalSize, optimizedSize)
	atomic.StoreInt64(&c.lastUpdate, time.Now().Unix())
}

// UpdateWorkerStats updates worker pool statistics
func (c *Collector) UpdateWorkerStats(active, idle, queueSize int64) {
	atomic.StoreInt64(&c.workersActive, active)
	atomic.StoreInt64(&c.workersIdle, idle)
	atomic.StoreInt64(&c.queueSize, queueSize)
	atomic.StoreInt64(&c.lastUpdate, time.Now().Unix())
}

// TotalRequests returns the total number of requests made
func (c *Collector) TotalRequests() int64 {
	return atomic.LoadInt64(&c.totalRequests)
}

// SuccessfulRequests returns the number of successful requests
func (c *Collector) SuccessfulRequests() int64 {
	return atomic.LoadInt64(&c.successfulReqs)
}

// FailedRequests returns the number of failed requests
func (c *Collector) FailedRequests() int64 {
	return atomic.LoadInt64(&c.failedRequests)
}

// Timeouts returns the number of timeout errors
func (c *Collector) Timeouts() int64 {
	return atomic.LoadInt64(&c.timeouts)
}

// SuccessRate returns the success rate as a percentage (0-100)
func (c *Collector) SuccessRate() float64 {
	total := c.TotalRequests()
	if total == 0 {
		return 0
	}
	return float64(c.SuccessfulRequests()) / float64(total) * 100
}

// RequestsPerSecond returns the average requests per second since start
func (c *Collector) RequestsPerSecond() float64 {
	elapsed := time.Since(c.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(c.TotalRequests()) / elapsed
}

// WorkersActive returns the number of currently active workers
func (c *Collector) WorkersActive() int64 {
	return atomic.LoadInt64(&c.workersActive)
}

// WorkersIdle returns the number of currently idle workers
func (c *Collector) WorkersIdle() int64 {
	return atomic.LoadInt64(&c.workersIdle)
}

// QueueSize returns the current queue size
func (c *Collector) QueueSize() int64 {
	return atomic.LoadInt64(&c.queueSize)
}

// GetErrors returns a copy of the error counts
func (c *Collector) GetErrors() map[string]int64 {
	c.errorsMu.RLock()
	defer c.errorsMu.RUnlock()
	
	errors := make(map[string]int64, len(c.errors))
	for k, v := range c.errors {
		errors[k] = v
	}
	return errors
}

// LastUpdated returns the time of the last update
func (c *Collector) LastUpdated() time.Time {
	timestamp := atomic.LoadInt64(&c.lastUpdate)
	return time.Unix(timestamp, 0)
}

// CollectorSummary provides a comprehensive summary of all statistics
type CollectorSummary struct {
	// Request statistics
	TotalRequests     int64   `json:"total_requests"`
	SuccessfulReqs    int64   `json:"successful_requests"`
	FailedRequests    int64   `json:"failed_requests"`
	Timeouts          int64   `json:"timeouts"`
	SuccessRate       float64 `json:"success_rate_percent"`
	RequestsPerSecond float64 `json:"requests_per_second"`
	
	// Timing statistics
	RequestTimeStats  Summary `json:"request_time_stats"`
	ResponseSizeStats Summary `json:"response_size_stats"`
	
	// Size reduction statistics
	SizeReduction SizeReductionSummary `json:"size_reduction"`
	
	// Worker statistics
	WorkersActive int64 `json:"workers_active"`
	WorkersIdle   int64 `json:"workers_idle"`
	QueueSize     int64 `json:"queue_size"`
	
	// Error breakdown
	Errors map[string]int64 `json:"errors"`
	
	// Timing
	StartTime   time.Time `json:"start_time"`
	LastUpdate  time.Time `json:"last_update"`
	ElapsedTime string    `json:"elapsed_time"`
}

// GetSummary returns a comprehensive summary of all statistics
func (c *Collector) GetSummary() CollectorSummary {
	elapsed := time.Since(c.startTime)
	
	return CollectorSummary{
		TotalRequests:     c.TotalRequests(),
		SuccessfulReqs:    c.SuccessfulRequests(),
		FailedRequests:    c.FailedRequests(),
		Timeouts:          c.Timeouts(),
		SuccessRate:       c.SuccessRate(),
		RequestsPerSecond: c.RequestsPerSecond(),
		RequestTimeStats:  c.RequestTimes.GetSummary(),
		ResponseSizeStats: c.ResponseSizes.GetSummary(),
		SizeReduction:     c.SizeReduction.GetSummary(),
		WorkersActive:     c.WorkersActive(),
		WorkersIdle:       c.WorkersIdle(),
		QueueSize:         c.QueueSize(),
		Errors:            c.GetErrors(),
		StartTime:         c.startTime,
		LastUpdate:        c.LastUpdated(),
		ElapsedTime:       elapsed.String(),
	}
}

// ToJSON returns the summary as JSON
func (c *Collector) ToJSON() ([]byte, error) {
	summary := c.GetSummary()
	return json.MarshalIndent(summary, "", "  ")
}

// Reset clears all statistics
func (c *Collector) Reset() {
	c.RequestTimes.Reset()
	c.ResponseSizes.Reset()
	c.SizeReduction.Reset()
	
	atomic.StoreInt64(&c.totalRequests, 0)
	atomic.StoreInt64(&c.successfulReqs, 0)
	atomic.StoreInt64(&c.failedRequests, 0)
	atomic.StoreInt64(&c.timeouts, 0)
	atomic.StoreInt64(&c.workersActive, 0)
	atomic.StoreInt64(&c.workersIdle, 0)
	atomic.StoreInt64(&c.queueSize, 0)
	
	c.errorsMu.Lock()
	c.errors = make(map[string]int64)
	c.errorsMu.Unlock()
	
	c.startTime = time.Now()
	atomic.StoreInt64(&c.lastUpdate, time.Now().Unix())
}