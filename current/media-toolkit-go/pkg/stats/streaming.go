package stats

import (
	"math"
	"sync"
	"time"
)

// StreamingStats provides memory-efficient statistics collection with O(1) memory usage
// using mutex for thread safety
type StreamingStats struct {
	mu          sync.RWMutex
	count       int64   // Number of values processed
	sum         float64 // Sum of all values
	sumSquares  float64 // Sum of squares for variance calculation
	min         float64 // Minimum value seen
	max         float64 // Maximum value seen
	lastUpdated int64   // Unix timestamp of last update
}

// NewStreamingStats creates a new StreamingStats instance
func NewStreamingStats() *StreamingStats {
	return &StreamingStats{
		count:       0,
		sum:         0,
		sumSquares:  0,
		min:         math.Inf(1),  // Positive infinity
		max:         math.Inf(-1), // Negative infinity
		lastUpdated: time.Now().Unix(),
	}
}

// Update adds a new value to the statistics
func (s *StreamingStats) Update(value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.count++
	s.sum += value
	s.sumSquares += value * value

	if value < s.min {
		s.min = value
	}
	if value > s.max {
		s.max = value
	}

	s.lastUpdated = time.Now().Unix()
}

// Count returns the number of values processed
func (s *StreamingStats) Count() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// Sum returns the sum of all values
func (s *StreamingStats) Sum() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sum
}

// Mean returns the arithmetic mean of all values
func (s *StreamingStats) Mean() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.count == 0 {
		return 0
	}
	return s.sum / float64(s.count)
}

// Min returns the minimum value seen
func (s *StreamingStats) Min() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if math.IsInf(s.min, 1) {
		return 0 // No values yet
	}
	return s.min
}

// Max returns the maximum value seen
func (s *StreamingStats) Max() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if math.IsInf(s.max, -1) {
		return 0 // No values yet
	}
	return s.max
}

// Variance returns the sample variance
func (s *StreamingStats) Variance() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.count <= 1 {
		return 0
	}

	mean := s.sum / float64(s.count)

	// Sample variance formula: (sum_squares - n * mean^2) / (n - 1)
	return (s.sumSquares - float64(s.count)*mean*mean) / float64(s.count-1)
}

// StandardDeviation returns the sample standard deviation
func (s *StreamingStats) StandardDeviation() float64 {
	return math.Sqrt(s.Variance())
}

// LastUpdated returns the Unix timestamp of the last update
func (s *StreamingStats) LastUpdated() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Unix(s.lastUpdated, 0)
}

// Summary returns a snapshot of all statistics
type Summary struct {
	Count      int64     `json:"count"`
	Sum        float64   `json:"sum"`
	Mean       float64   `json:"mean"`
	Min        float64   `json:"min"`
	Max        float64   `json:"max"`
	Variance   float64   `json:"variance"`
	StdDev     float64   `json:"std_dev"`
	LastUpdate time.Time `json:"last_update"`
}

// GetSummary returns a consistent snapshot of all statistics
func (s *StreamingStats) GetSummary() Summary {
	return Summary{
		Count:      s.Count(),
		Sum:        s.Sum(),
		Mean:       s.Mean(),
		Min:        s.Min(),
		Max:        s.Max(),
		Variance:   s.Variance(),
		StdDev:     s.StandardDeviation(),
		LastUpdate: s.LastUpdated(),
	}
}

// Reset clears all statistics
func (s *StreamingStats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.count = 0
	s.sum = 0
	s.sumSquares = 0
	s.min = math.Inf(1)
	s.max = math.Inf(-1)
	s.lastUpdated = time.Now().Unix()
}
