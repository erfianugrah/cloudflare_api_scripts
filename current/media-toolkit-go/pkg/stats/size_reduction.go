package stats

import (
	"sync/atomic"
	"time"
)

// SizeReductionStats tracks file size reduction metrics during optimization
type SizeReductionStats struct {
	originalSize   *StreamingStats
	optimizedSize  *StreamingStats
	reductionRatio *StreamingStats
	filesProcessed int64
	bytesSaved     int64
	lastUpdated    int64
}

// NewSizeReductionStats creates a new SizeReductionStats instance
func NewSizeReductionStats() *SizeReductionStats {
	return &SizeReductionStats{
		originalSize:   NewStreamingStats(),
		optimizedSize:  NewStreamingStats(),
		reductionRatio: NewStreamingStats(),
		filesProcessed: 0,
		bytesSaved:     0,
		lastUpdated:    time.Now().Unix(),
	}
}

// UpdateSizeReduction records a file size reduction
func (s *SizeReductionStats) UpdateSizeReduction(originalBytes, optimizedBytes int64) {
	if originalBytes <= 0 || optimizedBytes < 0 {
		return // Invalid input
	}

	atomic.AddInt64(&s.filesProcessed, 1)

	// Calculate reduction ratio (0-1, where 1 = 100% reduction)
	reductionRatio := 0.0
	if originalBytes > 0 {
		reductionRatio = float64(originalBytes-optimizedBytes) / float64(originalBytes)
	}

	// Update statistics
	s.originalSize.Update(float64(originalBytes))
	s.optimizedSize.Update(float64(optimizedBytes))
	s.reductionRatio.Update(reductionRatio)

	// Update bytes saved
	bytesSaved := originalBytes - optimizedBytes
	if bytesSaved > 0 {
		atomic.AddInt64(&s.bytesSaved, bytesSaved)
	}

	atomic.StoreInt64(&s.lastUpdated, time.Now().Unix())
}

// FilesProcessed returns the number of files processed
func (s *SizeReductionStats) FilesProcessed() int64 {
	return atomic.LoadInt64(&s.filesProcessed)
}

// BytesSaved returns the total bytes saved
func (s *SizeReductionStats) BytesSaved() int64 {
	return atomic.LoadInt64(&s.bytesSaved)
}

// AverageReductionRatio returns the average reduction ratio (0-1)
func (s *SizeReductionStats) AverageReductionRatio() float64 {
	return s.reductionRatio.Mean()
}

// AverageReductionPercentage returns the average reduction percentage (0-100)
func (s *SizeReductionStats) AverageReductionPercentage() float64 {
	return s.AverageReductionRatio() * 100
}

// TotalOriginalSize returns the total original size of all processed files
func (s *SizeReductionStats) TotalOriginalSize() int64 {
	return int64(s.originalSize.Sum())
}

// TotalOptimizedSize returns the total optimized size of all processed files
func (s *SizeReductionStats) TotalOptimizedSize() int64 {
	return int64(s.optimizedSize.Sum())
}

// OverallReductionRatio returns the overall reduction ratio based on total sizes
func (s *SizeReductionStats) OverallReductionRatio() float64 {
	totalOriginal := s.TotalOriginalSize()
	if totalOriginal == 0 {
		return 0
	}

	totalOptimized := s.TotalOptimizedSize()
	return float64(totalOriginal-totalOptimized) / float64(totalOriginal)
}

// OverallReductionPercentage returns the overall reduction percentage (0-100)
func (s *SizeReductionStats) OverallReductionPercentage() float64 {
	return s.OverallReductionRatio() * 100
}

// LastUpdated returns the time of the last update
func (s *SizeReductionStats) LastUpdated() time.Time {
	timestamp := atomic.LoadInt64(&s.lastUpdated)
	return time.Unix(timestamp, 0)
}

// SizeReductionSummary provides a comprehensive summary of size reduction metrics
type SizeReductionSummary struct {
	FilesProcessed          int64     `json:"files_processed"`
	BytesSaved              int64     `json:"bytes_saved"`
	TotalOriginalSize       int64     `json:"total_original_size"`
	TotalOptimizedSize      int64     `json:"total_optimized_size"`
	AverageReductionRatio   float64   `json:"average_reduction_ratio"`
	AverageReductionPercent float64   `json:"average_reduction_percent"`
	OverallReductionRatio   float64   `json:"overall_reduction_ratio"`
	OverallReductionPercent float64   `json:"overall_reduction_percent"`
	OriginalSizeStats       Summary   `json:"original_size_stats"`
	OptimizedSizeStats      Summary   `json:"optimized_size_stats"`
	ReductionRatioStats     Summary   `json:"reduction_ratio_stats"`
	LastUpdate              time.Time `json:"last_update"`
}

// GetSummary returns a comprehensive summary of all size reduction statistics
func (s *SizeReductionStats) GetSummary() SizeReductionSummary {
	return SizeReductionSummary{
		FilesProcessed:          s.FilesProcessed(),
		BytesSaved:              s.BytesSaved(),
		TotalOriginalSize:       s.TotalOriginalSize(),
		TotalOptimizedSize:      s.TotalOptimizedSize(),
		AverageReductionRatio:   s.AverageReductionRatio(),
		AverageReductionPercent: s.AverageReductionPercentage(),
		OverallReductionRatio:   s.OverallReductionRatio(),
		OverallReductionPercent: s.OverallReductionPercentage(),
		OriginalSizeStats:       s.originalSize.GetSummary(),
		OptimizedSizeStats:      s.optimizedSize.GetSummary(),
		ReductionRatioStats:     s.reductionRatio.GetSummary(),
		LastUpdate:              s.LastUpdated(),
	}
}

// Reset clears all size reduction statistics
func (s *SizeReductionStats) Reset() {
	s.originalSize.Reset()
	s.optimizedSize.Reset()
	s.reductionRatio.Reset()
	atomic.StoreInt64(&s.filesProcessed, 0)
	atomic.StoreInt64(&s.bytesSaved, 0)
	atomic.StoreInt64(&s.lastUpdated, time.Now().Unix())
}
