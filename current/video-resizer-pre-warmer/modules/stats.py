"""
Streaming statistics module for memory-efficient metrics collection.
"""
import math
import logging

logger = logging.getLogger(__name__)

class StreamingStats:
    """
    Memory-efficient streaming statistics calculator.
    Maintains running statistics without storing individual values.
    """
    
    def __init__(self, name=""):
        self.name = name
        self.count = 0
        self.sum = 0.0
        self.sum_of_squares = 0.0
        self.min_val = float('inf')
        self.max_val = float('-inf')
        
        # For percentile estimation using P-square algorithm
        self._p2_markers = None
        self._p2_positions = None
        self._p2_desired = None
        self._p2_increments = None
        
    def add(self, value):
        """Add a new value to the statistics."""
        if value is None or math.isnan(value):
            return
            
        self.count += 1
        self.sum += value
        self.sum_of_squares += value * value
        self.min_val = min(self.min_val, value)
        self.max_val = max(self.max_val, value)
        
        # Initialize P-square algorithm for percentile estimation on 5th value
        if self.count == 5 and self._p2_markers is None:
            self._init_p2_algorithm()
        elif self.count > 5 and self._p2_markers is not None:
            self._update_p2_algorithm(value)
    
    @property
    def mean(self):
        """Calculate the mean value."""
        return self.sum / self.count if self.count > 0 else 0.0
    
    @property
    def variance(self):
        """Calculate the variance."""
        if self.count < 2:
            return 0.0
        # Using the computational formula for variance
        mean_of_squares = self.sum_of_squares / self.count
        square_of_mean = (self.sum / self.count) ** 2
        return max(0, mean_of_squares - square_of_mean)
    
    @property
    def std_dev(self):
        """Calculate the standard deviation."""
        return math.sqrt(self.variance)
    
    @property
    def min(self):
        """Get minimum value."""
        return self.min_val if self.count > 0 else None
    
    @property
    def max(self):
        """Get maximum value."""
        return self.max_val if self.count > 0 else None
    
    def percentile(self, p):
        """
        Estimate percentile using P-square algorithm.
        Only available after 5+ values have been added.
        
        Args:
            p: Percentile to estimate (0-100)
            
        Returns:
            Estimated percentile value or None if not enough data
        """
        if self.count < 5 or self._p2_markers is None:
            return None
            
        # For now, return simple approximations
        if p <= 0:
            return self.min_val
        elif p >= 100:
            return self.max_val
        elif p == 50:
            # Rough median approximation
            return self.mean
        else:
            # Linear interpolation between min and max (very rough)
            return self.min_val + (self.max_val - self.min_val) * (p / 100.0)
    
    def _init_p2_algorithm(self):
        """Initialize P-square algorithm for percentile tracking."""
        # This is a simplified version - full implementation would track multiple percentiles
        # For now, we'll use simpler approximations
        pass
    
    def _update_p2_algorithm(self, value):
        """Update P-square markers with new value."""
        # Simplified - full implementation would maintain markers
        pass
    
    def to_dict(self):
        """Convert statistics to dictionary format."""
        if self.count == 0:
            return {
                'count': 0,
                'sum': 0,
                'mean': 0,
                'min': None,
                'max': None,
                'std_dev': 0
            }
            
        return {
            'count': self.count,
            'sum': self.sum,
            'mean': self.mean,
            'min': self.min_val,
            'max': self.max_val,
            'std_dev': self.std_dev,
            'variance': self.variance
        }
    
    def merge(self, other):
        """
        Merge another StreamingStats object into this one.
        Useful for combining statistics from multiple workers.
        
        Args:
            other: Another StreamingStats instance
        """
        if other.count == 0:
            return
            
        if self.count == 0:
            self.count = other.count
            self.sum = other.sum
            self.sum_of_squares = other.sum_of_squares
            self.min_val = other.min_val
            self.max_val = other.max_val
        else:
            self.count += other.count
            self.sum += other.sum
            self.sum_of_squares += other.sum_of_squares
            self.min_val = min(self.min_val, other.min_val)
            self.max_val = max(self.max_val, other.max_val)
    
    def __str__(self):
        """String representation of statistics."""
        if self.count == 0:
            return f"StreamingStats({self.name}): No data"
        return (f"StreamingStats({self.name}): "
                f"count={self.count}, mean={self.mean:.2f}, "
                f"std={self.std_dev:.2f}, min={self.min_val:.2f}, "
                f"max={self.max_val:.2f}")


class SizeReductionStats:
    """Track size reduction statistics efficiently."""
    
    def __init__(self):
        self.count = 0
        self.total_original = 0
        self.total_transformed = 0
        self.reduction_percentages = StreamingStats("reduction_percent")
        
    def add(self, original_size, transformed_size):
        """Add a size reduction data point."""
        if original_size <= 0:
            return
            
        self.count += 1
        self.total_original += original_size
        self.total_transformed += transformed_size
        
        reduction_percent = ((original_size - transformed_size) / original_size) * 100
        self.reduction_percentages.add(reduction_percent)
    
    @property
    def total_reduction_bytes(self):
        """Total bytes saved."""
        return self.total_original - self.total_transformed
    
    @property
    def overall_reduction_percent(self):
        """Overall reduction percentage."""
        if self.total_original == 0:
            return 0.0
        return (self.total_reduction_bytes / self.total_original) * 100
    
    def to_dict(self):
        """Convert to dictionary format."""
        return {
            'count': self.count,
            'total_original_bytes': self.total_original,
            'total_transformed_bytes': self.total_transformed,
            'total_reduction_bytes': self.total_reduction_bytes,
            'overall_reduction_percent': self.overall_reduction_percent,
            'reduction_stats': self.reduction_percentages.to_dict()
        }