from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta, timezone
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# API Limits and Constants
MAX_QUERIES_PER_5_MIN = 300
MIN_SLICE_MINUTES = 5
INITIAL_SLICE_MINUTES = 60
RATE_WINDOW_MINUTES = 5
MAX_TIME_RANGE_DAYS = 365
MIN_TIME_RANGE_MINUTES = 5
MAX_CONCURRENT_SLICES = 10  # Maximum concurrent time slices
DEFAULT_BATCH_SIZE = 5      # Default batch size for concurrent requests

@dataclass
class TimeSlice:
    """Time slice for concurrent processing."""
    start: datetime
    end: datetime
    size_minutes: int
    
    def __str__(self) -> str:
        return f"TimeSlice({self.start.isoformat()} to {self.end.isoformat()}, {self.size_minutes}min)"

    def __hash__(self) -> int:
        return hash((self.start, self.end, self.size_minutes))

ZONE_METRICS_BASIC_QUERY = """
query ZoneMetricsBasic($zoneTag: String!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter!) {
  viewer {
    zones(filter: { zoneTag: $zoneTag }) {
      httpRequestsAdaptiveGroups(
        limit: 5000
        filter: $filter
        orderBy: [datetimeMinute_DESC]
      ) {
        dimensions {
          datetimeMinute
          clientCountryName
          clientDeviceType
          clientRequestHTTPProtocol
          edgeResponseContentTypeName
          edgeResponseStatus
          cacheStatus
          coloCode
        }
        avg {
          sampleInterval
          edgeTimeToFirstByteMs
          originResponseDurationMs
        }
        sum {
          visits
          edgeResponseBytes
        }
        count
        ratio {
          status4xx
          status5xx
        }
      }
    }
  }
}
"""

ZONE_METRICS_DETAILED_QUERY = """
query ZoneMetricsDetailed($zoneTag: String!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter!) {
  viewer {
    zones(filter: { zoneTag: $zoneTag }) {
      httpRequestsAdaptiveGroups(
        limit: 5000
        filter: $filter
        orderBy: [datetimeMinute_DESC]
      ) {
        dimensions {
          datetimeMinute
          clientAsn
          clientIP
          clientRefererHost
          clientRequestHTTPHost
          clientRequestPath
          clientRequestHTTPMethodName
        }
        quantiles {
          edgeTimeToFirstByteMsP50
          edgeTimeToFirstByteMsP95
          edgeTimeToFirstByteMsP99
          originResponseDurationMsP50
          originResponseDurationMsP95
          originResponseDurationMsP99
        }
        sum {
          edgeTimeToFirstByteMs
          originResponseDurationMs
        }
      }
    }
  }
}
"""

def generate_time_slices(
    start_time: datetime,
    end_time: datetime,
    slice_minutes: int = INITIAL_SLICE_MINUTES,
    max_slices: int = MAX_CONCURRENT_SLICES
) -> List[TimeSlice]:
    """Generate optimal time slices for concurrent processing."""
    try:
        if not validate_time_range(start_time, end_time):
            return []

        # Round start and end times to nearest minute
        start = start_time.replace(second=0, microsecond=0)
        end = end_time.replace(second=0, microsecond=0)
        if end <= start:
            end = end + timedelta(minutes=1)

        total_minutes = (end - start).total_seconds() / 60
        
        # Calculate optimal slice size
        optimal_slice_size = max(
            min(total_minutes / max_slices, slice_minutes),
            MIN_SLICE_MINUTES
        )
        
        slices = []
        current = start
        
        while current < end:
            slice_end = min(
                current + timedelta(minutes=optimal_slice_size),
                end
            )
            # Ensure minimum slice size
            if (slice_end - current).total_seconds() / 60 >= MIN_SLICE_MINUTES:
                slices.append(TimeSlice(
                    start=current,
                    end=slice_end,
                    size_minutes=int((slice_end - current).total_seconds() / 60)
                ))
            current = slice_end

        logger.info(f"""
Time Slice Generation:
-------------------
Total Duration: {total_minutes:.1f} minutes
Slice Size: {optimal_slice_size:.1f} minutes
Generated Slices: {len(slices)}
First Slice: {slices[0] if slices else 'None'}
Last Slice: {slices[-1] if slices else 'None'}
""")

        return slices

    except Exception as e:
        logger.error(f"Error generating time slices: {str(e)}")
        return []

def validate_time_range(start_time: datetime, end_time: datetime) -> bool:
    """Validate time range with detailed error messages."""
    try:
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            logger.error("Invalid datetime objects provided")
            return False

        if start_time.tzinfo is None or end_time.tzinfo is None:
            logger.error("Timestamps must include timezone information")
            return False

        if start_time >= end_time:
            logger.error(f"Start time ({start_time.isoformat()}) must be before end time ({end_time.isoformat()})")
            return False

        time_range = end_time - start_time
        
        if time_range.days > MAX_TIME_RANGE_DAYS:
            logger.error(f"Time range exceeds maximum of {MAX_TIME_RANGE_DAYS} days (got {time_range.days} days)")
            return False

        if time_range.total_seconds() < MIN_TIME_RANGE_MINUTES * 60:
            logger.error(f"Time range must be at least {MIN_TIME_RANGE_MINUTES} minutes (got {time_range.total_seconds() / 60:.1f} minutes)")
            return False

        return True

    except Exception as e:
        logger.error(f"Error validating time range: {str(e)}")
        return False

def reduce_slice_size(current_minutes: int) -> int:
    """Calculate reduced slice size with validation."""
    try:
        if current_minutes <= MIN_SLICE_MINUTES:
            logger.warning(f"Cannot reduce slice size below minimum ({MIN_SLICE_MINUTES} minutes)")
            return MIN_SLICE_MINUTES

        new_size = max(MIN_SLICE_MINUTES, current_minutes // 2)
        logger.info(f"Reducing slice size from {current_minutes} to {new_size} minutes")
        return new_size

    except Exception as e:
        logger.error(f"Error reducing slice size: {str(e)}")
        return MIN_SLICE_MINUTES

def calculate_optimal_batch_size(
    total_slices: int,
    max_queries: int = MAX_QUERIES_PER_5_MIN,
    window_minutes: int = RATE_WINDOW_MINUTES
) -> int:
    """Calculate optimal batch size for concurrent processing."""
    try:
        # Account for both basic and detailed queries per slice
        queries_per_slice = 2
        max_concurrent = max_queries // queries_per_slice
        
        # Calculate optimal batch size considering rate limits
        optimal_batch = min(
            max_concurrent,
            total_slices,
            MAX_CONCURRENT_SLICES
        )
        
        logger.debug(f"""
Batch Size Calculation:
--------------------
Total Slices: {total_slices}
Max Queries per Window: {max_queries}
Queries per Slice: {queries_per_slice}
Optimal Batch Size: {optimal_batch}
""")
        
        return max(1, optimal_batch)

    except Exception as e:
        logger.error(f"Error calculating batch size: {str(e)}")
        return DEFAULT_BATCH_SIZE

def create_metrics_filter(
    start_time: datetime,
    end_time: datetime,
    sample_interval: Optional[int] = None
) -> Dict:
    """Create metrics filter for GraphQL query."""
    try:
        if not validate_time_range(start_time, end_time):
            raise ValueError("Invalid time range")

        # Format datetime strings properly
        start_str = start_time.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_time.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        filter_obj = {
            "datetimeMinute_gt": start_str,
            "datetimeMinute_lt": end_str
        }

        if sample_interval is not None:
            if not isinstance(sample_interval, int) or sample_interval <= 0:
                raise ValueError(f"Invalid sample interval: {sample_interval}")
            filter_obj["sampleInterval"] = sample_interval

        return filter_obj

    except Exception as e:
        logger.error(f"Error creating metrics filter: {str(e)}")
        return {
            "datetimeMinute_gt": start_time.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "datetimeMinute_lt": end_time.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

def split_range_into_batches(time_slices: List[TimeSlice], batch_size: int) -> List[List[TimeSlice]]:
    """Split time slices into batches for concurrent processing."""
    try:
        return [time_slices[i:i + batch_size] for i in range(0, len(time_slices), batch_size)]
    except Exception as e:
        logger.error(f"Error splitting time ranges: {str(e)}")
        return [time_slices]  # Return single batch on error
