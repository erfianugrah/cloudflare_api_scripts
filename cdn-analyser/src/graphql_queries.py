# graphql_queries.py
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta, timezone
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# GraphQL API Limits and Constants
MAX_QUERIES_PER_5_MIN = 300
MIN_SLICE_MINUTES = 5
INITIAL_SLICE_MINUTES = 60
RATE_WINDOW_MINUTES = 5
MAX_TIME_RANGE_DAYS = 365
MIN_TIME_RANGE_MINUTES = 5

@dataclass
class TimeSlice:
    start: datetime
    end: datetime
    size_minutes: int

def validate_time_range(start_time: datetime, end_time: datetime) -> bool:
    """
    Validate time range with detailed error messages.
    
    Args:
        start_time: Start of the time range
        end_time: End of the time range
        
    Returns:
        bool: True if time range is valid, False otherwise
    """
    try:
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            logger.error("Invalid datetime objects provided")
            return False

        # Ensure UTC timezone
        if start_time.tzinfo is None or end_time.tzinfo is None:
            logger.error("Timestamps must include timezone information")
            return False

        if start_time >= end_time:
            logger.error(f"Start time ({start_time.isoformat()}) must be before end time ({end_time.isoformat()})")
            return False

        time_range = end_time - start_time
        
        if time_range.days > MAX_TIME_RANGE_DAYS:
            logger.error(f"Time range exceeds maximum of {MAX_TIME_RANGE_DAYS} days "
                        f"(got {time_range.days} days)")
            return False

        if time_range.total_seconds() < MIN_TIME_RANGE_MINUTES * 60:
            logger.error(f"Time range must be at least {MIN_TIME_RANGE_MINUTES} minutes "
                        f"(got {time_range.total_seconds() / 60:.1f} minutes)")
            return False

        return True

    except Exception as e:
        logger.error(f"Error validating time range: {str(e)}")
        return False

def generate_time_slices(
    start_time: datetime,
    end_time: datetime,
    slice_minutes: int = INITIAL_SLICE_MINUTES
) -> List[TimeSlice]:
    """
    Generate time slices for paginated data fetching with validation.
    
    Args:
        start_time: Start of the overall time range
        end_time: End of the overall time range
        slice_minutes: Size of each time slice in minutes
        
    Returns:
        List[TimeSlice]: List of time slices
    """
    try:
        slices = []
        current = start_time

        # Validate inputs
        if not validate_time_range(start_time, end_time):
            return []

        if slice_minutes < MIN_SLICE_MINUTES:
            logger.error(f"Slice size must be at least {MIN_SLICE_MINUTES} minutes")
            return []

        # Generate slices
        while current < end_time:
            slice_end = min(current + timedelta(minutes=slice_minutes), end_time)
            slices.append(TimeSlice(current, slice_end, slice_minutes))
            current = slice_end

        if not slices:
            logger.error("No valid time slices generated")
            return []

        logger.info(f"Generated {len(slices)} time slices of {slice_minutes} minutes each")
        logger.info(f"First slice: {slices[0].start.isoformat()} to {slices[0].end.isoformat()}")
        logger.info(f"Last slice: {slices[-1].start.isoformat()} to {slices[-1].end.isoformat()}")

        return slices

    except Exception as e:
        logger.error(f"Error generating time slices: {str(e)}")
        return []

def reduce_slice_size(current_minutes: int) -> int:
    """
    Calculate reduced slice size with validation.
    
    Args:
        current_minutes: Current slice size in minutes
        
    Returns:
        int: New slice size in minutes
    """
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

# GraphQL Query with all required fields and documentation
ZONE_METRICS_QUERY = """
query ZoneMetrics($zoneTag: String!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter!) {
  viewer {
    zones(filter: { zoneTag: $zoneTag }) {
      httpRequestsAdaptiveGroups(
        limit: 1000
        filter: $filter
        orderBy: [datetime_ASC]
      ) {
        dimensions {
          datetime
          clientCountryName
          clientDeviceType
          clientRequestHTTPProtocol
          edgeResponseContentTypeName
          edgeResponseStatus
          cacheStatus
          coloCode
          clientAsn
          clientIP
          clientRefererHost
          clientRequestHTTPHost
          clientRequestPath
          clientRequestHTTPMethodName
        }
        avg {
          sampleInterval
          edgeTimeToFirstByteMs
          originResponseDurationMs
          edgeDnsResponseTimeMs
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
          visits
          edgeResponseBytes
          edgeTimeToFirstByteMs
          originResponseDurationMs
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

def validate_graphql_response(response_data: Dict) -> Tuple[bool, str]:
    """
    Validate GraphQL response structure.
    
    Args:
        response_data: GraphQL response data
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    try:
        if not isinstance(response_data, dict):
            return False, "Response is not a dictionary"

        if 'errors' in response_data:
            errors = response_data['errors']
            error_messages = [f"{e.get('message', 'Unknown error')}" for e in errors]
            return False, f"GraphQL errors: {'; '.join(error_messages)}"

        if 'data' not in response_data:
            return False, "No 'data' field in response"

        viewer = response_data['data'].get('viewer', {})
        zones = viewer.get('zones', [])

        if not zones:
            return False, "No zones data in response"

        metrics = zones[0].get('httpRequestsAdaptiveGroups', [])

        if not metrics:
            return False, "No metrics data in response"

        return True, "Response is valid"

    except Exception as e:
        return False, f"Error validating response: {str(e)}"

def create_metrics_filter(
    start_time: datetime,
    end_time: datetime,
    sample_interval: Optional[int] = None
) -> Dict:
    """
    Create a validated GraphQL metrics filter.
    
    Args:
        start_time: Start of the time range
        end_time: End of the time range
        sample_interval: Optional sampling interval
        
    Returns:
        Dict: GraphQL filter object
    """
    try:
        if not validate_time_range(start_time, end_time):
            raise ValueError("Invalid time range")

        filter_obj = {
            "datetime_geq": start_time.isoformat(),
            "datetime_leq": end_time.isoformat()
        }

        if sample_interval is not None:
            if not isinstance(sample_interval, int) or sample_interval <= 0:
                raise ValueError(f"Invalid sample interval: {sample_interval}")
            filter_obj["sampleInterval"] = sample_interval

        return filter_obj

    except Exception as e:
        logger.error(f"Error creating metrics filter: {str(e)}")
        return {
            "datetime_geq": start_time.isoformat(),
            "datetime_leq": end_time.isoformat()
        }
