from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

ZONE_METRICS_QUERY = """
query ZoneMetrics($zoneTag: String!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter!) {
  viewer {
    zones(filter: { zoneTag: $zoneTag }) {
      httpRequestsAdaptiveGroups(
        limit: 1000,
        filter: $filter,
        orderBy: [datetime_ASC]
      ) {
        dimensions {
          datetime
          clientCountryName
          clientDeviceType
          cacheStatus
          edgeResponseContentTypeName
          clientRequestHTTPProtocol
          edgeResponseStatus
          coloCode
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
