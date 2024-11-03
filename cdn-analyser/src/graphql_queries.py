ZONE_METRICS_QUERY = """
query ZoneMetrics($zoneTag: string!, $filter: ZoneHttpRequestsAdaptiveGroupsFilter_InputObject!) {
  viewer {
    zones(filter: { zoneTag: $zoneTag }) {
      httpRequestsAdaptiveGroups(
        limit: 100,
        filter: $filter
      ) {
        dimensions {
          datetime
          clientCountryName
          clientRequestHTTPHost
          clientRequestPath
          clientRequestHTTPProtocol
          clientRequestHTTPMethodName
          edgeResponseContentTypeName
          edgeResponseStatus
          cacheStatus
          coloCode
        }
        avg {
          edgeTimeToFirstByteMs
          originResponseDurationMs
          edgeDnsResponseTimeMs
          sampleInterval
        }
        quantiles {
          edgeDnsResponseTimeMsP50
          edgeDnsResponseTimeMsP95
          edgeDnsResponseTimeMsP99
          edgeTimeToFirstByteMsP50
          edgeTimeToFirstByteMsP95
          edgeTimeToFirstByteMsP99
          originResponseDurationMsP50
          originResponseDurationMsP95
          originResponseDurationMsP99
        }
        sum {
          edgeResponseBytes
          visits
        }
        ratio {
          status4xx
          status5xx
        }
        count
      }
    }
  }
}
"""
