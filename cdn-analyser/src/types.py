# types.py
from typing import TypedDict, List, Dict, Union, Optional
from datetime import datetime

class SamplingMetrics(TypedDict):
    sampling_rate: float
    sample_interval: int
    confidence_score: float
    sampled_requests: int
    estimated_requests: int

class PerformanceMetrics(TypedDict):
    ttfb_avg: float
    ttfb_p50: float
    ttfb_p75: float
    ttfb_p90: float
    ttfb_p95: float
    ttfb_p99: float
    origin_time_avg: float
    origin_time_p50: float
    origin_time_p75: float
    origin_time_p90: float
    origin_time_p95: float
    origin_time_p99: float

class CacheMetrics(TypedDict):
    cache_status: str
    cache_category: str
    hit_ratio: float
    bytes: int
    bytes_adjusted: int
    visits: int
    visits_adjusted: int

class ClientMetrics(TypedDict):
    country: str
    asn: str
    device_type: str
    protocol: str
    content_type: str

class MetricDimensions(TypedDict):
    datetime: str
    clientCountryName: str
    clientASN: str
    clientDeviceType: str
    clientRequestHTTPHost: str
    clientRequestPath: str
    clientRequestHTTPProtocol: str
    clientRequestMethod: str
    edgeResponseContentTypeName: str
    edgeResponseStatus: int
    cacheStatus: str
    coloCode: str

class MetricAverages(TypedDict):
    edgeTimeToFirstByteMs: float
    originResponseDurationMs: float
    sampleInterval: int
    edgeResponseBytes: int

class MetricQuantiles(TypedDict):
    edgeTimeToFirstByteMsP10: float
    edgeTimeToFirstByteMsP25: float
    edgeTimeToFirstByteMsP50: float
    edgeTimeToFirstByteMsP75: float
    edgeTimeToFirstByteMsP90: float
    edgeTimeToFirstByteMsP95: float
    edgeTimeToFirstByteMsP99: float
    originResponseDurationMsP10: float
    originResponseDurationMsP25: float
    originResponseDurationMsP50: float
    originResponseDurationMsP75: float
    originResponseDurationMsP90: float
    originResponseDurationMsP95: float
    originResponseDurationMsP99: float

class MetricSums(TypedDict):
    visits: int
    edgeResponseBytes: int
    encryptedBytes: int
    encryptedRequests: int

class MetricGroup(TypedDict):
    dimensions: MetricDimensions
    avg: MetricAverages
    quantiles: MetricQuantiles
    sum: MetricSums
    count: int

class AnalysisResult(TypedDict):
    zone_name: str
    overall: Dict[str, Union[int, float]]
    sampling_metrics: SamplingMetrics
    performance_metrics: PerformanceMetrics
    cache_metrics: CacheMetrics
    client_metrics: ClientMetrics
    temporal_analysis: Dict[str, Dict[str, Union[int, float]]]
