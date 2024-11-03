from typing import TypedDict, List, Dict, Union, Optional
from datetime import datetime

class MetricDimensions(TypedDict):
    datetime: str
    clientCountryName: str
    clientRequestHTTPHost: str
    clientRequestPath: str
    clientRequestHTTPProtocol: str
    clientRequestHTTPMethodName: str
    edgeResponseContentTypeName: str
    edgeResponseStatus: int
    cacheStatus: str
    coloCode: str

class MetricAverages(TypedDict):
    edgeTimeToFirstByteMs: float
    originResponseDurationMs: float
    edgeDnsResponseTimeMs: float
    sampleInterval: int

class MetricQuantiles(TypedDict):
    edgeDnsResponseTimeMsP50: float
    edgeDnsResponseTimeMsP95: float
    edgeDnsResponseTimeMsP99: float
    edgeTimeToFirstByteMsP50: float
    edgeTimeToFirstByteMsP95: float
    edgeTimeToFirstByteMsP99: float
    originResponseDurationMsP50: float
    originResponseDurationMsP95: float
    originResponseDurationMsP99: float

class MetricSums(TypedDict):
    edgeResponseBytes: int
    visits: int

class MetricRatios(TypedDict):
    status4xx: float
    status5xx: float

class MetricGroup(TypedDict):
    dimensions: MetricDimensions
    avg: MetricAverages
    quantiles: MetricQuantiles
    sum: MetricSums
    ratio: MetricRatios
    count: int

class ZoneMetrics(TypedDict):
    httpRequestsAdaptiveGroups: List[MetricGroup]

class AnalysisResult(TypedDict):
    zone_name: str
    overall: Dict[str, Union[int, float]]
    by_content_type: Dict[str, Dict[str, Union[int, float]]]
    by_path: Dict[str, Dict[str, Union[int, float]]]
    by_country: Dict[str, Dict[str, Union[int, float]]]
    by_status: Dict[str, Dict[str, Union[int, float]]]
    by_time: Dict[str, Dict[str, Dict[str, Union[int, float]]]]
