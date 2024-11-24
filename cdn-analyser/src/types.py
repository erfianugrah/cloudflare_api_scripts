from typing import TypedDict, List, Dict, Union, Optional
from datetime import datetime

class NetworkPathMetrics(TypedDict):
    client_asn: str
    client_asn_desc: str
    origin_asn: str
    origin_asn_desc: str
    colo_code: str
    upper_tier_colo: Optional[str]
    protocol: str
    country: str

class RequestDetails(TypedDict):
    method: str
    host: str
    path: str
    client_ip: str
    referer: str
    content_type: str
    status_code: int

class PerformanceMetrics(TypedDict):
    ttfb_avg: float
    ttfb_p50: float
    ttfb_p95: float
    ttfb_p99: float
    origin_time_avg: float
    origin_time_p50: float
    origin_time_p95: float
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Union, Any

@dataclass
class OriginPerformanceMetrics:
    """Container for origin server performance metrics"""
    response_time: Dict[str, float]  # avg, p50, p95, p99, std
    request_volume: Dict[str, float]  # total, per_second
    failure_rates: Dict[str, float]  # error_rate, timeout_rate
    bandwidth: Dict[str, float]      # total_bytes, avg_response_size
    health_status: str               # healthy, degraded, critical

    def get(self, key: str, default: Optional[Dict] = None) -> Dict:
        """Add dict-like get method"""
        return getattr(self, key, default)

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return asdict(self)

@dataclass
class OriginPathMetrics:
    """Container for origin network path metrics"""
    origin_asn: str
    origin_asn_desc: str
    upper_tier: Optional[str]
    direct_requests: int
    tiered_requests: int
    path_latency: float

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return asdict(self)

@dataclass
class NetworkPathMetrics:
    """Network path metrics"""
    client_asn: str
    client_asn_desc: str
    origin_asn: str
    origin_asn_desc: str
    colo_code: str
    upper_tier_colo: Optional[str]
    protocol: str
    country: str

@dataclass
class PerformanceMetrics:
    """Performance metrics"""
    ttfb_avg: float
    ttfb_p50: float
    ttfb_p95: float
    ttfb_p99: float
    origin_time_avg: float
    origin_time_p50: float
    origin_time_p95: float
    origin_time_p99: float
    dns_time_avg: Optional[float]

@dataclass
class RequestDetails:
    """Request details"""
    method: str
    host: str
    path: str
    client_ip: str
    referer: str
    content_type: str
    status_code: int

@dataclass
class CacheMetrics:
    """Cache metrics"""
    cache_status: str
    cache_category: str
    hit_ratio: float
    bytes: int
    bytes_adjusted: int
    visits: int
    visits_adjusted: int
    bandwidth_saving: float

@dataclass
class ErrorMetrics:
    """Error metrics"""
    error_rate_4xx: float
    error_rate_5xx: float
    total_errors: int
    error_status_distribution: Dict[int, int]

@dataclass
class SamplingMetrics:
    """Sampling metrics"""
    sampling_rate: float
    sample_interval: float
    confidence_score: float
    sampled_requests: int
    estimated_total_requests: int

class MetricDimensions:
    """Metric dimensions"""
    def __init__(self, data: Dict[str, Any]):
        self.datetime: str = data.get('datetimeMinute')
        self.country: str = data.get('clientCountryName')
        self.device_type: str = data.get('clientDeviceType')
        self.protocol: str = data.get('clientRequestHTTPProtocol')
        self.content_type: str = data.get('edgeResponseContentTypeName')
        self.status: int = data.get('edgeResponseStatus')
        self.cache_status: str = data.get('cacheStatus')
        self.colo: str = data.get('coloCode')
        self.upper_tier: Optional[str] = data.get('upperTierColoName')

class MetricGroup:
    """Metric group"""
    dimensions: MetricDimensions
    avg: Dict[str, float]
    sum: Dict[str, int]
    count: int
    ratio: Dict[str, float]
    quantiles: Dict[str, float]

class ProcessedMetrics:
    """Processed metrics"""
    timestamp: str
    network: NetworkPathMetrics
    performance: PerformanceMetrics
    cache: CacheMetrics
    request: RequestDetails
    errors: ErrorMetrics
    sampling: SamplingMetrics
