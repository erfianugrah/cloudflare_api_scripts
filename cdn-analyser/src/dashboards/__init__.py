from .cache import create_cache_dashboard
from .error import create_error_dashboard
from .performance import create_performance_dashboard
from .geographic import create_geographic_dashboard
from .rps import create_rps_dashboard

# Origin dashboards
from .origin_response_time import create_origin_response_time_dashboard
from .origin_error import create_origin_error_dashboard
from .origin_geographic import create_origin_geographic_dashboard
from .origin_endpoint import create_origin_endpoint_dashboard
from .origin_asn import create_origin_asn_dashboard

__all__ = [
    'create_cache_dashboard',
    'create_error_dashboard',
    'create_performance_dashboard',
    'create_geographic_dashboard',
    'create_rps_dashboard',
    'create_origin_response_time_dashboard',
    'create_origin_error_dashboard',
    'create_origin_geographic_dashboard',
    'create_origin_endpoint_dashboard',
    'create_origin_asn_dashboard'
]
