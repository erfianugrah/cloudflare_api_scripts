import pytest
from src.config import Config
import pandas as pd
import json
from datetime import datetime, UTC

@pytest.fixture
def mock_config(tmp_path):
    """Create a mock configuration for testing."""
    class MockConfig:
        def __init__(self, tmp_path):
            self.reports_dir = tmp_path / "reports"
            self.json_dir = self.reports_dir / "json"
            self.images_dir = self.reports_dir / "images"
            self.logs_dir = self.reports_dir / "logs"
            
            for directory in [self.reports_dir, self.json_dir, self.images_dir, self.logs_dir]:
                directory.mkdir(parents=True, exist_ok=True)
            
            self.account_id = "test_account"
            self.api_key = "test_key"
            self.email = "test@example.com"
            self.base_url = "https://api.cloudflare.com/client/v4"
            self.headers = {
                'X-Auth-Key': self.api_key,
                'X-Auth-Email': self.email,
                'Content-Type': 'application/json'
            }
    
    return MockConfig(tmp_path)

@pytest.fixture
def sample_metrics_data():
    """Create sample metrics data for testing."""
    return {
        "data": {
            "viewer": {
                "zones": [{
                    "httpRequestsAdaptiveGroups": [
                        {
                            "dimensions": {
                                "datetime": "2024-01-01T00:00:00Z",
                                "clientCountryName": "United States",
                                "clientRequestHTTPHost": "example.com",
                                "clientRequestPath": "/",
                                "clientRequestHTTPProtocol": "HTTP/2",
                                "clientRequestHTTPMethodName": "GET",
                                "edgeResponseContentTypeName": "text/html",
                                "edgeResponseStatus": 200,
                                "cacheStatus": "hit",
                                "coloCode": "DFW"
                            },
                            "avg": {
                                "edgeTimeToFirstByteMs": 50.0,
                                "originResponseDurationMs": 100.0,
                                "edgeDnsResponseTimeMs": 10.0,
                                "sampleInterval": 60
                            },
                            "quantiles": {
                                "edgeDnsResponseTimeMsP50": 10.0,
                                "edgeDnsResponseTimeMsP95": 20.0,
                                "edgeDnsResponseTimeMsP99": 30.0,
                                "edgeTimeToFirstByteMsP50": 45.0,
                                "edgeTimeToFirstByteMsP95": 75.0,
                                "edgeTimeToFirstByteMsP99": 100.0,
                                "originResponseDurationMsP50": 90.0,
                                "originResponseDurationMsP95": 150.0,
                                "originResponseDurationMsP99": 200.0
                            },
                            "sum": {
                                "edgeResponseBytes": 1048576,
                                "visits": 1000
                            },
                            "ratio": {
                                "status4xx": 0.01,
                                "status5xx": 0.001
                            },
                            "count": 1000
                        }
                    ]
                }]
            }
        }
    }

@pytest.fixture
def sample_dataframe(sample_metrics_data):
    """Create a sample DataFrame for testing."""
    from src.data_processor import DataProcessor
    processor = DataProcessor()
    return processor.process_zone_metrics(sample_metrics_data)
