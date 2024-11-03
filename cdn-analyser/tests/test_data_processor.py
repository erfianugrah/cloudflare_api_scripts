from src.data_processor import DataProcessor
import pandas as pd
import numpy as np

def test_process_zone_metrics(sample_metrics_data):
    processor = DataProcessor()
    df = processor.process_zone_metrics(sample_metrics_data)
    
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert 'cache_status' in df.columns
    assert 'ttfb_avg' in df.columns

def test_process_empty_metrics():
    processor = DataProcessor()
    empty_data = {"data": {"viewer": {"zones": []}}}
    df = processor.process_zone_metrics(empty_data)
    assert df is None
