import pandas as pd
import numpy as np
import json
from typing import Any, Union
import logging

logger = logging.getLogger(__name__)

def safe_series_conversion(series_or_value: Union[pd.Series, Any], convert_type: type) -> Any:
    """
    Safely convert a Series or single value to specified type.
    
    Args:
        series_or_value: pandas Series or single value
        convert_type: type to convert to (float or int)
    """
    try:
        if isinstance(series_or_value, pd.Series):
            if len(series_or_value) > 0:
                return convert_type(series_or_value.iloc[0])
            return convert_type(0)
        return convert_type(series_or_value)
    except (ValueError, TypeError) as e:
        logger.warning(f"Error converting value {series_or_value}: {str(e)}")
        return convert_type(0)

class NumpyJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                          np.int16, np.int32, np.int64, np.uint8,
                          np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        return super().default(obj)

def convert_to_serializable(obj: Any) -> Any:
    """Convert numpy/pandas types to JSON serializable Python types."""
    if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                      np.int16, np.int32, np.int64, np.uint8,
                      np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    elif isinstance(obj, dict):
        return {str(k): convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_serializable(item) for item in obj]
    return obj
