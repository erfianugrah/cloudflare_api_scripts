from typing import Dict, List, Any
import logging
from prettytable import PrettyTable
import pandas as pd

logger = logging.getLogger(__name__)

class TableFormatter:
    """Handles table formatting and presentation"""
    
    def __init__(self):
        self.alignments = {
            'numeric': 'r',
            'text': 'l',
            'percentage': 'r'
        }
        
    def format_table(self, data: List[Dict], columns: List[str], 
                    column_types: Dict[str, str]) -> PrettyTable:
        """Create consistently formatted table"""
        try:
            table = PrettyTable()
            table.field_names = columns
            
            # Set alignments
            for col in columns:
                col_type = column_types.get(col, 'text')
                table.align[col] = self.alignments.get(col_type, 'l')
            
            # Add rows with proper formatting
            for row in data:
                formatted_row = []
                for col in columns:
                    value = row.get(col, '')
                    col_type = column_types.get(col, 'text')
                    
                    formatted_value = self._format_value(value, col_type)
                    formatted_row.append(formatted_value)
                        
                table.add_row(formatted_row)
            
            return table
            
        except Exception as e:
            logger.error(f"Error formatting table: {str(e)}")
            empty_table = PrettyTable()
            empty_table.field_names = columns
            return empty_table

    def _format_value(self, value: Any, value_type: str) -> str:
        """Format individual values based on their type"""
        try:
            if pd.isna(value):
                return self._get_default_value(value_type)
                
            if isinstance(value, pd.Series):
                if value.empty:
                    return self._get_default_value(value_type)
                value = value.iloc[0]
            
            if value_type == 'numeric':
                if isinstance(value, (int, float)) and value >= 0:
                    return f"{value:,.2f}"
                return "0.00"
                    
            elif value_type == 'percentage':
                if isinstance(value, (int, float)) and value >= 0:
                    return f"{value:.1f}%"
                return "0.0%"
                    
            else:  # text type
                value_str = str(value)
                # Clean pandas metadata
                value_str = value_str.split('Name:')[0].split('dtype:')[0].strip()
                return value_str
                
        except Exception as e:
            logger.error(f"Error formatting value: {str(e)}")
            return self._get_default_value(value_type)
            
    def _get_default_value(self, value_type: str) -> str:
        """Get default value for different types"""
        if value_type == 'numeric':
            return "0.00"
        elif value_type == 'percentage':
            return "0.0%"
        else:
            return ""

    def format_performance_table(self, data: List[Dict]) -> PrettyTable:
        """Format performance metrics table"""
        columns = ['Metric', 'Value', 'Status']
        column_types = {
            'Metric': 'text',
            'Value': 'numeric',
            'Status': 'text'
        }
        return self.format_table(data, columns, column_types)
        
    def format_error_table(self, data: List[Dict]) -> PrettyTable:
        """Format error metrics table"""
        columns = ['Status', 'Count', 'Percentage', 'Avg Response', 'Avg Size']
        column_types = {
            'Status': 'text',
            'Count': 'numeric',
            'Percentage': 'percentage',
            'Avg Response': 'numeric',
            'Avg Size': 'numeric'
        }
        return self.format_table(data, columns, column_types)
        
    def format_cache_table(self, data: List[Dict]) -> PrettyTable:
        """Format cache metrics table"""
        columns = ['Status', 'Requests %', 'Bytes %', 'Avg TTFB']
        column_types = {
            'Status': 'text',
            'Requests %': 'percentage',
            'Bytes %': 'percentage',
            'Avg TTFB': 'numeric'
        }
        return self.format_table(data, columns, column_types)
