#!/usr/bin/env python3
"""
Integration test for performance improvements
"""
import sys
import json
import tempfile
import os

# Add the current directory to the path
sys.path.insert(0, '.')

from modules.stats import StreamingStats, SizeReductionStats
from modules import config
from modules import processing
from modules import storage
from modules import reporting

def test_streaming_stats_integration():
    """Test StreamingStats in the context of the main application"""
    print("Testing StreamingStats integration...")
    
    # Initialize stats like main.py does
    derivatives = ['desktop', 'tablet', 'mobile']
    from main import initialize_stats
    stats = initialize_stats(derivatives)
    
    # Verify structure
    assert 'ttfb_stats' in stats
    assert 'total_time_stats' in stats
    assert 'size_reduction_stats' in stats
    assert isinstance(stats['ttfb_stats'], StreamingStats)
    assert isinstance(stats['size_reduction_stats'], SizeReductionStats)
    
    # Simulate adding data
    stats['ttfb_stats'].add(0.5)
    stats['ttfb_stats'].add(0.3)
    stats['total_time_stats'].add(1.5)
    stats['total_time_stats'].add(2.0)
    stats['size_reduction_stats'].add(1000000, 800000)
    
    print("✓ Stats initialization and data addition working")
    
    # Test conversion for output
    stats_for_output = stats.copy()
    stats_for_output['ttfb_summary'] = stats['ttfb_stats'].to_dict()
    stats_for_output['total_time_summary'] = stats['total_time_stats'].to_dict()
    stats_for_output['size_reduction_summary'] = stats['size_reduction_stats'].to_dict()
    
    # Remove streaming objects
    stats_for_output.pop('ttfb_stats', None)
    stats_for_output.pop('total_time_stats', None)
    stats_for_output.pop('size_reduction_stats', None)
    
    print("✓ Stats conversion for output working")
    
    # Test report generation
    report = reporting.generate_stats_report(stats_for_output, 'markdown')
    assert "Time to First Byte" in report
    assert "Size Reduction Statistics" in report
    
    print("✓ Report generation working with new stats format")
    
    return stats_for_output

def test_head_request_support():
    """Test HEAD request configuration"""
    print("\nTesting HEAD request support...")
    
    # Parse arguments with HEAD request flag
    import argparse
    import sys
    original_argv = sys.argv
    sys.argv = ['test', '--use-head-for-size', '--base-url', 'https://example.com']
    args = config.parse_arguments()
    sys.argv = original_argv
    
    assert hasattr(args, 'use_head_for_size')
    assert args.use_head_for_size == True
    
    print("✓ HEAD request command line flag working")
    
    # Verify function signature
    import inspect
    sig = inspect.signature(processing.process_single_derivative)
    params = list(sig.parameters.keys())
    assert 'use_head_request' in params
    
    print("✓ HEAD request parameter in processing functions")

def test_batch_operations():
    """Test batch file operations"""
    print("\nTesting batch file operations...")
    
    # Verify function exists
    assert hasattr(storage, 'get_file_sizes_batch')
    
    # Verify get_file_sizes uses batch implementation
    import inspect
    source = inspect.getsource(storage.get_file_sizes)
    assert 'get_file_sizes_batch' in source
    
    print("✓ Batch operations integrated in storage module")

def test_full_workflow():
    """Test a minimal workflow with all improvements"""
    print("\nTesting full workflow integration...")
    
    # Create a minimal FileMetadata object
    class MockFileMetadata:
        def __init__(self):
            self.path = "test.mp4"
            self.size_bytes = 1000000
            self.size_category = "medium"
            self.processing_duration = 0
            
        def start_processing(self):
            pass
            
        def complete_processing(self):
            self.processing_duration = 0.5
            
        def start_derivative_processing(self, deriv):
            pass
            
        def complete_derivative_processing(self, deriv):
            pass
            
        def to_dict(self):
            return {
                'path': self.path,
                'size_bytes': self.size_bytes,
                'size_category': self.size_category
            }
    
    # Import and test the update_processing_stats function
    from main import initialize_stats
    stats = initialize_stats(['desktop'])
    obj = MockFileMetadata()
    results = {
        'desktop': {
            'status': 'success',
            'time_to_first_byte': 0.3,
            'total_time': 1.5,
            'original_size_bytes': 1000000,
            'response_size_bytes': 800000
        }
    }
    
    # Update stats
    processing.update_processing_stats(stats, obj, results, ['desktop'])
    
    # Verify stats were updated
    assert stats['ttfb_stats'].count == 1
    assert stats['total_time_stats'].count == 1
    assert stats['size_reduction_stats'].count == 1
    
    print("✓ Full workflow with all improvements working")

if __name__ == "__main__":
    print("Performance Improvements Integration Test")
    print("=" * 50)
    
    try:
        test_streaming_stats_integration()
        test_head_request_support()
        test_batch_operations()
        test_full_workflow()
        
        print("\n" + "=" * 50)
        print("✓ ALL INTEGRATION TESTS PASSED!")
        print("\nThe performance improvements are correctly integrated.")
        
    except Exception as e:
        print(f"\n✗ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)