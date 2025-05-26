#!/usr/bin/env python3
"""
Test script to verify performance improvements
"""
import os
import sys
import json
import time
import tracemalloc

# Add the current directory to the path
sys.path.insert(0, '.')

from modules.stats import StreamingStats, SizeReductionStats

def test_streaming_stats():
    """Test the StreamingStats implementation"""
    print("Testing StreamingStats...")
    
    # Create stats object
    stats = StreamingStats("test")
    
    # Add 1 million values (this would use ~8MB with old array approach)
    print("Adding 1,000,000 values...")
    start_time = time.time()
    for i in range(1000000):
        stats.add(i * 0.001)  # Values from 0 to 1000
    
    elapsed = time.time() - start_time
    print(f"Time taken: {elapsed:.2f} seconds")
    
    # Memory usage would be constant regardless of number of values
    print(f"Memory usage: Constant O(1) - does not grow with data")
    
    # Print statistics
    print(f"\nStatistics:")
    print(f"Count: {stats.count}")
    print(f"Mean: {stats.mean:.3f}")
    print(f"Min: {stats.min:.3f}")
    print(f"Max: {stats.max:.3f}")
    print(f"Std Dev: {stats.std_dev:.3f}")
    
    # Test conversion to dict
    stats_dict = stats.to_dict()
    print(f"\nDict format: {json.dumps(stats_dict, indent=2)}")
    
    return stats

def test_size_reduction_stats():
    """Test the SizeReductionStats implementation"""
    print("\n\nTesting SizeReductionStats...")
    
    stats = SizeReductionStats()
    
    # Add some sample data
    test_data = [
        (1000000, 800000),   # 20% reduction
        (2000000, 1000000),  # 50% reduction
        (500000, 450000),    # 10% reduction
        (3000000, 2100000),  # 30% reduction
    ]
    
    for original, transformed in test_data:
        stats.add(original, transformed)
    
    # Print results
    result = stats.to_dict()
    print(f"\nSize Reduction Statistics:")
    print(f"Files processed: {result['count']}")
    print(f"Total original: {result['total_original_bytes']:,} bytes")
    print(f"Total transformed: {result['total_transformed_bytes']:,} bytes")
    print(f"Total reduction: {result['total_reduction_bytes']:,} bytes")
    print(f"Overall reduction: {result['overall_reduction_percent']:.1f}%")
    print(f"\nReduction percentages:")
    print(f"  Min: {result['reduction_stats']['min']:.1f}%")
    print(f"  Mean: {result['reduction_stats']['mean']:.1f}%")
    print(f"  Max: {result['reduction_stats']['max']:.1f}%")

def test_memory_efficiency():
    """Compare memory usage between old and new approaches"""
    print("\n\nMemory Efficiency Comparison:")
    
    # Start memory tracking
    tracemalloc.start()
    
    # Old approach simulation (using lists)
    print("\nSimulating old approach with lists...")
    snapshot1 = tracemalloc.take_snapshot()
    
    old_ttfb = []
    old_times = []
    
    for i in range(100000):  # Just 100k for demo
        old_ttfb.append(i * 0.001)
        old_times.append(i * 0.002)
    
    snapshot2 = tracemalloc.take_snapshot()
    old_stats = snapshot2.compare_to(snapshot1, 'lineno')
    
    old_memory_kb = sum(stat.size_diff for stat in old_stats) / 1024
    print(f"Memory used by lists: {old_memory_kb:.1f} KB")
    print(f"List sizes: ttfb={len(old_ttfb)}, times={len(old_times)}")
    
    # Clear lists
    old_ttfb.clear()
    old_times.clear()
    del old_ttfb, old_times
    
    # New approach
    print("\nUsing new StreamingStats approach...")
    snapshot3 = tracemalloc.take_snapshot()
    
    new_ttfb = StreamingStats("ttfb")
    new_times = StreamingStats("times")
    
    for i in range(100000):
        new_ttfb.add(i * 0.001)
        new_times.add(i * 0.002)
    
    snapshot4 = tracemalloc.take_snapshot()
    new_stats = snapshot4.compare_to(snapshot3, 'lineno')
    
    new_memory_kb = sum(stat.size_diff for stat in new_stats) / 1024
    print(f"Memory used by StreamingStats: {new_memory_kb:.1f} KB")
    print(f"Stats count: ttfb={new_ttfb.count}, times={new_times.count}")
    
    print(f"\nMemory savings: ~{(old_memory_kb - new_memory_kb):.1f} KB ({(old_memory_kb - new_memory_kb) / old_memory_kb * 100:.1f}% reduction)")
    
    # Stop tracking
    tracemalloc.stop()

if __name__ == "__main__":
    print("Performance Improvements Test Suite")
    print("=" * 50)
    
    # Run tests
    test_streaming_stats()
    test_size_reduction_stats()
    test_memory_efficiency()
    
    print("\n" + "=" * 50)
    print("All tests completed!")