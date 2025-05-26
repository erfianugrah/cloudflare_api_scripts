# Performance Improvement Plan

## Overview
This document tracks performance and efficiency improvements for the video-resizer-pre-warmer codebase.

## Critical Issues

### 1. Memory Inefficiency in Statistics Collection
**Severity**: HIGH  
**Impact**: Memory usage grows unbounded with file count  
**Current State**: Arrays store all TTFB and timing values indefinitely

#### Problem Details
- `main.py:84-85`: `ttfb_values` and `total_time_values` arrays grow without limit
- `processing.py:391-401`: Every successful request appends to these arrays
- Processing 1M files = ~16MB just for float arrays (8 bytes × 2 arrays × 1M)
- Additional memory for size category arrays multiplies this by 3

#### Solution Design
```python
# Replace unbounded lists with streaming statistics
class StreamingStats:
    def __init__(self):
        self.count = 0
        self.sum = 0.0
        self.sum_of_squares = 0.0
        self.min_val = float('inf')
        self.max_val = float('-inf')
        
    def add(self, value):
        self.count += 1
        self.sum += value
        self.sum_of_squares += value * value
        self.min_val = min(self.min_val, value)
        self.max_val = max(self.max_val, value)
        
    @property
    def mean(self):
        return self.sum / self.count if self.count > 0 else 0
        
    @property
    def std_dev(self):
        if self.count < 2:
            return 0
        variance = (self.sum_of_squares / self.count) - (self.mean ** 2)
        return math.sqrt(max(0, variance))
```

#### Implementation Tasks
- [x] Create `StreamingStats` class in `modules/stats.py` ✅ COMPLETED
- [x] Replace all list-based stats with `StreamingStats` instances ✅ COMPLETED
- [x] Update `initialize_stats()` to use new class ✅ COMPLETED
- [x] Modify `update_processing_stats()` to use `.add()` method ✅ COMPLETED
- [x] Update report generation to use computed properties ✅ COMPLETED
- [ ] Add percentile estimation using P² algorithm (optional)

### 2. Inefficient I/O Patterns

#### 2.1 Individual rclone Calls for Size Retrieval
**Severity**: HIGH  
**Impact**: O(n) subprocess calls for n files

##### Problem Details
- `storage.py:179-196`: Each file size requires separate rclone subprocess
- 1000 files = 1000 subprocess spawns (~2-5 seconds overhead each)

##### Solution Design
```python
def get_file_sizes_batch(remote, bucket, directory, file_paths, logger=None):
    """Get sizes for multiple files in a single rclone call."""
    # Use rclone lsjson for batch operations
    cmd = ['rclone', 'lsjson', f'{remote}:{bucket}/{directory}', '--files-only']
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    
    # Parse JSON output
    files_data = json.loads(result.stdout)
    size_map = {f['Path']: f['Size'] for f in files_data}
    
    # Match requested paths
    return [(path, size_map.get(path, 0)) for path in file_paths]
```

##### Implementation Tasks
- [x] Implement `get_file_sizes_batch()` using rclone lsjson ✅ COMPLETED
- [ ] Add caching layer for file metadata
- [x] Batch file operations in chunks of 1000 ✅ COMPLETED
- [ ] Add retry logic for batch operations
- [x] Update all callers to use batch API ✅ COMPLETED (via backward-compatible wrapper)

#### 2.2 Full Video Downloads for Size Verification
**Severity**: MEDIUM  
**Impact**: Unnecessary bandwidth and time usage

##### Problem Details
- `processing.py:198-201`: Downloads entire video content to verify size
- 1GB video = 1GB bandwidth wasted just for size check

##### Solution Design
```python
def verify_transformed_size(url, timeout=30):
    """Get content size using HEAD request or Range request."""
    try:
        # Try HEAD first
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        if 'Content-Length' in response.headers:
            return int(response.headers['Content-Length'])
        
        # Fallback to partial GET
        headers = {'Range': 'bytes=0-0'}
        response = requests.get(url, headers=headers, timeout=timeout)
        if 'Content-Range' in response.headers:
            # Parse: "bytes 0-0/123456"
            return int(response.headers['Content-Range'].split('/')[-1])
    except:
        return None
```

##### Implementation Tasks
- [x] Implement HEAD request support in `process_single_derivative()` ✅ COMPLETED
- [x] Add Range request fallback for servers without HEAD support ✅ COMPLETED
- [x] Make full download optional via command line flag ✅ COMPLETED (--use-head-for-size)
- [x] Update size comparison logic to handle partial data ✅ COMPLETED
- [ ] Add server capability detection and caching

### 3. Redundant Operations

#### 3.1 Complex Statistics Calculation
**Severity**: LOW  
**Impact**: Unnecessary CPU cycles in post-processing

##### Problem Details
- `main.py:883-912`: Multiple passes over results dictionary
- Nested loops and redundant iterations

##### Solution Design
- Calculate statistics during initial processing
- Single-pass algorithm with running totals
- Eliminate post-processing statistics phase

##### Implementation Tasks
- [ ] Move size reduction calculations to `process_single_derivative()`
- [ ] Accumulate statistics in real-time
- [ ] Remove redundant post-processing loops
- [ ] Add unit tests for statistics accuracy

#### 3.2 Hardware Acceleration Detection
**Severity**: LOW  
**Impact**: Repeated subprocess calls for static information

##### Current State
- Already uses global caching (`HW_ACCELERATION_STATUS`)
- But could be improved with persistent cache

##### Implementation Tasks
- [ ] Add file-based cache for hardware capabilities
- [ ] Cache results between program runs
- [ ] Add cache invalidation mechanism
- [ ] Move detection to module initialization

### 4. Suboptimal Subprocess Usage

**Severity**: MEDIUM  
**Impact**: Process spawn overhead for every operation

#### Solution Design
```python
class RclonePool:
    """Manages a pool of persistent rclone processes."""
    def __init__(self, pool_size=5):
        self.pool = []
        self.available = queue.Queue()
        
    def execute(self, command):
        # Get available process or create new one
        proc = self.available.get() if not self.available.empty() else self._create_process()
        
        # Execute command
        result = proc.communicate(command)
        
        # Return to pool
        self.available.put(proc)
        return result
```

#### Alternative: rclone RC API
```python
class RcloneRC:
    """Use rclone's HTTP API instead of subprocess."""
    def __init__(self, port=5572):
        # Start rclone rcd daemon
        self.daemon = subprocess.Popen(['rclone', 'rcd', '--rc-addr', f':{port}'])
        self.base_url = f'http://localhost:{port}'
        
    def list_files(self, remote, path):
        response = requests.post(f'{self.base_url}/operations/list', 
                               json={'fs': remote, 'remote': path})
        return response.json()
```

#### Implementation Tasks
- [ ] Research rclone RC (remote control) API capabilities
- [ ] Implement RcloneRC wrapper class
- [ ] Add connection pooling for RC API
- [ ] Benchmark subprocess vs RC API performance
- [ ] Create migration path from subprocess to RC API
- [ ] Add fallback mechanism if RC unavailable

## Implementation Priority

1. **Phase 1 - Critical** (Week 1) ✅ COMPLETED
   - [x] Streaming statistics (prevents OOM) ✅
   - [x] HEAD request support (reduces bandwidth) ✅

2. **Phase 2 - High Impact** (Week 2) ✅ PARTIALLY COMPLETED
   - [x] Batch rclone operations ✅
   - [ ] Subprocess pooling or RC API

3. **Phase 3 - Optimization** (Week 3)
   - [ ] Statistics calculation optimization
   - [ ] Hardware detection caching
   - [ ] Performance benchmarking suite

## Testing Requirements

### Unit Tests
- [ ] StreamingStats accuracy tests
- [ ] Batch operation error handling
- [ ] HEAD/Range request fallback logic
- [ ] Statistics calculation verification

### Integration Tests
- [ ] Large file count scenarios (1M+ files)
- [ ] Network failure resilience
- [ ] Memory usage monitoring
- [ ] Performance regression tests

### Performance Benchmarks
- [ ] Memory usage: before vs after
- [ ] Processing time: 1K, 10K, 100K files
- [ ] Network bandwidth usage
- [ ] CPU utilization patterns

## Risk Assessment

### High Risk
- Changing statistics format may break downstream consumers
- HEAD requests might not be supported by all CDNs

### Medium Risk
- rclone RC API requires daemon process
- Batch operations need careful error handling

### Low Risk
- Hardware detection caching
- Statistics optimization

## Backward Compatibility

### Breaking Changes
- Statistics output format (provide migration tool)
- Command line options for new features

### Non-Breaking Changes
- Internal optimizations
- Optional performance features
- Caching mechanisms

## Success Metrics

1. **Memory Usage**: <100MB for 1M file operations (currently ~1GB+)
2. **Processing Speed**: 2x improvement for 10K+ files
3. **Network Efficiency**: 90% reduction in bandwidth for size checks
4. **Subprocess Overhead**: 80% reduction in process spawns

## Notes

- Consider adding `--performance-mode` flag to enable all optimizations
- Document memory/performance characteristics in README
- Add monitoring hooks for production deployments
- Consider Prometheus metrics export for observability