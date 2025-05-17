# FFmpeg Integration for Large Video Optimization

This document outlines a plan to enhance the video-resizer-pre-warmer tool with FFmpeg integration for optimizing large video files.

## Objective

Add capability to automatically identify and re-encode large video files to reduce their size while maintaining acceptable quality, incorporating this into the existing workflow.

## Implementation Plan

### Phase 1: Core FFmpeg Integration

1. **Add FFmpeg Dependency**
   - Verify FFmpeg installation in setup checks
   - Document FFmpeg version requirements (4.0+)
   - Add installation instructions for different platforms

2. **Create Video Optimization Module**
   - Develop core video processing functions
   - Implement flexible encoding options
   - Add progress tracking and reporting

3. **CLI Integration**
   - Add command-line arguments for optimization features
   - Integrate with existing size analysis workflow
   - Provide sensible defaults for common scenarios

### Phase 2: Advanced Features

1. **Multi-Codec Support**
   - Add support for H.264, H.265/HEVC, VP9, AV1
   - Create codec selection logic based on compatibility needs
   - Implement codec-specific optimization parameters

2. **Quality Analysis**
   - Add VMAF/SSIM quality metric calculation (optional)
   - Create quality vs. size tradeoff visualization
   - Implement automatic quality threshold selection

3. **Batch Processing Enhancements**
   - CPU-aware parallelization for encoding tasks
   - Disk space management for large batch operations
   - Checkpoint/resume functionality for large jobs

### Phase 3: Workflow Integration

1. **Integrated Analysis & Optimization Pipeline**
   - Single command for analysis, optimization, and reporting
   - Automatic selection of files to optimize based on criteria
   - Configurable threshold-based optimization

2. **Pre-warm Integration**
   - Option to use optimized files for pre-warming
   - Comparison of CDN performance with original vs. optimized
   - Side-by-side metrics for bandwidth and time savings

3. **Advanced Reporting**
   - Executive summary with total space/bandwidth saved
   - Detailed file-by-file optimization reports
   - Visual graphs of size reduction by file type/category

## Technical Specifications

### FFmpeg Command Parameters

#### Resolution Options

| Name | Width | Height | Typical Bitrate Range | Use Case |
|------|-------|--------|----------------------|----------|
| 4K | 3840 | 2160 | 35-45 Mbps | Premium content, master copies |
| 1080p | 1920 | 1080 | 8-12 Mbps | Desktop viewing, high quality |
| 720p | 1280 | 720 | 5-7.5 Mbps | Tablet, general purpose |
| 480p | 854 | 480 | 2.5-4 Mbps | Mobile, bandwidth-constrained |
| 360p | 640 | 360 | 1-2 Mbps | Mobile, low bandwidth |

#### Video Codec Options

| Codec | FFmpeg Name | Efficiency | Compatibility | Encoding Speed | Container | Use Case |
|-------|-------------|------------|---------------|----------------|-----------|----------|
| H.264 | libx264 | Good | Excellent | Fast | MP4/MOV | General purpose, maximum compatibility |
| H.265/HEVC | libx265 | Very Good | Good | Slow | MP4/MKV | Size optimization, supporting devices |
| VP9 | libvpx-vp9 | Very Good | Good | Very Slow | WebM/MKV | Web delivery, open format |
| VP8 | libvpx | Good | Good | Fast | WebM | Legacy WebM support |
| AV1 | libaom-av1 | Excellent | Limited | Extremely Slow | WebM/MP4/MKV | Future-proofing, best compression |

#### Quality Profiles

| Profile | CRF (H.264) | CRF (HEVC) | CRF (VP9) | CRF (AV1) | Preset | Target |
|---------|-------------|------------|-----------|-----------|--------|--------|
| Maximum | 18 | 22 | 31 | 25 | slower | Highest quality, larger size |
| High | 20 | 24 | 33 | 30 | slow | Visually lossless |
| Balanced | 23 | 28 | 36 | 34 | medium | Good quality, good compression |
| Efficient | 26 | 30 | 39 | 38 | fast | Priority to compression |
| Minimum | 28 | 32 | 42 | 42 | faster | Maximum compression, acceptable quality |

#### Audio Options

| Profile | Codec | Bitrate | Channels | Sampling | Use Case |
|---------|-------|---------|----------|----------|----------|
| High | aac | 192k | Original | Original | Music, high quality audio |
| Medium | aac | 128k | Original | 48000 Hz | General purpose |
| Low | aac | 96k | 2 | 44100 Hz | Dialog-focused, bandwidth saving |
| Minimum | aac | 64k | 2 | 44100 Hz | Maximum compression |

### Sample FFmpeg Commands

#### Balanced Quality 1080p H.264 (MP4)

```bash
ffmpeg -i input.mp4 -c:v libx264 -crf 23 -preset medium \
  -vf "scale=1920:1080:force_original_aspect_ratio=decrease" \
  -c:a aac -b:a 128k -movflags +faststart output.mp4
```

#### High Efficiency 720p HEVC (MP4)

```bash
ffmpeg -i input.mp4 -c:v libx265 -crf 28 -preset slow \
  -vf "scale=1280:720:force_original_aspect_ratio=decrease" \
  -c:a aac -b:a 96k -tag:v hvc1 -movflags +faststart output.mp4
```

#### Web Optimized VP9 (WebM)

```bash
ffmpeg -i input.mp4 -c:v libvpx-vp9 -crf 36 -b:v 0 -deadline good \
  -vf "scale=1280:720:force_original_aspect_ratio=decrease" \
  -c:a libopus -b:a 96k output.webm
```

#### Fast Web Delivery VP8 (WebM)

```bash
ffmpeg -i input.mp4 -c:v libvpx -crf 10 -b:v 1M \
  -vf "scale=1280:720:force_original_aspect_ratio=decrease" \
  -c:a libopus -b:a 128k output.webm
```

#### Maximum Compression AV1 (WebM)

```bash
ffmpeg -i input.mp4 -c:v libaom-av1 -crf 38 -b:v 0 -cpu-used 4 \
  -vf "scale=854:480:force_original_aspect_ratio=decrease" \
  -c:a libopus -b:a 64k output.webm
```

#### Maximum Compression AV1 (MKV)

```bash
ffmpeg -i input.mp4 -c:v libaom-av1 -crf 38 -b:v 0 -cpu-used 4 \
  -vf "scale=854:480:force_original_aspect_ratio=decrease" \
  -c:a libopus -b:a 64k output.mkv
```

## Function Implementation

```python
def optimize_video(input_path, output_path, options=None):
    """
    Re-encode a video file with optimized settings for size reduction.
    
    Args:
        input_path: Path to the input video file
        output_path: Path where the optimized video will be saved
        options: Dictionary with optimization options:
            - codec: Video codec to use (default: "h264")
            - quality_profile: Encoding quality (default: "balanced")
            - resolution: Target resolution (default: "1080p")
            - audio_profile: Audio encoding profile (default: "medium")
            - output_format: Container format (default: "mp4")
            - max_size_mb: Target maximum file size in MB (optional)
            - maintain_quality: Whether to prioritize quality over size (default: False)
            - create_webm: Whether to also create WebM version (default: False)
    
    Returns:
        Dictionary with optimization results:
            - original_size: Original file size in bytes
            - new_size: New file size in bytes
            - reduction_percent: Size reduction as percentage
            - original_duration: Video duration in seconds
            - encoding_time: Time taken to encode in seconds
            - output_path: Path to the optimized file
            - webm_output_path: Path to WebM version (if created)
            - webm_size: Size of WebM version (if created)
    """
    # Default options
    if options is None:
        options = {}
    
    codec = options.get("codec", "h264")
    quality_profile = options.get("quality_profile", "balanced")
    resolution = options.get("resolution", "1080p")
    audio_profile = options.get("audio_profile", "medium")
    output_format = options.get("output_format", "mp4")
    create_webm = options.get("create_webm", False)
    
    # Get file info before processing
    input_size = os.path.getsize(input_path)
    file_info = get_video_metadata(input_path)
    
    # Select encoding parameters based on codec and quality profile
    encoding_params = select_encoding_parameters(codec, quality_profile)
    
    # Select resolution parameters
    resolution_params = select_resolution_parameters(resolution, file_info)
    
    # Select audio parameters
    audio_params = select_audio_parameters(audio_profile)
    
    # Build FFmpeg command
    cmd = build_ffmpeg_command(
        input_path, 
        output_path, 
        encoding_params,
        resolution_params,
        audio_params,
        options
    )
    
    # Execute FFmpeg with progress monitoring
    start_time = time.time()
    execute_ffmpeg_with_progress(cmd)
    encoding_time = time.time() - start_time
    
    # Calculate results
    new_size = os.path.getsize(output_path)
    reduction_bytes = input_size - new_size
    reduction_percent = (reduction_bytes / input_size) * 100
    
    # Create WebM version if requested
    webm_results = None
    if create_webm and output_format != "webm":
        webm_output_path = f"{os.path.splitext(output_path)[0]}.webm"
        webm_options = options.copy()
        
        # Select appropriate codec for WebM
        if codec == "av1":
            webm_options["codec"] = "av1"  # Keep AV1 if already selected
        else:
            webm_options["codec"] = "vp9"  # Otherwise use VP9 for quality
            
        webm_options["output_format"] = "webm"
        webm_options["create_webm"] = False  # Prevent recursion
        
        # Create WebM version with separate encoding
        start_webm_time = time.time()
        
        # Build WebM command
        webm_encoding_params = select_encoding_parameters("vp9", quality_profile)
        webm_audio_params = {"codec": "libopus", "bitrate": audio_params["bitrate"]}
        
        webm_cmd = [
            "ffmpeg", "-i", input_path,
            "-c:v", "libvpx-vp9",
            "-crf", str(webm_encoding_params["crf"]),
            "-b:v", "0",
            "-deadline", "good",
            "-vf", resolution_params["scale"],
            "-c:a", "libopus",
            "-b:a", webm_audio_params["bitrate"],
            "-y", webm_output_path
        ]
        
        execute_ffmpeg_with_progress(webm_cmd)
        webm_encoding_time = time.time() - start_webm_time
        webm_size = os.path.getsize(webm_output_path)
        
        webm_results = {
            "webm_path": webm_output_path,
            "webm_size": webm_size,
            "webm_reduction_percent": (1 - webm_size/input_size) * 100,
            "webm_encoding_time": webm_encoding_time,
            "webm_codec": "vp9"
        }
    
    # Return detailed results
    results = {
        "original_size": input_size,
        "new_size": new_size,
        "reduction_bytes": reduction_bytes,
        "reduction_percent": reduction_percent,
        "original_duration": file_info.get("duration", 0),
        "encoding_time": encoding_time,
        "output_path": output_path,
        "encoding_params": encoding_params,
        "resolution": resolution_params,
        "codec": codec,
        "format": output_format
    }
    
    # Add WebM results if available
    if webm_results:
        results.update(webm_results)
        
    return results
```

## Integration with Existing Workflow

```python
# In main() function, after file size analysis
if args.optimize_large_files:
    # Get files above threshold from earlier analysis
    files_to_optimize = [f for f, size in file_sizes if size >= args.size_threshold * 1024 * 1024]
    
    if not files_to_optimize:
        logger.info("No files above threshold to optimize")
    else:
        logger.info(f"Will optimize {len(files_to_optimize)} files above {args.size_threshold} MiB")
        
        # Configure optimization options
        optimization_options = {
            "codec": args.video_codec,
            "quality_profile": args.quality_profile,
            "resolution": args.target_resolution,
            "audio_profile": args.audio_profile,
            "output_format": args.output_format,
            "create_webm": args.create_webm
        }
        
        # Determine optimal number of workers for CPU-bound encoding
        encoding_workers = max(1, min(os.cpu_count(), args.encoding_workers or 4))
        logger.info(f"Using {encoding_workers} workers for video encoding")
        
        # Create output directory if needed
        output_dir = args.optimization_output_dir or "optimized_videos"
        os.makedirs(output_dir, exist_ok=True)
        
        # Process files with thread pool
        optimization_results = []
        total_original_size = 0
        total_new_size = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=encoding_workers) as executor:
            futures = {}
            
            for input_path in files_to_optimize:
                # Create output path
                filename = os.path.basename(input_path)
                output_path = os.path.join(output_dir, f"optimized_{filename}")
                
                # Submit encoding task
                future = executor.submit(
                    optimize_video, 
                    input_path, 
                    output_path, 
                    optimization_options
                )
                futures[future] = input_path
            
            # Process results as they complete
            for future in tqdm(concurrent.futures.as_completed(futures), 
                              total=len(futures), 
                              desc="Optimizing videos"):
                input_path = futures[future]
                try:
                    result = future.result()
                    optimization_results.append(result)
                    
                    # Update totals
                    total_original_size += result["original_size"]
                    total_new_size += result["new_size"]
                    
                    logger.info(
                        f"Optimized {os.path.basename(input_path)}: "
                        f"{format_file_size(result['original_size'])} → "
                        f"{format_file_size(result['new_size'])} "
                        f"({result['reduction_percent']:.1f}% reduction)"
                    )
                except Exception as e:
                    logger.error(f"Failed to optimize {input_path}: {str(e)}")
        
        # Generate comprehensive report
        total_reduction = total_original_size - total_new_size
        reduction_percent = (total_reduction / total_original_size) * 100 if total_original_size > 0 else 0
        
        logger.info(f"Optimization complete. Total space saved: {format_file_size(total_reduction)} "
                   f"({reduction_percent:.1f}% reduction)")
        
        # Write detailed report
        generate_optimization_report(
            optimization_results, 
            args.optimization_report or "video_optimization_report.md",
            logger
        )
```

## CLI Arguments

Add these arguments to the existing argument parser:

```python
# Video optimization options
optimization_group = parser.add_argument_group('Video Optimization')
optimization_group.add_argument('--optimize-large-files', action='store_true',
                              help='Re-encode large files to reduce size')
optimization_group.add_argument('--video-codec', choices=['h264', 'h265', 'vp9', 'vp8', 'av1'],
                              default='h264', help='Video codec to use for optimization')
optimization_group.add_argument('--output-format', choices=['mp4', 'webm', 'mkv', 'mov'],
                              default='mp4', help='Container format for output files')
optimization_group.add_argument('--create-webm', action='store_true',
                              help='Also create WebM version alongside primary format')
optimization_group.add_argument('--quality-profile', 
                              choices=['maximum', 'high', 'balanced', 'efficient', 'minimum'],
                              default='balanced', help='Encoding quality profile')
optimization_group.add_argument('--target-resolution', 
                              choices=['4k', '1080p', '720p', '480p', '360p'],
                              default='1080p', help='Target resolution for optimized videos')
optimization_group.add_argument('--audio-profile',
                              choices=['high', 'medium', 'low', 'minimum'],
                              default='medium', help='Audio encoding profile')
optimization_group.add_argument('--encoding-workers', type=int,
                              help='Number of parallel encoding workers (default: auto)')
optimization_group.add_argument('--optimization-output-dir', type=str,
                              help='Directory for optimized videos (default: optimized_videos)')
optimization_group.add_argument('--optimization-report', type=str,
                              help='Path for optimization report (default: video_optimization_report.md)')
```

## Next Steps and Timeline

### Week 1: Foundation and Basic Integration
- [ ] Implement FFmpeg execution and helper functions
- [ ] Create basic optimization function with H.264 support
- [ ] Add CLI arguments and parameter validation
- [ ] Implement progress tracking and basic reporting

### Week 2: Expanded Codec Support and Profiles
- [ ] Add support for all listed codecs (H.265, VP9, AV1)
- [ ] Implement quality profiles and parameter selection
- [ ] Create advanced resolution handling
- [ ] Add audio encoding options

### Week 3: WebM Integration and Multi-Format Support
- [ ] Add WebM output support
- [ ] Implement parallel encoding for multiple formats
- [ ] Create format-specific encoding parameter selection
- [ ] Set up container-dependent codec validation

### Week 4: Workflow Integration and Testing
- [ ] Integrate with existing size analysis features
- [ ] Implement multi-threading for batch processing
- [ ] Create optimization reporting module
- [ ] Perform benchmarking and parameter tuning

### Week 5: Advanced Features and Documentation
- [ ] Add quality analysis features (optional)
- [ ] Implement adaptive quality/bitrate selection
- [ ] Create comprehensive documentation
- [ ] Finalize integration and testing