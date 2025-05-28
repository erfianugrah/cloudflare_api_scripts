#!/usr/bin/env python3
"""
Test Video Optimizer - Proof of concept for FFmpeg integration

This script downloads videos from rclone, re-encodes them using FFmpeg
with optimized settings, and saves them to an output directory.
"""

import os
import sys
import time
import json
import argparse
import subprocess
import concurrent.futures
from pathlib import Path
import logging
import tempfile
import shutil

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("video_optimizer.log")
    ]
)
logger = logging.getLogger(__name__)

def format_file_size(size_bytes):
    """Format file size in bytes to human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.2f} KiB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.2f} MiB"
    else:
        return f"{size_bytes/(1024*1024*1024):.2f} GiB"

def parse_duration(duration_str):
    """Parse FFmpeg duration string into seconds."""
    if not duration_str:
        return 0
    
    parts = duration_str.split(':')
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    return 0

def get_video_metadata(video_path):
    """Retrieve video metadata using FFprobe."""
    try:
        cmd = [
            "ffprobe", 
            "-v", "error", 
            "-show_entries", "format=duration,size,bit_rate:stream=width,height,codec_name", 
            "-of", "json", 
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        # Extract relevant information
        info = {}
        
        # Get format information
        if 'format' in data:
            format_data = data['format']
            info['duration'] = float(format_data.get('duration', 0))
            info['size'] = int(format_data.get('size', 0))
            info['bit_rate'] = int(format_data.get('bit_rate', 0)) if format_data.get('bit_rate') else 0
        
        # Get stream information (focusing on video stream)
        if 'streams' in data:
            video_stream = next((s for s in data['streams'] if s.get('codec_type') == 'video'), None)
            if not video_stream and data['streams']:
                video_stream = data['streams'][0]  # Fallback to first stream
                
            if video_stream:
                info['width'] = int(video_stream.get('width', 0))
                info['height'] = int(video_stream.get('height', 0))
                info['codec'] = video_stream.get('codec_name', 'unknown')
        
        return info
    
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error getting video metadata for {video_path}: {str(e)}")
        return {}

def select_encoding_parameters(codec, quality_profile):
    """
    Select encoding parameters based on codec and quality profile.
    """
    # Define CRF values for different codecs and quality profiles
    crf_values = {
        'h264': {'maximum': 18, 'high': 20, 'balanced': 23, 'efficient': 26, 'minimum': 28},
        'h265': {'maximum': 22, 'high': 24, 'balanced': 28, 'efficient': 30, 'minimum': 32},
        'vp9': {'maximum': 31, 'high': 33, 'balanced': 36, 'efficient': 39, 'minimum': 42},
        'vp8': {'maximum': 8, 'high': 9, 'balanced': 10, 'efficient': 12, 'minimum': 15},
        'av1': {'maximum': 25, 'high': 30, 'balanced': 34, 'efficient': 38, 'minimum': 42}
    }
    
    # Define presets for different quality profiles
    presets = {
        'h264': {
            'maximum': 'slower', 'high': 'slow', 'balanced': 'medium', 
            'efficient': 'fast', 'minimum': 'faster'
        },
        'h265': {
            'maximum': 'slower', 'high': 'slow', 'balanced': 'medium', 
            'efficient': 'fast', 'minimum': 'faster'
        },
        'vp9': {
            'maximum': 'good', 'high': 'good', 'balanced': 'good', 
            'efficient': 'realtime', 'minimum': 'realtime'
        },
        'vp8': {
            'maximum': 'good', 'high': 'good', 'balanced': 'good', 
            'efficient': 'realtime', 'minimum': 'realtime'
        },
        'av1': {
            # For libaom-av1, cpu-used (0=slowest/best, 8=fastest/worst)
            'maximum': 1, 'high': 2, 'balanced': 4, 'efficient': 6, 'minimum': 8
        }
    }
    
    # Default to h264/balanced if codec or profile not found
    if codec not in crf_values:
        logger.warning(f"Unknown codec: {codec}, defaulting to h264")
        codec = 'h264'
    
    if quality_profile not in crf_values[codec]:
        logger.warning(f"Unknown quality profile: {quality_profile}, defaulting to balanced")
        quality_profile = 'balanced'
    
    crf = crf_values[codec][quality_profile]
    preset = presets[codec][quality_profile]
    
    return {
        'codec': codec,
        'crf': crf,
        'preset': preset
    }

def select_resolution_parameters(resolution, video_info):
    """
    Select target resolution parameters.
    """
    resolution_values = {
        '4k': (3840, 2160),
        '1080p': (1920, 1080),
        '720p': (1280, 720),
        '480p': (854, 480),
        '360p': (640, 360)
    }
    
    if resolution not in resolution_values:
        logger.warning(f"Unknown resolution: {resolution}, defaulting to 1080p")
        resolution = '1080p'
    
    width, height = resolution_values[resolution]
    
    # Create FFmpeg scale filter
    scale_filter = f"scale={width}:{height}:force_original_aspect_ratio=decrease"
    
    return {
        'width': width,
        'height': height,
        'scale': scale_filter,
        'name': resolution
    }

def select_audio_parameters(audio_profile):
    """
    Select audio encoding parameters.
    """
    audio_presets = {
        'high': {'codec': 'aac', 'bitrate': '192k', 'channels': 0, 'sampling': 0},  # 0 = original
        'medium': {'codec': 'aac', 'bitrate': '128k', 'channels': 0, 'sampling': 48000},
        'low': {'codec': 'aac', 'bitrate': '96k', 'channels': 2, 'sampling': 44100},
        'minimum': {'codec': 'aac', 'bitrate': '64k', 'channels': 2, 'sampling': 44100}
    }
    
    if audio_profile not in audio_presets:
        logger.warning(f"Unknown audio profile: {audio_profile}, defaulting to medium")
        audio_profile = 'medium'
    
    return audio_presets[audio_profile]

def build_ffmpeg_command(input_path, output_path, encoding_params, resolution_params, audio_params, output_format="mp4"):
    """
    Build the FFmpeg command based on the encoding parameters.
    """
    codec = encoding_params['codec']
    
    # Base command
    cmd = ["ffmpeg", "-i", input_path]
    
    # Video codec parameters
    if codec == 'h264':
        cmd.extend([
            "-c:v", "libx264",
            "-crf", str(encoding_params['crf']),
            "-preset", encoding_params['preset']
        ])
        if output_format == 'mp4':
            cmd.extend(["-movflags", "+faststart"])
    
    elif codec == 'h265':
        cmd.extend([
            "-c:v", "libx265",
            "-crf", str(encoding_params['crf']),
            "-preset", encoding_params['preset']
        ])
        if output_format == 'mp4':
            cmd.extend(["-tag:v", "hvc1", "-movflags", "+faststart"])
    
    elif codec == 'vp9':
        cmd.extend([
            "-c:v", "libvpx-vp9",
            "-crf", str(encoding_params['crf']),
            "-b:v", "0",
            "-deadline", encoding_params['preset']
        ])
    
    elif codec == 'vp8':
        cmd.extend([
            "-c:v", "libvpx",
            "-crf", str(encoding_params['crf']),
            "-b:v", "1M",
            "-deadline", encoding_params['preset']
        ])
    
    elif codec == 'av1':
        cmd.extend([
            "-c:v", "libaom-av1",
            "-crf", str(encoding_params['crf']),
            "-b:v", "0",
            "-cpu-used", str(encoding_params['preset'])
        ])
    
    # Resolution parameters
    cmd.extend(["-vf", resolution_params['scale']])
    
    # Audio parameters
    audio_codec = 'libopus' if output_format == 'webm' else audio_params.get('codec', 'aac')
    cmd.extend(["-c:a", audio_codec, "-b:a", audio_params.get('bitrate', '128k')])
    
    channels = audio_params.get('channels', 0)
    if channels > 0:
        cmd.extend(["-ac", str(channels)])
    
    sampling = audio_params.get('sampling', 0)
    if sampling > 0:
        cmd.extend(["-ar", str(sampling)])
    
    # Output file (overwrite if exists)
    cmd.extend(["-y", output_path])
    
    return cmd

def execute_ffmpeg_with_progress(cmd, input_size=None):
    """
    Execute FFmpeg command with progress monitoring.
    """
    logger.info(f"Running FFmpeg: {' '.join(cmd)}")
    
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, 
        universal_newlines=True,
        bufsize=1
    )
    
    # Monitor progress
    for line in process.stderr:
        # Look for progress information
        if "time=" in line:
            # Extract time progress
            time_parts = line.split("time=")[1].split()[0].split(":")
            if len(time_parts) == 3:
                hours, minutes, seconds = time_parts
                progress_seconds = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                logger.debug(f"Progress: {line.strip()}")
    
    # Wait for process to complete
    process.wait()
    
    # Check for errors
    if process.returncode != 0:
        logger.error(f"FFmpeg error (code {process.returncode})")
        return False
    
    return True

def download_from_rclone(remote_path, local_path, use_temp=True):
    """Download a file from rclone remote."""
    try:
        if use_temp:
            # Use temp directory and then move to final location
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(local_path)[1]) as tmp:
                temp_path = tmp.name
            
            cmd = ["rclone", "copy", remote_path, os.path.dirname(temp_path)]
            logger.info(f"Downloading to temp file: {cmd}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Get the actual filename that rclone created
            temp_file = os.path.join(os.path.dirname(temp_path), 
                                   os.path.basename(remote_path))
            
            # Move to final location
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            shutil.move(temp_file, local_path)
            return True
        else:
            # Download directly to specified path
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            cmd = ["rclone", "copy", remote_path, os.path.dirname(local_path)]
            logger.info(f"Downloading file: {cmd}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error downloading {remote_path}: {e.stderr}")
        return False

def optimize_video(input_path, output_path, options=None):
    """
    Re-encode a video file with optimized settings for size reduction.
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
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Get file info before processing
    input_size = os.path.getsize(input_path)
    file_info = get_video_metadata(input_path)
    
    logger.info(f"Processing {input_path}: {format_file_size(input_size)}, "
               f"{file_info.get('width', 0)}x{file_info.get('height', 0)}, "
               f"{file_info.get('duration', 0):.2f}s")
    
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
        output_format
    )
    
    # Execute FFmpeg with progress monitoring
    start_time = time.time()
    success = execute_ffmpeg_with_progress(cmd, input_size)
    encoding_time = time.time() - start_time
    
    if not success:
        logger.error(f"Failed to encode {input_path}")
        return None
    
    # Calculate results
    new_size = os.path.getsize(output_path)
    reduction_bytes = input_size - new_size
    reduction_percent = (reduction_bytes / input_size) * 100 if input_size > 0 else 0
    
    logger.info(f"Encoded {input_path} → {output_path}")
    logger.info(f"Size: {format_file_size(input_size)} → {format_file_size(new_size)} "
               f"({reduction_percent:.1f}% reduction)")
    logger.info(f"Encoding time: {encoding_time:.1f}s")
    
    # Create WebM version if requested
    webm_results = None
    if create_webm and output_format != "webm":
        webm_output_path = f"{os.path.splitext(output_path)[0]}.webm"
        
        # Build WebM command for VP9
        webm_encoding_params = select_encoding_parameters("vp9", quality_profile)
        webm_audio_params = {
            "codec": "libopus", 
            "bitrate": audio_params["bitrate"],
            "channels": audio_params.get("channels", 0),
            "sampling": audio_params.get("sampling", 0)
        }
        
        webm_cmd = build_ffmpeg_command(
            input_path, 
            webm_output_path, 
            webm_encoding_params,
            resolution_params,
            webm_audio_params,
            "webm"
        )
        
        # Execute WebM encoding
        logger.info(f"Creating WebM version: {webm_output_path}")
        start_webm_time = time.time()
        webm_success = execute_ffmpeg_with_progress(webm_cmd)
        webm_encoding_time = time.time() - start_webm_time
        
        if webm_success:
            webm_size = os.path.getsize(webm_output_path)
            webm_reduction = (1 - webm_size/input_size) * 100 if input_size > 0 else 0
            
            logger.info(f"WebM encoding: {format_file_size(input_size)} → {format_file_size(webm_size)} "
                      f"({webm_reduction:.1f}% reduction)")
            logger.info(f"WebM encoding time: {webm_encoding_time:.1f}s")
            
            webm_results = {
                "webm_path": webm_output_path,
                "webm_size": webm_size,
                "webm_reduction_percent": webm_reduction,
                "webm_encoding_time": webm_encoding_time
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
        "resolution": resolution_params["name"],
        "codec": codec,
        "format": output_format
    }
    
    # Add WebM results if available
    if webm_results:
        results.update(webm_results)
    
    return results

def list_large_files(remote, bucket, directory, extension, size_threshold_mib, limit=0):
    """
    List large files from the remote storage.
    """
    path = f"{remote}:{bucket}/{directory}"
    
    # Use rclone ls to get files with sizes
    cmd = ["rclone", "ls", path]
    logger.info(f"Listing files: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse output format: "   12345 file.mp4"
        all_files = []
        for line in result.stdout.splitlines():
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                size_bytes = int(parts[0])
                file_path = parts[1]
                
                if file_path.lower().endswith(extension.lower()):
                    all_files.append((file_path, size_bytes))
        
        # Sort by size, largest first
        all_files.sort(key=lambda x: x[1], reverse=True)
        
        # Filter by size threshold
        size_threshold_bytes = size_threshold_mib * 1024 * 1024
        large_files = [(f, s) for f, s in all_files if s >= size_threshold_bytes]
        
        logger.info(f"Found {len(large_files)} files above {size_threshold_mib} MiB "
                   f"(out of {len(all_files)} total files)")
        
        # Apply limit if specified
        if limit > 0 and len(large_files) > limit:
            logger.info(f"Limiting to {limit} files")
            large_files = large_files[:limit]
        
        return large_files
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error listing files: {e.stderr}")
        return []

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Video Optimizer - FFmpeg Integration Test")
    
    # Remote and storage options
    parser.add_argument("--remote", required=True, help="Rclone remote name")
    parser.add_argument("--bucket", required=True, help="Bucket name")
    parser.add_argument("--directory", default="", help="Directory within bucket")
    
    # File selection options
    parser.add_argument("--extension", default=".mp4", help="File extension to filter")
    parser.add_argument("--size-threshold", type=int, default=256, 
                       help="Size threshold in MiB for file selection")
    parser.add_argument("--limit", type=int, default=0, 
                       help="Limit number of files to process (0 for no limit)")
    
    # Output options
    parser.add_argument("--output-dir", default="optimized_videos", 
                       help="Directory for optimized videos")
    parser.add_argument("--report", default="optimization_report.json", 
                       help="Path for optimization report")
    
    # Encoding options
    parser.add_argument("--codec", choices=["h264", "h265", "vp9", "vp8", "av1"], 
                       default="h264", help="Video codec to use")
    parser.add_argument("--quality", 
                       choices=["maximum", "high", "balanced", "efficient", "minimum"], 
                       default="balanced", help="Encoding quality profile")
    parser.add_argument("--resolution", 
                       choices=["4k", "1080p", "720p", "480p", "360p"], 
                       default="1080p", help="Target resolution")
    parser.add_argument("--audio", choices=["high", "medium", "low", "minimum"], 
                       default="medium", help="Audio encoding profile")
    parser.add_argument("--format", choices=["mp4", "webm", "mkv"], 
                       default="mp4", help="Output container format")
    parser.add_argument("--create-webm", action="store_true", 
                       help="Also create WebM version")
    parser.add_argument("--workers", type=int, default=1, 
                       help="Number of parallel encoding workers")
    
    # Parse arguments
    args = parser.parse_args()
    
    # List large files
    large_files = list_large_files(
        args.remote,
        args.bucket,
        args.directory,
        args.extension,
        args.size_threshold,
        args.limit
    )
    
    if not large_files:
        logger.error("No files found matching criteria")
        return 1
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Configure optimization options
    optimization_options = {
        "codec": args.codec,
        "quality_profile": args.quality,
        "resolution": args.resolution,
        "audio_profile": args.audio,
        "output_format": args.format,
        "create_webm": args.create_webm
    }
    
    # Process files
    logger.info(f"Processing {len(large_files)} files with {args.workers} workers")
    
    results = []
    total_original_size = 0
    total_new_size = 0
    
    # Create work directory for downloads
    work_dir = os.path.join(args.output_dir, "_downloads")
    os.makedirs(work_dir, exist_ok=True)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit tasks
        future_to_file = {}
        
        for file_path, file_size in large_files:
            # Create paths
            remote_path = f"{args.remote}:{args.bucket}/{args.directory}/{file_path}"
            download_path = os.path.join(work_dir, os.path.basename(file_path))
            output_name = f"optimized_{os.path.basename(file_path)}"
            output_path = os.path.join(args.output_dir, output_name)
            
            # Download file
            logger.info(f"Submitting {file_path} ({format_file_size(file_size)})")
            
            future = executor.submit(
                lambda r, d, o, opt: download_from_rclone(r, d) and optimize_video(d, o, opt),
                remote_path, 
                download_path,
                output_path,
                optimization_options
            )
            
            future_to_file[future] = (file_path, file_size)
        
        # Process results
        for future in concurrent.futures.as_completed(future_to_file):
            file_path, file_size = future_to_file[future]
            
            try:
                result = future.result()
                if result:
                    results.append(result)
                    total_original_size += result["original_size"]
                    total_new_size += result["new_size"]
                    
                    logger.info(f"Completed {file_path}: "
                               f"{format_file_size(file_size)} → {format_file_size(result['new_size'])} "
                               f"({result['reduction_percent']:.1f}% reduction)")
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
    
    # Generate final report
    if results:
        total_reduction = total_original_size - total_new_size
        percent_reduction = (total_reduction / total_original_size) * 100 if total_original_size > 0 else 0
        
        # Overall stats
        report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "stats": {
                "files_processed": len(results),
                "total_original_size": total_original_size,
                "total_original_size_formatted": format_file_size(total_original_size),
                "total_new_size": total_new_size,
                "total_new_size_formatted": format_file_size(total_new_size),
                "total_reduction_bytes": total_reduction,
                "total_reduction_bytes_formatted": format_file_size(total_reduction),
                "percent_reduction": percent_reduction
            },
            "files": results
        }
        
        # Save report
        with open(args.report, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Successfully processed {len(results)} files")
        logger.info(f"Total size reduction: {format_file_size(total_reduction)} "
                   f"({percent_reduction:.1f}%)")
        logger.info(f"Report saved to {args.report}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())