"""
Video utility functions for processing and analyzing video files.
"""
import os
import json
import subprocess
import logging

# Set up module logger
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
            "-show_entries", "format=duration,size,bit_rate:stream=width,height,codec_name,codec_type", 
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
            
            # Get audio stream info
            audio_stream = next((s for s in data['streams'] if s.get('codec_type') == 'audio'), None)
            if audio_stream:
                info['audio_codec'] = audio_stream.get('codec_name', 'unknown')
                info['audio_channels'] = int(audio_stream.get('channels', 0))
                info['audio_sample_rate'] = int(audio_stream.get('sample_rate', 0))
        
        return info
    
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error getting video metadata for {video_path}: {str(e)}")
        return {}

def count_video_streams(video_path):
    """Count the number of video streams in a file."""
    try:
        cmd = [
            "ffprobe", 
            "-v", "error", 
            "-select_streams", "v", 
            "-show_entries", "stream=index", 
            "-of", "json", 
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        return len(data.get('streams', []))
    
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"Error counting video streams in {video_path}: {str(e)}")
        return 0

def count_audio_streams(video_path):
    """Count the number of audio streams in a file."""
    try:
        cmd = [
            "ffprobe", 
            "-v", "error", 
            "-select_streams", "a", 
            "-show_entries", "stream=index", 
            "-of", "json", 
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        return len(data.get('streams', []))
    
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"Error counting audio streams in {video_path}: {str(e)}")
        return 0

def execute_ffmpeg_with_progress(cmd, input_size=None, duration=None):
    """
    Execute FFmpeg command with progress monitoring.
    
    Args:
        cmd: FFmpeg command as list
        input_size: Size of input file in bytes (optional)
        duration: Duration of input file in seconds (optional)
        
    Returns:
        bool: True if successful, False otherwise
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
                
                # Calculate progress percentage if duration is known
                if duration and duration > 0:
                    progress_pct = min(100, (progress_seconds / duration) * 100)
                    logger.debug(f"Progress: {progress_pct:.1f}% ({progress_seconds:.1f}/{duration:.1f}s)")
                else:
                    logger.debug(f"Progress: {line.strip()}")
    
    # Wait for process to complete
    process.wait()
    
    # Check for errors
    if process.returncode != 0:
        logger.error(f"FFmpeg error (code {process.returncode})")
        for line in process.stderr:
            logger.error(f"FFmpeg stderr: {line.strip()}")
        return False
    
    return True