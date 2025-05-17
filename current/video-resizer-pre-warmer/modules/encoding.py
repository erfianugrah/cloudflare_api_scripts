"""
Video encoding utilities for optimizing video files.
"""
import os
import time
import logging
from modules import video_utils

# Set up module logger
logger = logging.getLogger(__name__)

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
            - create_webm: Whether to also create WebM version (default: False)
    
    Returns:
        Dictionary with optimization results, or None if failed
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
    file_info = video_utils.get_video_metadata(input_path)
    
    logger.info(f"Processing {input_path}: {video_utils.format_file_size(input_size)}, "
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
    success = video_utils.execute_ffmpeg_with_progress(
        cmd, 
        input_size, 
        file_info.get('duration', 0)
    )
    encoding_time = time.time() - start_time
    
    if not success:
        logger.error(f"Failed to encode {input_path}")
        return None
    
    # Calculate results
    new_size = os.path.getsize(output_path)
    reduction_bytes = input_size - new_size
    reduction_percent = (reduction_bytes / input_size) * 100 if input_size > 0 else 0
    
    logger.info(f"Encoded {input_path} → {output_path}")
    logger.info(f"Size: {video_utils.format_file_size(input_size)} → {video_utils.format_file_size(new_size)} "
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
            "bitrate": audio_params.get("bitrate", "128k"),
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
        webm_success = video_utils.execute_ffmpeg_with_progress(
            webm_cmd, 
            input_size, 
            file_info.get('duration', 0)
        )
        webm_encoding_time = time.time() - start_webm_time
        
        if webm_success:
            webm_size = os.path.getsize(webm_output_path)
            webm_reduction = (1 - webm_size/input_size) * 100 if input_size > 0 else 0
            
            logger.info(f"WebM encoding: {video_utils.format_file_size(input_size)} → {video_utils.format_file_size(webm_size)} "
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