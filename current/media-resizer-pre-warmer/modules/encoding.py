"""
Video encoding utilities for optimizing video files.
"""
import os
import time
import logging
import subprocess
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

def select_resolution_parameters(resolution, video_info, fit_mode='contain'):
    """
    Select target resolution parameters.
    
    Args:
        resolution: Target resolution name ('4k', '1080p', etc.)
        video_info: Video file metadata
        fit_mode: How to fit video to target resolution:
            - 'contain': Preserve aspect ratio and fit entire video within frame (same as 'decrease')
            - 'cover': Preserve aspect ratio and fill entire frame, may crop (same as 'crop')
            - 'pad': Preserve aspect ratio and add letterbox/pillarbox to fill target resolution
            - 'stretch': Ignore aspect ratio and stretch to fill target resolution
            - 'decrease': Legacy alias for 'contain'
            - 'crop': Legacy alias for 'cover'
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
    
    # Create FFmpeg scale filter based on fit mode
    if fit_mode in ['contain', 'decrease']:
        # Preserve aspect ratio and fit entire video within frame
        scale_filter = f"scale={width}:{height}:force_original_aspect_ratio=decrease"
    elif fit_mode in ['cover', 'crop']:
        # Preserve aspect ratio and fill entire frame (may crop)
        scale_filter = f"scale={width}:{height}:force_original_aspect_ratio=increase,crop={width}:{height}"
    elif fit_mode == 'pad':
        # Preserve aspect ratio and add padding (letterbox/pillarbox)
        scale_filter = f"scale={width}:{height}:force_original_aspect_ratio=decrease,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2"
    elif fit_mode == 'stretch':
        # Ignore aspect ratio and stretch to fill target resolution
        scale_filter = f"scale={width}:{height}"
    else:
        # Default to contain if an unknown mode is provided
        logger.warning(f"Unknown fit mode: {fit_mode}, defaulting to 'contain'")
        scale_filter = f"scale={width}:{height}:force_original_aspect_ratio=decrease"
    
    # For reporting purposes, normalize aliases to their standard names
    normalized_fit_mode = fit_mode
    if fit_mode == 'decrease':
        normalized_fit_mode = 'contain'
    elif fit_mode == 'crop':
        normalized_fit_mode = 'cover'

    return {
        'width': width,
        'height': height,
        'scale': scale_filter,
        'name': resolution,
        'fit_mode': normalized_fit_mode
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

# Cache for hardware acceleration support detection
HW_ACCELERATION_STATUS = None

def detect_hardware_acceleration():
    """
    Detect available hardware acceleration options.
    
    Returns:
        dict: Dictionary with status of each type of hardware acceleration
    """
    global HW_ACCELERATION_STATUS
    
    if HW_ACCELERATION_STATUS is not None:
        return HW_ACCELERATION_STATUS
    
    logger.info("Detecting available hardware acceleration options...")
    
    hw_accel = {
        'nvidia': {
            'available': False,
            'h264': False,
            'h265': False
        },
        'intel': {
            'available': False,
            'h264': False,
            'h265': False
        },
        'amd': {
            'available': False,
            'h264': False,
            'h265': False
        },
        'apple': {
            'available': False,
            'h264': False,
            'h265': False
        }
    }
    
    try:
        # Check for NVIDIA NVENC
        try:
            nvenc_h264 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=h264_nvenc"],
                capture_output=True, text=True, check=False
            )
            nvenc_h265 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=hevc_nvenc"],
                capture_output=True, text=True, check=False
            )
            
            if nvenc_h264.returncode == 0 or nvenc_h265.returncode == 0:
                hw_accel['nvidia']['available'] = True
                hw_accel['nvidia']['h264'] = nvenc_h264.returncode == 0
                hw_accel['nvidia']['h265'] = nvenc_h265.returncode == 0
                logger.info(f"NVIDIA GPU acceleration detected: H264: {hw_accel['nvidia']['h264']}, H265: {hw_accel['nvidia']['h265']}")
        except Exception as e:
            logger.debug(f"Error checking NVIDIA support: {str(e)}")
        
        # Check for Intel QuickSync
        try:
            qsv_h264 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=h264_qsv"],
                capture_output=True, text=True, check=False
            )
            qsv_h265 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=hevc_qsv"],
                capture_output=True, text=True, check=False
            )
            
            if qsv_h264.returncode == 0 or qsv_h265.returncode == 0:
                hw_accel['intel']['available'] = True
                hw_accel['intel']['h264'] = qsv_h264.returncode == 0
                hw_accel['intel']['h265'] = qsv_h265.returncode == 0
                logger.info(f"Intel QuickSync acceleration detected: H264: {hw_accel['intel']['h264']}, H265: {hw_accel['intel']['h265']}")
        except Exception as e:
            logger.debug(f"Error checking Intel QuickSync support: {str(e)}")
        
        # Check for AMD AMF
        try:
            amf_h264 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=h264_amf"],
                capture_output=True, text=True, check=False
            )
            amf_h265 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=hevc_amf"],
                capture_output=True, text=True, check=False
            )
            
            if amf_h264.returncode == 0 or amf_h265.returncode == 0:
                hw_accel['amd']['available'] = True
                hw_accel['amd']['h264'] = amf_h264.returncode == 0
                hw_accel['amd']['h265'] = amf_h265.returncode == 0
                logger.info(f"AMD AMF acceleration detected: H264: {hw_accel['amd']['h264']}, H265: {hw_accel['amd']['h265']}")
        except Exception as e:
            logger.debug(f"Error checking AMD AMF support: {str(e)}")
        
        # Check for Apple VideoToolbox
        try:
            vtb_h264 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=h264_videotoolbox"],
                capture_output=True, text=True, check=False
            )
            vtb_h265 = subprocess.run(
                ["ffmpeg", "-hide_banner", "-h", "encoder=hevc_videotoolbox"],
                capture_output=True, text=True, check=False
            )
            
            if vtb_h264.returncode == 0 or vtb_h265.returncode == 0:
                hw_accel['apple']['available'] = True
                hw_accel['apple']['h264'] = vtb_h264.returncode == 0
                hw_accel['apple']['h265'] = vtb_h265.returncode == 0
                logger.info(f"Apple VideoToolbox acceleration detected: H264: {hw_accel['apple']['h264']}, H265: {hw_accel['apple']['h265']}")
        except Exception as e:
            logger.debug(f"Error checking Apple VideoToolbox support: {str(e)}")
            
    except Exception as e:
        logger.warning(f"Error during hardware acceleration detection: {str(e)}")
    
    # Cache the result for future use
    HW_ACCELERATION_STATUS = hw_accel
    
    # Summary of available acceleration options
    available_hw = [k for k, v in hw_accel.items() if v['available']]
    if available_hw:
        logger.info(f"Hardware acceleration available: {', '.join(available_hw)}")
    else:
        logger.info("No hardware acceleration detected, using CPU encoding")
    
    return hw_accel

def build_ffmpeg_command(input_path, output_path, encoding_params, resolution_params, audio_params, output_format="mp4", hw_accel_type="auto", disable_hw_accel=False):
    """
    Build the FFmpeg command based on the encoding parameters.
    
    Args:
        input_path: Path to input video file
        output_path: Path to output video file
        encoding_params: Dictionary with encoding parameters
        resolution_params: Dictionary with resolution parameters
        audio_params: Dictionary with audio parameters
        output_format: Output container format (default: mp4)
        hw_accel_type: Hardware acceleration type (auto, nvidia, intel, amd, apple, none)
        disable_hw_accel: Disable hardware acceleration even if available
    """
    codec = encoding_params['codec']
    
    # Determine whether to use hardware acceleration
    use_hw_accel = False
    selected_hw = None
    
    if not disable_hw_accel and hw_accel_type != 'none':
        # Detect available hardware acceleration
        hw_status = detect_hardware_acceleration()
        
        # Determine which acceleration to use
        if hw_accel_type == 'auto':
            # Try to pick the best available option
            for hw_type in ['nvidia', 'amd', 'intel', 'apple']:
                if hw_status[hw_type]['available'] and (
                    (codec == 'h264' and hw_status[hw_type]['h264']) or
                    (codec == 'h265' and hw_status[hw_type]['h265'])
                ):
                    use_hw_accel = True
                    selected_hw = hw_type
                    break
        else:
            # Use specific requested hardware if available
            if hw_status[hw_accel_type]['available'] and (
                (codec == 'h264' and hw_status[hw_accel_type]['h264']) or
                (codec == 'h265' and hw_status[hw_accel_type]['h265'])
            ):
                use_hw_accel = True
                selected_hw = hw_accel_type
    
    if use_hw_accel:
        logger.info(f"Using {selected_hw} hardware acceleration for {codec} encoding")
    else:
        logger.info(f"Using CPU encoding for {codec}")
    
    # Base command
    cmd = ["ffmpeg", "-i", input_path]
    
    # Video codec parameters
    if codec == 'h264':
        if use_hw_accel:
            if selected_hw == 'nvidia':
                # NVIDIA NVENC
                cmd.extend([
                    "-c:v", "h264_nvenc",
                    "-preset", "p4",  # NVENC presets: p1 (slowest/best) to p7 (fastest/worst)
                    "-tune", "hq",    # Focus on quality
                    "-rc", "vbr",     # Variable bitrate mode
                    "-cq", str(encoding_params['crf']),  # Quality level (lower = better quality)
                    "-b:v", "0"       # Let CQ control the bitrate
                ])
            elif selected_hw == 'intel':
                # Intel QuickSync
                cmd.extend([
                    "-c:v", "h264_qsv",
                    "-global_quality", str(encoding_params['crf']),
                    "-preset", "medium"  # QuickSync presets: veryfast, faster, fast, medium, slow, slower, veryslow
                ])
            elif selected_hw == 'amd':
                # AMD AMF
                cmd.extend([
                    "-c:v", "h264_amf",
                    "-quality", "quality",
                    "-rc", "cqp",
                    "-qp_i", str(encoding_params['crf']),
                    "-qp_p", str(encoding_params['crf'])
                ])
            elif selected_hw == 'apple':
                # Apple VideoToolbox
                cmd.extend([
                    "-c:v", "h264_videotoolbox",
                    "-q:v", str(encoding_params['crf'] / 2),  # VideoToolbox uses 0-100 scale
                    "-allow_sw", "1"   # Allow software encoding if hardware fails
                ])
        else:
            # CPU encoding with libx264
            cmd.extend([
                "-c:v", "libx264",
                "-crf", str(encoding_params['crf']),
                "-preset", encoding_params['preset']
            ])
        
        if output_format == 'mp4':
            cmd.extend(["-movflags", "+faststart"])
    
    elif codec == 'h265':
        if use_hw_accel:
            if selected_hw == 'nvidia':
                # NVIDIA NVENC for HEVC
                cmd.extend([
                    "-c:v", "hevc_nvenc",
                    "-preset", "p4",
                    "-tune", "hq",
                    "-rc", "vbr",
                    "-cq", str(encoding_params['crf']),
                    "-b:v", "0"
                ])
            elif selected_hw == 'intel':
                # Intel QuickSync for HEVC
                cmd.extend([
                    "-c:v", "hevc_qsv",
                    "-global_quality", str(encoding_params['crf']),
                    "-preset", "medium"
                ])
            elif selected_hw == 'amd':
                # AMD AMF for HEVC
                cmd.extend([
                    "-c:v", "hevc_amf",
                    "-quality", "quality",
                    "-rc", "cqp",
                    "-qp_i", str(encoding_params['crf']),
                    "-qp_p", str(encoding_params['crf'])
                ])
            elif selected_hw == 'apple':
                # Apple VideoToolbox for HEVC
                cmd.extend([
                    "-c:v", "hevc_videotoolbox",
                    "-q:v", str(encoding_params['crf'] / 2),  # Convert from 0-51 to 0-100 scale
                    "-allow_sw", "1"
                ])
        else:
            # CPU encoding with libx265
            cmd.extend([
                "-c:v", "libx265",
                "-crf", str(encoding_params['crf']),
                "-preset", encoding_params['preset']
            ])
        
        if output_format == 'mp4':
            cmd.extend(["-tag:v", "hvc1", "-movflags", "+faststart"])
    
    elif codec == 'vp9':
        # VP9 doesn't have widespread hardware acceleration support
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
        # AV1 hardware acceleration is emerging but not widespread yet
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
            - hardware_acceleration: Hardware acceleration type (default: "auto")
            - disable_hardware_acceleration: Disable hardware acceleration (default: False)
    
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
    hw_accel_type = options.get("hardware_acceleration", "auto")
    disable_hw_accel = options.get("disable_hardware_acceleration", False)
    
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
    
    # Get fit mode from options (default to 'decrease' if not specified)
    fit_mode = options.get("fit_mode", "decrease")
    
    # Select resolution parameters with fit mode
    resolution_params = select_resolution_parameters(resolution, file_info, fit_mode)
    
    # Select audio parameters
    audio_params = select_audio_parameters(audio_profile)
    
    # Build FFmpeg command with hardware acceleration options
    cmd = build_ffmpeg_command(
        input_path, 
        output_path, 
        encoding_params,
        resolution_params,
        audio_params,
        output_format,
        hw_accel_type,
        disable_hw_accel
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
        
        # WebM typically doesn't benefit from hardware acceleration as much as H.264/H.265
        # but we'll pass the hardware acceleration options anyway in case there's support
        webm_cmd = build_ffmpeg_command(
            input_path, 
            webm_output_path, 
            webm_encoding_params,
            resolution_params,
            webm_audio_params,
            "webm",
            hw_accel_type,
            disable_hw_accel
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
    
    # Get hardware acceleration status
    hw_accel_info = None
    if HW_ACCELERATION_STATUS:
        used_accel_type = None
        for accel_type, status in HW_ACCELERATION_STATUS.items():
            if status['available'] and (
                (codec == 'h264' and status['h264']) or
                (codec == 'h265' and status['h265'])
            ):
                if not disable_hw_accel and (hw_accel_type == 'auto' or hw_accel_type == accel_type):
                    used_accel_type = accel_type
                    break
        
        hw_accel_info = {
            "hardware_acceleration_available": any(status['available'] for status in HW_ACCELERATION_STATUS.values()),
            "hardware_acceleration_used": used_accel_type is not None,
            "hardware_acceleration_type": used_accel_type if used_accel_type else "none"
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
        "fit_mode": resolution_params.get("fit_mode", "contain"),
        "codec": codec,
        "format": output_format
    }
    
    # Add hardware acceleration info if available
    if hw_accel_info:
        results.update({"hardware_acceleration": hw_accel_info})
    
    # Add WebM results if available
    if webm_results:
        results.update(webm_results)
    
    return results