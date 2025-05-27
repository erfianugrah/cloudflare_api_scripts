"""
Video validation module for checking integrity of pre-warmed video files.
"""
import json
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import tempfile

logger = logging.getLogger(__name__)

class VideoValidator:
    """Validates video files for corruption and integrity issues."""
    
    def __init__(self, workers: int = 10):
        self.workers = workers
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'total_files': 0,
            'valid_files': 0,
            'corrupted_files': 0,
            'missing_files': 0,
            'validation_errors': [],
            'file_results': {}
        }
    
    def validate_single_video(self, video_path: str) -> Tuple[str, Dict]:
        """
        Validate a single video file using ffprobe.
        
        Returns:
            Tuple of (video_path, validation_result)
        """
        result = {
            'path': video_path,
            'exists': os.path.exists(video_path),
            'size': 0,
            'is_valid': False,
            'has_video_stream': False,
            'has_audio_stream': False,
            'duration': None,
            'resolution': None,
            'codec': None,
            'error': None,
            'checks': {
                'file_readable': False,
                'metadata_extractable': False,
                'duration_valid': False,
                'streams_valid': False,
                'no_corruption_detected': False
            }
        }
        
        if not result['exists']:
            result['error'] = 'File does not exist'
            return video_path, result
        
        try:
            # Check file size
            result['size'] = os.path.getsize(video_path)
            result['checks']['file_readable'] = True
            
            if result['size'] == 0:
                result['error'] = 'File is empty (0 bytes)'
                return video_path, result
            
            # Run comprehensive ffprobe check
            probe_cmd = [
                'ffprobe',
                '-v', 'error',
                '-show_error',
                '-show_format',
                '-show_streams',
                '-print_format', 'json',
                video_path
            ]
            
            probe_result = subprocess.run(
                probe_cmd,
                capture_output=True,
                text=True,
                timeout=60  # Increased timeout for large files
            )
            
            if probe_result.returncode != 0:
                result['error'] = f'FFprobe failed: {probe_result.stderr}'
                return video_path, result
            
            probe_data = json.loads(probe_result.stdout)
            result['checks']['metadata_extractable'] = True
            
            # Check for errors in probe data
            if 'error' in probe_data:
                result['error'] = f"FFprobe error: {probe_data['error']}"
                return video_path, result
            
            # Validate format data
            if 'format' in probe_data:
                format_data = probe_data['format']
                
                # Check duration
                if 'duration' in format_data:
                    duration = float(format_data['duration'])
                    result['duration'] = duration
                    result['checks']['duration_valid'] = duration > 0
                
                # Additional format checks
                if 'bit_rate' in format_data and format_data['bit_rate'] == 'N/A':
                    result['error'] = 'Invalid bitrate detected'
                    return video_path, result
            
            # Validate streams
            if 'streams' in probe_data:
                video_streams = []
                audio_streams = []
                
                for stream in probe_data['streams']:
                    if stream.get('codec_type') == 'video':
                        video_streams.append(stream)
                        if 'width' in stream and 'height' in stream:
                            result['resolution'] = f"{stream['width']}x{stream['height']}"
                        if 'codec_name' in stream:
                            result['codec'] = stream['codec_name']
                    elif stream.get('codec_type') == 'audio':
                        audio_streams.append(stream)
                
                result['has_video_stream'] = len(video_streams) > 0
                result['has_audio_stream'] = len(audio_streams) > 0
                result['checks']['streams_valid'] = result['has_video_stream']
            
            # Run corruption detection using ffmpeg (decode a few frames)
            corruption_cmd = [
                'ffmpeg',
                '-v', 'error',
                '-i', video_path,
                '-f', 'null',
                '-frames:v', '10',  # Check first 10 frames
                '-'
            ]
            
            corruption_result = subprocess.run(
                corruption_cmd,
                capture_output=True,
                text=True,
                timeout=30  # Increased timeout for large files
            )
            
            if corruption_result.returncode == 0 and not corruption_result.stderr:
                result['checks']['no_corruption_detected'] = True
            else:
                result['error'] = f'Corruption detected: {corruption_result.stderr}'
                return video_path, result
            
            # File is valid if all critical checks pass
            result['is_valid'] = (
                result['checks']['file_readable'] and
                result['checks']['metadata_extractable'] and
                result['checks']['streams_valid'] and
                result['checks']['no_corruption_detected'] and
                (result['checks']['duration_valid'] or result['duration'] is None)  # Some formats don't have duration
            )
            
        except subprocess.TimeoutExpired:
            result['error'] = 'Validation timeout - file may be corrupted or too large'
        except json.JSONDecodeError:
            result['error'] = 'Invalid ffprobe output - file may be severely corrupted'
        except PermissionError:
            result['error'] = 'Permission denied accessing file'
        except OSError as e:
            result['error'] = f'OS error accessing file: {str(e)}'
        except Exception as e:
            result['error'] = f'Validation error: {str(e)}'
        
        return video_path, result
    
    def validate_videos(self, video_paths: List[str], show_progress: bool = True) -> Dict:
        """
        Validate multiple video files in parallel.
        
        Args:
            video_paths: List of video file paths to validate
            show_progress: Whether to show progress bar
            
        Returns:
            Dictionary with validation results
        """
        self.validation_results['total_files'] = len(video_paths)
        
        if show_progress:
            logger.info(f"Starting validation of {len(video_paths)} video files...")
        
        completed = 0
        
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self.validate_single_video, path): path
                for path in video_paths
            }
            
            for future in as_completed(futures):
                try:
                    video_path, result = future.result()
                    
                    self.validation_results['file_results'][video_path] = result
                    
                    if result['is_valid']:
                        self.validation_results['valid_files'] += 1
                    elif not result['exists']:
                        self.validation_results['missing_files'] += 1
                    else:
                        self.validation_results['corrupted_files'] += 1
                        self.validation_results['validation_errors'].append({
                            'path': video_path,
                            'error': result['error'],
                            'checks': result['checks']
                        })
                    
                    completed += 1
                    if show_progress and completed % 10 == 0:
                        logger.info(f"Progress: {completed}/{len(video_paths)} files validated ({completed/len(video_paths)*100:.1f}%)")
                        
                except Exception as e:
                    logger.error(f"Error validating {futures[future]}: {str(e)}")
                    completed += 1
        
        if show_progress:
            logger.info(f"Validation complete: {completed}/{len(video_paths)} files processed")
        
        return self.validation_results
    
    def validate_from_results(self, results_file: str, base_path: str = "") -> Dict:
        """
        Validate videos based on a pre-warming results file.
        
        Args:
            results_file: Path to the JSON results file from pre-warming
            base_path: Base path where videos are stored (or base URL for remote videos)
            
        Returns:
            Dictionary with validation results
        """
        try:
            with open(results_file, 'r') as f:
                results_data = json.load(f)
            
            # Check if we have a base URL from parameters
            base_url = results_data.get('parameters', {}).get('base_url', '')
            
            if base_url and not base_path:
                # Validate pre-warmed URLs instead of local files
                return self.validate_prewarmed_urls(results_data)
            else:
                # Original local file validation
                video_paths = []
                
                # Extract video paths from results
                if 'results' in results_data:
                    for original_path, result in results_data['results'].items():
                        # Add original file
                        full_path = os.path.join(base_path, original_path.lstrip('/'))
                        if full_path not in video_paths:
                            video_paths.append(full_path)
                        
                        # Add derivative files if stored locally
                        if 'derivatives' in result:
                            for derivative, details in result['derivatives'].items():
                                if 'local_path' in details:
                                    derivative_path = os.path.join(base_path, details['local_path'].lstrip('/'))
                                    if derivative_path not in video_paths:
                                        video_paths.append(derivative_path)
                
                logger.info(f"Found {len(video_paths)} video files to validate from results")
                return self.validate_videos(video_paths)
            
        except Exception as e:
            logger.error(f"Error loading results file: {str(e)}")
            return self.validation_results
    
    def validate_prewarmed_urls(self, results_data: Dict) -> Dict:
        """
        Validate pre-warmed video URLs from results data.
        
        Args:
            results_data: The results data from pre-warming
            
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating pre-warmed video URLs...")
        
        # Reset validation results
        self.validation_results = {
            'timestamp': datetime.now().isoformat(),
            'total_files': 0,
            'valid_files': 0,
            'corrupted_files': 0,
            'missing_files': 0,
            'validation_errors': [],
            'file_results': {}
        }
        
        if 'results' not in results_data:
            logger.error("No results found in data")
            return self.validation_results
        
        base_url = results_data.get('parameters', {}).get('base_url', '').rstrip('/')
        # Get url_format from parameters, but this doesn't exist in the saved results
        # Check if use_derivatives is True to determine format
        use_derivatives = results_data.get('parameters', {}).get('use_derivatives', False)
        url_format = 'derivative' if use_derivatives else 'imwidth'
        
        # Count total URLs to validate
        total_urls = 0
        for path, result in results_data['results'].items():
            if 'derivatives' in result:
                total_urls += len(result['derivatives'])
        
        self.validation_results['total_files'] = total_urls
        logger.info(f"Found {total_urls} pre-warmed URLs to validate")
        
        if total_urls == 0:
            return self.validation_results
        
        validated = 0
        
        # Validate each pre-warmed URL
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = []
            
            for path, result in results_data['results'].items():
                if 'derivatives' not in result:
                    continue
                    
                for derivative, details in result['derivatives'].items():
                    # Skip if this derivative had errors during pre-warming
                    if details.get('error'):
                        self.validation_results['validation_errors'].append({
                            'path': f"{path}:{derivative}",
                            'error': f"Pre-warming error: {details['error']}",
                            'checks': {}
                        })
                        self.validation_results['corrupted_files'] += 1
                        validated += 1
                        continue
                    
                    # Get the URL that was pre-warmed - this should be stored in the results
                    url = details.get('url', '')
                    if not url:
                        # This shouldn't happen with recent pre-warming runs
                        logger.warning(f"No URL found for {path}:{derivative}, skipping")
                        self.validation_results['validation_errors'].append({
                            'path': f"{path}:{derivative}",
                            'error': 'No URL found in results',
                            'checks': {}
                        })
                        validated += 1
                        continue
                    
                    # Submit validation task
                    future = executor.submit(self.validate_remote_video_url, url, path, derivative)
                    futures.append(future)
            
            # Process results
            for future in as_completed(futures):
                try:
                    url, path, derivative, is_valid, error = future.result()
                    key = f"{path}:{derivative}"
                    
                    self.validation_results['file_results'][key] = {
                        'url': url,
                        'is_valid': is_valid,
                        'error': error
                    }
                    
                    if is_valid:
                        self.validation_results['valid_files'] += 1
                    else:
                        self.validation_results['corrupted_files'] += 1
                        self.validation_results['validation_errors'].append({
                            'path': key,
                            'error': error or 'Unknown validation error',
                            'url': url
                        })
                    
                    validated += 1
                    if validated % 10 == 0:
                        logger.info(f"Progress: {validated}/{total_urls} URLs validated ({validated/total_urls*100:.1f}%)")
                        
                except Exception as e:
                    logger.error(f"Error processing validation result: {str(e)}")
                    validated += 1
        
        logger.info(f"URL validation complete: {validated}/{total_urls} URLs processed")
        return self.validation_results
    
    def validate_remote_video_url(self, url: str, path: str, derivative: str) -> Tuple[str, str, str, bool, Optional[str]]:
        """
        Validate a remote video URL by downloading and checking it.
        
        Args:
            url: The video URL to validate
            path: Original file path
            derivative: Derivative name
            
        Returns:
            Tuple of (url, path, derivative, is_valid, error_message)
        """
        try:
            # First check if URL is accessible
            response = requests.head(url, timeout=30, allow_redirects=True)
            if response.status_code != 200:
                return url, path, derivative, False, f"HTTP {response.status_code}"
            
            # Check content type
            content_type = response.headers.get('Content-Type', '')
            if 'video' not in content_type and 'application/octet-stream' not in content_type:
                return url, path, derivative, False, f"Invalid content type: {content_type}"
            
            # For large files, just verify headers
            content_length = int(response.headers.get('Content-Length', 0))
            if content_length > 100 * 1024 * 1024:  # 100MB
                # For large files, assume valid if headers are OK
                return url, path, derivative, True, None
            
            # For smaller files, download and validate with ffprobe
            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp:
                tmp_path = tmp.name
                
                # Download with streaming
                response = requests.get(url, stream=True, timeout=60)
                response.raise_for_status()
                
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        tmp.write(chunk)
                
                tmp.flush()
                
            # Validate the downloaded file
            try:
                # Quick ffprobe check
                probe_cmd = [
                    'ffprobe',
                    '-v', 'error',
                    '-select_streams', 'v:0',
                    '-show_entries', 'stream=codec_type',
                    '-of', 'default=noprint_wrappers=1:nokey=1',
                    tmp_path
                ]
                
                result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10)
                
                # Clean up temp file
                try:
                    os.unlink(tmp_path)
                except:
                    pass
                
                if result.returncode == 0 and result.stdout.strip() == 'video':
                    return url, path, derivative, True, None
                else:
                    return url, path, derivative, False, "Invalid video stream"
                    
            except subprocess.TimeoutExpired:
                try:
                    os.unlink(tmp_path)
                except:
                    pass
                return url, path, derivative, False, "Validation timeout"
            except Exception as e:
                try:
                    os.unlink(tmp_path)
                except:
                    pass
                return url, path, derivative, False, f"Validation error: {str(e)}"
                
        except requests.exceptions.Timeout:
            return url, path, derivative, False, "Request timeout"
        except requests.exceptions.RequestException as e:
            return url, path, derivative, False, f"Request error: {str(e)}"
        except Exception as e:
            return url, path, derivative, False, f"Error: {str(e)}"
    
    def generate_report(self, output_format: str = 'text') -> str:
        """
        Generate a validation report.
        
        Args:
            output_format: 'text', 'json', or 'markdown'
            
        Returns:
            Formatted report string
        """
        if output_format == 'json':
            return json.dumps(self.validation_results, indent=2)
        
        total = self.validation_results['total_files']
        valid = self.validation_results['valid_files']
        corrupted = self.validation_results['corrupted_files']
        missing = self.validation_results['missing_files']
        
        if output_format == 'markdown':
            report = [
                "# Video Validation Report",
                f"Generated: {self.validation_results['timestamp']}",
                "",
                "## Summary",
                f"- Total files checked: {total}",
                f"- Valid files: {valid} ({valid/total*100:.1f}%)" if total > 0 else "- Valid files: 0",
                f"- Corrupted files: {corrupted}",
                f"- Missing files: {missing}",
                "",
            ]
            
            if self.validation_results['validation_errors']:
                report.extend([
                    "## Corrupted Files",
                    "",
                    "| File | Error | Checks Failed |",
                    "|------|-------|---------------|",
                ])
                
                for error in self.validation_results['validation_errors'][:50]:  # Limit to 50
                    failed_checks = [k for k, v in error['checks'].items() if not v]
                    report.append(
                        f"| {os.path.basename(error['path'])} | "
                        f"{error['error'][:50]}... | "
                        f"{', '.join(failed_checks)} |"
                    )
            
            return "\n".join(report)
        
        else:  # text format
            report = [
                "Video Validation Report",
                f"Generated: {self.validation_results['timestamp']}",
                "-" * 50,
                f"Total files checked: {total}",
                f"Valid files: {valid} ({valid/total*100:.1f}%)" if total > 0 else "Valid files: 0",
                f"Corrupted files: {corrupted}",
                f"Missing files: {missing}",
                "",
            ]
            
            if self.validation_results['validation_errors']:
                report.append("Corrupted files:")
                for error in self.validation_results['validation_errors'][:20]:
                    report.append(f"  - {error['path']}: {error['error']}")
            
            return "\n".join(report)


def validate_directory(directory: str, pattern: str = "*.mp4", workers: int = 10) -> Dict:
    """
    Validate all video files in a directory.
    
    Args:
        directory: Directory path to scan
        pattern: File pattern to match (default: *.mp4)
        workers: Number of parallel workers
        
    Returns:
        Validation results dictionary
    """
    if not os.path.exists(directory):
        logger.error(f"Directory does not exist: {directory}")
        return {
            'timestamp': datetime.now().isoformat(),
            'total_files': 0,
            'valid_files': 0,
            'corrupted_files': 0,
            'missing_files': 0,
            'validation_errors': [],
            'file_results': {},
            'error': f"Directory not found: {directory}"
        }
    
    video_files = list(Path(directory).rglob(pattern))
    video_paths = [str(f) for f in video_files if f.is_file()]
    
    logger.info(f"Found {len(video_paths)} video files to validate in {directory}")
    
    validator = VideoValidator(workers=workers)
    return validator.validate_videos(video_paths)