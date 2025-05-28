"""
Storage utilities for accessing remote and local storage systems.
"""
import os
import shutil
import tempfile
import subprocess
import logging
import re
import json
from modules import video_utils

# Set up module logger
logger = logging.getLogger(__name__)

def list_objects(remote, bucket, directory, extension, limit=0, logger=None, use_aws_cli=False, get_sizes=True):
    """
    List objects in the specified bucket and directory, optionally with their sizes.
    Supports both rclone and AWS CLI for retrieving object lists.
    
    Args:
        remote: The rclone remote name
        bucket: The S3 bucket name
        directory: The directory within the bucket
        extension: File extension to filter by (can be comma-separated for multiple extensions)
        limit: Maximum number of objects to return (0 for no limit)
        logger: Logger instance
        use_aws_cli: Whether to use AWS CLI instead of rclone
        get_sizes: Whether to retrieve file sizes (slightly slower but more informative)
        
    Returns:
        If get_sizes=False: List of file paths
        If get_sizes=True: List of dicts with 'path' and 'size' keys
    """
    path = f"{remote}:{bucket}/{directory}"
    path = path.rstrip('/') # Remove trailing slash if present
    
    if use_aws_cli:
        # Use AWS CLI for listing (can be faster for very large buckets)
        prefix = directory.rstrip('/') + '/' if directory else ''
        cmd = [
            "aws", "s3", "ls", f"s3://{bucket}/{prefix}", 
            "--recursive"
        ]
        
        if extension:
            # We'll filter by extension later, as aws cli doesn't support this directly
            pass
            
        logger.info(f"Listing objects using AWS CLI: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Parse output format: "YYYY-MM-DD HH:MM:SS 12345 file.mp4"
            all_files = []
            for line in result.stdout.splitlines():
                parts = line.strip().split()
                if len(parts) >= 4:
                    size_bytes = int(parts[2])
                    file_path = ' '.join(parts[3:])  # Handle paths with spaces
                    
                    # Handle multiple extensions (comma-separated)
                    if extension:
                        extensions = [ext.strip().lower() for ext in extension.split(',')]
                        if any(file_path.lower().endswith(ext) for ext in extensions):
                            if get_sizes:
                                all_files.append((file_path, size_bytes))
                            else:
                                all_files.append(file_path)
                    else:
                        # No extension filter
                        if get_sizes:
                            all_files.append((file_path, size_bytes))
                        else:
                            all_files.append(file_path)
            
        except subprocess.CalledProcessError as e:
            if logger:
                logger.error(f"Error listing with AWS CLI: {e.stderr}")
            return []
            
    else:
        # Use rclone for listing
        # Note: 'ls' command in rclone is already recursive by default, no need for --recursive flag
        cmd = ["rclone", "ls", path]
        
        if logger:
            logger.info(f"Listing objects using rclone: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Parse output format: "   12345 path/to/file.mp4"
            all_files = []
            for line in result.stdout.splitlines():
                parts = line.strip().split(maxsplit=1)
                if len(parts) == 2:
                    size_bytes = int(parts[0])
                    file_path = parts[1]
                    
                    # Handle multiple extensions (comma-separated)
                    if extension:
                        extensions = [ext.strip().lower() for ext in extension.split(',')]
                        if any(file_path.lower().endswith(ext) for ext in extensions):
                            if get_sizes:
                                all_files.append((file_path, size_bytes))
                            else:
                                all_files.append(file_path)
                    else:
                        # No extension filter
                        if get_sizes:
                            all_files.append((file_path, size_bytes))
                        else:
                            all_files.append(file_path)
                            
        except subprocess.CalledProcessError as e:
            if logger:
                logger.error(f"Error listing with rclone: {e.stderr}")
            return []
    
    # Sort files (by size if available, otherwise by name)
    if get_sizes:
        all_files.sort(key=lambda x: x[1], reverse=True)
    else:
        all_files.sort()
    
    # Apply limit if specified
    if limit > 0 and len(all_files) > limit:
        if logger:
            logger.info(f"Limiting to {limit} files (out of {len(all_files)})")
        all_files = all_files[:limit]
    
    # Format output based on get_sizes flag
    if get_sizes:
        results = [{'path': path, 'size': size} for path, size in all_files]
    else:
        results = all_files
        
    return results

def get_file_sizes_batch(remote, bucket, directory, file_paths, logger=None, batch_size=1000):
    """
    Get sizes of multiple files in batch using rclone lsjson.
    Much more efficient than individual size queries.
    
    Args:
        remote: The rclone remote name
        bucket: The S3 bucket name
        directory: The directory within the bucket
        file_paths: List of file paths to get sizes for
        logger: Logger instance
        batch_size: Number of files to process in each batch
        
    Returns:
        list: List of tuples (file_path, size_in_bytes)
    """
    if logger:
        logger.info(f"Getting file sizes for {len(file_paths)} files using batch operations")
    
    file_sizes = []
    
    # Process files in batches to avoid command line length limits
    for i in range(0, len(file_paths), batch_size):
        batch_paths = file_paths[i:i + batch_size]
        
        try:
            # Use rclone lsjson for efficient batch retrieval
            path = f"{remote}:{bucket}/{directory}"
            cmd = ['rclone', 'lsjson', path, '--files-only', '--no-modtime', '--no-mimetype']
            
            if logger:
                logger.debug(f"Executing batch rclone command for {len(batch_paths)} files")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Parse JSON output
            files_data = json.loads(result.stdout)
            
            # Create a map for quick lookup
            size_map = {}
            for file_info in files_data:
                # Handle both Name and Path fields
                file_path = file_info.get('Path', file_info.get('Name', ''))
                file_size = file_info.get('Size', 0)
                size_map[file_path] = file_size
            
            # Match requested paths
            for path in batch_paths:
                # Try exact match first
                if path in size_map:
                    file_sizes.append((path, size_map[path]))
                else:
                    # Try with directory prefix removed
                    path_without_dir = path.split('/')[-1]
                    if path_without_dir in size_map:
                        file_sizes.append((path, size_map[path_without_dir]))
                    else:
                        if logger:
                            logger.warning(f"File not found in batch: {path}")
        
        except subprocess.CalledProcessError as e:
            if logger:
                logger.error(f"Error in batch file size retrieval: {e.stderr}")
        except json.JSONDecodeError as e:
            if logger:
                logger.error(f"Error parsing JSON from rclone: {str(e)}")
    
    if logger:
        logger.info(f"Retrieved sizes for {len(file_sizes)} out of {len(file_paths)} files")
    
    return file_sizes

def get_file_sizes(remote, bucket, directory, file_paths, logger=None, use_aws_cli=False):
    """
    Get sizes of files using either rclone or AWS CLI.
    
    NOTE: This function is kept for backward compatibility but now uses
    the more efficient batch implementation internally.
    
    Args:
        remote: The rclone remote name
        bucket: The S3 bucket name
        directory: The directory within the bucket
        file_paths: List of file paths to get sizes for
        logger: Logger instance
        use_aws_cli: Whether to use AWS CLI instead of rclone
        
    Returns:
        list: List of tuples (file_path, size_in_bytes)
    """
    # Use the more efficient batch implementation
    if not use_aws_cli:
        return get_file_sizes_batch(remote, bucket, directory, file_paths, logger)
    
    # AWS CLI approach kept for compatibility
    if logger:
        logger.info(f"Getting file sizes for {len(file_paths)} files using AWS CLI")
    file_sizes = []
    
    try:
        # AWS CLI approach for getting file sizes
        s3_path = f"s3://{bucket}/{directory}"
        if directory and not s3_path.endswith('/'):
            s3_path += '/'
        
        # For AWS CLI, we get all file sizes at once, then filter
        cmd = ['aws', 's3', 'ls', '--recursive', '--human-readable', s3_path]
        if logger:
            logger.debug(f"Executing AWS CLI command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse output and build a lookup
        size_lookup = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 4:  # Format: DATE TIME SIZE FILENAME
                size_str = parts[2]
                filename = ' '.join(parts[3:])  # Join in case of spaces in filename
                
                # Convert human-readable size to bytes
                size_bytes = parse_human_readable_size(size_str)
                
                size_lookup[filename] = size_bytes
        
        # Find matching files
        for path in file_paths:
            # For S3, the paths in the listing might include the directory
            full_path = os.path.join(directory, path).replace('\\', '/')
            if full_path in size_lookup:
                file_sizes.append((path, size_lookup[full_path]))
            elif path in size_lookup:
                file_sizes.append((path, size_lookup[path]))
    
    except Exception as e:
        if logger:
            logger.error(f"Error getting file sizes with AWS CLI: {str(e)}")
    
    return file_sizes

def parse_human_readable_size(size_str):
    """
    Parse human-readable size strings like "1.5 GiB" into bytes.
    
    Args:
        size_str: Size string to parse
        
    Returns:
        int: Size in bytes
    """
    units = {
        'B': 1,
        'KiB': 1024,
        'MiB': 1024**2,
        'GiB': 1024**3,
        'TiB': 1024**4,
        'KB': 1000,
        'MB': 1000**2,
        'GB': 1000**3,
        'TB': 1000**4
    }
    
    # Remove commas and handle decimal points
    size_str = size_str.replace(',', '')
    
    # Check for a recognized unit suffix
    for unit, multiplier in units.items():
        if size_str.endswith(f" {unit}"):
            size_value = float(size_str.split(unit)[0].strip())
            return int(size_value * multiplier)
    
    # If no unit or unrecognized, try to convert directly to int
    try:
        return int(size_str)
    except ValueError:
        return 0

def download_from_rclone(remote_path, local_path, use_temp=True):
    """
    Download a file from rclone remote.
    
    Args:
        remote_path: Path to the remote file (e.g. "r2:videos/file.mp4")
        local_path: Path where to save the file locally
        use_temp: Whether to use a temporary file for download (safer)
        
    Returns:
        bool: True if successful, False otherwise
    """
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

def list_large_files(remote, bucket, directory, extension, size_threshold_mib, limit=0):
    """
    List large files from the remote storage.
    
    Args:
        remote: Rclone remote name
        bucket: Bucket name
        directory: Directory within bucket
        extension: File extension to filter
        size_threshold_mib: Size threshold in MiB
        limit: Maximum number of files to return (0 for no limit)
        
    Returns:
        list: List of tuples (file_path, size_bytes)
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

def upload_to_rclone(local_path, remote_path):
    """
    Upload a file to rclone remote.
    
    Args:
        local_path: Path to the local file
        remote_path: Path to the remote file (e.g. "r2:videos/file.mp4")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cmd = ["rclone", "copy", local_path, os.path.dirname(remote_path)]
        logger.info(f"Uploading file: {cmd}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error uploading {local_path}: {e.stderr}")
        return False

def replace_file_in_place(remote, bucket, directory, file_path, local_optimized_path):
    """
    Replace a file in the remote storage with a local optimized version.
    
    Args:
        remote: Rclone remote name
        bucket: Bucket name  
        directory: Directory within bucket
        file_path: Path to the file within the directory
        local_optimized_path: Path to the local optimized file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        full_remote_path = f"{remote}:{bucket}/{directory}/{file_path}"
        logger.info(f"Replacing file in-place: {full_remote_path}")
        
        # Upload the optimized file to replace the original
        cmd = ["rclone", "copyto", local_optimized_path, full_remote_path]
        logger.info(f"Running command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(f"Successfully replaced {file_path} with optimized version")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error replacing {file_path}: {e.stderr}")
        return False