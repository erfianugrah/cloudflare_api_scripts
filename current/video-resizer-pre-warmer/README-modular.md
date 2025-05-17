# Modular Video Resizer Pre-Warmer and Optimizer

This project has been refactored into a modular architecture to improve maintainability and extensibility.

## Module Structure

- `main.py`: Main entry point for the application
- `modules/`: Directory containing modular components
  - `__init__.py`: Package initialization
  - `config.py`: Configuration and argument parsing
  - `storage.py`: Remote storage access functions (rclone/S3)
  - `video_utils.py`: Video file analysis and utility functions
  - `encoding.py`: Video encoding parameters and optimization
  - `processing.py`: Core URL processing functionality
  - `reporting.py`: Report generation and statistics

## Running the Application

You can run the application using either:

```bash
# Using the main script directly
python3 main.py [options]

# Or using the run.sh wrapper
./run.sh [options]
```

## Key Features

1. **Pre-warming Video Transformation Cache**
   - Process videos with multiple derivatives (desktop, tablet, mobile)
   - Concurrent processing with size-optimized worker allocation
   - Performance measurement and reporting

2. **Video Optimization**
   - Re-encode large video files using FFmpeg
   - Support for multiple codecs (H.264, H.265, VP9, VP8, AV1)
   - Quality profiles from maximum quality to minimum size
   - WebM format support alongside primary format

3. **File Analysis**
   - Generate file size reports
   - Size-based categorization and optimized processing

## Dependencies

- Python 3.6+
- FFmpeg (for video optimization)
- rclone (for remote storage access)
- Python packages listed in requirements.txt

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Ensure FFmpeg and rclone are installed and in your PATH

## Usage Examples

### Pre-warming video transformations:

```bash
./run.sh --remote my-remote --bucket my-bucket --directory videos \
  --base-url https://example.com --derivatives desktop,tablet,mobile \
  --workers 8 --optimize-by-size
```

### Video optimization:

```bash
./run.sh --remote my-remote --bucket my-bucket --directory videos \
  --optimize-videos --codec h265 --quality balanced --create-webm \
  --workers 4 --limit 10
```

### File analysis:

```bash
./run.sh --remote my-remote --bucket my-bucket --directory videos \
  --list-files --size-threshold 100
```