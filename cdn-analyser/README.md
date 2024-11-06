# Cloudflare Analytics Tool

An advanced analytics tool for analyzing Cloudflare zone performance metrics with emphasis on cache efficiency, response times, and comprehensive performance analysis. The tool handles sampling rate considerations and provides detailed insights through various visualization methods.

## Key Features

### Cache Performance Analytics
- Detailed cache hit ratio tracking with sampling rate awareness
- Cache status distribution analysis across content types
- Geographic cache performance patterns
- Temporal cache efficiency analysis
- Content-type specific caching patterns

### Performance Metrics
- Time to First Byte (TTFB) analysis
- Origin response time tracking
- Performance percentiles (p50, p95, p99)
- Geographic performance distribution
- Protocol and device type impact analysis

### Error Analytics
- Status code distribution tracking
- Error rate patterns by endpoint
- Geographic error distribution
- Temporal error analysis
- Error correlation with performance metrics

### Advanced Analysis Features
- Automatic sampling rate adjustment
- Confidence score calculation for metrics
- Progressive time slice analysis
- Rate limit management
- Comprehensive error tracking
- Origin server performance analysis

### Visualization Capabilities
- Interactive performance dashboards
- Cache hit ratio visualization
- Geographic distribution heatmaps
- Temporal trend analysis
- Error rate visualization
- Protocol performance comparison

## Installation

1. Clone the repository:
```bash
git clone https://github.com/erfianugrah/cloudflare_api_scripts.git
cd cloudflare-api-scripts
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
```

## Configuration

Edit `.env` with your Cloudflare credentials:

```plaintext
CLOUDFLARE_ACCOUNT_ID=your_account_id
CLOUDFLARE_API_KEY=your_api_key
CLOUDFLARE_EMAIL=your_email
```

## Usage

### Basic Analysis
```bash
python -m src.main
```

### Custom Analysis Parameters
```bash
python -m src.main --start-time "2024-01-01T00:00:00Z" --end-time "2024-01-02T00:00:00Z" --sample-interval 10
```

### Interactive Mode
The tool will:
1. Display available zones
2. Allow zone selection
3. Configure analysis parameters
4. Process and analyze metrics
5. Generate comprehensive reports

## Understanding Results

### Sampling Rate Analysis
- Each metric includes sampling rate indicators
- Confidence scores based on sampling rates
- Adjusted metrics accounting for sampling

### Cache Analysis
The tool provides:
- Overall cache hit ratios
- Content-type specific analysis
- Geographic cache performance
- Temporal cache patterns

### Performance Metrics
Includes analysis of:
- Response times with confidence intervals
- Geographic performance patterns
- Protocol impact analysis
- Device type performance impact

### Error Analysis
Provides insights into:
- Error patterns by endpoint
- Geographic error distribution
- Temporal error trends
- Error impact on performance

## Output Structure

```
reports/
├── json/
│   ├── raw_response_{zone_id}_{timestamp}.json
│   ├── {zone_name}_cache_analysis.json
│   └── {zone_name}_performance_analysis.json
├── images/
│   └── {zone_name}/
│       ├── cache/
│       │   ├── cache_hit_ratio.png
│       │   ├── cache_distribution.png
│       │   └── geographic_cache.png
│       └── performance/
│           ├── ttfb_distribution.png
│           ├── performance_trends.png
│           └── geo_performance.png
└── logs/
    └── cloudflare_analytics.log
```

## Development Guide

### Project Structure
```
src/
├── __init__.py
├── main.py             # Main execution script
├── config.py           # Configuration management
├── types.py           # Type definitions
├── api_client.py       # Cloudflare API interaction
├── data_processor.py   # Data processing
├── analyzer.py         # Metrics analysis
├── visualizer.py       # Visualization generation
├── origin_analyzer.py  # Origin server analysis
├── origin_reporter.py  # Origin performance reporting
└── reporter.py         # Report generation
```

### Adding New Features

1. Analysis Components:
   - Add types in `types.py`
   - Implement processing in `data_processor.py`
   - Add analysis methods in `analyzer.py`

2. Visualizations:
   - Add visualization methods in `visualizer.py`
   - Update report templates in `reporter.py`

### Error Handling
The tool implements comprehensive error handling for:
- API rate limiting
- Sampling rate variations
- Data validation
- Network issues
- GraphQL query errors

## Testing

Run the test suite:
```bash
python -m pytest tests/
```

For coverage report:
```bash
python -m pytest --cov=src tests/
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Implement changes with appropriate error handling and logging
4. Add tests for new functionality
5. Update documentation
6. Submit a Pull Request

## Error Handling Guidelines

When contributing, ensure proper handling of:
- API rate limits
- Sampling rate variations
- Network timeouts
- Data validation
- GraphQL query errors

## Logging

The tool provides detailed logging with:
- Request/response tracking
- Sampling rate information
- Error tracking
- Performance metrics
- API rate limit status

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Cloudflare GraphQL API documentation
- Contributors to the project
- Open source libraries used in development
