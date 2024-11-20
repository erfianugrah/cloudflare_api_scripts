# Cloudflare CDN Analytics Tool

An advanced analytics tool for analyzing Cloudflare zone performance metrics with emphasis on cache efficiency, response times, and comprehensive performance analysis. The tool handles sampling rate considerations and provides detailed insights through various visualization methods.

## Key Features

### Cache Performance Analytics
- Detailed cache hit ratio tracking with sampling rate awareness
- Cache status distribution analysis across content types
- Geographic cache performance patterns
- Temporal cache efficiency analysis
- Content-type specific caching patterns
- Bandwidth savings tracking
- Cache purge impact analysis

### Performance Metrics
- Edge vs Origin response time analysis
- Time to First Byte (TTFB) metrics
- Origin server performance tracking
- Performance percentiles (p50, p95, p99)
- Geographic performance distribution
- Protocol and device type impact analysis
- ASN-level performance tracking

### Error Analytics
- Status code distribution tracking
- Error rate patterns by endpoint
- Geographic error distribution
- Temporal error analysis
- Error correlation with performance metrics
- Origin error tracking
- Error rate trending

### Advanced Analysis Features
- Automatic sampling rate adjustment
- Confidence score calculation for metrics
- Progressive time slice analysis
- Rate limit management
- Comprehensive error tracking
- Origin server performance analysis
- Request volume impact assessment

### Visualization Capabilities
- Interactive performance dashboards
- Cache hit ratio visualization
- Geographic distribution heatmaps
- Temporal trend analysis
- Error rate visualization
- Protocol performance comparison
- Origin response time analysis

## Installation

1. Clone the repository:
```bash
git clone https://github.com/erfianugrah/cloudflare_api_scripts.git
cd cloudflare-api-scripts/cdn-analyzer
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

## Usage

### Basic Analysis
```bash
python -m src.main
```

The tool will guide you through an interactive setup:

1. **Time Range Selection:**
   - Last 3 hours
   - Last 24 hours
   - Last 7 days
   - Last 30 days
   - Custom range

2. **Sampling Configuration:**
   - Auto (Cloudflare optimized)
   - High precision (100% sampling)
   - Balanced (10% sampling)
   - Low precision (1% sampling)
   - Custom sampling rate

3. **Zone Selection:**
   - View available zones
   - Select specific zones or analyze all
   - Confirm selection

### Analysis Process

The tool will then:
1. Fetch metrics using Cloudflare's GraphQL API
2. Process data with appropriate sampling rates
3. Generate performance visualizations
4. Create interactive dashboards
5. Save detailed reports

### Output

Analysis results are saved in the `reports` directory:
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
