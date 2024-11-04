# Cloudflare Analytics Tool

An advanced analytics tool for analyzing Cloudflare zone performance metrics, providing detailed insights into cache efficiency, response times, and overall performance with sampling rate considerations.

## Features

### Cache Performance Analysis
- Cache hit ratio tracking over time with sampling rate indicators
- Detailed cache status distribution analysis
- Content type-specific caching patterns
- Geographic cache performance analysis
- Temporal pattern analysis with confidence scoring

### Performance Metrics
- Time to First Byte (TTFB) analysis
- Origin response time tracking
- Performance percentiles (p50, p95, p99)
- Geographic performance distribution
- Device type impact analysis

### Advanced Features
- Automatic sampling rate adjustment
- Confidence score calculation
- Progressive time slice handling
- Rate limit management
- Detailed error tracking and logging

### Visualization
- Cache hit ratio graphs with sampling rate indicators
- Performance heatmaps
- Geographic distribution visualizations
- Temporal trend analysis
- Error rate visualizations

## Installation

1. Clone the repository:
```bash
git clonehttps://github.com/erfianugrah/cloudflare_api_scripts.git 
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

4. Copy and configure environment variables:
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

### Analysis with Custom Parameters
```bash
python -m src.main --start-time "2024-01-01T00:00:00Z" --end-time "2024-01-02T00:00:00Z" --sample-interval 10
```

The tool will:
1. Fetch available zones from your Cloudflare account
2. Allow zone selection for analysis
3. Process metrics with sampling rate consideration
4. Generate detailed reports and visualizations

## Understanding Results

### Sampling Rates (SR)
- SR indicators show the percentage of traffic sampled for each data point
- Higher SR values (e.g., 100%) indicate complete data
- Lower SR values (e.g., 1%) indicate sampled data
- Confidence scores are provided based on sampling rates

### Cache Analysis
- Hit ratio calculations account for sampling rates
- Cache status distribution shows actual vs. sampled requests
- Performance metrics are weighted by sampling rates

### Performance Metrics
- Response times are analyzed with confidence intervals
- Geographic performance includes sampling rate context
- Error rates are adjusted based on sampling data

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

## Development

### Project Structure
```
src/
├── __init__.py
├── main.py             # Main execution script
├── config.py           # Configuration management
├── types.py            # Type definitions
├── api_client.py       # Cloudflare API interaction
├── data_processor.py   # Data processing
├── analyzer.py         # Metrics analysis
├── visualizer.py       # Visualization generation
├── reporter.py         # Report generation
└── ui.py              # User interface
```

### Adding New Features

1. Metric Analysis:
   - Add types in `types.py`
   - Implement processing in `data_processor.py`
   - Add analysis methods in `analyzer.py`

2. Visualizations:
   - Add methods to `visualizer.py`
   - Update report templates in `reporter.py`

### Error Handling
The tool includes comprehensive error handling for:
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
- Request/response details
- Sampling rate information
- Error tracking
- Performance metrics
- API rate limit status

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Cloudflare GraphQL API documentation
- Contributors to the project
- Open source libraries used in development
