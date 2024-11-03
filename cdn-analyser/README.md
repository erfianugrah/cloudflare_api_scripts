# Cloudflare Analytics Tool

This tool analyzes Cloudflare zone performance metrics, providing insights into cache efficiency, response times, and overall performance.

## Features

- Cache performance analysis
- Response time metrics
- Geographic distribution analysis
- Error rate monitoring
- Automated visualization generation
- Comprehensive reporting

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/cloudflare-analytics.git
cd cloudflare-analytics
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Copy the environment template and fill in your Cloudflare credentials:
```bash
cp .env.example .env
```

## Configuration

Edit `.env` file with your Cloudflare credentials:

```plaintext
CLOUDFLARE_ACCOUNT_ID=your_account_id
CLOUDFLARE_API_KEY=your_api_key
CLOUDFLARE_EMAIL=your_email
```

## Usage

Run the analysis:
```bash
python -m src.main
```

The tool will:
1. Fetch available zones from your Cloudflare account
2. Allow you to select specific zones to analyze
3. Generate analysis reports and visualizations in the `reports` directory

## Output Structure

```
reports/
├── json/
│   ├── zones.json
│   ├── {zone_name}_cache_analysis.json
│   └── {zone_name}_performance_analysis.json
├── images/
│   └── {zone_name}/
│       ├── cache/
│       │   ├── cache_status_distribution.png
│       │   ├── hourly_cache.png
│       │   └── ...
│       └── performance/
│           ├── response_time_distribution.png
│           ├── performance_trends.png
│           └── ...
└── logs/
    └── cloudflare_analytics.log
```

## Generated Reports

Each zone analysis includes:

### Cache Analysis
- Overall cache hit ratio
- Cache status distribution
- Content type performance
- Geographic cache performance
- Hourly trends

### Performance Analysis
- Response time metrics (TTFB, Origin)
- Error rates
- Geographic performance
- Performance trends
- Percentile analysis

## Development

### Project Structure
```
src/
├── __init__.py
├── main.py          # Main execution script
├── config.py        # Configuration management
├── types.py         # Type definitions
├── api_client.py    # Cloudflare API interaction
├── data_processor.py # Data processing
├── analyzer.py      # Metrics analysis
├── visualizer.py    # Visualization generation
├── reporter.py      # Report generation
└── ui.py            # User interface
```

### Adding New Features

1. For new metrics:
   - Add type definitions in `types.py`
   - Update data processing in `data_processor.py`
   - Add analysis methods in `analyzer.py`
   - Create visualizations in `visualizer.py`

2. For new visualizations:
   - Add methods to `visualizer.py`
   - Update report templates in `reporter.py`

## Testing

To run tests:
```bash
python -m pytest tests/
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Cloudflare GraphQL API documentation
- Various open-source libraries used in this project
