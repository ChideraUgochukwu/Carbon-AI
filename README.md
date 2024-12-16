# Enhanced Wikipedia Content Scraper

A robust, feature-rich Wikipedia content scraping tool with support for concurrent downloads, checkpointing, and detailed statistics tracking.

## Features

- Asynchronous concurrent downloading
- Checkpoint system for resuming interrupted scrapes
- Detailed statistics and progress tracking
- Resource monitoring (CPU, memory usage)
- Configurable rate limiting and retry mechanisms
- Content validation and structure preservation
- Comprehensive logging system
- Command-line interface

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

## Installation

1. Clone or download this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The scraper can be configured through `scraper_config.json`:

```json
{
    "max_retries": 3,          # Maximum number of retry attempts
    "timeout": 30,             # Request timeout in seconds
    "batch_size": 10,          # Number of concurrent downloads
    "min_content_length": 100, # Minimum valid content length
    "checkpoint_frequency": 50, # Save progress every N articles
    "rate_limit": 1.0,         # Seconds between requests
    "user_agents": [...]       # List of user agents to rotate
}
```

## Usage

### Basic Usage

Run the scraper with default settings:
```bash
python wiki_content_scraper_enhanced.py
```

### Custom Input/Output Files

Specify custom input and output files:
```bash
python wiki_content_scraper_enhanced.py --input my_links.csv --output my_data.csv
```

### Custom Configuration

Use a custom configuration file:
```bash
python wiki_content_scraper_enhanced.py --config my_config.json
```

### Disable Resume

Start fresh without resuming from checkpoint:
```bash
python wiki_content_scraper_enhanced.py --no-resume
```

## Input File Format

The input CSV file should contain at least a 'URL' column with Wikipedia article URLs:
```csv
URL
https://en.wikipedia.org/wiki/Article1
https://en.wikipedia.org/wiki/Article2
...
```

## Output Format

The scraper generates a CSV file containing:
- Title: Article title
- Content: Cleaned article text
- Metadata: Additional article information
- Size: Content size in bytes
- Error: Any error messages (if applicable)

## Logging

Logs are written to:
- Console: Basic progress and statistics
- scraper.log: Detailed debug information
- scraper_checkpoint.json: Progress checkpoints

## Statistics

The scraper provides detailed statistics including:
- Total articles processed
- Successful/failed downloads
- Total content size
- Average download rate
- Execution time
- Memory usage

## Error Handling

The scraper includes robust error handling:
- Automatic retries for failed requests
- Rate limiting for API compliance
- Content validation
- Graceful shutdown on interruption
- Progress preservation

## Resource Management

System resource usage is monitored and logged:
- Memory consumption
- CPU usage
- Network statistics
- Disk usage for saved data

## Advanced Features

### Metadata Extraction
- Article categories
- Last modified dates
- Related links
- Section structure

### Content Processing
- HTML cleaning
- Structure preservation
- Content validation
- Size tracking

## Troubleshooting

1. Rate Limiting
   - Adjust `rate_limit` in config if experiencing API blocks
   - Use different user agents

2. Memory Issues
   - Reduce `batch_size` in config
   - Enable checkpointing more frequently

3. Connection Errors
   - Increase `timeout` value
   - Adjust `max_retries`

4. Incomplete Content
   - Check `min_content_length` setting
   - Verify input URLs are valid

## Notes

- Be respectful of Wikipedia's servers
- Consider using their official API for large-scale scraping
- Check Wikipedia's robots.txt and terms of service
- Monitor system resources during large scrapes
