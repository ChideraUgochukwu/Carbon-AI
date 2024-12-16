import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from tqdm import tqdm
import asyncio
import aiohttp
import json
import logging
import argparse
from pathlib import Path
import sys
from typing import Dict, List, Optional
import psutil
import urllib3
from datetime import datetime
import signal

class WikiContentScraper:
    def __init__(self, config_file: str = 'scraper_config.json'):
        self.load_config(config_file)
        self.setup_logging()
        self.setup_session()
        self.initialize_stats()
        self.setup_signal_handlers()
        
    def setup_signal_handlers(self):
        """Setup handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
    def handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        self.logger.info("Shutdown signal received. Saving progress...")
        self.save_checkpoint()
        sys.exit(0)
        
    def load_config(self, config_file: str):
        """Load configuration from JSON file or use defaults"""
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"Config file {config_file} not found. Using defaults.")
            self.config = {
                'max_retries': 3,
                'timeout': 30,
                'batch_size': 10,
                'min_content_length': 100,
                'checkpoint_frequency': 50,
                'rate_limit': 1.0,  # seconds between requests
                'user_agents': [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                ]
            }
            # Save default config
            with open(config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
    
    def setup_logging(self):
        """Configure logging with different levels"""
        self.logger = logging.getLogger('WikiScraper')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        fh = logging.FileHandler('scraper.log')
        fh.setLevel(logging.DEBUG)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    
    def setup_session(self):
        """Setup HTTP session with connection pooling"""
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=urllib3.util.Retry(
                total=self.config['max_retries'],
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def initialize_stats(self):
        """Initialize statistics tracking"""
        self.stats = {
            'total_size_bytes': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'retried_downloads': 0,
            'start_time': None,
            'last_checkpoint': None,
            'processed_urls': set()
        }
    
    def clean_text(self, text: str) -> str:
        """Enhanced text cleaning with structure preservation"""
        # Remove citations [1], [2], etc.
        text = re.sub(r'\[\d+\]', '', text)
        # Remove multiple spaces while preserving newlines
        text = re.sub(r'[ \t]+', ' ', text)
        # Remove special characters but keep basic punctuation and structure
        text = re.sub(r'[^\w\s.,;?!():\-\n]', '', text)
        return text.strip()
    
    async def get_page_content(self, url: str, session: aiohttp.ClientSession) -> Dict:
        """Asynchronously get page content with enhanced error handling and validation"""
        result = {
            'url': url,
            'content': '',
            'metadata': {},
            'error': None,
            'size_bytes': 0
        }
        
        try:
            async with session.get(url, timeout=self.config['timeout']) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract metadata
                result['metadata'] = {
                    'title': soup.title.string if soup.title else '',
                    'last_modified': response.headers.get('last-modified', ''),
                    'categories': [tag.string for tag in soup.find_all('div', {'class': 'mw-normal-catlinks'})]
                }
                
                # Remove unwanted elements
                for element in soup.find_all(['script', 'style', 'footer', 'header', 'nav']):
                    element.decompose()
                
                # Get main content
                content = soup.find(id='mw-content-text')
                if not content:
                    raise Exception("No main content found")
                
                # Extract structured content
                structured_content = []
                for element in content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'table']):
                    if element.name.startswith('h'):
                        structured_content.append(f"\n== {element.get_text().strip()} ==\n")
                    elif element.name in ['ul', 'ol']:
                        for li in element.find_all('li'):
                            structured_content.append(f"- {li.get_text().strip()}")
                    elif element.name == 'table':
                        structured_content.append("[Table content preserved]")
                    else:
                        structured_content.append(element.get_text().strip())
                
                text = '\n'.join(structured_content)
                cleaned_text = self.clean_text(text)
                
                # Validate content
                if len(cleaned_text) < self.config['min_content_length']:
                    raise Exception("Content too short")
                
                result['content'] = cleaned_text
                result['size_bytes'] = len(cleaned_text.encode('utf-8'))
                
                # Update statistics
                self.stats['successful_downloads'] += 1
                self.stats['total_size_bytes'] += result['size_bytes']
                
                # Log success
                self.logger.debug(f"Successfully scraped {url} ({result['size_bytes'] / 1024:.2f} KB)")
                
        except Exception as e:
            self.stats['failed_downloads'] += 1
            result['error'] = str(e)
            self.logger.error(f"Error scraping {url}: {str(e)}")
        
        return result
    
    def save_checkpoint(self):
        """Save current progress to checkpoint file"""
        checkpoint = {
            'stats': self.stats,
            'timestamp': datetime.now().isoformat()
        }
        with open('scraper_checkpoint.json', 'w') as f:
            json.dump(checkpoint, f)
        self.stats['last_checkpoint'] = time.time()
        self.logger.info("Checkpoint saved")
    
    def load_checkpoint(self) -> bool:
        """Load progress from checkpoint file"""
        try:
            with open('scraper_checkpoint.json', 'r') as f:
                checkpoint = json.load(f)
            self.stats = checkpoint['stats']
            self.logger.info(f"Checkpoint loaded from {checkpoint['timestamp']}")
            return True
        except FileNotFoundError:
            return False
    
    def monitor_resources(self):
        """Monitor system resources"""
        process = psutil.Process()
        memory_info = process.memory_info()
        cpu_percent = process.cpu_percent()
        self.logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        self.logger.info(f"CPU usage: {cpu_percent}%")
    
    async def process_batch(self, urls: List[str]) -> List[Dict]:
        """Process a batch of URLs concurrently"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in urls:
                if url not in self.stats['processed_urls']:
                    tasks.append(self.get_page_content(url, session))
                    self.stats['processed_urls'].add(url)
                    await asyncio.sleep(self.config['rate_limit'])  # Rate limiting
            
            return await asyncio.gather(*tasks)
    
    async def scrape_links(self, input_file: str = 'carbon_family_links.csv', 
                          output_file: str = 'carbon_enhanced.csv',
                          resume: bool = True):
        """Main scraping function with batching and checkpointing"""
        try:
            # Initialize or resume
            if resume and self.load_checkpoint():
                self.logger.info("Resuming from checkpoint")
            else:
                self.stats['start_time'] = time.time()
            
            # Read input file
            df = pd.read_csv(input_file)
            self.logger.info(f"Found {len(df)} links in total")
            
            # Process in batches
            results = []
            for i in range(0, len(df), self.config['batch_size']):
                batch_urls = df['URL'].iloc[i:i + self.config['batch_size']].tolist()
                
                # Process batch
                batch_results = await self.process_batch(batch_urls)
                results.extend(batch_results)
                
                # Periodic checkpoint
                if i % self.config['checkpoint_frequency'] == 0:
                    self.save_checkpoint()
                    self.monitor_resources()
                
                # Update progress
                self.logger.info(f"Processed {i + len(batch_results)}/{len(df)} URLs")
            
            # Create output dataframe
            output_df = pd.DataFrame(results)
            
            # Save results
            output_df.to_csv(output_file, index=False)
            self.logger.info(f"Results saved to {output_file}")
            
            # Final statistics
            execution_time = time.time() - self.stats['start_time']
            total_size_mb = self.stats['total_size_bytes'] / (1024 * 1024)
            
            self.logger.info("\n=== Final Statistics ===")
            self.logger.info(f"Total articles processed: {len(df)}")
            self.logger.info(f"Successful downloads: {self.stats['successful_downloads']}")
            self.logger.info(f"Failed downloads: {self.stats['failed_downloads']}")
            self.logger.info(f"Total content size: {total_size_mb:.2f} MB")
            self.logger.info(f"Average download rate: {total_size_mb / execution_time:.2f} MB/s")
            self.logger.info(f"Total execution time: {execution_time:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Error in scrape_links: {str(e)}")
            self.save_checkpoint()  # Save progress on error
            raise

def main():
    parser = argparse.ArgumentParser(description='Enhanced Wikipedia Content Scraper')
    parser.add_argument('--input', default='carbon_family_links.csv', help='Input CSV file')
    parser.add_argument('--output', default='carbon_enhanced.csv', help='Output CSV file')
    parser.add_argument('--config', default='scraper_config.json', help='Configuration file')
    parser.add_argument('--no-resume', action='store_true', help='Do not resume from checkpoint')
    args = parser.parse_args()
    
    scraper = WikiContentScraper(config_file=args.config)
    asyncio.run(scraper.scrape_links(args.input, args.output, not args.no_resume))

if __name__ == "__main__":
    main()
