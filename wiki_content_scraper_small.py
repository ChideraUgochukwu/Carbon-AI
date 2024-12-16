import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from tqdm import tqdm

class WikiContentScraper:
    def __init__(self, max_pages=100):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.max_pages = max_pages
        self.total_size_bytes = 0
        self.successful_downloads = 0
        self.failed_downloads = 0

    def clean_text(self, text):
        # Remove citations [1], [2], etc.
        text = re.sub(r'\[\d+\]', '', text)
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text)
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s.,;?!-]', '', text)
        return text.strip()

    def get_page_content(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove unwanted elements
            for element in soup.find_all(['script', 'style', 'footer', 'header', 'nav']):
                element.decompose()

            # Get the main content div
            content = soup.find(id='mw-content-text')
            if not content:
                self.failed_downloads += 1
                return ""

            # Extract text from paragraphs
            paragraphs = content.find_all('p')
            text = ' '.join(p.get_text() for p in paragraphs)
            
            # Clean the text
            cleaned_text = self.clean_text(text)
            
            # Calculate and print size information
            size_bytes = len(cleaned_text.encode('utf-8'))
            size_kb = size_bytes / 1024
            print(f"Downloaded {size_kb:.2f} KB from {url}")
            
            self.total_size_bytes += size_bytes
            self.successful_downloads += 1
            
            return cleaned_text

        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")
            self.failed_downloads += 1
            return ""

    def scrape_links(self, input_file='carbon_family_links.csv', output_file='carbon.csv'):
        try:
            # Read the input CSV
            df = pd.read_csv(input_file)
            print(f"Found {len(df)} links in total")
            
            # Limit to max_pages
            df = df.head(self.max_pages)
            print(f"Processing first {len(df)} links")

            # Add a new column for content
            df['Content'] = ''
            
            # Scrape content for each URL with progress bar
            start_time = time.time()
            for index in tqdm(df.index, desc="Scraping Wikipedia pages"):
                url = df.loc[index, 'URL']
                content = self.get_page_content(url)
                df.loc[index, 'Content'] = content
                
                # Add a small delay to be nice to Wikipedia's servers
                time.sleep(1)

            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Save to CSV
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            # Print comprehensive statistics
            print("\n=== Scraping Statistics ===")
            print(f"Total articles attempted: {len(df)}")
            print(f"Successful downloads: {self.successful_downloads}")
            print(f"Failed downloads: {self.failed_downloads}")
            print(f"Total execution time: {execution_time:.2f} seconds")
            
            # Size statistics
            total_size_mb = self.total_size_bytes / (1024 * 1024)
            avg_size_kb = (self.total_size_bytes / max(1, self.successful_downloads)) / 1024
            print(f"\n=== Size Statistics ===")
            print(f"Total content size: {total_size_mb:.2f} MB")
            print(f"Average article size: {avg_size_kb:.2f} KB")
            
            # Content statistics
            content_lengths = df['Content'].str.len()
            print(f"\n=== Content Statistics ===")
            print(f"Average content length: {content_lengths.mean():.0f} characters")
            print(f"Minimum content length: {content_lengths.min()} characters")
            print(f"Maximum content length: {content_lengths.max()} characters")
            print(f"Empty articles: {(content_lengths == 0).sum()}")
            
            # Download rate
            download_rate = self.total_size_bytes / (1024 * 1024 * execution_time)
            print(f"\n=== Performance ===")
            print(f"Average download rate: {download_rate:.2f} MB/s")

        except Exception as e:
            print(f"Error processing files: {str(e)}")

def main():
    scraper = WikiContentScraper(max_pages=100)
    scraper.scrape_links()

if __name__ == "__main__":
    main()
