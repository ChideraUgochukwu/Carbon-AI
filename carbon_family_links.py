import requests
import pandas as pd
import time
from urllib.parse import quote

class WikipediaLinkGenerator:
    def __init__(self):
        self.base_url = "https://en.wikipedia.org/w/api.php"
        self.links = set()
        self.seen_titles = set()
        
    def make_request(self, params):
        try:
            response = requests.get(self.base_url, params=params)
            return response.json()
        except Exception as e:
            print(f"Error making request: {e}")
            return None

    def search_wikipedia(self, search_term, limit=50):
        params = {
            "action": "query",
            "format": "json",
            "list": "search",
            "srsearch": search_term,
            "srlimit": limit,
            "utf8": 1
        }
        
        data = self.make_request(params)
        if not data or 'query' not in data:
            return []
            
        return [item['title'] for item in data['query']['search']]

    def get_related_links(self, title):
        params = {
            "action": "query",
            "format": "json",
            "titles": title,
            "prop": "links|categories",
            "pllimit": "max",
            "cllimit": "max"
        }
        
        data = self.make_request(params)
        if not data or 'query' not in data or 'pages' not in data['query']:
            return []
            
        pages = data['query']['pages']
        related_titles = []
        
        for page_id in pages:
            page = pages[page_id]
            if 'links' in page:
                related_titles.extend([link['title'] for link in page['links']])
                
        return related_titles

    def generate_wiki_url(self, title):
        encoded_title = quote(title.replace(" ", "_"))
        return f"https://en.wikipedia.org/wiki/{encoded_title}"

    def collect_links(self, target_count=100000):
        search_terms = [
            # Core elements
            "Carbon element", "Silicon element", "Germanium element", 
            "Tin element", "Lead element", "Flerovium",
            
            # Compounds and related terms
            "Carbon compounds", "Silicon compounds", "Germanium compounds",
            "Tin compounds", "Lead compounds",
            "Carbon allotropes", "Silicon allotropes",
            "Carbon nanotubes", "Graphene", "Diamond",
            "Silicates", "Germanates", "Stannates", "Lead compounds",
            
            # Applications and properties
            "Carbon chemistry", "Silicon chemistry", "Germanium chemistry",
            "Tin chemistry", "Lead chemistry",
            "Carbon materials", "Silicon materials",
            "Semiconductor materials", "Group 14 elements",
            
            # Industrial and technological applications
            "Carbon fiber", "Silicon chips", "Germanium transistors",
            "Tin plating", "Lead acid battery",
            
            # Environmental and safety
            "Carbon cycle", "Silicon cycle", "Lead poisoning",
            "Carbon emissions", "Carbon sequestration",
            
            # Analytical methods
            "Carbon dating", "Silicon analysis", "Lead testing",
            "Carbon isotopes", "Silicon isotopes"
        ]
        
        print("Starting to collect Wikipedia links...")
        
        for search_term in search_terms:
            if len(self.links) >= target_count:
                break
                
            print(f"Searching for: {search_term}")
            titles = self.search_wikipedia(search_term)
            
            for title in titles:
                if title not in self.seen_titles:
                    self.seen_titles.add(title)
                    url = self.generate_wiki_url(title)
                    self.links.add((title, url, search_term))
                    
                    # Get related links
                    related_titles = self.get_related_links(title)
                    for related_title in related_titles:
                        if len(self.links) >= target_count:
                            break
                        if related_title not in self.seen_titles:
                            self.seen_titles.add(related_title)
                            related_url = self.generate_wiki_url(related_title)
                            self.links.add((related_title, related_url, f"Related to {search_term}"))
                
                if len(self.links) >= target_count:
                    break
                    
            time.sleep(1)  # Be nice to Wikipedia's servers

        return list(self.links)[:target_count]

    def save_to_csv(self, filename="carbon_family_links.csv"):
        df = pd.DataFrame(list(self.links), columns=['Title', 'URL', 'Search_Term'])
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nSaved {len(df)} links to {filename}")
        print(f"\nSample of collected links:")
        print(df.head())

def main():
    generator = WikipediaLinkGenerator()
    generator.collect_links()
    generator.save_to_csv()

if __name__ == "__main__":
    main()
