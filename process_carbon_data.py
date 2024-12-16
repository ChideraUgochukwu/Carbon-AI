import pandas as pd
import re
from tqdm import tqdm

def clean_and_format_text(text):
    # Remove any remaining HTML tags
    text = re.sub(r'<[^>]+>', '', str(text))
    # Remove special characters but keep necessary punctuation
    text = re.sub(r'[^\w\s.,;?!()\-:]', '', text)
    # Remove multiple spaces
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def process_carbon_data(input_file='carbon.csv', output_file='carbon.txt'):
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        print(f"Processing {len(df)} articles...")

        with open(output_file, 'w', encoding='utf-8') as f:
            for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing articles"):
                # Write section header
                f.write(f"\n{'='*80}\n")
                f.write(f"SUBJECT: {row['Title']}\n")
                f.write(f"CATEGORY: {row['Search_Term']}\n")
                f.write(f"{'='*80}\n\n")

                # Clean and write content
                content = clean_and_format_text(row['Content'])
                f.write(content)
                f.write('\n\n')

        print(f"\nSuccessfully processed data and saved to {output_file}")
        
        # Print some statistics
        with open(output_file, 'r', encoding='utf-8') as f:
            text = f.read()
            print(f"\nText File Statistics:")
            print(f"Total characters: {len(text):,}")
            print(f"Total words: {len(text.split()):,}")
            print(f"Total lines: {len(text.splitlines()):,}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    process_carbon_data()
