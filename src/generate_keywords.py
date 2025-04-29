import argparse
from utils.keyword_generator import KeywordGenerator
from main import main as crawl_main

def generate_and_crawl(num_keywords: int = 100):
    """Generate Vietnamese keywords and start crawling process.
    
    Args:
        num_keywords (int): Number of keywords to generate
    """
    # Initialize keyword generator
    generator = KeywordGenerator()
    
    try:
        # Get statistics about available keywords
        stats = generator.get_keyword_stats()
        # print("\nKeyword Categories Statistics:")
        for category, count in stats.items():
            print(f"{category}: {count} keywords")
        
        # Generate keywords
        # print(f"\nGenerating {num_keywords} Vietnamese keywords...")
        keywords = generator.generate_keywords(num_keywords)
        
        # Save keywords to keywords.txt for crawling
        with open("keywords.txt", "w", encoding="utf-8") as f:
            for keyword in keywords:
                f.write(f"{keyword}\n")
        
        # print(f"\nGenerated {len(keywords)} unique keywords")
        # print("Starting crawling process...")
        
        # Start crawling process
        crawl_main()
        
    finally:
        # Close MongoDB connection
        generator.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Vietnamese keywords and start crawling process")
    parser.add_argument("--num-keywords", type=int, default=100,
                       help="Number of keywords to generate (default: 100)")
    
    args = parser.parse_args()
    generate_and_crawl(args.num_keywords) 