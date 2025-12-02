"""
Master Scraper Runner
Runs all individual scrapers and merges results
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path

# Import individual scrapers
from scrapers.sliit_scraper import SLIITScholarshipScraper
from scrapers.mohe_scraper import MOHEScholarshipScraper
from scrapers.scholarship_positions_scraper import ScholarshipPositionsScraper
from scrapers.ou_scholarships_scraper import OUScholarshipsScraper
from scrapers.mohe_student_loans_scraper import MOHEStudentLoansScraper
from scrapers.bank_education_loans_scraper import BankEducationLoansScraper
from scrapers.daad_scholarships_scraper import DAADScholarshipScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/master_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MasterScraper:
    def __init__(self):
        self.scrapers = []
        self.all_data = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create directories if they don't exist
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)

    def register_scraper(self, scraper_class, name):
        """Register a scraper to be run"""
        self.scrapers.append({
            'class': scraper_class,
            'name': name
        })
        logger.info(f"Registered scraper: {name}")

    def run_all(self):
        """Run all registered scrapers"""
        logger.info(
            f"Starting master scraping process with {len(self.scrapers)} scrapers")
        print("\n" + "="*70)
        print("MASTER SCHOLARSHIP SCRAPER")
        print("="*70 + "\n")

        for scraper_info in self.scrapers:
            try:
                print(f"▶ Running {scraper_info['name']}...")
                scraper = scraper_info['class']()
                scraper.scrape()

                if scraper.data:
                    scraper.save_to_csv()
                    scraper.save_to_json()
                    scraper.display_summary()

                    # Collect data
                    self.all_data.extend(scraper.data)
                    print(f"✓ {scraper_info['name']} completed successfully\n")
                else:
                    print(f"✗ {scraper_info['name']} returned no data\n")

            except Exception as e:
                logger.error(f"Error running {scraper_info['name']}: {e}")
                print(f"✗ {scraper_info['name']} failed: {e}\n")

        logger.info(
            f"Master scraping completed. Total records: {len(self.all_data)}")

    def merge_and_save(self):
        """Merge all data and save combined file"""
        if not self.all_data:
            logger.warning("No data to merge")
            return

        df = pd.DataFrame(self.all_data)

        # Remove exact duplicates
        df = df.drop_duplicates(subset=['name', 'source'], keep='first')

        # Save merged CSV
        merged_csv = f'data/all_scholarships_{self.timestamp}.csv'
        df.to_csv(merged_csv, index=False, encoding='utf-8')
        logger.info(f"Saved merged data to {merged_csv}")

        # Save merged JSON
        import json
        merged_json = f'data/all_scholarships_{self.timestamp}.json'
        with open(merged_json, 'w', encoding='utf-8') as f:
            json.dump(self.all_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved merged data to {merged_json}")

        return df

    def display_final_summary(self, df):
        """Display final summary statistics"""
        print("\n" + "="*70)
        print("FINAL SCRAPING SUMMARY")
        print("="*70)
        print(f"Total Records: {len(df)}")
        print(f"Unique Sources: {df['source'].nunique()}")
        print(f"Scraping Timestamp: {self.timestamp}")

        print(f"\nRecords by Source:")
        print(df['source'].value_counts().to_string())

        print(f"\nData Quality Metrics:")
        print(
            f"  - Records with funding amount: {(df['funding_amount'] != 'N/A').sum()}/{len(df)}")
        print(
            f"  - Records with deadline: {(df['deadline'] != 'N/A').sum()}/{len(df)}")
        print(
            f"  - Records with description: {(df['description'] != '').sum()}/{len(df)}")

        print(f"\nColumn Structure: {list(df.columns)}")

        print("\n=== ALL SCHOLARSHIPS ===")
        print(df[['name', 'source', 'funding_amount']].to_string())

        print("="*70 + "\n")


def main():
    """Main execution"""
    master = MasterScraper()

    # Register all scrapers
    master.register_scraper(SLIITScholarshipScraper, "SLIIT Scholarships")
    master.register_scraper(MOHEScholarshipScraper, "MOHE Scholarships")
    master.register_scraper(ScholarshipPositionsScraper,
                            "Scholarship Positions (Sri Lanka)")
    master.register_scraper(OUScholarshipsScraper,
                            "Open University Scholarships"),
    master.register_scraper(DAADScholarshipScraper,
                            "DAAD Scholarships (Germany)")

    # Loans
    master.register_scraper(MOHEStudentLoansScraper, "MOHE Student Loans")
    master.register_scraper(BankEducationLoansScraper,
                            "Bank Education Loans (All 6 Banks)")

    # Add more scrapers here as you create them:
    # master.register_scraper(AnotherScraper, "Another Source")

    # Run all scrapers
    master.run_all()

    # Merge and save combined data
    if master.all_data:
        df = master.merge_and_save()
        master.display_final_summary(df)
    else:
        logger.warning("No data was scraped from any source")


if __name__ == "__main__":
    main()
