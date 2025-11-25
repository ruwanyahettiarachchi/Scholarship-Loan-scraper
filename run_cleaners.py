"""
Master Data Cleaner Runner
Runs all data cleaners to produce final scholarships.csv and loans.csv
"""

import sys
import os
import logging
from datetime import datetime

# Add scrapers to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_cleaner_master.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_cleaners():
    """Run all data cleaners"""
    logger.info("Starting Master Data Cleaner")
    print("\n" + "="*70)
    print("MASTER DATA CLEANER")
    print("="*70 + "\n")

    try:
        # Import cleaners
        from scholarship_data_cleaner import ScholarshipDataCleaner
        from loans_data_cleaner import LoansDataCleaner

        # Run scholarship cleaner
        print("▶ Running Scholarship Data Cleaner...")
        scholarship_cleaner = ScholarshipDataCleaner()
        scholarship_success = scholarship_cleaner.clean()

        if scholarship_success:
            print("✓ Scholarship cleaning completed\n")
        else:
            print("✗ Scholarship cleaning failed\n")

        # Run loans cleaner
        print("▶ Running Loans Data Cleaner...")
        loans_cleaner = LoansDataCleaner()
        loans_success = loans_cleaner.clean()

        if loans_success:
            print("✓ Loans cleaning completed\n")
        else:
            print("✗ Loans cleaning failed\n")

        # Summary
        print("\n" + "="*70)
        print("CLEANING SUMMARY")
        print("="*70)

        if scholarship_success:
            print("✓ Scholarships: data/scholarships_cleaned.csv")
        else:
            print("✗ Scholarships: FAILED")

        if loans_success:
            print("✓ Loans: data/loans_cleaned.csv")
        else:
            print("✗ Loans: FAILED")

        print("\n" + "="*70)
        print("NEXT STEPS:")
        print("="*70)
        print("1. Open data/scholarships_cleaned.csv for model training")
        print("2. Open data/loans_cleaned.csv for model training")
        print("3. Check cleaning reports:")
        print("   - data/scholarship_cleaning_report_*.txt")
        print("   - data/loans_cleaning_report_*.txt")
        print("="*70 + "\n")

        logger.info("Master data cleaner completed")
        return scholarship_success and loans_success

    except Exception as e:
        logger.error(f"Error running cleaners: {e}")
        print(f"\n✗ Error: {e}")
        return False


if __name__ == "__main__":
    success = run_cleaners()
    sys.exit(0 if success else 1)
