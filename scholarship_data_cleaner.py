"""
Scholarship Data Cleaner
Cleans, merges, and standardizes scholarship data from multiple scrapers
Output: Single cleaned scholarships.csv file ready for ML models
"""

import pandas as pd
import numpy as np
import re
import logging
from datetime import datetime
from pathlib import Path
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scholarship_cleaner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ScholarshipDataCleaner:
    def __init__(self):
        self.df = None
        self.original_count = 0
        self.cleaned_count = 0

    def load_data(self):
        """Load all scholarship CSV files from data folder"""
        logger.info("Loading scholarship data files...")

        data_folder = 'data'
        scholarship_files = []

        # Find all scholarship-related CSV files (exclude loans)
        for file in os.listdir(data_folder):
            if file.endswith('.csv') and 'scholarship' in file.lower():
                if 'loan' not in file.lower():
                    scholarship_files.append(os.path.join(data_folder, file))

        logger.info(f"Found {len(scholarship_files)} scholarship files:")
        for file in scholarship_files:
            logger.info(f"  - {file}")

        if not scholarship_files:
            logger.error("No scholarship files found!")
            return False

        # Load and combine all files
        dfs = []
        for file in scholarship_files:
            try:
                df = pd.read_csv(file, encoding='utf-8')
                dfs.append(df)
                logger.info(f"Loaded: {file} ({len(df)} records)")
            except Exception as e:
                logger.error(f"Error loading {file}: {e}")

        if dfs:
            self.df = pd.concat(dfs, ignore_index=True)
            self.original_count = len(self.df)
            logger.info(f"Total records loaded: {self.original_count}")
            return True

        return False

    def standardize_columns(self):
        """Standardize column names across different sources"""
        logger.info("Standardizing column names...")

        # Expected columns for scholarships
        expected_columns = {
            'name', 'description', 'eligibility', 'funding_amount',
            'deadline', 'contact', 'application_url', 'source',
            'url', 'scrape_date'
        }

        # Add any missing columns
        for col in expected_columns:
            if col not in self.df.columns:
                self.df[col] = 'N/A'
                logger.info(f"Added missing column: {col}")

        # Select only expected columns
        self.df = self.df[list(expected_columns)]
        logger.info(f"Standardized to {len(self.df.columns)} columns")

    def remove_duplicates(self):
        """Remove duplicate scholarships"""
        logger.info("Removing duplicates...")

        initial_count = len(self.df)

        # Remove exact duplicates based on name and source
        self.df = self.df.drop_duplicates(
            subset=['name', 'source'], keep='first')

        # Remove near-duplicates (same name, different source but similar content)
        self.df = self.df.drop_duplicates(subset=['name'], keep='first')

        removed_count = initial_count - len(self.df)
        logger.info(
            f"Removed {removed_count} duplicates. Remaining: {len(self.df)}")

    def clean_text_fields(self):
        """Clean and standardize text fields"""
        logger.info("Cleaning text fields...")

        text_columns = ['name', 'description', 'eligibility', 'contact']

        for col in text_columns:
            if col in self.df.columns:
                # Remove extra whitespace
                self.df[col] = self.df[col].str.strip()

                # Remove multiple spaces
                self.df[col] = self.df[col].str.replace(
                    r'\s+', ' ', regex=True)

                # Remove null-like strings
                self.df[col] = self.df[col].replace(
                    ['N/A', 'n/a', 'NA', 'None', 'none', ''], 'N/A')

                # Remove HTML tags if any
                self.df[col] = self.df[col].str.replace(
                    r'<[^>]+>', '', regex=True)

        logger.info("Text fields cleaned")

    def extract_funding_amount(self):
        """Extract and standardize funding amounts"""
        logger.info("Extracting and standardizing funding amounts...")

        def extract_amount(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text)

            # Look for currency patterns
            patterns = [
                r'Rs\.?\s*([\d,]+(?:\.\d{2})?)',  # Rs. amounts
                r'LKR\s*([\d,]+(?:\.\d{2})?)',    # LKR amounts
                r'\$([\d,]+(?:\.\d{2})?)',        # $ amounts
                r'USD\s*([\d,]+(?:\.\d{2})?)',    # USD amounts
            ]

            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    amount = match.group(1).replace(',', '')
                    return f"Rs. {amount}"

            # If contains percentage
            if '%' in text:
                return text.strip()

            # If it's a range or description
            if any(word in text.lower() for word in ['varies', 'based', 'up to', 'minimum']):
                return text.strip()

            return text if text else 'N/A'

        self.df['funding_amount_cleaned'] = self.df['funding_amount'].apply(
            extract_amount)
        self.df['funding_amount'] = self.df['funding_amount_cleaned']
        self.df = self.df.drop('funding_amount_cleaned', axis=1)

        logger.info("Funding amounts standardized")

    def extract_deadline(self):
        """Extract and standardize deadline information"""
        logger.info("Extracting and standardizing deadlines...")

        def extract_deadline_date(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text).strip()

            # Patterns for dates
            date_patterns = [
                r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',  # DD/MM/YYYY
                r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
            ]

            for pattern in date_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    return match.group(1)

            # If contains "weeks" or "months" or "days"
            if any(word in text.lower() for word in ['week', 'month', 'day', 'hour']):
                return text.strip()

            # Check for "ongoing" or "rolling"
            if any(word in text.lower() for word in ['ongoing', 'rolling', 'continuous']):
                return 'Ongoing'

            return text if text else 'N/A'

        self.df['deadline_cleaned'] = self.df['deadline'].apply(
            extract_deadline_date)
        self.df['deadline'] = self.df['deadline_cleaned']
        self.df = self.df.drop('deadline_cleaned', axis=1)

        logger.info("Deadlines standardized")

    def clean_eligibility(self):
        """Clean and structure eligibility criteria"""
        logger.info("Cleaning eligibility criteria...")

        def clean_eligibility_text(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text).strip()

            # Remove extra newlines and replace with space
            text = re.sub(r'\n\s*\n', '\n', text)
            text = re.sub(r'\n+', '\n', text)

            # Limit to 500 characters for consistency
            if len(text) > 500:
                text = text[:500] + "..."

            return text if text else 'N/A'

        self.df['eligibility'] = self.df['eligibility'].apply(
            clean_eligibility_text)
        logger.info("Eligibility criteria cleaned")

    def remove_empty_rows(self):
        """Remove rows where critical fields are all N/A"""
        logger.info("Removing rows with missing critical information...")

        initial_count = len(self.df)

        # Keep only rows with at least name and source
        self.df = self.df[
            (self.df['name'] != 'N/A') &
            (self.df['source'] != 'N/A')
        ]

        removed_count = initial_count - len(self.df)
        logger.info(
            f"Removed {removed_count} rows with missing critical info. Remaining: {len(self.df)}")

    def add_data_quality_score(self):
        """Add data quality score (0-100) based on field completeness"""
        logger.info("Calculating data quality scores...")

        def calculate_quality_score(row):
            score = 0
            fields = ['name', 'description', 'eligibility',
                      'funding_amount', 'deadline', 'contact']

            for field in fields:
                if row[field] != 'N/A' and pd.notna(row[field]):
                    score += 100 / len(fields)

            return round(score, 2)

        self.df['data_quality_score'] = self.df.apply(
            calculate_quality_score, axis=1)
        logger.info("Data quality scores added")

    def add_scholarship_type(self):
        """Categorize scholarship type based on name and description"""
        logger.info("Categorizing scholarship types...")

        def categorize_scholarship(row):
            text = (str(row['name']) + ' ' + str(row['description'])).lower()

            if any(word in text for word in ['merit', 'academic', 'performance', 'exam', 'gpa']):
                return 'Merit-Based'
            elif any(word in text for word in ['need', 'income', 'poor', 'low-income', 'financial']):
                return 'Need-Based'
            elif any(word in text for word in ['sport', 'athletic', 'talent']):
                return 'Talent-Based'
            elif any(word in text for word in ['bursary', 'grant']):
                return 'Grant/Bursary'
            elif any(word in text for word in ['government', 'mahapola']):
                return 'Government'
            else:
                return 'General'

        self.df['scholarship_type'] = self.df.apply(
            categorize_scholarship, axis=1)
        logger.info("Scholarship types categorized")

    def add_eligibility_region(self):
        """Extract eligible regions/countries"""
        logger.info("Extracting eligible regions...")

        def extract_region(row):
            text = (str(row['name']) + ' ' + str(row['description']
                                                 ) + ' ' + str(row['eligibility'])).lower()

            if 'sri lanka' in text or 'sliit' in text or 'ousl' in text:
                return 'Sri Lanka'
            elif any(word in text for word in ['local', 'domestic']):
                return 'Local'
            elif any(word in text for word in ['foreign', 'overseas', 'international', 'abroad']):
                return 'International'
            elif any(word in text for word in ['both', 'local or']):
                return 'Both'
            else:
                return 'Unknown'

        self.df['eligible_region'] = self.df.apply(extract_region, axis=1)
        logger.info("Eligible regions extracted")

    def reorder_columns(self):
        """Reorder columns for better readability"""
        logger.info("Reordering columns...")

        # Define desired column order
        column_order = [
            'name',
            'scholarship_type',
            'eligible_region',
            'description',
            'eligibility',
            'funding_amount',
            'deadline',
            'contact',
            'application_url',
            'source',
            'url',
            'data_quality_score',
            'scrape_date'
        ]

        # Add any missing columns
        for col in self.df.columns:
            if col not in column_order:
                column_order.append(col)

        self.df = self.df[column_order]
        logger.info("Columns reordered")

    def generate_cleaning_report(self):
        """Generate data cleaning report"""
        logger.info("Generating cleaning report...")

        report = f"""
{'='*70}
SCHOLARSHIP DATA CLEANING REPORT
{'='*70}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

STATISTICS:
-----------
Original Records: {self.original_count}
Cleaned Records: {len(self.df)}
Removed Records: {self.original_count - len(self.df)}
Removal Rate: {round((self.original_count - len(self.df)) / self.original_count * 100, 2)}%

DATA QUALITY:
-----------
Average Quality Score: {self.df['data_quality_score'].mean():.2f}/100
Median Quality Score: {self.df['data_quality_score'].median():.2f}/100
Min Quality Score: {self.df['data_quality_score'].min():.2f}/100
Max Quality Score: {self.df['data_quality_score'].max():.2f}/100

Records by Quality:
  • Excellent (80-100): {len(self.df[self.df['data_quality_score'] >= 80])}
  • Good (60-79): {len(self.df[(self.df['data_quality_score'] >= 60) & (self.df['data_quality_score'] < 80)])}
  • Average (40-59): {len(self.df[(self.df['data_quality_score'] >= 40) & (self.df['data_quality_score'] < 60)])}
  • Poor (<40): {len(self.df[self.df['data_quality_score'] < 40])}

DISTRIBUTION:
-----------
By Scholarship Type:
{self.df['scholarship_type'].value_counts().to_string()}

By Eligible Region:
{self.df['eligible_region'].value_counts().to_string()}

By Source:
{self.df['source'].value_counts().to_string()}

FIELD COMPLETENESS:
-----------
Name: {(self.df['name'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['name'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Description: {(self.df['description'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['description'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Eligibility: {(self.df['eligibility'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['eligibility'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Funding Amount: {(self.df['funding_amount'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['funding_amount'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Deadline: {(self.df['deadline'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['deadline'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Contact: {(self.df['contact'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['contact'] != 'N/A').sum()/len(self.df)*100, 2)}%)

{'='*70}
"""

        print(report)

        # Save report
        report_file = f'data/scholarship_cleaning_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        with open(report_file, 'w') as f:
            f.write(report)

        logger.info(f"Report saved to {report_file}")

    def save_cleaned_data(self):
        """Save cleaned data to CSV"""
        logger.info("Saving cleaned data...")

        output_file = f'data/scholarships_cleaned.csv'
        self.df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Cleaned data saved to {output_file}")
        print(f"\n✓ Cleaned scholarships saved: {output_file}")

        # Also save with timestamp
        timestamped_file = f'data/scholarships_cleaned_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.df.to_csv(timestamped_file, index=False, encoding='utf-8')

        return output_file

    def clean(self):
        """Execute full cleaning pipeline"""
        logger.info("Starting scholarship data cleaning pipeline...")

        try:
            if not self.load_data():
                logger.error("Failed to load data")
                return False

            self.standardize_columns()
            self.remove_duplicates()
            self.clean_text_fields()
            self.extract_funding_amount()
            self.extract_deadline()
            self.clean_eligibility()
            self.remove_empty_rows()
            self.add_data_quality_score()
            self.add_scholarship_type()
            self.add_eligibility_region()
            self.reorder_columns()
            self.generate_cleaning_report()
            self.save_cleaned_data()

            logger.info("Cleaning pipeline completed successfully")
            return True

        except Exception as e:
            logger.error(f"Error during cleaning: {e}")
            return False


def main():
    """Main execution"""
    print("Starting Scholarship Data Cleaner...\n")

    cleaner = ScholarshipDataCleaner()
    success = cleaner.clean()

    if success:
        print("\n✓ Scholarship data cleaning completed successfully!")
        print("Output: data/scholarships_cleaned.csv")
    else:
        print("\n✗ Scholarship data cleaning failed. Check logs for details.")


if __name__ == "__main__":
    main()
