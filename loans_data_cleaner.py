"""
Loans Data Cleaner
Cleans, merges, and standardizes loan data from multiple scrapers
Output: Single cleaned loans.csv file ready for ML models
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
        logging.FileHandler('logs/loans_cleaner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class LoansDataCleaner:
    def __init__(self):
        self.df = None
        self.original_count = 0

    def load_data(self):
        """Load all loan CSV files from data folder"""
        logger.info("Loading loan data files...")

        data_folder = 'data'
        loan_files = []

        # Find all loan-related CSV files
        for file in os.listdir(data_folder):
            if file.endswith('.csv'):
                if any(keyword in file.lower() for keyword in ['loan', 'dai', 'institution']):
                    if 'scholarship' not in file.lower():
                        loan_files.append(os.path.join(data_folder, file))

        logger.info(f"Found {len(loan_files)} loan files:")
        for file in loan_files:
            logger.info(f"  - {file}")

        if not loan_files:
            logger.error("No loan files found!")
            return False

        # Load and combine all files
        dfs = []
        for file in loan_files:
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

        # Common columns for loans
        common_columns = {
            'name', 'description', 'eligibility', 'funding_amount',
            'deadline', 'contact', 'application_url', 'source',
            'url', 'scrape_date'
        }

        # Bank-specific columns
        bank_columns = {
            'bank_name', 'loan_product_name', 'maximum_loan_amount',
            'minimum_loan_amount', 'interest_rate', 'repayment_period',
            'age_criteria', 'income_criteria', 'documents_required',
            'special_benefits', 'contact_info', 'website_url', 'bank_code'
        }

        all_expected = common_columns | bank_columns

        # Add missing columns
        for col in all_expected:
            if col not in self.df.columns:
                self.df[col] = 'N/A'

        # Normalize name columns
        if 'bank_name' in self.df.columns and 'name' not in self.df.columns:
            self.df['name'] = self.df['bank_name']

        if 'loan_product_name' in self.df.columns and self.df['name'].isna().all():
            self.df['name'] = self.df['loan_product_name']

        if 'contact_info' in self.df.columns and 'contact' not in self.df.columns:
            self.df['contact'] = self.df['contact_info']

        if 'website_url' in self.df.columns and 'application_url' not in self.df.columns:
            self.df['application_url'] = self.df['website_url']

        logger.info(f"Standardized to {len(self.df.columns)} total columns")

    def remove_duplicates(self):
        """Remove duplicate loans"""
        logger.info("Removing duplicates...")

        initial_count = len(self.df)

        # Remove exact duplicates
        if 'name' in self.df.columns and 'source' in self.df.columns:
            self.df = self.df.drop_duplicates(
                subset=['name', 'source'], keep='first')

        # Remove near-duplicates (same name)
        self.df = self.df.drop_duplicates(subset=['name'], keep='first')

        removed_count = initial_count - len(self.df)
        logger.info(
            f"Removed {removed_count} duplicates. Remaining: {len(self.df)}")

    def clean_text_fields(self):
        """Clean and standardize text fields"""
        logger.info("Cleaning text fields...")

        text_columns = ['name', 'description',
                        'eligibility', 'contact', 'loan_product_name']

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

                # Remove HTML tags
                self.df[col] = self.df[col].str.replace(
                    r'<[^>]+>', '', regex=True)

        logger.info("Text fields cleaned")

    def extract_loan_amounts(self):
        """Extract and standardize loan amounts"""
        logger.info("Extracting and standardizing loan amounts...")

        def extract_amount(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text)

            # Look for currency patterns
            patterns = [
                r'Rs\.?\s*([\d,]+(?:\.\d{2})?)',
                r'LKR\s*([\d,]+(?:\.\d{2})?)',
                r'\$([\d,]+(?:\.\d{2})?)',
                r'USD\s*([\d,]+(?:\.\d{2})?)',
            ]

            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    amount = match.group(1).replace(',', '')
                    try:
                        # Try to convert to float for sorting
                        float(amount)
                        return f"Rs. {amount}"
                    except:
                        return f"Rs. {amount}"

            # If contains percentage
            if '%' in text:
                return text.strip()

            # If it's a range or description
            if any(word in text.lower() for word in ['varies', 'based', 'up to', 'minimum', 'maximum']):
                return text.strip()

            return text if text else 'N/A'

        # Process both maximum and minimum amounts
        for col in ['funding_amount', 'maximum_loan_amount', 'minimum_loan_amount']:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(extract_amount)

        logger.info("Loan amounts standardized")

    def extract_repayment_period(self):
        """Extract and standardize repayment periods"""
        logger.info("Extracting repayment periods...")

        def extract_period(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text).strip()

            # Look for year patterns
            year_match = re.search(
                r'(\d+)\s*(?:year|yr|years|yrs)', text, re.IGNORECASE)
            if year_match:
                years = year_match.group(1)
                return f"{years} years"

            # Look for month patterns
            month_match = re.search(
                r'(\d+)\s*(?:month|months|mo)', text, re.IGNORECASE)
            if month_match:
                months = month_match.group(1)
                return f"{months} months"

            # Look for installment patterns
            install_match = re.search(
                r'(\d+)\s*(?:installment|installments|EMI)', text, re.IGNORECASE)
            if install_match:
                installments = install_match.group(1)
                return f"{installments} installments"

            return text if text else 'N/A'

        if 'repayment_period' in self.df.columns:
            self.df['repayment_period'] = self.df['repayment_period'].apply(
                extract_period)
        elif 'deadline' in self.df.columns:
            # Sometimes deadline contains repayment info
            self.df['repayment_period'] = self.df['deadline'].apply(
                extract_period)

        logger.info("Repayment periods extracted")

    def clean_eligibility(self):
        """Clean eligibility criteria"""
        logger.info("Cleaning eligibility criteria...")

        def clean_eligibility_text(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text).strip()

            # Remove extra newlines
            text = re.sub(r'\n\s*\n', '\n', text)
            text = re.sub(r'\n+', '\n', text)

            # Limit to 500 characters
            if len(text) > 500:
                text = text[:500] + "..."

            return text if text else 'N/A'

        if 'eligibility' in self.df.columns:
            self.df['eligibility'] = self.df['eligibility'].apply(
                clean_eligibility_text)

        logger.info("Eligibility criteria cleaned")

    def extract_interest_rate(self):
        """Extract interest rate information"""
        logger.info("Extracting interest rates...")

        def extract_rate(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text).strip()

            # Look for percentage patterns
            percent_match = re.search(r'([\d.]+)\s*%', text)
            if percent_match:
                rate = percent_match.group(1)
                return f"{rate}%"

            # Check for special rates
            if any(word in text.lower() for word in ['free', 'zero', '0%', 'interest-free']):
                return 'Interest-Free'

            if 'competitive' in text.lower() or 'market' in text.lower():
                return text.strip()

            return text if text else 'N/A'

        if 'interest_rate' in self.df.columns:
            self.df['interest_rate'] = self.df['interest_rate'].apply(
                extract_rate)

        logger.info("Interest rates extracted")

    def remove_empty_rows(self):
        """Remove rows with missing critical information"""
        logger.info("Removing rows with missing critical information...")

        initial_count = len(self.df)

        # Keep only rows with name
        self.df = self.df[self.df['name'] != 'N/A']

        removed_count = initial_count - len(self.df)
        logger.info(f"Removed {removed_count} rows. Remaining: {len(self.df)}")

    def add_data_quality_score(self):
        """Add data quality score"""
        logger.info("Calculating data quality scores...")

        def calculate_quality_score(row):
            score = 0
            key_fields = ['name', 'description',
                          'eligibility', 'funding_amount', 'contact']

            for field in key_fields:
                if field in row.index and row[field] != 'N/A' and pd.notna(row[field]):
                    score += 100 / len(key_fields)

            return round(score, 2)

        self.df['data_quality_score'] = self.df.apply(
            calculate_quality_score, axis=1)
        logger.info("Data quality scores added")

    def add_loan_type(self):
        """Categorize loan type"""
        logger.info("Categorizing loan types...")

        def categorize_loan(row):
            text = (str(row.get('name', '')) + ' ' +
                    str(row.get('description', ''))).lower()

            if 'government' in text or 'mohe' in text:
                return 'Government Loan'
            elif any(bank in text for bank in ['bank', 'boc', 'commercial', 'peoples', 'hnb', 'nsb', 'pabc']):
                return 'Bank Loan'
            elif 'buddhi' in text or 'nsb' in text:
                return 'NSB Loan'
            elif 'dai' in text or 'awarding' in text:
                return 'DAI Related'
            else:
                return 'Other'

        self.df['loan_type'] = self.df.apply(categorize_loan, axis=1)
        logger.info("Loan types categorized")

    def add_loan_duration_category(self):
        """Categorize loan duration"""
        logger.info("Categorizing loan duration...")

        def categorize_duration(row):
            text = str(row.get('repayment_period', '')).lower()

            if pd.isna(text) or text == 'N/A':
                return 'Unknown'

            # Extract number of years
            year_match = re.search(r'(\d+)', text)
            if year_match:
                years = int(year_match.group(1))
                if years <= 3:
                    return 'Short-term (≤3 years)'
                elif years <= 7:
                    return 'Medium-term (4-7 years)'
                else:
                    return 'Long-term (>7 years)'

            return 'Unknown'

        self.df['loan_duration_category'] = self.df.apply(
            categorize_duration, axis=1)
        logger.info("Loan duration categories added")

    def extract_age_range(self):
        """Extract age eligibility range"""
        logger.info("Extracting age ranges...")

        def extract_age(text):
            if text == 'N/A' or pd.isna(text):
                return 'N/A'

            text = str(text).strip()

            # Look for age patterns like "18-50" or "18 to 65"
            age_pattern = r'(\d+)\s*(?:to|-|and)\s*(\d+)'
            match = re.search(age_pattern, text)
            if match:
                min_age = match.group(1)
                max_age = match.group(2)
                return f"{min_age}-{max_age} years"

            # Single age
            single_age = re.search(
                r'(\d+)\s*(?:years?|yrs?)', text, re.IGNORECASE)
            if single_age:
                return f"{single_age.group(1)}+ years"

            return text if text else 'N/A'

        if 'age_criteria' in self.df.columns:
            self.df['age_criteria'] = self.df['age_criteria'].apply(
                extract_age)

        logger.info("Age ranges extracted")

    def reorder_columns(self):
        """Reorder columns for better readability"""
        logger.info("Reordering columns...")

        # Define desired column order
        column_order = [
            'name',
            'loan_type',
            'loan_duration_category',
            'description',
            'eligibility',
            'maximum_loan_amount',
            'minimum_loan_amount',
            'funding_amount',
            'interest_rate',
            'repayment_period',
            'age_criteria',
            'income_criteria',
            'contact',
            'application_url',
            'source',
            'data_quality_score',
            'scrape_date'
        ]

        # Keep only columns that exist
        available_cols = [
            col for col in column_order if col in self.df.columns]

        # Add any remaining columns
        for col in self.df.columns:
            if col not in available_cols:
                available_cols.append(col)

        self.df = self.df[available_cols]
        logger.info("Columns reordered")

    def generate_cleaning_report(self):
        """Generate data cleaning report"""
        logger.info("Generating cleaning report...")

        report = f"""
{'='*70}
LOANS DATA CLEANING REPORT
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
By Loan Type:
{self.df['loan_type'].value_counts().to_string() if 'loan_type' in self.df.columns else 'N/A'}

By Loan Duration:
{self.df['loan_duration_category'].value_counts().to_string() if 'loan_duration_category' in self.df.columns else 'N/A'}

By Source:
{self.df['source'].value_counts().to_string() if 'source' in self.df.columns else 'N/A'}

FIELD COMPLETENESS:
-----------
Name: {(self.df['name'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['name'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Description: {(self.df['description'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['description'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Eligibility: {(self.df['eligibility'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['eligibility'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Funding Amount: {(self.df['funding_amount'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['funding_amount'] != 'N/A').sum()/len(self.df)*100, 2)}%)
Interest Rate: {(self.df['interest_rate'] != 'N/A').sum()}/{len(self.df)} ({round((self.df['interest_rate'] != 'N/A').sum()/len(self.df)*100, 2)}%) if 'interest_rate' in self.df.columns else 0

{'='*70}
"""

        print(report)

        # Save report
        report_file = f'data/loans_cleaning_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        with open(report_file, 'w') as f:
            f.write(report)

        logger.info(f"Report saved to {report_file}")

    def save_cleaned_data(self):
        """Save cleaned data to CSV"""
        logger.info("Saving cleaned data...")

        output_file = f'data/loans_cleaned.csv'
        self.df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Cleaned data saved to {output_file}")
        print(f"\n✓ Cleaned loans saved: {output_file}")

        # Also save with timestamp
        timestamped_file = f'data/loans_cleaned_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.df.to_csv(timestamped_file, index=False, encoding='utf-8')

        return output_file

    def clean(self):
        """Execute full cleaning pipeline"""
        logger.info("Starting loans data cleaning pipeline...")

        try:
            if not self.load_data():
                logger.error("Failed to load data")
                return False

            self.standardize_columns()
            self.remove_duplicates()
            self.clean_text_fields()
            self.extract_loan_amounts()
            self.extract_repayment_period()
            self.extract_interest_rate()
            self.extract_age_range()
            self.clean_eligibility()
            self.remove_empty_rows()
            self.add_data_quality_score()
            self.add_loan_type()
            self.add_loan_duration_category()
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
    print("Starting Loans Data Cleaner...\n")

    cleaner = LoansDataCleaner()
    success = cleaner.clean()

    if success:
        print("\n✓ Loans data cleaning completed successfully!")
        print("Output: data/loans_cleaned.csv")
    else:
        print("\n✗ Loans data cleaning failed. Check logs for details.")


if __name__ == "__main__":
    main()
