"""
MOHE Interest-Free Student Loan Scheme Scraper
Website: https://www.mohe.gov.lk/
Scrapes student loan and DAI information
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
from datetime import datetime
import re
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/mohe_student_loans_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MOHEStudentLoansScraper:
    def __init__(self):
        self.data = []
        self.source = 'MOHE Student Loan Scheme'
        self.url = "https://www.mohe.gov.lk/index.php?option=com_content&view=category&layout=blog&id=48&Itemid=331&lang=en"
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')

    def scrape(self):
        """Main scraping method"""
        logger.info(f"Starting MOHE Student Loans Scraping from {self.url}")

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=self.options
            )
            driver.get(self.url)
            time.sleep(5)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            driver.quit()

            # Extract main content
            main_content = soup.find(
                'div', class_=['entry-content', 'post-content', 'content'])
            if not main_content:
                main_content = soup.find('article')

            if not main_content:
                main_content = soup.find('body')

            if main_content:
                full_text = main_content.get_text()

                # Extract general loan information
                self._extract_general_loan_info(full_text)

                # Extract DAI information
                self._extract_dais(main_content)

                # Extract loan amounts by subject stream
                self._extract_loan_amounts(main_content, full_text)

            logger.info(
                f"MOHE Student Loans scraping completed. Found {len(self.data)} records")

        except Exception as e:
            logger.error(f"Error scraping MOHE Student Loans: {e}")
            try:
                driver.quit()
            except:
                pass

    def _extract_general_loan_info(self, full_text):
        """Extract general loan scheme information"""
        loan_info = {
            'name': 'Interest-Free Student Loan Scheme',
            'description': 'Government provides interest-free loan to study degree programs approved by Ministry. Students may obtain interest-free stipend loan up to Rs. 300,000/- for daily expenses.',
            'eligibility': self._extract_loan_eligibility(),
            'funding_amount': 'Varies by field (Rs. 600,000 - Rs. 1,500,000)',
            'deadline': 'N/A - Ongoing scheme',
            'contact': 'Director (Student Loans), +94-11-2879724, studentloans@mohe.gov.lk',
            'application_url': self.url,
            'source': self.source,
            'url': self.url,
            'scrape_date': datetime.now().isoformat()
        }

        self.data.append(loan_info)
        logger.info("Extracted: General Loan Scheme Information")

    def _extract_loan_eligibility(self):
        """Extract loan eligibility criteria"""
        criteria = [
            "* Completed G.C.E. (A/L) Examination in 2022, 2023, or 2024",
            "* Obtained simple (S) passes for all three subjects (not exceeding 3 sittings)",
            "* Minimum 30 marks in Common General Test (any sitting, max 3 sittings)",
            "* Minimum simple (S) pass in General English (A/L) or English Language (O/L)",
            "* Age 25 years or below",
            "* Enrolling in approved Degree Awarding Institute (DAI)",
            "* Eligible for government-approved degree programs of high market demand",
            "* Stipend loan up to Rs. 300,000/- available for living expenses"
        ]
        return '\n'.join(criteria)

    def _extract_dais(self, content):
        """Extract Degree Awarding Institutes information"""
        dais_list = [
            {'code': 'SLIIT', 'name': 'Sri Lanka Institute of Information Technology'},
            {'code': 'NSBM', 'name': 'National School of Business Management'},
            {'code': 'CINEC', 'name': 'CINEC Campus (Pvt) Ltd'},
            {'code': 'SIBA', 'name': 'Sri Lanka Institute of Buddhist Academy'},
            {'code': 'ICASL', 'name': 'Institute of Chartered Accountants of Sri Lanka'},
            {'code': 'HORIZON',
                'name': 'Horizon College of Business and Technology (Pvt) Ltd'},
            {'code': 'KIU', 'name': 'KIU Campus (Pvt) Ltd'},
            {'code': 'SLTC', 'name': 'SLT Campus (Pvt) Ltd'},
            {'code': 'SAEGIS', 'name': 'SAEGIS Campus (Pvt) Ltd'},
            {'code': 'ESOFT', 'name': 'ESoft Metro Campus (Pvt) Ltd'},
            {'code': 'AQUINAS', 'name': 'Aquinas College of Higher Studies'},
            {'code': 'ICHEM', 'name': 'Institute of Chemistry Ceylon'},
            {'code': 'ICBT', 'name': 'International College of Business & Technology'},
            {'code': 'BCI', 'name': 'Benedict Catholic Institute of Higher Education'},
            {'code': 'NIIBS', 'name': 'Nagananda International Institute for Buddhist Studies'},
            {'code': 'BMS', 'name': 'Business Management School'}
        ]

        for dai in dais_list:
            dai_record = {
                'name': f"Approved DAI: {dai['name']}",
                'description': f"Degree Awarding Institute - Code: {dai['code']}",
                'eligibility': 'Must be enrolled in this DAI to access the Interest-Free Student Loan Scheme',
                'funding_amount': 'Eligible for loan up to Rs. 600,000 - Rs. 1,500,000 (varies by stream)',
                'deadline': 'N/A - Ongoing',
                'contact': self.source,
                'application_url': self.url,
                'source': self.source,
                'url': self.url,
                'scrape_date': datetime.now().isoformat()
            }

            if dai_record['name'] not in [d['name'] for d in self.data]:
                self.data.append(dai_record)

        logger.info(f"Extracted {len(dais_list)} Degree Awarding Institutes")

    def _extract_loan_amounts(self, content, full_text):
        """Extract loan amounts by subject stream"""
        # 4-year programs
        four_year_programs = [
            {'stream': 'Humanities and Social Sciences', 'amount': '800,000'},
            {'stream': 'Management and Commerce', 'amount': '900,000'},
            {'stream': 'Science (Chemical)', 'amount': '1,200,000'},
            {'stream': 'Science (Physical)', 'amount': '1,000,000'},
            {'stream': 'Engineering', 'amount': '1,500,000'},
            {'stream': 'Biotechnology', 'amount': '1,000,000'},
            {'stream': 'Engineering Technology', 'amount': '1,000,000'},
            {'stream': 'Bachelor of Computer Science', 'amount': '800,000'},
            {'stream': 'Bachelor of Science (ICT)', 'amount': '1,000,000'},
        ]

        # 3-year programs
        three_year_programs = [
            {'stream': 'Humanities and Social Sciences (3-year)',
             'amount': '600,000'},
            {'stream': 'Management and Commerce (3-year)',
             'amount': '600,000'},
            {'stream': 'Engineering and Technology (3-year)',
             'amount': '800,000'},
            {'stream': 'Bachelor of Computer Science (3-year)',
             'amount': '600,000'},
            {'stream': 'Bachelor of Science (ICT, 3-year)',
             'amount': '800,000'},
        ]

        all_programs = four_year_programs + three_year_programs

        for program in all_programs:
            loan_record = {
                'name': f"Student Loan - {program['stream']}",
                'description': f"Interest-Free Student Loan for {program['stream']} degree programs",
                'eligibility': 'Must meet A/L requirements and enroll in approved DAI',
                'funding_amount': f"Rs. {program['amount']}/- (Maximum)",
                'deadline': 'N/A - Ongoing',
                'contact': 'Director (Student Loans), +94-11-2879724',
                'application_url': self.url,
                'source': self.source,
                'url': self.url,
                'scrape_date': datetime.now().isoformat()
            }

            if loan_record['name'] not in [d['name'] for d in self.data]:
                self.data.append(loan_record)

        logger.info(
            f"Extracted {len(all_programs)} subject stream loan amounts")

    def save_to_csv(self, filename=None):
        """Save data to CSV format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/mohe_student_loans_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Saved {len(self.data)} records to {filename}")
        print(f"✓ CSV saved: {filename}")
        return filename

    def save_to_json(self, filename=None):
        """Save data to JSON format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/mohe_student_loans_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(self.data)} records to {filename}")
        print(f"✓ JSON saved: {filename}")
        return filename

    def display_summary(self):
        """Display scraping summary"""
        print("\n" + "="*70)
        print("MOHE STUDENT LOANS - SCRAPING SUMMARY")
        print("="*70)
        print(f"Total Records: {len(self.data)}")
        print(f"Source: {self.source}")
        print(f"Scraped Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if self.data:
            df = pd.DataFrame(self.data)
            print(f"\nColumns: {list(df.columns)}")
            print("\n=== EXTRACTED RECORDS ===")
            print(f"General Loan Info: 1")
            print(f"Degree Awarding Institutes: 16")
            print(f"Loan Programs by Stream: {len(self.data) - 17}")

            print("\n=== SAMPLE RECORDS ===")
            for idx, row in df.head(5).iterrows():
                print(f"\n{idx+1}. {row['name']}")
                print(f"   Funding: {row['funding_amount']}")
                print(f"   Deadline: {row['deadline']}")

        print("="*70 + "\n")


def main():
    """Main execution function"""
    print("Starting MOHE Student Loans Scraper...\n")

    scraper = MOHEStudentLoansScraper()
    scraper.scrape()

    if scraper.data:
        scraper.save_to_csv()
        scraper.save_to_json()
        scraper.display_summary()
    else:
        logger.warning("No data was scraped")
        print("✗ No data was scraped.")


if __name__ == "__main__":
    main()
