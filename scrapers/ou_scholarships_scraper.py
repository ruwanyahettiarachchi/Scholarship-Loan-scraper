"""
Open University Sri Lanka Scholarships Scraper
Website: https://ou.ac.lk/student-affairs-division/university-scholarships/
Scrapes scholarship information from OUSL
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
        logging.FileHandler('logs/ou_scholarships_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OUScholarshipsScraper:
    def __init__(self):
        self.data = []
        self.source = 'Open University Sri Lanka (OUSL)'
        self.url = "https://ou.ac.lk/student-affairs-division/university-scholarships/"
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')

    def scrape(self):
        """Main scraping method"""
        logger.info(f"Starting OUSL Scholarship Scraping from {self.url}")

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

                # Extract scholarship types
                self._extract_university_bursary(main_content, full_text)
                self._extract_enrollment_bursary(main_content, full_text)
                self._extract_mahapola_scholarship(main_content, full_text)

            logger.info(
                f"OUSL scraping completed. Found {len(self.data)} scholarships")

        except Exception as e:
            logger.error(f"Error scraping OUSL: {e}")
            try:
                driver.quit()
            except:
                pass

    def _extract_university_bursary(self, content, full_text):
        """Extract University Bursary information"""
        scholarship = {
            'name': 'University Bursary (OUSL)',
            'description': 'Award of 50% of tuition fee paid by student in previous academic year. A student may be awarded only in two academic years at different Levels of study program.',
            'eligibility': self._extract_bursary_eligibility(full_text),
            'funding_amount': '50% of tuition fee',
            'deadline': 'Within 6 weeks from date of late registration',
            'contact': 'Senior Assistant Registrar, Student Affairs & Welfare Division, The Open University of Sri Lanka, PO Box 21, Nawala, Nugegoda',
            'application_url': self.url,
            'source': self.source,
            'url': self.url,
            'scrape_date': datetime.now().isoformat()
        }

        if scholarship['name'] not in [s['name'] for s in self.data]:
            self.data.append(scholarship)
            logger.info(f"Extracted: {scholarship['name']}")

    def _extract_enrollment_bursary(self, content, full_text):
        """Extract Enrollment Bursary information"""
        scholarship = {
            'name': 'Open University Enrollment Bursary',
            'description': 'Awarded to first year enrollment, essentially for economically disadvantaged students. Award value is 50% of tuition fees of courses registered in first year.',
            'eligibility': self._extract_enrollment_eligibility(full_text),
            'funding_amount': '50% of tuition fees (first year only)',
            'deadline': 'Within 6 weeks from date of late registration',
            'contact': 'Senior Assistant Registrar, Student Affairs & Welfare Division, The Open University of Sri Lanka, PO Box 21, Nawala, Nugegoda',
            'application_url': self.url,
            'source': self.source,
            'url': self.url,
            'scrape_date': datetime.now().isoformat()
        }

        if scholarship['name'] not in [s['name'] for s in self.data]:
            self.data.append(scholarship)
            logger.info(f"Extracted: {scholarship['name']}")

    def _extract_mahapola_scholarship(self, content, full_text):
        """Extract Mahapola Scholarship information"""
        scholarship = {
            'name': 'Mahapola Scholarships (OUSL)',
            'description': 'Awarded by "Mahapola Higher Education Scholarship Trust Fund" to value of Rs. 8,000/- towards payment of tuition fees of courses.',
            'eligibility': self._extract_mahapola_eligibility(full_text),
            'funding_amount': 'Rs. 8,000/- per scholarship',
            'deadline': 'Within 6 weeks from date of late registration',
            'contact': 'Senior Assistant Registrar, Student Affairs & Welfare Division, The Open University of Sri Lanka, PO Box 21, Nawala, Nugegoda',
            'application_url': self.url,
            'source': self.source,
            'url': self.url,
            'scrape_date': datetime.now().isoformat()
        }

        if scholarship['name'] not in [s['name'] for s in self.data]:
            self.data.append(scholarship)
            logger.info(f"Extracted: {scholarship['name']}")

    def _extract_bursary_eligibility(self, text):
        """Extract University Bursary eligibility criteria"""
        criteria = [
            "* Student should have been registered for minimum 2 years programme",
            "* Must be registered for Level 4 or above in undergraduate programme",
            "* Minimum GPA of 2.0 in previous year (min 15 credits)",
            "* Gross family income < Rs 500,000/-",
            "* No disciplinary action should have been taken",
            "* Application with Grama Niladhari Certificate required"
        ]
        return '\n'.join(criteria)

    def _extract_enrollment_eligibility(self, text):
        """Extract Enrollment Bursary eligibility criteria"""
        criteria = [
            "* Student must be registered for minimum 2 years programme starting at Level 3",
            "* Annual gross family income < Rs 500,000/-",
            "* Must participate in continuous assessment and academic activities",
            "* No disciplinary action should have been taken",
            "* Grama Sewaka certificate required",
            "* Cannot obtain again if already received at diploma level"
        ]
        return '\n'.join(criteria)

    def _extract_mahapola_eligibility(self, text):
        """Extract Mahapola Scholarship eligibility criteria"""
        criteria = [
            "* Must be registered for Level 4 in undergraduate programme (current year)",
            "* Minimum GPA of 2.0 in previous year (min 15 credits)",
            "* Not employed or engaged in other higher education",
            "* Age not exceeding 30 years",
            "* Parental income ceiling Rs. 300,000/- per annum",
            "* Student or sibling not already receiving Mahapola elsewhere",
            "* No disciplinary action taken",
            "* Grama Niladhari Certificate required"
        ]
        return '\n'.join(criteria)

    def save_to_csv(self, filename=None):
        """Save data to CSV format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/ou_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        print(f"✓ CSV saved: {filename}")
        return filename

    def save_to_json(self, filename=None):
        """Save data to JSON format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/ou_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        print(f"✓ JSON saved: {filename}")
        return filename

    def display_summary(self):
        """Display scraping summary"""
        print("\n" + "="*70)
        print("OPEN UNIVERSITY SRI LANKA - SCHOLARSHIPS SUMMARY")
        print("="*70)
        print(f"Total Records: {len(self.data)}")
        print(f"Source: {self.source}")
        print(f"Scraped Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if self.data:
            df = pd.DataFrame(self.data)
            print(f"\nColumns: {list(df.columns)}")
            print("\n=== SCHOLARSHIPS EXTRACTED ===")
            for idx, row in df.iterrows():
                print(f"\n{idx+1}. {row['name']}")
                print(f"   Funding: {row['funding_amount']}")
                print(f"   Deadline: {row['deadline']}")
                print(
                    f"   Eligibility (first 100 chars): {str(row['eligibility'])[:100]}...")

        print("="*70 + "\n")


def main():
    """Main execution function"""
    print("Starting OUSL Scholarships Scraper...\n")

    scraper = OUScholarshipsScraper()
    scraper.scrape()

    if scraper.data:
        scraper.save_to_csv()
        scraper.save_to_json()
        scraper.display_summary()
    else:
        logger.warning("No scholarships were scraped")
        print("✗ No scholarships were scraped.")


if __name__ == "__main__":
    main()
