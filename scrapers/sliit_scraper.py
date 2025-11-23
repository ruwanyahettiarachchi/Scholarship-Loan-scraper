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
        logging.FileHandler('logs/sliit_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SLIITScholarshipScraper:
    def __init__(self):
        self.data = []
        self.source = 'SLIIT'
        self.url = "https://www.sliit.lk/scholarships/"
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')

    def scrape(self):
        """Main scraping method for SLIIT scholarships"""
        logger.info(f"Starting SLIIT Scholarship Scraping from {self.url}")

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=self.options
            )
            driver.get(self.url)
            time.sleep(5)

            # Wait for content to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.TAG_NAME, "h3"))
            )

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Find all scholarship items - SLIIT uses h3 for scholarship names
            headings = soup.find_all('h3')
            logger.info(f"Found {len(headings)} h3 headings")

            for heading in headings:
                scholarship = self._extract_scholarship(heading)

                # Avoid duplicates
                if scholarship['name'] and scholarship['name'] not in [s['name'] for s in self.data]:
                    self.data.append(scholarship)

            driver.quit()
            logger.info(
                f"SLIIT scraping completed. Found {len(self.data)} scholarships")

        except Exception as e:
            logger.error(f"Error scraping SLIIT: {e}")
            try:
                driver.quit()
            except:
                pass

    def _extract_scholarship(self, heading_element):
        """Extract individual scholarship information from heading element"""
        scholarship = {
            'name': heading_element.get_text(strip=True),
            'description': '',
            'eligibility': 'N/A',
            'funding_amount': 'N/A',
            'deadline': 'N/A',
            'contact': 'N/A',
            'application_url': 'N/A',
            'source': self.source,
            'url': self.url,
            'scrape_date': datetime.now().isoformat()
        }

        try:
            # Get description from following paragraph
            next_elem = heading_element.find_next(['p', 'div'])
            if next_elem:
                text = next_elem.get_text(strip=True)
                if len(text) > 10:
                    scholarship['description'] = text[:500]

            # Look for more details in parent element
            parent = heading_element.parent
            if parent:
                parent_text = parent.get_text()

                # Extract funding amount
                if 'Rs.' in parent_text or 'LKR' in parent_text:
                    amount_match = re.search(
                        r'(Rs\.|LKR)\s*([\d,]+)', parent_text)
                    if amount_match:
                        scholarship['funding_amount'] = amount_match.group(0)

                # Extract deadline
                if 'deadline' in parent_text.lower() or 'closing' in parent_text.lower():
                    lines = parent_text.split('\n')
                    for line in lines:
                        if 'deadline' in line.lower() or 'closing' in line.lower():
                            scholarship['deadline'] = line.strip()[:100]
                            break

            # Find application link
            link = heading_element.find_next('a', href=True)
            if link:
                href = link.get('href')
                if href.startswith('http'):
                    scholarship['application_url'] = href
                elif href.startswith('/'):
                    scholarship['application_url'] = 'https://www.sliit.lk' + href

        except Exception as e:
            logger.warning(f"Error extracting details: {e}")

        return scholarship

    def save_to_csv(self, filename=None):
        """Save data to CSV format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/sliit_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        return filename

    def save_to_json(self, filename=None):
        """Save data to JSON format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/sliit_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        return filename

    def display_summary(self):
        """Display scraping summary"""
        print("\n" + "="*60)
        print("SLIIT SCHOLARSHIPS - SCRAPING SUMMARY")
        print("="*60)
        print(f"Total Records: {len(self.data)}")
        print(f"Source: {self.source}")
        print(f"Scraped Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if self.data:
            df = pd.DataFrame(self.data)
            print(f"\nColumns: {list(df.columns)}")
            print("\n=== FIRST 5 SCHOLARSHIPS ===")
            print(df[['name', 'funding_amount', 'deadline']].head().to_string())

            # Data quality check
            print("\n=== DATA QUALITY ===")
            non_na_count = df[df['funding_amount'] != 'N/A'].shape[0]
            print(
                f"Scholarships with funding amount: {non_na_count}/{len(df)}")

        print("="*60 + "\n")


def main():
    """Main execution function"""
    scraper = SLIITScholarshipScraper()
    scraper.scrape()

    if scraper.data:
        scraper.save_to_csv()
        scraper.save_to_json()
        scraper.display_summary()
    else:
        logger.warning("No scholarships were scraped")


if __name__ == "__main__":
    main()
