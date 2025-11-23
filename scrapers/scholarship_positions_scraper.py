"""
Scholarship Positions Scraper
Website: https://scholarship-positions.com/category/sri-lanka-scholarships/
Scrapes Sri Lanka scholarships from scholarship-positions.com
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
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scholarship_positions_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ScholarshipPositionsScraper:
    def __init__(self):
        self.data = []
        self.source = 'Scholarship Positions'
        self.base_url = "https://scholarship-positions.com"
        self.category_url = "https://scholarship-positions.com/category/sri-lanka-scholarships/"
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument(
            '--disable-blink-features=AutomationControlled')

    def scrape(self):
        """Main scraping method"""
        logger.info(
            f"Starting Scholarship Positions Scraping from {self.category_url}")

        try:
            # Get all scholarship links from category page
            scholarship_links = self._get_scholarship_links()
            logger.info(f"Found {len(scholarship_links)} scholarship links")

            # Scrape details from each scholarship page
            for idx, link in enumerate(scholarship_links, 1):
                logger.info(f"Scraping {idx}/{len(scholarship_links)}: {link}")
                scholarship = self._scrape_scholarship_page(link)
                if scholarship['name'] and scholarship['name'] != 'N/A':
                    self.data.append(scholarship)

            logger.info(
                f"Scraping completed. Found {len(self.data)} scholarships")

        except Exception as e:
            logger.error(f"Error during scraping: {e}")

    def _get_scholarship_links(self):
        """Get all scholarship links from category page"""
        links = []
        page = 1
        max_pages = 5  # Limit to 5 pages to avoid too long scraping

        try:
            while page <= max_pages:
                if page == 1:
                    url = self.category_url
                else:
                    url = f"{self.category_url}page/{page}/"

                logger.info(f"Fetching page {page}: {url}")

                driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=self.options
                )
                driver.get(url)
                time.sleep(3)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                driver.quit()

                # Find all article links
                articles = soup.find_all('article')

                if not articles:
                    logger.info(f"No articles found on page {page}. Stopping.")
                    break

                page_links = []
                for article in articles:
                    # Find the link in the article
                    link_elem = article.find('a', href=True)
                    if link_elem and link_elem.get('href'):
                        link = link_elem.get('href')
                        if link not in links and link not in page_links:
                            page_links.append(link)
                            links.append(link)

                logger.info(f"Page {page}: Found {len(page_links)} new links")

                if len(page_links) == 0:
                    break

                page += 1
                time.sleep(2)  # Be nice to the server

        except Exception as e:
            logger.error(f"Error getting scholarship links: {e}")

        return links

    def _scrape_scholarship_page(self, scholarship_url):
        """Scrape details from individual scholarship page"""
        scholarship = {
            'name': 'N/A',
            'description': '',
            'eligibility': 'N/A',
            'funding_amount': 'N/A',
            'deadline': 'N/A',
            'contact': 'N/A',
            'application_url': scholarship_url,
            'source': self.source,
            'url': self.category_url,
            'scrape_date': datetime.now().isoformat()
        }

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=self.options
            )
            driver.get(scholarship_url)
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            driver.quit()

            # Extract title
            title_elem = soup.find('h1', class_=['entry-title', 'post-title'])
            if title_elem:
                scholarship['name'] = title_elem.get_text(strip=True)
            else:
                # Try alternative selector
                title_elem = soup.find('h1')
                if title_elem:
                    scholarship['name'] = title_elem.get_text(strip=True)

            # Extract main content
            article_content = soup.find(
                'div', class_=['entry-content', 'post-content', 'content'])
            if not article_content:
                article_content = soup.find('article')

            if article_content:
                # Extract full text
                full_text = article_content.get_text()

                # Extract description (first few paragraphs)
                paragraphs = article_content.find_all('p')
                if paragraphs:
                    scholarship['description'] = ' '.join(
                        [p.get_text(strip=True) for p in paragraphs[:2]])[:300]

                # ===== EXTRACT DEADLINE =====
                scholarship['deadline'] = self._extract_deadline(full_text)

                # ===== EXTRACT ELIGIBILITY =====
                scholarship['eligibility'] = self._extract_eligibility(
                    article_content, full_text)

                # ===== EXTRACT FUNDING AMOUNT =====
                scholarship['funding_amount'] = self._extract_funding(
                    full_text)

                # ===== EXTRACT CONTACT INFO =====
                contact_match = re.search(
                    r'(?:contact|email)[:\s]+([^\n]+)',
                    full_text,
                    re.IGNORECASE
                )
                if contact_match:
                    scholarship['contact'] = contact_match.group(1)[:100]

        except Exception as e:
            logger.warning(f"Error scraping page {scholarship_url}: {e}")
            try:
                driver.quit()
            except:
                pass

        return scholarship

    def _extract_deadline(self, text):
        """Extract deadline from text"""
        # Look for various deadline patterns
        patterns = [
            r'(?:deadline|closing\s+date|application\s+deadline)[:\s]+([^\n]+?)(?:\n|$)',
            r'(?:Deadline|Closing Date)[:\s]+([^\n]+)',
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',  # DD/MM/YYYY or MM/DD/YYYY
            r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                deadline = match.group(1).strip()
                # Clean up the deadline string
                deadline = deadline.replace(
                    '\n', ' ').replace('\r', '').strip()
                if len(deadline) > 0 and len(deadline) < 100:
                    return deadline

        return 'N/A'

    def _extract_eligibility(self, article_element, full_text):
        """Extract eligibility information in structured format"""
        eligibility_info = []

        # Look for eligibility section heading
        eligibility_section = None

        # Search for eligibility/requirements headings
        for heading in article_element.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
            heading_text = heading.get_text(strip=True).lower()
            if any(keyword in heading_text for keyword in ['eligibility', 'requirement', 'criteria', 'qualify', 'eligible']):
                eligibility_section = heading
                break

        # If we found an eligibility section, extract content after it
        if eligibility_section:
            current = eligibility_section.find_next()
            eligible_countries = 'N/A'
            eligible_subjects = 'N/A'
            criteria_list = []

            # Extract next 10 elements after heading
            for _ in range(15):
                if not current:
                    break

                text = current.get_text(strip=True)

                # Check for Eligible Countries
                if 'eligible countries' in text.lower():
                    eligible_countries = text

                # Check for Eligible Course/Subjects
                if any(keyword in text.lower() for keyword in ['eligible course', 'subject', 'acceptable course']):
                    eligible_subjects = text

                # Check for list items
                if current.name in ['li', 'ul', 'ol']:
                    list_items = current.find_all('li')
                    for item in list_items:
                        item_text = item.get_text(strip=True)
                        if item_text and len(item_text) > 5:
                            criteria_list.append(f"• {item_text}")

                # Check for bullet points marked with asterisks or dashes
                if current.name == 'p':
                    if text.startswith('*') or text.startswith('-') or text.startswith('•'):
                        criteria_list.append(text)

                current = current.find_next()

            # Build comprehensive eligibility string
            if eligible_countries != 'N/A':
                eligibility_info.append(eligible_countries)

            if eligible_subjects != 'N/A':
                eligibility_info.append(eligible_subjects)

            if criteria_list:
                eligibility_info.append('Eligibility Criteria:')
                # Limit to 10 criteria
                eligibility_info.extend(criteria_list[:10])

        # If no eligibility section found, extract from full text
        if not eligibility_info:
            # Look for countries mentioned
            if 'Sri Lanka' in full_text:
                eligibility_info.append('* Eligible Countries: Sri Lanka')

            # Look for any list items that mention requirements
            ul_lists = article_element.find_all('ul')
            for ul in ul_lists:
                list_items = ul.find_all('li')
                for item in list_items:
                    item_text = item.get_text(strip=True)
                    if any(keyword in item_text.lower() for keyword in ['gpa', 'grade', 'pass', 'first', 'full-time', 'part-time', 'degree']):
                        if len(item_text) < 200:
                            eligibility_info.append(f"• {item_text}")

        # Format final eligibility string
        if eligibility_info:
            result = '\n'.join(eligibility_info[:15])  # Limit to 15 lines
            return result if len(result) > 10 else 'N/A'

        return 'N/A'

    def _extract_funding(self, text):
        """Extract funding/award amount from text"""
        # Look for various currency formats
        patterns = [
            r'(?:award|amount|fund|scholarship|grant)[:\s]*(?:\$|USD|Rs\.?|LKR)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)',
            r'(?:\$|USD|Rs\.?|LKR)\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)',
            r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)\s*(?:\$|USD|Rs\.?|LKR)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)

        return 'N/A'

    def save_to_csv(self, filename=None):
        """Save data to CSV format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/scholarship_positions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

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
            filename = f'data/scholarship_positions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        print(f"✓ JSON saved: {filename}")
        return filename

    def display_summary(self):
        """Display scraping summary"""
        print("\n" + "="*70)
        print("SCHOLARSHIP POSITIONS - SCRAPING SUMMARY")
        print("="*70)
        print(f"Total Records: {len(self.data)}")
        print(f"Source: {self.source}")
        print(f"Scraped Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Category: Sri Lanka Scholarships")

        if self.data:
            df = pd.DataFrame(self.data)
            print(f"\nColumns: {list(df.columns)}")
            print("\n=== FIRST 5 SCHOLARSHIPS ===")
            for idx, row in df.head(5).iterrows():
                print(f"\n{idx+1}. {row['name']}")
                print(f"   Deadline: {row['deadline']}")
                print(f"   Funding: {row['funding_amount']}")
                print(f"   Eligibility: {str(row['eligibility'])[:100]}...")

            # Data quality check
            print("\n\n=== DATA QUALITY ===")
            non_na_count = df[df['funding_amount'] != 'N/A'].shape[0]
            print(
                f"Scholarships with funding amount: {non_na_count}/{len(df)}")
            deadline_count = df[df['deadline'] != 'N/A'].shape[0]
            print(f"Scholarships with deadline: {deadline_count}/{len(df)}")
            eligibility_count = df[df['eligibility'] != 'N/A'].shape[0]
            print(
                f"Scholarships with eligibility: {eligibility_count}/{len(df)}")
            desc_count = df[df['description'] != ''].shape[0]
            print(f"Scholarships with description: {desc_count}/{len(df)}")

        print("="*70 + "\n")


def main():
    """Main execution function"""
    print("Starting Scholarship Positions Scraper...")
    print("This may take several minutes as it scrapes each scholarship page\n")

    scraper = ScholarshipPositionsScraper()
    scraper.scrape()

    if scraper.data:
        scraper.save_to_csv()
        scraper.save_to_json()
        scraper.display_summary()
    else:
        logger.warning("No scholarships were scraped")
        print("✗ No scholarships were scraped. Please check the website or logs.")


if __name__ == "__main__":
    main()
