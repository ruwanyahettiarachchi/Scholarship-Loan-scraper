"""
DAAD Scholarship Database Scraper
Website: https://www.daad-sri-lanka.org/en/find-funding/scholarship-database/
Scrapes scholarship opportunities for Sri Lankan students from DAAD
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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
        logging.FileHandler('logs/daad_scholarships_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DAADScholarshipScraper:
    def __init__(self):
        self.data = []
        self.source = 'DAAD Sri Lanka'
        self.base_url = "https://www.daad-sri-lanka.org"
        self.search_url = "https://www.daad-sri-lanka.org/en/find-funding/scholarship-database/?type=a&origin=195&target=195&status=0&intention=0&subject=0&q="
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument(
            '--disable-blink-features=AutomationControlled')
        self.options.add_argument('--window-size=1920,1080')

    def scrape(self):
        """Main scraping method"""
        logger.info(
            f"Starting DAAD Scholarship Scraping from {self.search_url}")

        try:
            # Get scholarship links from search results
            scholarship_links = self._get_scholarship_links()
            logger.info(f"Found {len(scholarship_links)} scholarship programs")

            # Scrape each scholarship page
            for idx, link in enumerate(scholarship_links, 1):
                logger.info(f"Scraping {idx}/{len(scholarship_links)}: {link}")
                scholarship = self._scrape_scholarship_page(link)
                if scholarship['name'] and scholarship['name'] != 'N/A':
                    self.data.append(scholarship)
                time.sleep(2)  # Be respectful to the server

            logger.info(
                f"DAAD scraping completed. Found {len(self.data)} scholarships")

        except Exception as e:
            logger.error(f"Error during scraping: {e}")

    def _get_scholarship_links(self):
        """Get all scholarship links from the database search results"""
        links = []

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=self.options
            )
            driver.get(self.search_url)
            time.sleep(5)

            # Wait for results to load
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CLASS_NAME, "c-search-result"))
                )
            except TimeoutException:
                logger.warning("Timeout waiting for search results")

            # Scroll to load all results
            last_height = driver.execute_script(
                "return document.body.scrollHeight")
            while True:
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script(
                    "return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # Find all scholarship result items
            results = soup.find_all(
                'div', class_=['c-search-result', 'search-result'])

            if not results:
                # Try alternative selector
                results = soup.find_all('article')

            logger.info(f"Found {len(results)} result items")

            for result in results:
                # Find link to scholarship detail page
                link_elem = result.find('a', href=True)
                if link_elem:
                    href = link_elem.get('href')
                    if href:
                        # Make absolute URL
                        if href.startswith('/'):
                            full_url = self.base_url + href
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            full_url = self.base_url + '/' + href

                        if full_url not in links:
                            links.append(full_url)

            driver.quit()
            logger.info(f"Extracted {len(links)} unique scholarship links")

        except Exception as e:
            logger.error(f"Error getting scholarship links: {e}")
            try:
                driver.quit()
            except:
                pass

        return links

    def _scrape_scholarship_page(self, scholarship_url):
        """Scrape individual scholarship page"""
        scholarship = {
            'name': 'N/A',
            'description': '',
            'eligibility': 'N/A',
            'funding_amount': 'N/A',
            'deadline': 'N/A',
            'contact': 'N/A',
            'application_url': scholarship_url,
            'source': self.source,
            'url': self.search_url,
            'scrape_date': datetime.now().isoformat(),
            'program_type': 'N/A',
            'target_group': 'N/A',
            'subject_area': 'N/A',
            'study_level': 'N/A',
            'duration': 'N/A',
            'country': 'Germany',
            'language_requirements': 'N/A'
        }

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=self.options
            )
            driver.get(scholarship_url)
            time.sleep(3)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            driver.quit()

            # Extract title
            title = soup.find('h1')
            if title:
                scholarship['name'] = title.get_text(strip=True)

            # Extract main content
            content = soup.find(
                'div', class_=['c-content', 'content', 'main-content'])
            if not content:
                content = soup.find('article')

            if content:
                full_text = content.get_text()

                # Extract description (first paragraph or summary)
                summary = content.find(
                    'div', class_=['summary', 'intro', 'description'])
                if summary:
                    scholarship['description'] = summary.get_text(strip=True)[
                        :500]
                else:
                    paragraphs = content.find_all('p')
                    if paragraphs:
                        scholarship['description'] = ' '.join(
                            [p.get_text(strip=True) for p in paragraphs[:2]])[:500]

                # Extract structured information
                scholarship = self._extract_structured_info(
                    content, scholarship, full_text)

                # Extract deadline
                scholarship['deadline'] = self._extract_deadline(full_text)

                # Extract eligibility
                scholarship['eligibility'] = self._extract_eligibility(
                    content, full_text)

                # Extract funding details
                scholarship['funding_amount'] = self._extract_funding(
                    full_text)

                # Extract contact
                scholarship['contact'] = self._extract_contact(full_text)

        except Exception as e:
            logger.warning(f"Error scraping {scholarship_url}: {e}")
            try:
                driver.quit()
            except:
                pass

        return scholarship

    def _extract_structured_info(self, content, scholarship, full_text):
        """Extract structured information from content"""
        try:
            # Look for key-value pairs or labeled sections
            labels = content.find_all(['dt', 'strong', 'b', 'label'])

            for label in labels:
                label_text = label.get_text(strip=True).lower()

                # Get the value (next sibling or parent's next sibling)
                value_elem = label.find_next(['dd', 'p', 'span', 'div'])
                if value_elem:
                    value = value_elem.get_text(strip=True)

                    # Program type
                    if 'type' in label_text or 'programme type' in label_text:
                        scholarship['program_type'] = value

                    # Target group
                    elif 'target group' in label_text or 'status' in label_text or 'who' in label_text:
                        scholarship['target_group'] = value

                    # Subject area
                    elif 'subject' in label_text or 'field' in label_text:
                        scholarship['subject_area'] = value

                    # Study level
                    elif 'level' in label_text or 'degree' in label_text:
                        scholarship['study_level'] = value

                    # Duration
                    elif 'duration' in label_text or 'period' in label_text:
                        scholarship['duration'] = value

                    # Language
                    elif 'language' in label_text:
                        scholarship['language_requirements'] = value

            # Extract from full text if not found
            if scholarship['target_group'] == 'N/A':
                if any(word in full_text.lower() for word in ['doctoral', 'phd', 'doctorate']):
                    scholarship['target_group'] = 'Doctoral candidates/PhD students'
                elif 'master' in full_text.lower():
                    scholarship['target_group'] = 'Master students'
                elif 'bachelor' in full_text.lower():
                    scholarship['target_group'] = 'Bachelor students'
                elif 'graduate' in full_text.lower():
                    scholarship['target_group'] = 'Graduates'

            if scholarship['study_level'] == 'N/A':
                if 'doctoral' in full_text.lower() or 'phd' in full_text.lower():
                    scholarship['study_level'] = 'Doctoral/PhD'
                elif 'master' in full_text.lower():
                    scholarship['study_level'] = "Master's"
                elif 'bachelor' in full_text.lower():
                    scholarship['study_level'] = "Bachelor's"

        except Exception as e:
            logger.warning(f"Error extracting structured info: {e}")

        return scholarship

    def _extract_deadline(self, text):
        """Extract application deadline"""
        # Look for deadline patterns
        deadline_patterns = [
            r'(?:deadline|application deadline|closing date)[:\s]+([^\n]+?)(?:\n|$)',
            r'(\d{1,2}[./]\d{1,2}[./]\d{2,4})',
            r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
        ]

        for pattern in deadline_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                deadline = match.group(1) if len(
                    match.groups()) > 0 else match.group(0)
                return deadline.strip()[:100]

        return 'N/A'

    def _extract_eligibility(self, content, full_text):
        """Extract eligibility criteria"""
        eligibility_info = []

        # Look for eligibility section
        for heading in content.find_all(['h2', 'h3', 'h4', 'strong']):
            heading_text = heading.get_text(strip=True).lower()
            if any(keyword in heading_text for keyword in ['eligibility', 'requirement', 'who can apply', 'qualification']):
                # Get content after heading
                current = heading.find_next()
                for _ in range(10):
                    if not current:
                        break

                    if current.name in ['h2', 'h3', 'h4']:
                        break

                    text = current.get_text(strip=True)
                    if text and len(text) > 10:
                        if current.name == 'li':
                            eligibility_info.append(f"• {text}")
                        elif current.name == 'p':
                            eligibility_info.append(text)

                    current = current.find_next()

                break

        # If no section found, look for key phrases
        if not eligibility_info:
            if 'doctoral' in full_text.lower():
                eligibility_info.append("• Doctoral candidates/PhD students")
            if 'master' in full_text.lower():
                eligibility_info.append(
                    "• Master's degree holders or students")
            if 'graduate' in full_text.lower():
                eligibility_info.append("• University graduates")

        if eligibility_info:
            return '\n'.join(eligibility_info[:10])

        return 'N/A'

    def _extract_funding(self, text):
        """Extract funding amount details"""
        # Look for funding/grant/scholarship amount
        funding_patterns = [
            r'(?:monthly|per month)[:\s]*€?\s*([0-9,]+)',
            r'€\s*([0-9,]+)',
            r'([0-9,]+)\s*(?:EUR|Euro)',
        ]

        for pattern in funding_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount = match.group(1)
                return f"€{amount} per month (approx.)"

        # Check for general funding description
        if 'monthly allowance' in text.lower() or 'stipend' in text.lower():
            return 'Monthly stipend provided (contact for details)'

        if 'fully funded' in text.lower() or 'full scholarship' in text.lower():
            return 'Fully funded'

        return 'N/A'

    def _extract_contact(self, text):
        """Extract contact information"""
        # Look for email
        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        if email_match:
            return email_match.group(0)

        # Look for contact section
        if 'contact' in text.lower():
            lines = text.split('\n')
            for i, line in enumerate(lines):
                if 'contact' in line.lower():
                    # Return next few lines
                    contact_info = ' '.join(lines[i:i+3])
                    return contact_info[:150]

        return 'DAAD Sri Lanka - https://www.daad-sri-lanka.org'

    def save_to_csv(self, filename=None):
        """Save data to CSV format"""
        if not self.data:
            logger.warning("No data to save")
            return

        if filename is None:
            filename = f'data/daad_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

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
            filename = f'data/daad_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        print(f"✓ JSON saved: {filename}")
        return filename

    def display_summary(self):
        """Display scraping summary"""
        print("\n" + "="*70)
        print("DAAD SCHOLARSHIPS - SCRAPING SUMMARY")
        print("="*70)
        print(f"Total Records: {len(self.data)}")
        print(f"Source: {self.source}")
        print(f"Scraped Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if self.data:
            df = pd.DataFrame(self.data)
            print(f"\nColumns: {list(df.columns)}")

            print("\n=== FIRST 5 SCHOLARSHIPS ===")
            for idx, row in df.head(5).iterrows():
                print(f"\n{idx+1}. {row['name']}")
                print(f"   Target: {row['target_group']}")
                print(f"   Level: {row['study_level']}")
                print(f"   Deadline: {row['deadline']}")
                print(f"   Funding: {row['funding_amount']}")

            # Distribution
            if 'target_group' in df.columns:
                print("\n=== BY TARGET GROUP ===")
                print(df['target_group'].value_counts().to_string())

            if 'study_level' in df.columns:
                print("\n=== BY STUDY LEVEL ===")
                print(df['study_level'].value_counts().to_string())

        print("="*70 + "\n")


def main():
    """Main execution function"""
    print("Starting DAAD Scholarship Scraper...")
    print("This may take several minutes as it scrapes each scholarship page\n")

    scraper = DAADScholarshipScraper()
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
