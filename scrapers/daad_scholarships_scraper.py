"""
DAAD Scholarship Database Scraper
Website: https://www.daad-sri-lanka.org/en/find-funding/scholarship-database/
Scrapes scholarship opportunities for Sri Lankan students from DAAD
"""

import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import logging
from datetime import datetime
import re

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

    def scrape(self):
        """Main scraping method - keeps user's working approach"""
        print("\n" + "="*70)
        print("DAAD SCHOLARSHIP SCRAPER")
        print("="*70)
        print("Setting up Selenium WebDriver...")

        # Setup Selenium (same as user's code)
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )

        # Base URL (same as user's code)
        base_url = "https://www.daad-sri-lanka.org/en/find-funding/scholarship-database/"
        base_params = "?type=a&origin=195&target=195&status=0&intention=0&subject=0&q="

        page = 1
        total_found = 0

        print("Starting scraping process...")

        try:
            while True:
                # Construct URL with pagination (same as user's code)
                url = f"{base_url}{base_params}&pg={page}"
                print(f"\nScraping Page {page}: {url}")

                driver.get(url)
                time.sleep(4)  # Wait for JavaScript

                soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Extract scholarship entries (same approach as user's code)
                entries_found_on_page = 0

                for h3 in soup.find_all('h3'):
                    link_tag = h3.find('a')

                    if link_tag and link_tag.get('href', '').startswith('?type=a'):
                        # Extract basic info
                        title = link_tag.get_text(strip=True)
                        relative_link = link_tag['href']
                        full_link = f"{base_url}{relative_link}"

                        # Get parent container (same as user's code)
                        container = h3.find_parent()
                        container_text = container.get_text(
                            " | ", strip=True) if container else ""

                        # ENHANCED: Extract all available details
                        scholarship = self._extract_all_details(
                            title,
                            full_link,
                            container,
                            container_text
                        )

                        self.data.append(scholarship)
                        entries_found_on_page += 1

                        # Show progress
                        if entries_found_on_page % 10 == 0:
                            print(
                                f"  Extracted {entries_found_on_page} scholarships so far...")

                if entries_found_on_page == 0:
                    print("No entries found on this page. Stopping.")
                    break

                print(
                    f"✓ Found {entries_found_on_page} scholarships on page {page}.")
                total_found += entries_found_on_page
                page += 1

                # Safety break
                if page > 30:
                    print("Reached maximum page limit (30). Stopping.")
                    break

        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            print(f"✗ An error occurred: {e}")
        finally:
            driver.quit()

        print(f"\n{'='*70}")
        print(f"Total scholarships scraped: {total_found}")
        print(f"{'='*70}\n")

        logger.info(f"DAAD scraping completed. Total: {total_found}")

    def _extract_all_details(self, title, link, container, container_text):
        """Enhanced extraction to get ALL scholarship details"""

        # Helper function to extract field (from user's code)
        def get_field(text, label):
            if label in text:
                try:
                    part = text.split(label)[1].split('|')[0]
                    return part.strip()
                except IndexError:
                    return "N/A"
            return "N/A"

        # Basic fields (same as user's code)
        status = get_field(container_text, "Status:")
        subject = get_field(container_text, "Subject area:")
        deadline = get_field(container_text, "Application deadline:")

        # ENHANCED: Extract additional fields

        # Description (look for paragraphs in container)
        description = "N/A"
        desc_elem = container.find('p') if container else None
        if desc_elem:
            description = desc_elem.get_text(strip=True)[:500]
        elif container:
            # Get first substantial text block
            text_blocks = [t.strip()
                           for t in container.stripped_strings if len(t.strip()) > 50]
            if text_blocks:
                description = text_blocks[0][:500]

        # Target group / Eligibility
        eligibility = []
        if status != "N/A":
            eligibility.append(f"• Status: {status}")
        if subject != "N/A":
            eligibility.append(f"• Subject: {subject}")

        eligibility_text = '\n'.join(eligibility) if eligibility else "N/A"

        # Study level (infer from status)
        study_level = "N/A"
        if status != "N/A":
            status_lower = status.lower()
            if 'doctoral' in status_lower or 'phd' in status_lower:
                study_level = "Doctoral/PhD"
            elif 'master' in status_lower:
                study_level = "Master's"
            elif 'bachelor' in status_lower:
                study_level = "Bachelor's"
            elif 'graduate' in status_lower:
                study_level = "Graduate"

        # Program type
        program_type = "N/A"
        if 'research' in title.lower():
            program_type = "Research Grant"
        elif 'study' in title.lower() or 'scholarship' in title.lower():
            program_type = "Study Scholarship"
        elif 'summer' in title.lower():
            program_type = "Summer Program"

        # Duration (look for duration info)
        duration = "N/A"
        duration_patterns = [
            r'(\d+)\s*(?:month|months|mo)',
            r'(\d+)\s*(?:year|years|yr)',
        ]
        for pattern in duration_patterns:
            match = re.search(pattern, container_text, re.IGNORECASE)
            if match:
                duration = match.group(0)
                break

        # Funding amount (look for money references)
        funding_amount = "N/A"
        if '€' in container_text or 'EUR' in container_text:
            amount_match = re.search(r'€\s*([0-9,]+)', container_text)
            if amount_match:
                funding_amount = f"€{amount_match.group(1)} per month"
        elif 'monthly allowance' in container_text.lower() or 'stipend' in container_text.lower():
            funding_amount = "Monthly stipend provided"
        elif 'fully funded' in container_text.lower():
            funding_amount = "Fully funded"

        # Build comprehensive scholarship record
        scholarship = {
            'name': title,
            'description': description,
            'eligibility': eligibility_text,
            'funding_amount': funding_amount,
            'deadline': deadline,
            'contact': 'DAAD Sri Lanka - info@daad-sri-lanka.org',
            'application_url': link,
            'source': self.source,
            'url': 'https://www.daad-sri-lanka.org/en/find-funding/scholarship-database/',
            'scrape_date': datetime.now().isoformat(),

            # Additional fields
            'status': status,
            'subject_area': subject,
            'study_level': study_level,
            'program_type': program_type,
            'duration': duration,
            'target_group': status,
            'country': 'Germany',
            'language_requirements': 'Varies by program'
        }

        return scholarship

    def save_to_csv(self, filename=None):
        """Save data to CSV format"""
        if not self.data:
            print("✗ No data to save")
            return

        if filename is None:
            filename = f'data/daad_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n✓ CSV saved: {filename}")
        print(f"✓ Total records: {len(self.data)}")
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        return filename

    def save_to_json(self, filename=None):
        """Save data to JSON format"""
        if not self.data:
            return

        if filename is None:
            filename = f'data/daad_scholarships_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        import json
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        print(f"✓ JSON saved: {filename}")
        logger.info(f"Saved {len(self.data)} scholarships to {filename}")
        return filename

    def display_summary(self):
        """Display scraping summary"""
        if not self.data:
            return

        df = pd.DataFrame(self.data)

        print("\n" + "="*70)
        print("SCRAPING SUMMARY")
        print("="*70)
        print(f"Total Records: {len(self.data)}")
        print(f"Source: {self.source}")

        print(f"\nColumns ({len(df.columns)}):")
        print(", ".join(df.columns))

        print("\n=== FIRST 5 SCHOLARSHIPS ===")
        for idx, row in df.head(5).iterrows():
            print(f"\n{idx+1}. {row['name'][:65]}")
            print(f"   Status: {row['status']}")
            print(f"   Subject: {row['subject_area']}")
            print(f"   Deadline: {row['deadline']}")
            print(f"   Link: {row['application_url']}")

        # Distribution by status
        print("\n=== BY STATUS/TARGET GROUP ===")
        status_counts = df['status'].value_counts().head(10)
        for status, count in status_counts.items():
            print(f"  {status}: {count}")

        # Distribution by subject
        print("\n=== BY SUBJECT AREA (Top 10) ===")
        subject_counts = df['subject_area'].value_counts().head(10)
        for subject, count in subject_counts.items():
            print(f"  {subject}: {count}")

        # Data completeness
        print("\n=== DATA QUALITY ===")
        print(f"With deadline: {(df['deadline'] != 'N/A').sum()}/{len(df)}")
        print(
            f"With description: {(df['description'] != 'N/A').sum()}/{len(df)}")
        print(
            f"With subject area: {(df['subject_area'] != 'N/A').sum()}/{len(df)}")
        print(
            f"With funding info: {(df['funding_amount'] != 'N/A').sum()}/{len(df)}")

        print("="*70 + "\n")


def main():
    """Main execution function"""
    scraper = DAADScholarshipScraper()
    scraper.scrape()

    if scraper.data:
        scraper.save_to_csv()
        scraper.save_to_json()
        scraper.display_summary()
    else:
        print("✗ No scholarships were scraped")


if __name__ == "__main__":
    main()
