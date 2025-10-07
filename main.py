import os
import time
import schedule
import pandas as pd
import requests
import csv
import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ---------------- CONFIG ----------------
LINKEDIN_URL = "https://www.linkedin.com/company/extrastaff-recruitment"
SCRAPINGDOG_API_KEY = "68e0d21330bd5e034a8de319"

SUPABASE_URL = "https://nvuccudmqgileyjmjxko.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im52dWNjdWRtcWdpbGV5am1qeGtvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTgwNTk4OSwiZXhwIjoyMDc1MzgxOTg5fQ.IvYJIXhdh2cSTjz6I6guFRL2F7g6Z8CJ7sS5aeaNEiM"
BUCKET_NAME = "csv-files"

FOLLOWERS_CSV = "linkedin_followers.csv"
POSTS_CSV = "lnkdn.csv"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_service.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------- LINKEDIN SCRAPER -------------
class LinkedInFollowerExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept-Language': 'en-US,en;q=0.9'
        })

    def extract_followers(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        patterns = [
            r'(\d+(?:,\d+)*)\s+followers',
            r'followerCount["\']?\s*:\s*["\']?(\d+(?:,\d+)*)'
        ]
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                return int(m.group(1).replace(',', ''))
        return None

    def get_followers(self, url):
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code != 200:
                logging.error(f"HTTP {r.status_code} while fetching {url}")
                return None
            return self.extract_followers(r.text)
        except Exception as e:
            logging.error(f"Follower fetch error: {e}")
            return None

def save_follower_data(followers):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = {'timestamp': ts, 'linkedin_url': LINKEDIN_URL, 'followers': followers}
    exists = os.path.isfile(FOLLOWERS_CSV)
    with open(FOLLOWERS_CSV, 'a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if not exists:
            w.writeheader()
        w.writerow(row)
    logging.info(f"Saved follower count {followers} at {ts}")

def fetch_linkedin_followers():
    extractor = LinkedInFollowerExtractor()
    followers = extractor.get_followers(LINKEDIN_URL)
    if followers:
        save_follower_data(followers)
    else:
        logging.error("Failed to get follower count")

def fetch_linkedin_posts():
    try:
        url = "https://api.scrapingdog.com/linkedin"
        params = {
            "api_key": SCRAPINGDOG_API_KEY,
            "type": "company",
            "linkId": "extrastaff-recruitment"
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data or not isinstance(data, list):
            logging.error("Invalid API response")
            return
        posts = data[0].get("updates", [])
        pd.DataFrame(posts).to_csv(POSTS_CSV, index=False)
        logging.info(f"Saved {len(posts)} posts to {POSTS_CSV}")
    except Exception as e:
        logging.error(f"Post fetch error: {e}")

# ----------- SUPABASE UPLOADER -----------
def upload_csv(file_path):
    file_name = os.path.basename(file_path)
    with open(file_path, "rb") as f:
        file_bytes = f.read()
    try:
        supabase.storage.from_(BUCKET_NAME).remove([file_name])
    except Exception:
        pass  # ignore if not exists
    try:
        supabase.storage.from_(BUCKET_NAME).upload(file_name, file_bytes)
        logging.info(f"Uploaded {file_name} → Supabase")
    except Exception as e:
        logging.error(f"Upload failed for {file_name}: {e}")

def upload_all_csvs():
    if os.path.exists(FOLLOWERS_CSV):
        upload_csv(FOLLOWERS_CSV)
    if os.path.exists(POSTS_CSV):
        upload_csv(POSTS_CSV)

# ----------- SCHEDULER -----------
def setup_scheduler():
    schedule.every(5).minutes.do(fetch_linkedin_followers)
    schedule.every(4).hours.do(fetch_linkedin_posts)
    schedule.every(4).hours.do(upload_all_csvs)

    # initial run
    fetch_linkedin_followers()
    fetch_linkedin_posts()
    upload_all_csvs()

    logging.info("Scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    logging.info("Starting LinkedIn Data Service...")
    setup_scheduler()
