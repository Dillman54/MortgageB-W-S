import argparse
import os
import time
import logging
import re
from pathlib import Path

from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials

URL = "https://sudburyrealestateboard.com/find-a-realtor/"
SHEET_ID = "1-kGNDW07iQ7WarkpHmhPCgI6wEHFyFbf6-VqnIEiYQs"
SHEET_TAB = "Mortgage Agent List"


def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "scrape.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def fetch_with_requests(url: str) -> str:
    resp = requests.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} while fetching {url}")
    time.sleep(1)  # throttle
    return resp.text


def fetch_with_playwright(url: str, headless: bool) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(url)
        time.sleep(1)  # throttle
        content = page.content()
        browser.close()
        return content


def parse_entries(html: str):
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    for mailto in soup.select('a[href^="mailto:"]'):
        email = mailto.get("href", "").replace("mailto:", "").strip()
        if not email:
            continue
        block = mailto.find_parent(["div", "li", "tr"]) or mailto.parent
        text = block.get_text(" \n", strip=True)
        phone_match = re.search(r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})", text)
        phone = phone_match.group(1) if phone_match else ""
        lines = [t for t in text.splitlines() if t and email not in t and phone not in t]
        name = lines[0] if lines else ""
        brokerage = lines[1] if len(lines) > 1 else ""
        entries.append({
            "Name": name,
            "Brokerage": brokerage,
            "Email": email,
            "Phone": phone,
        })
    return entries


def deduplicate(rows):
    seen = set()
    unique = []
    for r in rows:
        key = r.get("Email", "").lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def append_to_sheet(rows):
    creds_path = os.environ.get("GOOGLE_CREDS_JSON")
    if not creds_path or not Path(creds_path).exists():
        raise RuntimeError("Missing GOOGLE_CREDS_JSON environment variable or file")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_TAB)
    data = [[r["Name"], r["Brokerage"], r["Email"], r["Phone"]] for r in rows]
    if data:
        sheet.append_rows(data, value_input_option="USER_ENTERED")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Scrape Sudbury Real Estate Board")
    parser.add_argument("--headless", default=True, help="Run browser headless", type=lambda x: str(x).lower() != "false")
    args = parser.parse_args()

    setup_logging()
    logging.info("Starting scrape")

    html = fetch_with_requests(URL)
    entries = parse_entries(html)
    if not entries:
        logging.info("Switching to Playwright scraping")
        html = fetch_with_playwright(URL, headless=args.headless)
        entries = parse_entries(html)

    rows = deduplicate(entries)
    logging.info("Parsed %d unique rows", len(rows))
    append_to_sheet(rows)
    logging.info("Completed upload")


if __name__ == "__main__":
    main()
