#!/usr/bin/env python3
"""
MCSO PAID (Public Access Inmate Data) Scraper

Monitors https://apps.mcso.us/PAID/ for specific names in:
- Booked Today
- Released Last 7 Days

Posts to Slack when matches are found, remembers bookings to avoid duplicates.
"""

__version__ = "0.2.0"

import argparse
import os
import json
import time
import re
import sys
from datetime import datetime
from pathlib import Path

import ssl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import urllib3

# Disable SSL warnings since MCSO site has certificate chain issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global debug flag
DEBUG = False

# Error tracking state
ERROR_REPORT_INTERVAL_HOURS = 4
last_error_report_time = None
failure_count = 0


class LegacySSLAdapter(HTTPAdapter):
    """
    Custom adapter to handle servers with weak DH keys.
    The MCSO site uses outdated SSL configuration that modern OpenSSL rejects.
    """
    def init_poolmanager(self, *args, **kwargs):
        # Create a custom SSL context with lowered security level
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # Set security level to 1 to allow weak DH keys
        ctx.set_ciphers('DEFAULT:@SECLEVEL=1')
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

# Load environment variables
load_dotenv()

# Configuration
WATCH_NAMES = [n.strip() for n in os.getenv("WATCH_NAMES", "").split(",") if n.strip()]
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
POLL_INTERVAL_MINUTES = int(os.getenv("POLL_INTERVAL_MINUTES", "15"))
DATA_FILE = os.getenv("DATA_FILE", "data/seen_bookings.json")

BASE_URL = "https://apps.mcso.us/PAID/"
SEARCH_URL = "https://apps.mcso.us/PAID/Home/SearchResults"

# Search type values from the form
SEARCH_TYPE_NOW_IN_CUSTODY = "0"
SEARCH_TYPE_RELEASED_LAST_7_DAYS = "1"
SEARCH_TYPE_EMERGENCY_RELEASES = "2"
SEARCH_TYPE_BOOKED_LAST_7_DAYS = "3"
SEARCH_TYPE_BOOKED_TODAY = "4"
SEARCH_TYPE_BOOKED_YESTERDAY = "5"

# Session for maintaining cookies
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})
# Mount the legacy SSL adapter for HTTPS connections to handle weak DH keys
session.mount('https://', LegacySSLAdapter())


def log(message: str) -> None:
    """Print timestamped log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def debug_log(message: str) -> None:
    """Print debug message only if DEBUG mode is enabled."""
    if DEBUG:
        log(f"[DEBUG] {message}")


def load_seen_bookings() -> dict:
    """Load previously seen bookings from file."""
    try:
        if Path(DATA_FILE).exists():
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                log(f"Loaded {len(data.get('booked', []))} booked and {len(data.get('released', []))} released records")
                return data
    except (json.JSONDecodeError, IOError) as e:
        log(f"Warning: Could not load seen bookings: {e}")
    
    return {"booked": [], "released": []}


def save_seen_bookings(seen: dict) -> None:
    """Save seen bookings to file."""
    try:
        # Ensure directory exists
        Path(DATA_FILE).parent.mkdir(parents=True, exist_ok=True)
        
        with open(DATA_FILE, "w") as f:
            json.dump(seen, f, indent=2)
        log(f"Saved {len(seen.get('booked', []))} booked and {len(seen.get('released', []))} released records")
    except IOError as e:
        log(f"Error saving seen bookings: {e}")


def send_slack_message(message: str) -> None:
    """Send message to Slack webhook or stdout if no webhook configured."""
    if not SLACK_WEBHOOK_URL:
        log(f"[SLACK MESSAGE] {message}")
        return
    
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=10
        )
        if response.status_code == 200:
            log("Slack message sent successfully")
        else:
            log(f"Slack webhook returned status {response.status_code}")
    except requests.RequestException as e:
        log(f"Error sending Slack message: {e}")


def report_scraping_error(error_type: str, error_details: str) -> None:
    """
    Report a scraping error to Slack, rate-limited to once every 4 hours.
    Tracks failure count which is reset on success.
    """
    global last_error_report_time, failure_count
    
    failure_count += 1
    log(f"Scraping error ({error_type}): {error_details} [failure #{failure_count}]")
    
    now = datetime.now()
    
    # Check if we should send a Slack notification
    should_report = False
    if last_error_report_time is None:
        should_report = True
    else:
        hours_since_last_report = (now - last_error_report_time).total_seconds() / 3600
        if hours_since_last_report >= ERROR_REPORT_INTERVAL_HOURS:
            should_report = True
    
    if should_report:
        message = f"⚠️ *SCRAPER ERROR*\n"
        message += f"*Error Type:* {error_type}\n"
        message += f"*Details:* {error_details}\n"
        message += f"*Failure Count:* {failure_count} since last success\n"
        message += f"_Next error report in {ERROR_REPORT_INTERVAL_HOURS} hours if errors persist_"
        
        send_slack_message(message)
        last_error_report_time = now
    else:
        hours_until_next = ERROR_REPORT_INTERVAL_HOURS - (now - last_error_report_time).total_seconds() / 3600
        debug_log(f"Suppressing error report (next report in {hours_until_next:.1f} hours)")


def reset_failure_count() -> None:
    """Reset the failure count after a successful scrape."""
    global failure_count
    if failure_count > 0:
        log(f"Scraping recovered after {failure_count} failures")
        failure_count = 0


def name_matches(first_name: str, last_name: str, watch_names: list) -> bool:
    """Check if a name matches any of our watch names."""
    first_name = first_name.lower().strip()
    last_name = last_name.lower().strip()
    full_name = f"{last_name} {first_name}"
    
    for watch in watch_names:
        watch_lower = watch.lower().strip()
        parts = watch_lower.split()
        
        if len(parts) == 1:
            # Just last name provided
            if last_name == watch_lower or first_name == watch_lower:
                return True
        else:
            # Last name and first name provided (format: "LastName FirstName")
            watch_last = parts[0]
            watch_first = " ".join(parts[1:])
            if last_name == watch_last and first_name.startswith(watch_first):
                return True
            # Also try reverse order (FirstName LastName)
            if first_name == parts[0] and last_name == " ".join(parts[1:]):
                return True
    
    return False


def generate_booking_id(record: dict) -> str:
    """Generate a unique ID for a booking record."""
    # Combine key fields to create unique identifier
    parts = [
        record.get("last_name", ""),
        record.get("first_name", ""),
        record.get("booking_date", ""),
        record.get("booking_number", ""),
    ]
    return "|".join(str(p).strip() for p in parts if p)


def get_form_fields(soup: BeautifulSoup) -> dict:
    """Extract hidden form fields (like __VIEWSTATE) for ASP.NET forms."""
    fields = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        if name:
            fields[name] = inp.get("value", "")
    return fields


def parse_results_table(soup: BeautifulSoup) -> list | None:
    """
    Parse the results table and extract inmate records.
    Returns None if no table found (indicates potential blocking/error).
    Returns empty list if table exists but has no records.
    """
    # Find the search-results table
    table = soup.find("table", {"class": "search-results"})
    if not table:
        # Fallback: try finding any table with tbody
        table = soup.find("table")
    
    if not table:
        debug_log("No table found on page!")
        return None  # Signal that something is wrong
    
    debug_log(f"Found table with class={table.get('class')}")
    records = []
    
    # Get rows from tbody if present, otherwise all tr elements
    tbody = table.find("tbody")
    if tbody:
        rows = tbody.find_all("tr")
    else:
        # Skip header row
        all_rows = table.find_all("tr")
        rows = all_rows[1:] if len(all_rows) > 1 else []
    
    debug_log(f"Found {len(rows)} data rows")
    
    # Parse each row
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        
        # First cell: Name in "Last, First" format, inside an <a> tag
        name_cell = cells[0]
        name_link = name_cell.find("a")
        
        if name_link:
            full_name = name_link.get_text(strip=True)
            # Extract booking number from URL like /PAID/Home/Booking/1638002
            href = name_link.get("href", "")
            booking_number = href.split("/")[-1] if href else ""
        else:
            full_name = name_cell.get_text(strip=True)
            booking_number = ""
        
        # Parse "Last, First" format
        if "," in full_name:
            parts = full_name.split(",", 1)
            last_name = parts[0].strip()
            first_name = parts[1].strip() if len(parts) > 1 else ""
        else:
            last_name = full_name
            first_name = ""
        
        # Second cell: Booking/Release date
        date_cell = cells[1]
        booking_date = date_cell.get_text(strip=True)
        
        record = {
            "last_name": last_name,
            "first_name": first_name,
            "booking_date": booking_date,
            "booking_number": booking_number,
            "full_name": full_name,
        }
        
        records.append(record)
        debug_log(f"  Found: {last_name}, {first_name} (#{booking_number}) - {booking_date}")
    
    if not records:
        debug_log("No records found in table")
    
    return records


def fetch_booked_today(seen: dict) -> tuple[list, bool]:
    """
    Fetch and parse 'Booked Today' results.
    Returns (matches, success) tuple.
    """
    log("Checking 'Booked Today'...")
    matches = []
    
    try:
        # Submit the search form - no need to get the page first, just POST directly
        form_data = {
            "FirstName": "",
            "LastName": "",
            "SearchType": SEARCH_TYPE_BOOKED_TODAY
        }
        
        debug_log(f"Submitting search: SearchType={SEARCH_TYPE_BOOKED_TODAY} (Booked Today)")
        
        response = session.post(SEARCH_URL, data=form_data, timeout=60, verify=False)
        response.raise_for_status()
        
        debug_log(f"Response status: {response.status_code}, length: {len(response.text)}")
        
        soup = BeautifulSoup(response.text, "lxml")
        
        # Save response HTML for inspection (only in debug mode)
        if DEBUG:
            debug_file = Path(DATA_FILE).parent / "debug_booked_today.html"
            with open(debug_file, "w") as f:
                f.write(response.text)
            debug_log(f"Saved response HTML to {debug_file}")
        
        records = parse_results_table(soup)
        
        # Check if table was found
        if records is None:
            report_scraping_error(
                "No Results Table",
                "Could not find results table on 'Booked Today' page - site may have changed or be blocking requests"
            )
            return matches, False
        
        log(f"Found {len(records)} records in 'Booked Today'")
        
        for record in records:
            first_name = record.get("first_name", "")
            last_name = record.get("last_name", "")
            
            if name_matches(first_name, last_name, WATCH_NAMES):
                booking_id = generate_booking_id(record)
                
                if booking_id not in seen["booked"]:
                    seen["booked"].append(booking_id)
                    matches.append(record)
                    
                    # Build Slack message
                    message = f"🚨 *BOOKING ALERT*\n"
                    message += f"*Name:* {last_name}, {first_name}\n"
                    if record.get("booking_date"):
                        message += f"*Booking Date:* {record['booking_date']}\n"
                    if record.get("booking_number"):
                        message += f"*Booking #:* {record['booking_number']}\n"
                    if record.get("charges"):
                        message += f"*Charges:* {record['charges']}\n"
                    
                    send_slack_message(message)
                else:
                    debug_log(f"Already seen booking for {last_name}, {first_name}")
        
        return matches, True
    
    except requests.HTTPError as e:
        report_scraping_error(
            f"HTTP Error {e.response.status_code if e.response else 'Unknown'}",
            f"Failed to fetch 'Booked Today': {e}"
        )
        return matches, False
    except requests.RequestException as e:
        report_scraping_error(
            "Connection Error",
            f"Failed to connect for 'Booked Today': {e}"
        )
        return matches, False
    except Exception as e:
        report_scraping_error(
            "Processing Error",
            f"Error processing 'Booked Today': {e}"
        )
        return matches, False


def fetch_released_last_7_days(seen: dict) -> tuple[list, bool]:
    """
    Fetch and parse 'Released Last 7 Days' results.
    Returns (matches, success) tuple.
    """
    log("Checking 'Released Last 7 Days'...")
    matches = []
    
    try:
        # Submit the search form
        form_data = {
            "FirstName": "",
            "LastName": "",
            "SearchType": SEARCH_TYPE_RELEASED_LAST_7_DAYS
        }
        
        debug_log(f"Submitting search: SearchType={SEARCH_TYPE_RELEASED_LAST_7_DAYS} (Released Last 7 Days)")
        
        response = session.post(SEARCH_URL, data=form_data, timeout=60, verify=False)
        response.raise_for_status()
        
        debug_log(f"Response status: {response.status_code}, length: {len(response.text)}")
        
        soup = BeautifulSoup(response.text, "lxml")
        
        # Save response HTML for inspection (only in debug mode)
        if DEBUG:
            debug_file = Path(DATA_FILE).parent / "debug_released_7_days.html"
            with open(debug_file, "w") as f:
                f.write(response.text)
            debug_log(f"Saved response HTML to {debug_file}")
        
        records = parse_results_table(soup)
        
        # Check if table was found
        if records is None:
            report_scraping_error(
                "No Results Table",
                "Could not find results table on 'Released Last 7 Days' page - site may have changed or be blocking requests"
            )
            return matches, False
        
        log(f"Found {len(records)} records in 'Released Last 7 Days'")
        
        for record in records:
            first_name = record.get("first_name", "")
            last_name = record.get("last_name", "")
            
            if name_matches(first_name, last_name, WATCH_NAMES):
                booking_id = generate_booking_id(record)
                
                if booking_id not in seen["released"]:
                    seen["released"].append(booking_id)
                    matches.append(record)
                    
                    # Build Slack message
                    message = f"✅ *RELEASE ALERT*\n"
                    message += f"*Name:* {last_name}, {first_name}\n"
                    if record.get("release_date"):
                        message += f"*Release Date:* {record['release_date']}\n"
                    if record.get("booking_date"):
                        message += f"*Original Booking Date:* {record['booking_date']}\n"
                    if record.get("booking_number"):
                        message += f"*Booking #:* {record['booking_number']}\n"
                    
                    send_slack_message(message)
                else:
                    debug_log(f"Already seen release for {last_name}, {first_name}")
        
        return matches, True
    
    except requests.HTTPError as e:
        report_scraping_error(
            f"HTTP Error {e.response.status_code if e.response else 'Unknown'}",
            f"Failed to fetch 'Released Last 7 Days': {e}"
        )
        return matches, False
    except requests.RequestException as e:
        report_scraping_error(
            "Connection Error",
            f"Failed to connect for 'Released Last 7 Days': {e}"
        )
        return matches, False
    except Exception as e:
        report_scraping_error(
            "Processing Error",
            f"Error processing 'Released Last 7 Days': {e}"
        )
        return matches, False


def run_check(seen: dict) -> None:
    """Run a single check cycle."""
    log("=" * 50)
    log("Starting check cycle...")
    
    _, booked_success = fetch_booked_today(seen)
    _, released_success = fetch_released_last_7_days(seen)
    
    # Reset failure count if both succeeded
    if booked_success and released_success:
        reset_failure_count()
    
    save_seen_bookings(seen)
    
    log("Check cycle complete")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="MCSO PAID (Public Access Inmate Data) Scraper"
    )
    parser.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug output"
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    global DEBUG
    
    args = parse_args()
    DEBUG = args.debug
    
    log(f"MCSO PAID Scraper v{__version__} starting...")
    if DEBUG:
        log("Debug mode enabled")
    
    # Validate configuration
    if not WATCH_NAMES:
        log("ERROR: No names configured to watch. Set WATCH_NAMES in .env")
        sys.exit(1)
    
    log(f"Watching for names: {', '.join(WATCH_NAMES)}")
    
    if not SLACK_WEBHOOK_URL:
        log("WARNING: No Slack webhook configured. Messages will be printed to stdout.")
    else:
        log("Slack webhook configured")
    
    log(f"Poll interval: {POLL_INTERVAL_MINUTES} minutes")
    log(f"Data file: {DATA_FILE}")
    
    # Load previously seen bookings
    seen = load_seen_bookings()
    
    # Run initial check
    run_check(seen)
    
    # Continue polling
    while True:
        log(f"Sleeping for {POLL_INTERVAL_MINUTES} minutes...")
        time.sleep(POLL_INTERVAL_MINUTES * 60)
        run_check(seen)


if __name__ == "__main__":
    main()

