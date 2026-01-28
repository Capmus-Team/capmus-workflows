#!/usr/bin/env python3
"""
ticketmaster_to_supabase.py
Fetch Ticketmaster events and insert into Supabase table: staging_ticketmaster.event
"""

import os
import time
import logging
import requests
import psycopg2
import psycopg2.extras
import json
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys
# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
load_dotenv()

DB_URL = os.getenv("DB_URL")

if not DB_URL:
    raise RuntimeError("Missing DB_URL environment variable.")

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(DB_URL)

API_KEY = "lxtr0oQ81Ke8lpKxVCfklmSZ9iFoSlGL"
API_URL = "https://app.ticketmaster.com/discovery/v2/events.json"

#COUNTRY_CODE = "US"
PAGE_START = 0
PAGE_END = 30
PAGE_SIZE = 200
DELAY_BETWEEN_CALLS = 1.0  # seconds

TABLE_NAME = "event"

supported_countries = [
    "US", "CA", "IE", "GB", "AU", "NZ", "MX", "AE", "AT", "BE",
    "DE", "DK", "ES", "FI", "NL", "NO", "PL", "SE", "CH", "CZ",
    "IT", "FR", "ZA", "TR", "BR", "CL", "PE"
]

# -------------------------------------------------------------------
# LOGGER SETUP
# -------------------------------------------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler("ticketmaster_loader.log"),
#         logging.StreamHandler()
#     ]
# )
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ticketmaster_loader.log", encoding="utf-8"),
        logging.StreamHandler(stream=sys.stdout)
    ]
)

# -------------------------------------------------------------------
# FETCH DATA FUNCTION
# -------------------------------------------------------------------
def fetch_events(page: int,country_code: str) -> dict | None:
    """Fetch one page of events from Ticketmaster API."""
    params = {
        "countryCode": country_code,
        "apikey": API_KEY,
        "page": page,
        "size": PAGE_SIZE
    }

    try:
        logging.info(f"Fetching events from Ticketmaster API, page {page}...")
        resp = requests.get(API_URL, params=params, timeout=20)
        resp.raise_for_status()
        response_json = resp.json()
        logging.info(f"Fetched {len(response_json.get('_embedded', {}).get('events', []))} events from page {page}.")
        return response_json
    except Exception as e:
        logging.error(f"Error fetching page {page}: {e}")
        return None

# -------------------------------------------------------------------
# UPSERT TO DATABASE
# -------------------------------------------------------------------
def upsert_event_data(cur, event_id: str, data: dict):
    """Insert or update one event row into PostgreSQL."""
    now = datetime.now(timezone.utc)
    
    cur.execute("""
        INSERT INTO staging_ticketmaster.event (event_id, data, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (event_id)
        DO UPDATE SET
            data = EXCLUDED.data,
            updated_at = EXCLUDED.updated_at
    """, (event_id, json.dumps(data), now))

def get_university_alpha_codes(cur, supported_countries):
    # Turn list into SQL string: 'US','CA','IE',...
    country_list = ",".join(f"'{c}'" for c in supported_countries)

    query = f"""
        SELECT alpha_two_code
        FROM public.university_new
        WHERE alpha_two_code IN ({country_list})
        GROUP BY alpha_two_code;
    """

    cur.execute(query)
    return cur.fetchall()   # return results

def try_lock_country(cur, country_code: str) -> bool:
    cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (country_code,))
    row = cur.fetchone()
    return bool(row[0]) if row else False

def unlock_country(cur, country_code: str) -> None:
    cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (country_code,))

# -------------------------------------------------------------------
# MAIN PROCESS
# -------------------------------------------------------------------
def main():
    total_added = 0

    parser = argparse.ArgumentParser(
        description="Fetch Ticketmaster events and upsert into staging_ticketmaster.event"
    )
    parser.add_argument(
        "--skip-locked",
        action="store_true",
        help="Skip countries locked by another concurrent run"
    )
    args = parser.parse_args()

    conn = None
    try:
        conn = get_db_connection()
        logging.info("Starting Ticketmaster -> PostgreSQL load...")

        supported_country_codes = get_university_alpha_codes(conn.cursor(), supported_countries)

        with conn.cursor() as lock_cur:
            for country_record in supported_country_codes:
                country_code = country_record[0]
                locked = True
                if args.skip_locked:
                    locked = try_lock_country(lock_cur, country_code)
                    if not locked:
                        logging.info(
                            f"Skipping country {country_code} (locked by another run)"
                        )
                        continue

                try:
                    for page in range(PAGE_START, PAGE_END + 1):
                        logging.info(f"Fetching page {page} of {country_code}...")
                        data = fetch_events(page, country_code) # returns JSON
                        if not data:
                            break

                        #logging.info(f"fetched data for page {data.__len__()}, processing...")
                        # If no events key, skip
                        events = data.get("_embedded", {}).get("events", [])
                        if not events:
                            logging.warning(f"No events found on page {page}.")
                            break

                        # Insert each event in a batch transaction
                        try:
                            with conn.cursor() as cur:
                                logging.info(f"Inserting {len(events)} events from page {page}...")
                                for event in events:
                                    event_id = event.get("id")
                                    if not event_id:
                                        continue
                                    upsert_event_data(cur, event_id, event)
                                    total_added += 1

                                    # Log progress every 50 events
                                    if total_added % 50 == 0:
                                        logging.info(f"  Progress: {total_added} events inserted so far...")

                            conn.commit()
                            logging.info(f"Page {page} committed ({len(events)} events). Total so far: {total_added}")
                        except Exception as e:
                            conn.rollback()
                            logging.error(f"Error processing page {page}: {e}")
                            continue

                        # Be polite to API
                        time.sleep(DELAY_BETWEEN_CALLS)
                finally:
                    if args.skip_locked and locked:
                        unlock_country(lock_cur, country_code)

        logging.info(f"Finished. Total rows processed: {total_added}")

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")
# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
