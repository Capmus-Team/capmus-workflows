#!/usr/bin/env python3

import os
import psycopg2
import boto3
import requests
import argparse
import time
import logging
from urllib.parse import urlparse
from dotenv import load_dotenv
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError
from psycopg2.errors import DeadlockDetected
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------
# ENV & LOGGING
# ---------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_URL = os.getenv("SUPABASE_DB_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

S3_PREFIX = "event/meetup/"
CDN_BASE_URL = "https://cdn.capmus.com/event/meetup/"

MAX_WORKERS = 12
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2  # seconds

# =====================================================================
# MAIN CLASS
# =====================================================================

class NonRetryableError(Exception):
    pass


class PhotoSyncManager:

    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.http = requests.Session()
        self.http.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/*"
        })
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    # -----------------------------------------------------------------
    # DB CONNECTION
    # -----------------------------------------------------------------
    def _get_db(self):
        return psycopg2.connect(
            DB_URL,
            application_name="photo_downloader"
        )

    # -----------------------------------------------------------------
    # EXTRACT PHOTO URLS
    # -----------------------------------------------------------------
    def extract_photo_urls(self, data: dict) -> Dict[str, Optional[str]]:
        out = {"featured": None, "keygroup": None}

        f = data.get("featuredEventPhoto")
        if f and f.get("baseUrl") and f.get("id"):
            out["featured"] = f"{f['baseUrl']}{f['id']}"

        g = data.get("group", {})
        k = g.get("keyGroupPhoto")
        if k and k.get("baseUrl") and k.get("id"):
            out["keygroup"] = f"{k['baseUrl']}{k['id']}"

        return out

    # -----------------------------------------------------------------
    # FETCH EVENTS (ROW-LOCKED WORKER PATTERN)
    # -----------------------------------------------------------------
    def fetch_events(self, limit=None):
        conn = self._get_db()
        cur = conn.cursor()

        query = """
            SELECT e.event_instance_id, e.data
            FROM staging_meetup.event e
            JOIN staging_meetup.post_transform p
              ON p.source_api_id = e.event_instance_id
            WHERE p.media IS NULL
            AND (
                 (e.data->'featuredEventPhoto'->>'baseUrl' IS NOT NULL
                  AND e.data->'featuredEventPhoto'->>'id' IS NOT NULL)
              OR
                 (e.data->'group'->'keyGroupPhoto'->>'baseUrl' IS NOT NULL
                  AND e.data->'group'->'keyGroupPhoto'->>'id' IS NOT NULL)
            )
            ORDER BY e.event_instance_id
            FOR UPDATE SKIP LOCKED
        """

        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        rows = cur.fetchall()

        events = [{
            "event_instance_id": str(r[0]),
            "data": r[1]
        } for r in rows]

        cur.close()
        conn.close()

        logger.info(f"Fetched {len(events)} events for processing")
        return events

    # -----------------------------------------------------------------
    # RETRY WRAPPER
    # -----------------------------------------------------------------
    def retry(self, func, *args):
        last_exc = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                return func(*args)
            except NonRetryableError:
                raise
            except Exception as e:
                last_exc = e
                sleep = RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{RETRY_ATTEMPTS} in {sleep}s: {e}")
                time.sleep(sleep)
        raise last_exc

    # -----------------------------------------------------------------
    # S3 HELPERS
    # -----------------------------------------------------------------
    def s3_exists(self, filename: str) -> bool:
        try:
            self.s3_client.head_object(
                Bucket=AWS_BUCKET,
                Key=f"{S3_PREFIX}{filename}"
            )
            return True
        except Exception:
            return False

    def download_image(self, url: str) -> bytes:
        resp = self.http.get(url, timeout=30)
        try:
            resp.raise_for_status()
        except HTTPError as e:
            if resp.status_code == 403:
                raise NonRetryableError(f"403 Forbidden for {url}") from e
            raise
        return resp.content

    def upload_to_s3(self, filename: str, data: bytes, content_type: str):
        self.s3_client.put_object(
            Bucket=AWS_BUCKET,
            Key=f"{S3_PREFIX}{filename}",
            Body=data,
            ContentType=content_type,
            CacheControl="public, max-age=31536000"
        )

    def get_content_type(self, url: str):
        path = urlparse(url).path.lower()
        if path.endswith(".png"):
            return "image/png"
        if path.endswith(".gif"):
            return "image/gif"
        if path.endswith(".webp"):
            return "image/webp"
        return "image/jpeg"

    # -----------------------------------------------------------------
    # PROCESS SINGLE PHOTO
    # -----------------------------------------------------------------
    def process_photo(self, uuid: str, p_type: str, url: str):
        filename = f"{uuid}-{p_type}"

        if self.s3_exists(filename):
            return f"{CDN_BASE_URL}{filename}"

        data = self.retry(self.download_image, url)
        content_type = self.get_content_type(url)
        self.retry(self.upload_to_s3, filename, data, content_type)

        return f"{CDN_BASE_URL}{filename}"

    # -----------------------------------------------------------------
    # BATCH UPDATE WITH DEADLOCK RETRY
    # -----------------------------------------------------------------
    def batch_update(self, updates):
        for attempt in range(RETRY_ATTEMPTS):
            conn = None
            cur = None
            try:
                conn = self._get_db()
                cur = conn.cursor()

                values = [
                    (u["event_instance_id"], u["media_list"])
                    for u in updates
                ]
                execute_values(
                    cur,
                    """
                    UPDATE staging_meetup.post_transform AS p
                    SET media = v.media_list,
                        time_modified = NOW()
                    FROM (VALUES %s) AS v(source_api_id, media_list)
                    WHERE p.source_api_id = v.source_api_id
                      AND p.media IS NULL
                    """,
                    values
                )

                conn.commit()
                return

            except DeadlockDetected:
                if conn:
                    conn.rollback()
                logger.warning(
                    f"Deadlock detected during batch update "
                    f"(attempt {attempt+1}/{RETRY_ATTEMPTS})"
                )
                time.sleep(RETRY_BACKOFF * (attempt + 1))
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

        raise RuntimeError("batch_update failed after repeated deadlocks")

    # -----------------------------------------------------------------
    # MAIN PROCESS
    # -----------------------------------------------------------------
    def process_events(self, limit=None):
        events = self.fetch_events(limit)
        logger.info("Starting photo sync job")

        db_updates = []

        for event in events:
            uuid = event["event_instance_id"]
            photo_urls = self.extract_photo_urls(event["data"])

            futures = {
                self.executor.submit(
                    self.process_photo, uuid, p_type, url
                ): p_type
                for p_type, url in photo_urls.items()
                if url
            }

            results = {}
            for future in as_completed(futures):
                p_type = futures[future]
                try:
                    results[p_type] = future.result()
                except Exception as e:
                    logger.error(f"[{uuid}] {p_type} failed: {e}")

            media_list = []
            if "featured" in results:
                media_list.append(results["featured"])
            if "keygroup" in results:
                media_list.append(results["keygroup"])

            db_updates.append({
                "event_instance_id": uuid,
                "media_list": media_list
            })

            if len(db_updates) >= self.batch_size:
                self.batch_update(db_updates)
                db_updates = []

        if db_updates:
            self.batch_update(db_updates)

        logger.info("Photo sync job completed successfully")


# =====================================================================
# ENTRYPOINT
# =====================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--poll-interval", type=int, default=60)
    parser.add_argument("--max-runs", type=int, default=None)

    args = parser.parse_args()

    sync = PhotoSyncManager(batch_size=args.batch_size)

    if args.continuous:
        runs = 0
        while True:
            sync.process_events(limit=args.limit)
            runs += 1
            if args.max_runs is not None and runs >= args.max_runs:
                break
            time.sleep(args.poll_interval)
    else:
        sync.process_events(limit=args.limit)


if __name__ == "__main__":
    main()
