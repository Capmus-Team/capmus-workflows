import os
import psycopg2
import boto3
import requests
import argparse
import time
import logging
from urllib.parse import urlparse
from dotenv import load_dotenv
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more detail
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ENV variables
DB_URL = os.getenv("SUPABASE_DB_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

# S3/CDN
S3_PREFIX = "event/meetup/"
CDN_BASE_URL = "https://cdn.capmus.com/event/meetup/"

# Parallel workers
MAX_WORKERS = 12

# Retry settings
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2  # seconds


# =====================================================================
# MAIN CLASS
# =====================================================================

class PhotoSyncManager:

    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    # -----------------------------------------------------------------
    # DB connection
    # -----------------------------------------------------------------
    def _get_db(self):
        return psycopg2.connect(DB_URL)

    # -----------------------------------------------------------------
    # Extract featured/keygroup photo URLs
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
    # FETCH ONLY RECORDS WHERE media IS NULL OR EMPTY
    # -----------------------------------------------------------------
    def fetch_events(self, limit=None):
        conn = self._get_db()
        cur = conn.cursor()

        query = """
            SELECT e.event_instance_id, e.data
            FROM staging_meetup.event e
            JOIN staging_meetup.post_transform p
              ON p.source_api_id = e.event_instance_id
            WHERE (p.media IS NULL OR p.media = '{}'::text[])
            AND (
                 (e.data->'featuredEventPhoto'->>'baseUrl' IS NOT NULL
                  AND e.data->'featuredEventPhoto'->>'id' IS NOT NULL)
              OR
                 (e.data->'group'->'keyGroupPhoto'->>'baseUrl' IS NOT NULL
                  AND e.data->'group'->'keyGroupPhoto'->>'id' IS NOT NULL)
            )
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

        logger.info(f"Fetched {len(events)} unprocessed events.")
        return events

    # -----------------------------------------------------------------
    # Retry wrapper
    # -----------------------------------------------------------------
    def retry(self, func, *args, **kwargs):
        last_exc = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exc = e
                sleep_time = RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Retry {attempt+1}/{RETRY_ATTEMPTS} after {sleep_time}s: {str(e)}")
                time.sleep(sleep_time)
        raise last_exc

    # -----------------------------------------------------------------
    # Check if photo already exists in S3
    # -----------------------------------------------------------------
    def s3_exists(self, filename: str) -> bool:
        try:
            self.s3_client.head_object(
                Bucket=AWS_BUCKET,
                Key=f"{S3_PREFIX}{filename}"
            )
            return True
        except:
            return False

    # -----------------------------------------------------------------
    # Download image
    # -----------------------------------------------------------------
    def download_image(self, url: str) -> bytes:
        logger.debug(f"Downloading image → {url}")
        resp = requests.get(url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/*"
        })
        resp.raise_for_status()
        return resp.content

    # -----------------------------------------------------------------
    # Upload to S3
    # -----------------------------------------------------------------
    def upload_to_s3(self, filename: str, data: bytes, content_type: str):
        logger.debug(f"S3 upload BEGIN → {filename}")
        self.s3_client.put_object(
            Bucket=AWS_BUCKET,
            Key=f"{S3_PREFIX}{filename}",
            Body=data,
            ContentType=content_type,
            CacheControl="public, max-age=31536000"
        )
        logger.debug(f"S3 upload COMPLETE → {filename}")

    # -----------------------------------------------------------------
    # Guess content type
    # -----------------------------------------------------------------
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
    # Process a single photo
    # -----------------------------------------------------------------
    def process_photo(self, uuid: str, p_type: str, url: str):
        filename = f"{uuid}-{p_type}"

        logger.info(f"[{uuid}] Starting {p_type} → {url}")

        # Skip if exists
        if self.s3_exists(filename):
            cdn = f"{CDN_BASE_URL}{filename}"
            logger.info(f"[{uuid}] {p_type} already on S3 → {cdn}")
            return cdn

        # Download
        logger.info(f"[{uuid}] Downloading {p_type}...")
        data = self.retry(self.download_image, url)
        logger.info(f"[{uuid}] Downloaded {p_type} ({len(data)} bytes)")

        # Upload
        logger.info(f"[{uuid}] Uploading {p_type} to S3 as {filename}...")
        content_type = self.get_content_type(url)
        self.retry(self.upload_to_s3, filename, data, content_type)
        logger.info(f"[{uuid}] Uploaded {p_type} to S3")

        cdn_url = f"{CDN_BASE_URL}{filename}"
        logger.info(f"[{uuid}] {p_type} CDN URL → {cdn_url}")

        return cdn_url

    # -----------------------------------------------------------------
    # MAIN PROCESSING LOOP
    # -----------------------------------------------------------------
    def process_events(self, limit=None):
        events = self.fetch_events(limit)

        logger.info("Starting photo sync job...")

        db_updates = []

        for event in events:
            uuid = event["event_instance_id"]
            logger.info(f"Processing event {uuid}")

            # Extract image URLs
            photo_urls = self.extract_photo_urls(event["data"])

            futures = {}
            results = {}

            # Parallel workers
            for p_type, url in photo_urls.items():
                if url:
                    futures[self.executor.submit(
                        self.process_photo, uuid, p_type, url
                    )] = p_type

            # Collect results
            for future in as_completed(futures):
                p_type = futures[future]
                try:
                    results[p_type] = future.result()
                except Exception as e:
                    logger.error(f"[{uuid}] FAILED {p_type}: {str(e)}")

            # Build media array (featured first)
            media_list = []
            if "featured" in results:
                media_list.append(results["featured"])
            if "keygroup" in results:
                media_list.append(results["keygroup"])

            # Queue DB update (no need to compare — all are unprocessed)
            db_updates.append({
                "event_instance_id": uuid,
                "media_list": media_list
            })

            # Batch flush
            if len(db_updates) >= self.batch_size:
                self.batch_update(db_updates)
                db_updates = []

        # Final batch
        if db_updates:
            self.batch_update(db_updates)

        logger.info("Photo sync job completed.")

    # -----------------------------------------------------------------
    # Update media[] in post_transform
    # -----------------------------------------------------------------
    def batch_update(self, updates):
        conn = self._get_db()
        cur = conn.cursor()

        for u in updates:
            logger.info(f"Updating DB media[] for {u['event_instance_id']} → {u['media_list']}")
            cur.execute("""
                UPDATE staging_meetup.post_transform
                SET media = %s,
                    time_modified = NOW()
                WHERE source_api_id = %s
            """, (
                u["media_list"],
                u["event_instance_id"]
            ))

        conn.commit()
        cur.close()
        conn.close()


# =====================================================================
# SCRIPT ENTRYPOINT
# =====================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)

    args = parser.parse_args()

    sync = PhotoSyncManager(batch_size=args.batch_size)
    sync.process_events(limit=args.limit)


if __name__ == "__main__":
    main()