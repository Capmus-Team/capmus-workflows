import os
import psycopg2
import boto3
import requests
import argparse
import time
import logging
from urllib.parse import urlparse
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# ENV + LOGGING
# ============================================================

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

S3_PREFIX = "event/seatgeek/"
CDN_BASE_URL = "https://cdn.capmus.com/event/seatgeek/"

MAX_WORKERS = 12
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2


# ============================================================
# MAIN CLASS
# ============================================================

class SeatGeekPhotoSync:

    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    # --------------------------------------------------------
    def _db(self):
        return psycopg2.connect(DB_URL)

    # --------------------------------------------------------
    # SAFE IMAGE COLLECTOR
    # --------------------------------------------------------
    def _safe_add(self, url, seen, out):
        if url and isinstance(url, str) and url not in seen:
            out.append(url)
            seen.add(url)

    # --------------------------------------------------------
    # Extract ALL images (performers + venue)
    # --------------------------------------------------------
    def extract_images(self, data: dict):
        images = []
        seen = set()

        # ==========================
        # PERFORMER IMAGES
        # ==========================
        performers = data.get("performers", [])
        primary = [p for p in performers if p.get("primary")]
        others = [p for p in performers if not p.get("primary")]
        ordered = primary + others

        for p in ordered:
            self._safe_add(
                p.get("images", {}).get("huge"),
                seen, images
            )
            self._safe_add(
                p.get("image"),
                seen, images
            )

        # ==========================
        # VENUE IMAGES (defensive)
        # ==========================
        venue = data.get("venue", {})

        # Common SeatGeek venue image patterns (future-proof)
        self._safe_add(venue.get("image"), seen, images)

        images_block = venue.get("images", {})
        if isinstance(images_block, dict):
            for v in images_block.values():
                self._safe_add(v, seen, images)

        passes = venue.get("passes", [])
        for p in passes:
            self._safe_add(p.get("image"), seen, images)

        return images

    # --------------------------------------------------------
    # Fetch unprocessed rows
    # --------------------------------------------------------
    def fetch_events(self, limit=None):
        conn = self._db()
        cur = conn.cursor()

        query = """
            SELECT e.event_instance_id, e.data
            FROM staging_seatgeek.event e
            JOIN staging_seatgeek.post_transform p
              ON p.source_api_id = e.event_instance_id
            WHERE (p.media IS NULL OR p.media = '{}'::text[])
        """

        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        logger.info(f"Fetched {len(rows)} SeatGeek events")
        return rows

    # --------------------------------------------------------
    def retry(self, fn, *args):
        last = None
        for i in range(RETRY_ATTEMPTS):
            try:
                return fn(*args)
            except Exception as e:
                last = e
                wait = RETRY_BACKOFF * (i + 1)
                logger.warning(f"Retry {i+1}/{RETRY_ATTEMPTS} after {wait}s: {e}")
                time.sleep(wait)
        raise last

    # --------------------------------------------------------
    def s3_exists(self, key):
        try:
            self.s3.head_object(Bucket=AWS_BUCKET, Key=key)
            return True
        except:
            return False

    # --------------------------------------------------------
    def download(self, url):
        r = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        r.raise_for_status()
        return r.content

    # --------------------------------------------------------
    def content_type(self, url):
        path = urlparse(url).path.lower()
        if path.endswith(".png"):
            return "image/png"
        if path.endswith(".gif"):
            return "image/gif"
        if path.endswith(".webp"):
            return "image/webp"
        return "image/jpeg"

    # --------------------------------------------------------
    def upload(self, filename, data, ctype):
        self.s3.put_object(
            Bucket=AWS_BUCKET,
            Key=f"{S3_PREFIX}{filename}",
            Body=data,
            ContentType=ctype,
            CacheControl="public, max-age=31536000"
        )

    # --------------------------------------------------------
    def process_image(self, event_id, idx, url):
        filename = f"{event_id}-{idx}"
        key = f"{S3_PREFIX}{filename}"

        if self.s3_exists(key):
            return f"{CDN_BASE_URL}{filename}"

        img = self.retry(self.download, url)
        ctype = self.content_type(url)
        self.retry(self.upload, filename, img, ctype)

        return f"{CDN_BASE_URL}{filename}"

    # --------------------------------------------------------
    def batch_update(self, updates):
        conn = self._db()
        cur = conn.cursor()

        for u in updates:
            logger.info(f"Updating media for {u['id']} → {len(u['media'])} images")
            cur.execute("""
                UPDATE staging_seatgeek.post_transform
                SET media = %s,
                    time_modified = NOW()
                WHERE source_api_id = %s
            """, (u["media"], u["id"]))

        conn.commit()
        cur.close()
        conn.close()

    # --------------------------------------------------------
    def run(self, limit=None):
        rows = self.fetch_events(limit)
        updates = []

        for event_id, data in rows:
            logger.info(f"Processing {event_id}")

            urls = self.extract_images(data)
            if not urls:
                continue

            futures = {}
            results = []

            for i, url in enumerate(urls):
                futures[self.executor.submit(
                    self.process_image, event_id, i, url
                )] = i

            for f in as_completed(futures):
                try:
                    results.append(f.result())
                except Exception as e:
                    logger.error(f"{event_id} image failed: {e}")

            if results:
                updates.append({
                    "id": event_id,
                    "media": results
                })

            if len(updates) >= self.batch_size:
                self.batch_update(updates)
                updates = []

        if updates:
            self.batch_update(updates)

        logger.info("SeatGeek photo sync completed.")


# ============================================================
# ENTRYPOINT
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()

    SeatGeekPhotoSync(
        batch_size=args.batch_size
    ).run(limit=args.limit)


if __name__ == "__main__":
    main()
