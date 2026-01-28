#!/usr/bin/env python3

import os
import logging
import requests
import argparse
import boto3
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
from dotenv import load_dotenv
from typing import Union

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

load_dotenv()

DB_URL = os.getenv("DB_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

if not DB_URL:
    raise RuntimeError("Missing DB_URL environment variable")

S3_PREFIX = "event/ticketmaster/"
CDN_BASE_URL = "https://cdn.capmus.com/event/ticketmaster/"

ADVISORY_LOCK_ID = 3001
BATCH_SIZE = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# S3 CLIENT
# -------------------------------------------------------------------

class S3Client:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    def upload(self, key: str, data: bytes, content_type: str):
        self.client.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl="public, max-age=31536000"
        )

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def download_image(url: str) -> Union[bytes, str, None]:
    """
    Returns:
      - bytes        → success
      - "not_found"  → 404 / 410
      - None         → transient failure
    """
    try:
        res = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*"},
            timeout=20
        )

        if res.status_code in (404, 410):
            logger.warning(f"Image not found ({res.status_code}): {url}")
            return "not_found"

        res.raise_for_status()
        return res.content

    except requests.RequestException as e:
        logger.warning(f"Transient image error: {url} → {e}")
        return None

def content_type_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".png"):
        return "image/png"
    if path.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ticketmaster image downloader")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--allow-concurrent",
        action="store_true",
        help="Allow concurrent workers by skipping advisory lock"
    )
    args = parser.parse_args()

    s3 = S3Client()

    conn = psycopg2.connect(
        DB_URL,
        application_name="ticketmaster_image_downloader"
    )
    conn.autocommit = False

    completed = failed = not_found = 0
    use_advisory_lock = not args.allow_concurrent

    try:
        # ------------------------------------------------------------
        # Advisory lock
        # ------------------------------------------------------------
        if use_advisory_lock:
            with conn.cursor() as cur:
                logger.info("Acquiring advisory lock...")
                cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
        else:
            logger.info("Skipping advisory lock (concurrent workers enabled)")

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            query = """
                SELECT id, data
                FROM staging_ticketmaster.event
                WHERE aws_photo_downloaded NOT IN ('completed', 'not_found')
                  AND data->'images' IS NOT NULL
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            """

            limit = args.limit or BATCH_SIZE
            cur.execute(query, (limit,))
            rows = cur.fetchall()

            logger.info(f"Fetched {len(rows)} events to process")

            updates = []

            for row in rows:
                event_id = row["id"]
                images = row["data"].get("images", [])

                if not images or not images[0].get("url"):
                    updates.append((None, "not_found", event_id))
                    not_found += 1
                    continue

                url = images[0]["url"]
                result = download_image(url)

                if result == "not_found":
                    updates.append((None, "not_found", event_id))
                    not_found += 1
                    continue

                if result is None:
                    updates.append((None, "failed", event_id))
                    failed += 1
                    continue

                s3.upload(
                    f"{S3_PREFIX}{event_id}",
                    result,
                    content_type_from_url(url)
                )

                updates.append((
                    f"{CDN_BASE_URL}{event_id}",
                    "completed",
                    event_id
                ))
                completed += 1

            if updates:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    UPDATE staging_ticketmaster.event
                    SET aws_photo_link = %s,
                        aws_photo_downloaded = %s
                    WHERE id = %s
                    """,
                    updates
                )

            conn.commit()

            logger.info(
                f"Run summary → completed={completed}, "
                f"not_found={not_found}, failed={failed}"
            )

    except Exception as e:
        conn.rollback()
        logger.error(f"Job failed: {e}")
        raise

    finally:
        if use_advisory_lock:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
        conn.close()

# -------------------------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()
