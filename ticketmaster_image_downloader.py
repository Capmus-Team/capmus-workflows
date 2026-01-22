#!/usr/bin/env python3

import os
import time
import logging
import requests
import argparse
import boto3
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
from dotenv import load_dotenv
from psycopg2.errors import DeadlockDetected

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

load_dotenv()

DB_URL = os.getenv("DB_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

S3_PREFIX = "event/ticketmaster/"
CDN_BASE_URL = "https://cdn.capmus.com/event/ticketmaster/"
ADVISORY_LOCK_ID = 3001   # Ticketmaster image downloader

BATCH_SIZE = 200
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# S3
# -------------------------------------------------------------------

class S3Client:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=AWS_BUCKET, Key=key)
            return True
        except Exception:
            return False

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

def download_image(url: str) -> bytes:
    res = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "image/*"},
        timeout=20
    )
    res.raise_for_status()
    return res.content

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    s3 = S3Client()

    conn = psycopg2.connect(DB_URL, application_name="ticketmaster_image_downloader")
    conn.autocommit = False

    with conn.cursor() as cur:
        logger.info("Acquiring advisory lock...")
        cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            query = """
                SELECT id, data
                FROM staging_ticketmaster.event
                WHERE aws_photo_downloaded IS DISTINCT FROM 'completed'
                AND data->'images' IS NOT NULL
                ORDER BY id
                FOR UPDATE SKIP LOCKED
            """

            if args.limit:
                query += f" LIMIT {args.limit}"
            else:
                query += f" LIMIT {BATCH_SIZE}"

            cur.execute(query)
            rows = cur.fetchall()

            logger.info(f"Fetched {len(rows)} events to process")

            updates = []

            for row in rows:
                event_id = row["id"]
                images = row["data"].get("images", [])
                if not images:
                    continue

                url = images[0].get("url")
                if not url:
                    continue

                s3_key = f"{S3_PREFIX}{event_id}"

                if not s3.exists(s3_key):
                    img = download_image(url)
                    s3.upload(
                        s3_key,
                        img,
                        content_type_from_url(url)
                    )

                updates.append((
                    f"{CDN_BASE_URL}{event_id}",
                    "completed",
                    event_id
                ))

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
            logger.info("Ticketmaster image sync completed successfully")

    except DeadlockDetected:
        conn.rollback()
        logger.warning("Deadlock detected, retrying later")

    except Exception as e:
        conn.rollback()
        logger.error(f"Job failed: {e}")
        raise

    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
        conn.close()

# -------------------------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()
