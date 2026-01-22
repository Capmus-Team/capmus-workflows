#!/usr/bin/env python3

import os
import logging
import requests
import boto3
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
from dotenv import load_dotenv
from typing import Optional

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

load_dotenv()

DB_URL = os.getenv("SUPABASE_DB_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

if not DB_URL:
    raise RuntimeError("Missing SUPABASE_DB_URL")

S3_PREFIX = "event/ll/"
CDN_BASE_URL = "https://cdn.capmus.com/event/ll/"

ADVISORY_LOCK_ID = 42002
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

def download_image(url: str) -> Optional[bytes]:
    try:
        res = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30
        )
        res.raise_for_status()
        return res.content
    except Exception as e:
        logger.warning(f"Image download failed: {url} → {e}")
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
    s3 = S3Client()

    conn = psycopg2.connect(
        DB_URL,
        application_name="localist_photo_sync"
    )
    conn.autocommit = False

    try:
        # ------------------------------------------------------------
        # Advisory lock (cron safety)
        # ------------------------------------------------------------
        with conn.cursor() as cur:
            logger.info("Acquiring advisory lock...")
            cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # --------------------------------------------------------
            # Fetch rows safely
            # --------------------------------------------------------
            cur.execute("""
                SELECT event_instance_id, data
                FROM staging_localist.event
                WHERE aws_photo_link IS NULL
                  AND data->>'photo_url' IS NOT NULL
                  AND data->>'photo_url' != ''
                ORDER BY event_instance_id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            """, (BATCH_SIZE,))

            rows = cur.fetchall()
            logger.info(f"Fetched {len(rows)} Localist events to process")

            updates = []

            for row in rows:
                event_id = row["event_instance_id"]
                photo_url = row["data"]["photo_url"]

                image_data = download_image(photo_url)
                if not image_data:
                    continue  # skip silently for now

                s3.upload(
                    f"{S3_PREFIX}{event_id}",
                    image_data,
                    content_type_from_url(photo_url)
                )

                updates.append((
                    f"{CDN_BASE_URL}{event_id}",
                    event_id
                ))

            if updates:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    UPDATE staging_localist.event
                    SET aws_photo_link = %s,
                        updated_at = now()
                    WHERE event_instance_id = %s
                    """,
                    updates
                )

        conn.commit()
        logger.info("Localist photo sync completed successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Localist photo sync failed: {e}")
        raise

    finally:
        # ------------------------------------------------------------
        # Always release advisory lock safely
        # ------------------------------------------------------------
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
        except Exception:
            pass
        conn.close()

# -------------------------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    main()
