#!/usr/bin/env python3

import os
import logging
import requests
import boto3
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
from dotenv import load_dotenv
from typing import Union

load_dotenv()

DB_URL = os.getenv("SUPABASE_DB_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

S3_PREFIX = "event/ll/"
CDN_BASE_URL = "https://cdn.capmus.com/event/ll/"
ADVISORY_LOCK_ID = 42002
BATCH_SIZE = 200

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    def upload(self, key, data, content_type):
        self.client.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl="public, max-age=31536000"
        )

def download_image(url: str) -> Union[bytes, str, None]:
    try:
        res = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if res.status_code in (404, 410):
            return "not_found"
        res.raise_for_status()
        return res.content
    except Exception:
        return None

def content_type(url: str) -> str:
    p = urlparse(url).path.lower()
    if p.endswith(".png"):
        return "image/png"
    if p.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"

def main():
    s3 = S3Client()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT event_instance_id, data
                FROM staging_localist.event
                WHERE aws_photo_downloaded IS DISTINCT FROM 'completed'
                  AND aws_photo_downloaded IS DISTINCT FROM 'not_found'
                  AND data->>'photo_url' IS NOT NULL
                ORDER BY event_instance_id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            """, (BATCH_SIZE,))

            rows = cur.fetchall()
            updates = []

            for r in rows:
                eid = r["event_instance_id"]
                url = r["data"]["photo_url"]

                result = download_image(url)

                if result == "not_found":
                    updates.append((None, "not_found", eid))
                    continue

                if result is None:
                    updates.append((None, "failed", eid))
                    continue

                s3.upload(f"{S3_PREFIX}{eid}", result, content_type(url))
                updates.append((f"{CDN_BASE_URL}{eid}", "completed", eid))

            psycopg2.extras.execute_batch(
                cur,
                """
                UPDATE staging_localist.event
                SET aws_photo_link = %s,
                    aws_photo_downloaded = %s
                WHERE event_instance_id = %s
                """,
                updates
            )

        conn.commit()
        logger.info(f"Processed {len(updates)} Localist images")

    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
        conn.close()

if __name__ == "__main__":
    main()
