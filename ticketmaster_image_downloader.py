import os
import time
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
import boto3
import argparse
import psycopg2
import psycopg2.extras
from typing import Optional

# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------
load_dotenv()

DB_URL = os.getenv("DB_URL")   # NEW
if not DB_URL:
    raise RuntimeError("Missing DB_URL environment variable.")

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
AWS_BUCKET = os.getenv('AWS_BUCKET')

S3_PREFIX = 'event/ticketmaster/'
CDN_BASE_URL = 'https://cdn.capmus.com/event/ticketmaster/'

# Logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ticketmaster_loader.log"),
        logging.StreamHandler()
    ]
)

# -------------------------------------------------------------------
# S3 + IMAGE HANDLER
# -------------------------------------------------------------------
class PhotoSyncManager:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    def upload_to_s3(self, uuid: str, image_data: bytes, content_type: str) -> bool:
        s3_key = f"{S3_PREFIX}{uuid}"
        try:
            self.s3_client.put_object(
                Bucket=AWS_BUCKET,
                Key=s3_key,
                Body=image_data,
                ContentType=content_type,
                CacheControl='public, max-age=31536000'
            )
            logging.info(f"Uploaded image to S3: {s3_key}")
            return True
        except Exception as e:
            logging.error(f"Error uploading to S3: {e}")
            return False

    def check_s3_file_exists(self, uuid: str) -> bool:
        s3_key = f"{S3_PREFIX}{uuid}"
        try:
            self.s3_client.head_object(Bucket=AWS_BUCKET, Key=s3_key)
            return True
        except self.s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            logging.error(f"Error checking S3 existence: {e}")
            raise

    def download_image(self, url: str) -> Optional[bytes]:
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'image/*'
        }
        try:
            res = requests.get(url, headers=headers, timeout=20)
            res.raise_for_status()
            return res.content
        except Exception as e:
            logging.error(f"Failed downloading image from {url}: {e}")
            return None

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():

    parser = argparse.ArgumentParser(description="Sync event photos to S3")
    parser.add_argument(
        "--skip-exists",
        action="store_true",
        help="Skip items already uploaded to S3"
    )
    args = parser.parse_args()

    sync = PhotoSyncManager()

    # ------------------------------
    # Connect to Postgres via psycopg2
    # ------------------------------
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # ------------------------------
    # Fetch event rows
    # ------------------------------
    if args.skip_exists:
        query = """
            SELECT id, data
            FROM staging_ticketmaster.event
            WHERE aws_photo_link IS NULL OR aws_photo_downloaded = 'pending'
        """
    else:
        query = """
            SELECT id, data
            FROM staging_ticketmaster.event
        """

    cursor.execute(query)
    rows = cursor.fetchall()

    logging.info(f"Fetched {len(rows)} events.")

    for row in rows:
        id = row["id"]
        data = row.get("data", {})
        images = data.get("images", [])

        url = images[0].get("url") if images else None
        if not url:
            logging.error(f"Skipping row {id}, invalid or missing image URL.")
            continue

        # Optional S3 existence check
        if args.skip_exists and sync.check_s3_file_exists(id):
            new_link = f"{CDN_BASE_URL}{id}"
            cursor.execute(
                """
                UPDATE staging_ticketmaster.event
                SET aws_photo_link = %s, aws_photo_downloaded = 'completed'
                WHERE id = %s
                """,
                (new_link, id)
            )
            conn.commit()
            logging.info(f"S3 already has image for {id}, skipping...")
            continue

        # Download image
        img = sync.download_image(url)
        if not img:
            logging.error(f"Download failed for {id}")
            continue

        # Upload
        if sync.upload_to_s3(id, img, "image/jpeg"):
            new_link = f"{CDN_BASE_URL}{id}"
            cursor.execute(
                """
                UPDATE staging_ticketmaster.event
                SET aws_photo_link = %s, aws_photo_downloaded = 'completed'
                WHERE id = %s
                """,
                (new_link, id)
            )
            conn.commit()
            logging.info(f"Updated DB for {id}")

    cursor.close()
    conn.close()


# -------------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------------
if __name__ == "__main__":
    main()