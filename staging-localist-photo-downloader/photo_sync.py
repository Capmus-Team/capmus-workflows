import os
import psycopg2
import boto3
import requests
import argparse
from urllib.parse import urlparse
from dotenv import load_dotenv
from typing import List, Dict, Optional
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database and AWS configuration
DB_URL = os.getenv('SUPABASE_DB_URL')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
AWS_BUCKET = os.getenv('AWS_BUCKET')

# S3 configuration
S3_PREFIX = 'event/ll/'
CDN_BASE_URL = 'https://cdn.capmus.com/event/ll/'

class PhotoSyncManager:
    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.s3_client = self._initialize_s3_client()
        
    def _initialize_s3_client(self):
        """Initialize S3 client"""
        return boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    
    def _get_db_connection(self):
        """Create database connection"""
        return psycopg2.connect(DB_URL)
    
    def fetch_events_without_aws_link(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Fetch events where aws_photo_link is NULL and photo_url exists in data
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                event_instance_id,
                data,
                source_event_id,
                source_api
            FROM staging_localist.event
            WHERE aws_photo_link IS NULL
                AND data->>'photo_url' IS NOT NULL
                AND data->>'photo_url' != ''
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        events = []
        for row in rows:
            events.append({
                'event_instance_id': str(row[0]),
                'data': row[1],
                'source_event_id': row[2],
                'source_api': row[3]
            })
        
        cursor.close()
        conn.close()
        
        logger.info(f"Fetched {len(events)} events without AWS photo link")
        return events
    
    def check_s3_file_exists(self, uuid: str) -> bool:
        """
        Check if file already exists in S3
        """
        s3_key = f"{S3_PREFIX}{uuid}"
        
        try:
            self.s3_client.head_object(Bucket=AWS_BUCKET, Key=s3_key)
            return True
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking S3 file existence: {e}")
                raise
    
    def download_image(self, url: str) -> Optional[bytes]:
        """
        Download image from URL
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://calendar.cofc.edu/'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Error downloading image from {url}: {e}")
            return None
    
    def get_content_type_from_url(self, url: str) -> str:
        """
        Determine content type from URL extension
        """
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        
        if path.endswith('.jpg') or path.endswith('.jpeg'):
            return 'image/jpeg'
        elif path.endswith('.png'):
            return 'image/png'
        elif path.endswith('.gif'):
            return 'image/gif'
        elif path.endswith('.webp'):
            return 'image/webp'
        else:
            return 'image/jpeg'  # Default to JPEG
    
    def upload_to_s3(self, uuid: str, image_data: bytes, content_type: str) -> bool:
        """
        Upload image to S3
        """
        s3_key = f"{S3_PREFIX}{uuid}"
        
        try:
            self.s3_client.put_object(
                Bucket=AWS_BUCKET,
                Key=s3_key,
                Body=image_data,
                ContentType=content_type,
                CacheControl='public, max-age=31536000'
            )
            logger.info(f"Uploaded image to S3: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return False
    
    def batch_update_aws_links(self, updates: List[Dict]):
        """
        Update aws_photo_link in database in batch
        """
        if not updates:
            return
        
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Use UPDATE with VALUES for batch update
            update_query = """
                UPDATE staging_localist.event AS e
                SET 
                    aws_photo_link = u.aws_photo_link,
                    updated_at = NOW()
                FROM (VALUES %s) AS u(event_instance_id, aws_photo_link)
                WHERE e.event_instance_id = u.event_instance_id::uuid
            """
            
            # Prepare values
            values = ','.join([
                f"('{update['event_instance_id']}', '{update['aws_photo_link']}')"
                for update in updates
            ])
            
            full_query = update_query.replace('%s', values)
            cursor.execute(full_query)
            conn.commit()
            
            logger.info(f"Batch updated {len(updates)} records in database")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error in batch update: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def process_events(self, limit: Optional[int] = None):
        """
        Main processing function
        """
        events = self.fetch_events_without_aws_link(limit)
        
        if not events:
            logger.info("No events to process")
            return
        
        updates = []
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        for event in events:
            event_uuid = event['event_instance_id']
            photo_url = event['data'].get('photo_url')
            
            if not photo_url:
                logger.warning(f"Event {event_uuid}: photo_url is null or empty, skipping")
                skipped_count += 1
                continue
            
            # Check if already exists in S3
            if self.check_s3_file_exists(event_uuid):
                logger.info(f"Event {event_uuid}: Image already exists in S3, updating database only")
                cdn_url = f"{CDN_BASE_URL}{event_uuid}"
                updates.append({
                    'event_instance_id': event_uuid,
                    'aws_photo_link': cdn_url
                })
                processed_count += 1
                
                # Batch update when we reach batch_size
                if len(updates) >= self.batch_size:
                    self.batch_update_aws_links(updates)
                    updates = []
                
                continue
            
            # Download image
            logger.info(f"Event {event_uuid}: Downloading image from {photo_url}")
            image_data = self.download_image(photo_url)
            
            if not image_data:
                logger.error(f"Event {event_uuid}: Failed to download image")
                error_count += 1
                continue
            
            # Upload to S3
            content_type = self.get_content_type_from_url(photo_url)
            if self.upload_to_s3(event_uuid, image_data, content_type):
                cdn_url = f"{CDN_BASE_URL}{event_uuid}"
                updates.append({
                    'event_instance_id': event_uuid,
                    'aws_photo_link': cdn_url
                })
                processed_count += 1
                
                # Batch update when we reach batch_size
                if len(updates) >= self.batch_size:
                    self.batch_update_aws_links(updates)
                    updates = []
            else:
                error_count += 1
        
        # Update remaining records
        if updates:
            self.batch_update_aws_links(updates)
        
        logger.info(f"""
        Processing complete:
        - Total events: {len(events)}
        - Successfully processed: {processed_count}
        - Skipped: {skipped_count}
        - Errors: {error_count}
        """)


def main():
    parser = argparse.ArgumentParser(
        description='Sync event photos from JSONB data to AWS S3'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit the number of events to process'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for database updates (default: 100)'
    )
    
    args = parser.parse_args()
    
    logger.info("Starting photo sync process...")
    
    sync_manager = PhotoSyncManager(batch_size=args.batch_size)
    sync_manager.process_events(limit=args.limit)
    
    logger.info("Photo sync process completed")


if __name__ == '__main__':
    main()
