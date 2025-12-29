#!/usr/bin/env python3
"""
SeatGeek Events Fetcher
Fetches upcoming events from SeatGeek API and stores them in PostgreSQL database.
"""

import os
import sys
import json
import logging
import time
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dotenv import load_dotenv
import requests
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values, Json


# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('seatgeek_fetcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SeatGeekConfig:
    """Configuration for SeatGeek API"""
    def __init__(self):
        self.client_id: str = os.getenv('SEATGEEK_CLIENT_ID', '')
        self.client_secret: str = os.getenv('SEATGEEK_CLIENT_SECRET', '')
        self.base_url: str = 'https://api.seatgeek.com/2'
        self.rate_limit: float = float(os.getenv('API_RATE_LIMIT', '5'))

        if not self.client_id:
            raise ValueError("SEATGEEK_CLIENT_ID is required in .env file")
        if not self.client_secret:
            raise ValueError("SEATGEEK_CLIENT_SECRET is required in .env file")


class DatabaseConfig:
    """Configuration for PostgreSQL/Supabase database"""
    def __init__(self):
        # First try to use DATABASE_URL (Supabase connection string)
        self.database_url: Optional[str] = os.getenv('DATABASE_URL')

        # Fallback to individual parameters if DATABASE_URL not provided
        if not self.database_url:
            self.host: str = os.getenv('DB_HOST', 'localhost')
            self.port: int = int(os.getenv('DB_PORT', '5432'))
            self.database: str = os.getenv('DB_NAME', '')
            self.user: str = os.getenv('DB_USER', '')
            self.password: str = os.getenv('DB_PASSWORD', '')

            if not all([self.database, self.user, self.password]):
                raise ValueError("Either DATABASE_URL or individual database credentials (DB_NAME, DB_USER, DB_PASSWORD) are required in .env file")
        else:
            # Parse connection URL for logging purposes
            self.host = 'supabase'
            self.port = 5432
            self.database = 'postgres'
            self.user = 'postgres'
            self.password = '***'


class SeatGeekFetcher:
    """Handles fetching events from SeatGeek API"""

    def __init__(self, config: SeatGeekConfig):
        self.config = config
        self.session = requests.Session()
        self.last_request_time: float = 0.0

    def _rate_limit(self) -> None:
        """Implement rate limiting between requests"""
        if self.config.rate_limit > 0:
            elapsed = time.time() - self.last_request_time
            sleep_time = (1.0 / self.config.rate_limit) - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
        self.last_request_time = time.time()

    def fetch_events(self, page: int = 1, per_page: int = 100) -> Optional[Dict[str, Any]]:
        """
        Fetch events from SeatGeek API

        Args:
            page: Page number to fetch
            per_page: Number of events per page (max 100)

        Returns:
            API response as dictionary or None if error
        """
        self._rate_limit()

        url = f"{self.config.base_url}/events"
        params = {
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret,
            'per_page': min(per_page, 100),
            'page': page,
            'datetime_utc.gte': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
            'sort': 'datetime_utc.asc'
        }

        try:
            logger.debug(f"Fetching page {page} from SeatGeek API")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching events from SeatGeek API: {e}")
            return None

    def fetch_all_events(self) -> List[Dict[str, Any]]:
        """
        Fetch all available upcoming events from SeatGeek API

        Returns:
            List of all events
        """
        all_events: List[Dict[str, Any]] = []
        page = 1
        total_pages = 1

        logger.info("Starting to fetch all upcoming events from SeatGeek API")

        while page <= total_pages:
            response = self.fetch_events(page=page)

            if not response:
                logger.error(f"Failed to fetch page {page}, stopping")
                break

            events = response.get('events', [])
            meta = response.get('meta', {})
            total = meta.get('total', 0)
            per_page = meta.get('per_page', 100)
            total_pages = (total + per_page - 1) // per_page  # Calculate total pages

            all_events.extend(events)

            logger.info(f"Fetched page {page}/{total_pages} - {len(events)} events (Total so far: {len(all_events)})")

            if not events:  # No more events
                break

            page += 1

        logger.info(f"Finished fetching. Total events retrieved: {len(all_events)}")
        return all_events

    def fetch_and_store_streaming(self, database: 'EventDatabase', batch_size: int = 1000) -> Tuple[int, int]:
        """
        Fetch events from API and store them in streaming mode (process as we fetch)
        This reduces memory usage and provides faster feedback

        Args:
            database: EventDatabase instance to store events
            batch_size: Number of events to accumulate before writing to database

        Returns:
            Tuple of (total_inserted, total_skipped)
        """
        all_inserted = 0
        all_skipped = 0
        page = 1
        total_pages = 1
        accumulated_events: List[Dict[str, Any]] = []

        logger.info("Starting streaming fetch and store from SeatGeek API")

        while page <= total_pages:
            response = self.fetch_events(page=page)

            if not response:
                logger.error(f"Failed to fetch page {page}, stopping")
                break

            events = response.get('events', [])
            meta = response.get('meta', {})
            total = meta.get('total', 0)
            per_page = meta.get('per_page', 100)
            total_pages = (total + per_page - 1) // per_page

            accumulated_events.extend(events)

            logger.info(f"Fetched page {page}/{total_pages} - {len(events)} events")

            # Process accumulated events when we reach batch_size
            if len(accumulated_events) >= batch_size:
                inserted, skipped = database.insert_events(accumulated_events, batch_size=500)
                all_inserted += inserted
                all_skipped += skipped
                logger.info(f"Streaming progress: {all_inserted} inserted, {all_skipped} skipped so far")
                accumulated_events = []

            if not events:
                break

            page += 1

        # Process remaining events
        if accumulated_events:
            inserted, skipped = database.insert_events(accumulated_events, batch_size=500)
            all_inserted += inserted
            all_skipped += skipped

        logger.info(f"Streaming complete: {all_inserted} total inserted, {all_skipped} total skipped")
        return (all_inserted, all_skipped)


class EventDatabase:
    """Handles database operations for events"""

    def __init__(self, config: DatabaseConfig, use_pooling: bool = False, min_connections: int = 1, max_connections: int = 5):
        self.config = config
        self.use_pooling = use_pooling
        self.conn_pool: Optional[pool.SimpleConnectionPool] = None
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.min_connections = min_connections
        self.max_connections = max_connections

    def connect(self) -> None:
        """Establish database connection or connection pool"""
        try:
            if self.use_pooling:
                # Use connection pooling for better performance with concurrent operations
                if self.config.database_url:
                    logger.info(f"Creating connection pool (min={self.min_connections}, max={self.max_connections})")
                    self.conn_pool = pool.SimpleConnectionPool(
                        self.min_connections,
                        self.max_connections,
                        self.config.database_url
                    )
                else:
                    logger.info(f"Creating connection pool at {self.config.host}:{self.config.port}")
                    self.conn_pool = pool.SimpleConnectionPool(
                        self.min_connections,
                        self.max_connections,
                        host=self.config.host,
                        port=self.config.port,
                        database=self.config.database,
                        user=self.config.user,
                        password=self.config.password
                    )
                # Get one connection from pool for operations
                self.conn = self.conn_pool.getconn()
                logger.info("Successfully created connection pool and acquired connection")
            else:
                # Use single connection (default behavior)
                if self.config.database_url:
                    logger.info("Connecting to database using DATABASE_URL (Supabase)")
                    self.conn = psycopg2.connect(self.config.database_url)
                else:
                    logger.info(f"Connecting to database at {self.config.host}:{self.config.port}")
                    self.conn = psycopg2.connect(
                        host=self.config.host,
                        port=self.config.port,
                        database=self.config.database,
                        user=self.config.user,
                        password=self.config.password
                    )
                logger.info("Successfully connected to database")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection or connection pool"""
        if self.conn_pool:
            if self.conn:
                self.conn_pool.putconn(self.conn)
                self.conn = None
            self.conn_pool.closeall()
            logger.info("Connection pool closed")
        elif self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")

    def _extract_location_data(self, event: Dict[str, Any]) -> Tuple[float, float]:
        """
        Extract latitude and longitude from event data

        Args:
            event: Event data from SeatGeek API

        Returns:
            Tuple of (latitude, longitude)
        """
        venue = event.get('venue', {})
        location = venue.get('location', {})

        lat = location.get('lat', 0.0)
        lon = location.get('lon', 0.0)

        # Default to 0.0 if not available
        return (float(lat) if lat else 0.0, float(lon) if lon else 0.0)

    def _compute_hash(self, data: Dict[str, Any]) -> str:
        """
        Compute SHA256 hash of event data for change detection

        Args:
            data: Event data dictionary

        Returns:
            Hexadecimal hash string
        """
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode('utf-8')
        ).hexdigest()

    def _get_existing_events_bulk(self, event_ids: List[str], source_api: str = 'seatgeek') -> Dict[str, Dict[str, Any]]:
        """
        Fetch all existing events in one query for bulk comparison

        Args:
            event_ids: List of event IDs to check
            source_api: Source API name (default: 'seatgeek')

        Returns:
            Dictionary mapping source_event_id to event data
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        if not event_ids:
            return {}

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT source_event_id, data
                    FROM staging_seatgeek.event
                    WHERE source_event_id = ANY(%s) AND source_api = %s
                """, (event_ids, source_api))
                return {row[0]: row[1] for row in cur.fetchall()}
        except psycopg2.Error as e:
            logger.error(f"Error fetching existing events: {e}")
            return {}

    def _insert_batch(self, batch_data: List[Tuple]) -> int:
        """
        Insert a batch of events using execute_values for optimal performance

        Args:
            batch_data: List of tuples containing event data

        Returns:
            Number of events inserted
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        if not batch_data:
            return 0

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO staging_seatgeek.event
                    (data, source_event_id, source_api, latitude, longitude, radius, created_at)
                    VALUES %s
                    ON CONFLICT (event_instance_id)
                    DO UPDATE SET
                        data = EXCLUDED.data,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        updated_at = now()
                """, batch_data)
                self.conn.commit()
                return len(batch_data)
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error inserting batch: {e}")
            raise

    def insert_events(self, events: List[Dict[str, Any]], batch_size: int = 500) -> Tuple[int, int]:
        """
        Insert events into database using optimized batch processing

        Args:
            events: List of event data from SeatGeek API
            batch_size: Number of events to insert per batch (default: 500)

        Returns:
            Tuple of (inserted_count, skipped_count)
        """
        if not self.conn:
            raise RuntimeError("Database not connected")

        if not events:
            return (0, 0)

        inserted = 0
        skipped = 0

        try:
            # Step 1: Deduplicate events within the batch (SeatGeek API may return duplicates across pages)
            seen_ids = set()
            unique_events = []
            duplicate_count = 0

            for event in events:
                event_id = str(event.get('id', ''))
                if event_id and event_id not in seen_ids:
                    seen_ids.add(event_id)
                    unique_events.append(event)
                elif event_id:
                    duplicate_count += 1

            if duplicate_count > 0:
                logger.info(f"Removed {duplicate_count} duplicate events within batch")

            # Step 2: Fetch all existing events in one query
            event_ids = [str(event.get('id', '')) for event in unique_events if event.get('id')]
            existing_events = self._get_existing_events_bulk(event_ids)

            logger.info(f"Found {len(existing_events)} existing events out of {len(unique_events)} unique events")

            # Step 3: Prepare batch data
            batch_data = []
            processed_in_batch = set()  # Track IDs already added to current batch

            for event in unique_events:
                source_event_id = str(event.get('id', ''))

                if not source_event_id:
                    logger.warning("Event missing ID, skipping")
                    skipped += 1
                    continue

                # Skip if already in current batch (extra safety)
                if source_event_id in processed_in_batch:
                    skipped += 1
                    continue

                # Check if event data has changed using in-memory comparison
                if source_event_id in existing_events:
                    stored_data = existing_events[source_event_id]
                    # Use hash comparison for faster change detection
                    stored_hash = self._compute_hash(stored_data)
                    new_hash = self._compute_hash(event)

                    if stored_hash == new_hash:
                        logger.debug(f"Event {source_event_id} unchanged, skipping")
                        skipped += 1
                        continue

                # Extract location data
                lat, lon = self._extract_location_data(event)

                # Add to batch
                batch_data.append((
                    Json(event),
                    source_event_id,
                    'seatgeek',
                    lat,
                    lon,
                    0.0,  # Default radius
                    datetime.now(timezone.utc)
                ))
                processed_in_batch.add(source_event_id)

                # Insert when batch is full
                if len(batch_data) >= batch_size:
                    inserted += self._insert_batch(batch_data)
                    logger.info(f"Processed {inserted + skipped} events ({inserted} inserted, {skipped} skipped)")
                    batch_data = []
                    processed_in_batch.clear()

            # Step 4: Insert remaining events
            if batch_data:
                inserted += self._insert_batch(batch_data)

            logger.info(f"Successfully processed all events: {inserted} inserted, {skipped} skipped")

        except psycopg2.Error as e:
            logger.error(f"Error inserting events: {e}")
            raise

        return (inserted, skipped)


def main() -> None:
    """Main execution function"""
    start_time = time.time()

    try:
        # Load configurations
        logger.info("Loading configuration")
        seatgeek_config = SeatGeekConfig()
        db_config = DatabaseConfig()

        # Check if streaming mode is enabled (default: True for better performance)
        use_streaming = os.getenv('USE_STREAMING_MODE', 'true').lower() in ('true', '1', 'yes')
        use_pooling = os.getenv('USE_CONNECTION_POOLING', 'false').lower() in ('true', '1', 'yes')
        streaming_batch_size = int(os.getenv('STREAMING_BATCH_SIZE', '1000'))

        logger.info(f"Streaming mode: {use_streaming}")
        logger.info(f"Connection pooling: {use_pooling}")

        # Initialize fetcher and database
        fetcher = SeatGeekFetcher(seatgeek_config)
        database = EventDatabase(db_config, use_pooling=use_pooling)

        # Connect to database
        database.connect()

        if use_streaming:
            # Streaming mode: fetch and store in batches (memory efficient, faster feedback)
            logger.info("Using STREAMING mode for optimal memory usage and performance")
            inserted, skipped = fetcher.fetch_and_store_streaming(database, batch_size=streaming_batch_size)
            total_events = inserted + skipped
        else:
            # Traditional mode: fetch all, then store (backwards compatible)
            logger.info("Using TRADITIONAL mode (fetch all, then store)")
            events = fetcher.fetch_all_events()

            if not events:
                logger.warning("No events fetched from SeatGeek API")
                return

            # Insert events into database
            logger.info(f"Inserting {len(events)} events into database")
            inserted, skipped = database.insert_events(events)
            total_events = len(events)

        # Summary
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Mode: {'STREAMING' if use_streaming else 'TRADITIONAL'}")
        logger.info(f"Total events processed: {total_events}")
        logger.info(f"Events inserted/updated: {inserted}")
        logger.info(f"Events skipped (unchanged): {skipped}")
        logger.info(f"Execution time: {elapsed_time:.2f} seconds")
        if total_events > 0:
            logger.info(f"Throughput: {total_events / elapsed_time:.1f} events/second")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}", exc_info=True)
        sys.exit(1)

    finally:
        # Cleanup
        if 'database' in locals():
            database.disconnect()


if __name__ == "__main__":
    main()
