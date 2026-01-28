#!/usr/bin/env python3
"""
Fetch University Events Script

Syncs Meetup events for active universities into the staging database.
Uses clean architecture with separation of concerns.

Usage:
    python fetch_university_events.py [OPTIONS]
    
Examples:
    # Fetch events for all universities
    python fetch_university_events.py
    
    # Specify radius and limit
    python fetch_university_events.py --radius 25 --university-limit 10
    
    # Process specific feed source
    python fetch_university_events.py --feed-source 123
"""

import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.meetup_sync import (
    Config,
    DatabaseClient,
    MeetupAPIClient,
    University,
    EventData,
)

logger = logging.getLogger(__name__)


def process_university(
    db_client: DatabaseClient,
    api_client: MeetupAPIClient,
    university: University,
    radius_miles: int,
    batch_size: int
) -> int:
    """
    Process all events for a single university.
    
    Follows the Single Responsibility Principle - only coordinates
    fetching and saving events for one university.
    
    Args:
        db_client: Database client for persistence
        api_client: Meetup API client for fetching events
        university: University to process
        radius_miles: Search radius in miles
        batch_size: Batch size for database upserts
        
    Returns:
        Number of events successfully persisted
    """
    logger.info(
        "Processing university id=%s name=%s radius=%s",
        university.id,
        university.name,
        radius_miles
    )
    
    inserted = 0
    batch: list[EventData] = []
    
    # Fetch events using iterator for memory efficiency
    for event in api_client.fetch_university_events(university, radius_miles):
        batch.append(event)
        
        if len(batch) >= batch_size:
            logger.debug(
                "Flushing event batch size=%s university_id=%s",
                len(batch),
                university.id
            )
            inserted += db_client.upsert_events_batch(batch)
            batch.clear()
            logger.info(
                "Persisted %s events so far university_id=%s",
                inserted,
                university.id
            )
    
    # Flush remaining events
    if batch:
        logger.debug(
            "Flushing final event batch size=%s university_id=%s",
            len(batch),
            university.id
        )
        inserted += db_client.upsert_events_batch(batch)
    
    logger.info(
        "Finished processing university id=%s events=%s",
        university.id,
        inserted
    )
    
    return inserted


def main() -> None:
    """
    Main entry point.
    
    Orchestrates the entire sync process using clean architecture principles.
    Each component has a single responsibility and clear interfaces.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Sync Meetup events for active universities into staging database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--radius",
        type=int,
        default=10,
        help="Search radius in miles (1-100)"
    )
    parser.add_argument(
        "--university-limit",
        type=int,
        help="Limit number of universities to process"
    )
    parser.add_argument(
        "--feed-source",
        type=int,
        help="Only process the specified feed source id"
    )
    parser.add_argument(
        "--worker-id",
        type=int,
        help="1-based worker index for sharding"
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        help="Total worker count for sharding"
    )
    parser.add_argument(
        "--skip-locked",
        action="store_true",
        help="Skip universities locked by other transactions"
    )
    args = parser.parse_args()
    
    # Validate arguments
    if args.radius < 1 or args.radius > 100:
        parser.error("--radius must be between 1 and 100")
    if args.university_limit is not None and args.university_limit < 1:
        parser.error("--university-limit must be a positive integer")
    if args.feed_source is not None and args.feed_source < 1:
        parser.error("--feed-source must be a positive integer")
    if (args.worker_id is None) != (args.worker_count is None):
        parser.error("--worker-id and --worker-count must be used together")
    if args.worker_count is not None and args.worker_count < 1:
        parser.error("--worker-count must be a positive integer")
    if args.worker_id is not None and args.worker_id < 1:
        parser.error("--worker-id must be a positive integer")
    if (
        args.worker_id is not None
        and args.worker_count is not None
        and args.worker_id > args.worker_count
    ):
        parser.error("--worker-id must be <= --worker-count")
    
    # Load configuration and set up logging
    try:
        config = Config.from_environment()
    except RuntimeError as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)
    
    config.configure_logging()
    
    # Initialize clients using context managers for proper resource cleanup
    with DatabaseClient(config.database_url) as db_client, \
         MeetupAPIClient(
             api_token=config.meetup_api_token,
             api_endpoint=config.meetup_api_endpoint,
             events_per_page=config.events_per_page,
             max_pages=config.max_pages_per_university,
             max_radius_miles=config.max_radius_miles
         ) as api_client:
        
        total_events = 0
        processed_universities = 0
        
        # Process each university
        if args.worker_id is not None and args.worker_count is not None:
            logger.info(
                "Worker sharding enabled worker_id=%s worker_count=%s",
                args.worker_id,
                args.worker_count
            )
        if args.skip_locked:
            logger.info("Row locking enabled (FOR UPDATE SKIP LOCKED)")

        for university in db_client.fetch_active_universities(
            limit=args.university_limit,
            feed_source_id=args.feed_source,
            worker_id=args.worker_id,
            worker_count=args.worker_count,
            skip_locked=args.skip_locked,
        ):
            try:
                events_count = process_university(
                    db_client,
                    api_client,
                    university,
                    args.radius,
                    config.upsert_batch_size
                )
                total_events += events_count
                processed_universities += 1
            except Exception as e:
                logger.error(
                    "Failed to process university id=%s name=%s error=%s",
                    university.id,
                    university.name,
                    e,
                    exc_info=True
                )
        
        logger.info(
            "Event sync complete universities_processed=%s events_persisted=%s",
            processed_universities,
            total_events,
        )


if __name__ == "__main__":
    main()
