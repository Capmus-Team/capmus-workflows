#!/usr/bin/env python3
"""
Verify Database Script

Verifies that events were successfully saved to the database.
Displays statistics and a sample event.

Usage:
    python verify_database.py
"""

import os
import sys
from pathlib import Path

# Add parent directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.meetup_sync import DatabaseClient


def main() -> None:
    """Main entry point."""
    # Get database URL directly - we don't need full Config
    database_url = os.environ.get("SUPABASE_DB_URL")
    if not database_url:
        print("Configuration error: SUPABASE_DB_URL environment variable is required")
        sys.exit(1)
    
    print("Checking events in database...")
    print("=" * 80)
    
    # Use database client with context manager
    with DatabaseClient(database_url) as db_client:
        # Get total event count
        total_count = db_client.get_event_count()
        print(f"\nTotal events in database: {total_count}")
        print(f"Meetup events: {total_count}")
        
        # Get events by university
        print("\nEvents by university (feed_source_id):")
        counts = db_client.get_event_counts_by_university(limit=10)
        
        for row in counts:
            print(f"  {row['name']}: {row['event_count']} events")
        
        # Show sample event
        print("\nSample event:")
        print("-" * 80)
        
        conn = db_client.connect()
        from psycopg.rows import dict_row
        
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT 
                    e.*,
                    u.name as university_name
                FROM staging_meetup.event e
                LEFT JOIN staging_meetup.feed_source_university u ON e.feed_source_id = u.id
                WHERE e.source_api = 'meetup'
                LIMIT 1
            """)
            
            sample = cur.fetchone()
            if sample:
                print(f"Source Event ID: {sample['source_event_id']}")
                print(f"Source API: {sample['source_api']}")
                print(f"Event Instance ID: {sample['event_instance_id']}")
                print(f"University: {sample.get('university_name', 'N/A')}")
                print(f"Latitude: {sample.get('latitude')}")
                print(f"Longitude: {sample.get('longitude')}")
                print(f"Radius: {sample.get('radius')} miles")
                print(f"Created at: {sample['created_at']}")
                print(f"Has JSON data: {sample.get('data') is not None}")
        
        print("\n" + "=" * 80)
        print("✓ Verification complete!")


if __name__ == "__main__":
    main()
