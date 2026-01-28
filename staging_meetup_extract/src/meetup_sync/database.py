"""
Database operations module.

Handles all database interactions using the Repository Pattern.
Encapsulates database logic for easy testing and maintenance.
"""

import json
import logging
from typing import Iterator, List, Optional

from psycopg import Connection, connect
from psycopg.rows import dict_row

from .models import University, EventData

logger = logging.getLogger(__name__)


class DatabaseClient:
    """
    Database client using Repository Pattern.
    
    Encapsulates all database operations, providing a clean interface
    for data access. Follows Single Responsibility Principle.
    """
    
    def __init__(self, database_url: str):
        """
        Initialize database client.
        
        Args:
            database_url: PostgreSQL connection string
        """
        self._database_url = database_url
        self._connection: Optional[Connection] = None
    
    def connect(self) -> Connection:
        """
        Establish database connection.
        
        Returns:
            Active database connection
        """
        if self._connection is None or self._connection.closed:
            self._connection = connect(self._database_url)
            self._connection.autocommit = True
            logger.debug("Database connection established")
        
        return self._connection
    
    def close(self) -> None:
        """Close database connection if open."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.debug("Database connection closed")
    
    def __enter__(self) -> "DatabaseClient":
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def fetch_active_universities(
        self,
        limit: Optional[int] = None,
        feed_source_id: Optional[int] = None,
        worker_id: Optional[int] = None,
        worker_count: Optional[int] = None,
        skip_locked: bool = False
    ) -> Iterator[University]:
        """
        Fetch active universities from database.
        
        Uses iterator for memory efficiency with large datasets.
        
        Args:
            limit: Optional limit on number of universities
            feed_source_id: Optional filter for specific university
            worker_id: Optional 1-based worker index for sharding
            worker_count: Optional total worker count for sharding
            skip_locked: Skip rows locked by other transactions
            
        Yields:
            University instances
        """
        conn = self.connect()
        
        with conn.cursor(row_factory=dict_row) as cur:
            query_parts = [
                "SELECT id, name, latitude, longitude",
                "FROM staging_meetup.feed_source_university",
                "WHERE status = 'active'",
                "AND latitude IS NOT NULL",
                "AND longitude IS NOT NULL"
            ]
            params: List[object] = []
            
            if feed_source_id is not None:
                query_parts.append("AND id = %s")
                params.append(feed_source_id)

            if worker_id is not None and worker_count is not None:
                query_parts.append("AND MOD(id, %s) = %s")
                params.append(worker_count)
                params.append(worker_id - 1)
            
            query_parts.append("ORDER BY id")
            
            if limit is not None:
                query_parts.append("LIMIT %s")
                params.append(limit)

            if skip_locked:
                query_parts.append("FOR UPDATE SKIP LOCKED")
            
            sql = "\n".join(query_parts)
            cur.execute(sql, params if params else None)
            rows = cur.fetchall()
        
        if worker_id is not None and worker_count is not None:
            logger.info(
                "Discovered %s active universities for worker %s/%s",
                len(rows),
                worker_id,
                worker_count
            )
        else:
            logger.info("Discovered %s active universities", len(rows))
        
        for row in rows:
            yield University(
                id=row["id"],
                name=row["name"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"])
            )
    
    def upsert_events_batch(self, events: List[EventData]) -> int:
        """
        Batch upsert events into database.
        
        Uses PostgreSQL's ON CONFLICT for efficient upsert operations.
        Implements batch processing for performance.
        
        Args:
            events: List of EventData instances to insert/update
            
        Returns:
            Number of events successfully upserted
        """
        if not events:
            return 0
        
        # Deduplicate by source_event_id (using dict to preserve order)
        unique_events = {event.source_event_id: event for event in events}
        
        if not unique_events:
            return 0
        
        conn = self.connect()
        
        # Build batch insert statement
        placeholders = ",".join(
            ["(%s,%s,%s,%s,%s,%s,%s,%s)"] * len(unique_events)
        )
        params: List[object] = []
        
        for event in unique_events.values():
            params.extend([
                event.source_event_id,
                event.source_api,
                json.dumps(event.data, separators=(",", ":")),
                event.feed_source_id,
                event.university_id,
                event.latitude,
                event.longitude,
                event.radius,
            ])
        
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO staging_meetup.event (
                    source_event_id,
                    source_api,
                    data,
                    feed_source_id,
                    university_id,
                    latitude,
                    longitude,
                    radius
                )
                VALUES {placeholders}
                ON CONFLICT (source_event_id, source_api)
                DO UPDATE SET
                    data = EXCLUDED.data,
                    feed_source_id = EXCLUDED.feed_source_id,
                    university_id = EXCLUDED.university_id,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    radius = EXCLUDED.radius,
                    updated_at = timezone('utc', now())
                """,
                params,
            )
        
        return len(unique_events)
    
    def get_event_count(self, source_api: str = "meetup") -> int:
        """
        Get total count of events in database.
        
        Args:
            source_api: Filter by source API (default: "meetup")
            
        Returns:
            Total count of events
        """
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM staging_meetup.event WHERE source_api = %s",
                (source_api,)
            )
            result = cur.fetchone()
            return result[0] if result else 0
    
    def get_event_counts_by_university(self, limit: int = 10) -> List[dict]:
        """
        Get event counts grouped by university.
        
        Args:
            limit: Maximum number of universities to return
            
        Returns:
            List of dicts with 'name' and 'event_count' keys
        """
        conn = self.connect()
        
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT 
                    u.name,
                    COUNT(e.event_instance_id) as event_count
                FROM staging_meetup.feed_source_university u
                LEFT JOIN staging_meetup.event e ON e.feed_source_id = u.id
                WHERE u.status = 'active'
                GROUP BY u.id, u.name
                HAVING COUNT(e.event_instance_id) > 0
                ORDER BY event_count DESC
                LIMIT %s
            """, (limit,))
            
            return cur.fetchall()

