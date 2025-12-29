import argparse
import json
import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional

import requests
from psycopg import Connection
from psycopg import connect
from psycopg.rows import dict_row

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("staging_localist.event_sync")


@dataclass
class FeedSource:
    id: int
    api_endpoint: str
    university_id: int


def configure_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL")
    if not level_name:
        return

    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.getLogger().setLevel(level)
    logger.setLevel(level)
    logger.debug("Logger configured with level=%s", level_name.upper())


def load_settings() -> None:
    if load_dotenv is not None:
        load_dotenv()
    configure_logging()


def get_connection() -> Connection:
    database_url = os.environ.get("SUPABASE_DB_URL")
    if not database_url:
        raise RuntimeError("SUPABASE_DB_URL environment variable is not set")
    return connect(database_url)


def get_upsert_batch_size() -> int:
    raw_value = os.environ.get("UPSERT_BATCH_SIZE")
    if raw_value is None:
        return 200

    try:
        value = int(raw_value)
    except ValueError:
        logger.warning(
            "Invalid UPSERT_BATCH_SIZE value=%s falling back to default", raw_value
        )
        return 200

    if value < 1:
        logger.warning(
            "UPSERT_BATCH_SIZE must be positive value=%s fallback=200", raw_value
        )
        return 200

    return value
def fetch_feed_sources(
    conn: Connection, feed_source_id: Optional[int] = None
) -> Iterator[FeedSource]:
    with conn.cursor(row_factory=dict_row) as cur:
        query_parts = [
            "select id, api_endpoint, university_id",
            "from staging_localist.feed_source",
            "where active is true",
        ]
        params: List[int] = []
        if feed_source_id is not None:
            query_parts.append("and id = %s")
            params.append(feed_source_id)
        query_parts.append("order by id")

        sql = "\n".join(query_parts)
        cur.execute(sql, params if params else None)
        rows = cur.fetchall()

    if feed_source_id is not None:
        if rows:
            logger.info(
                "Discovered %s active feed sources feed_source_id=%s",
                len(rows),
                feed_source_id,
            )
        else:
            logger.warning(
                "No active feed source found feed_source_id=%s", feed_source_id
            )
    else:
        logger.info("Discovered %s active feed sources", len(rows))
    for row in rows:
        yield FeedSource(
            id=row["id"],
            api_endpoint=row["api_endpoint"],
            university_id=row["university_id"],
        )


def build_events_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/events"):
        return base
    return f"{base}/events"


def iter_feed_events(
    feed_source: FeedSource,
    localist_days: int,
    session: Optional[requests.Session] = None,
) -> Iterator[Dict]:
    url = build_events_url(feed_source.api_endpoint)
    client = session or requests.Session()
    page = 1
    while True:
        try:
            logger.debug(
                "Requesting events feed feed_source_id=%s page=%s days=%s url=%s",
                feed_source.id,
                page,
                localist_days,
                url,
            )
            response = client.get(
                url,
                params={"page": page, "days": localist_days},
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover
            logger.warning(
                "Failed to fetch events feed_source_id=%s page=%s days=%s url=%s error=%s",
                feed_source.id,
                page,
                localist_days,
                url,
                exc,
            )
            return

        payload = response.json()
        wrappers = payload.get("events", []) or []
        logger.info(
            "Fetched %s events feed_source_id=%s page=%s days=%s url=%s",
            len(wrappers),
            feed_source.id,
            page,
            localist_days,
            url,
        )
        for wrapper in wrappers:
            event = wrapper.get("event")
            if event:
                yield event

        page_info = payload.get("page") or {}
        current = page_info.get("current")
        total = page_info.get("total")
        if not current or not total or current >= total:
            break
        page += 1


def upsert_events_batch(conn: Connection, feed_source: FeedSource, events: List[Dict]) -> int:
    unique_rows: "OrderedDict[str, str]" = OrderedDict()
    duplicates = 0
    for event in events:
        source_event_id = event.get("id")
        if source_event_id is None:
            logger.debug("Skipping event without id feed_source_id=%s", feed_source.id)
            continue

        key = str(source_event_id)
        if key in unique_rows:
            duplicates += 1
            continue

        unique_rows[key] = json.dumps(event, separators=(",", ":"))

    if duplicates:
        logger.debug(
            "Deduplicated %s duplicate events feed_source_id=%s", duplicates, feed_source.id
        )

    if not unique_rows:
        return 0

    placeholders = ",".join(["(%s,%s,%s,%s,%s)"] * len(unique_rows))
    params: List[object] = []
    for source_event_id, payload in unique_rows.items():
        params.extend(
            (
                source_event_id,
                feed_source.api_endpoint,
                payload,
                feed_source.id,
                feed_source.university_id,
            )
        )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into staging_localist.event (
                source_event_id,
                source_api,
                data,
                feed_source_id,
                university_id
            )
            values {placeholders}
            on conflict (source_event_id, source_api)
            do update set
                data = excluded.data,
                feed_source_id = excluded.feed_source_id,
                university_id = excluded.university_id,
                updated_at = timezone('utc', now())
            """,
            params,
        )

    return len(unique_rows)


def process_feed_source(conn: Connection, feed_source: FeedSource, localist_days: int) -> int:
    logger.info(
        "Processing feed source id=%s endpoint=%s localist_days=%s",
        feed_source.id,
        feed_source.api_endpoint,
        localist_days,
    )
    inserted = 0
    batch: List[Dict] = []
    batch_size = get_upsert_batch_size()
    with requests.Session() as session:
        for event in iter_feed_events(feed_source, localist_days, session=session):
            batch.append(event)
            if len(batch) >= batch_size:
                logger.debug(
                    "Flushing event batch size=%s feed_source_id=%s", len(batch), feed_source.id
                )
                inserted += upsert_events_batch(conn, feed_source, batch)
                batch.clear()
                logger.info(
                    "Persisted %s events so far feed_source_id=%s", inserted, feed_source.id
                )

    if batch:
        logger.debug(
            "Flushing final event batch size=%s feed_source_id=%s", len(batch), feed_source.id
        )
        inserted += upsert_events_batch(conn, feed_source, batch)
        logger.info(
            "Persisted %s events so far feed_source_id=%s", inserted, feed_source.id
        )
    logger.info("Finished processing feed source id=%s events=%s", feed_source.id, inserted)
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Localist events into the staging database."
    )
    parser.add_argument(
        "--localist-days",
        type=int,
        default=30,
        help="Number of days of events to fetch from Localist (default: 30).",
    )
    parser.add_argument(
        "--feed-source",
        type=int,
        help="Only process the specified feed source id (default: all active feed sources).",
    )
    args = parser.parse_args()
    if args.localist_days < 1:
        parser.error("--localist-days must be a positive integer")
    if args.feed_source is not None and args.feed_source < 1:
        parser.error("--feed-source must be a positive integer")

    load_settings()
    with get_connection() as conn:
        # Explicit autocommit ensures inserts persist as we go.
        conn.autocommit = True
        total_events = 0
        processed_sources = 0
        for feed_source in fetch_feed_sources(conn, feed_source_id=args.feed_source):
            total_events += process_feed_source(conn, feed_source, args.localist_days)
            processed_sources += 1

    logger.info(
        "Event sync complete feed_sources_processed=%s events_persisted=%s",
        processed_sources,
        total_events,
    )


if __name__ == "__main__":
    main()
