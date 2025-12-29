"""
Meetup API client module.

Handles all interactions with the Meetup GraphQL API.
Encapsulates API logic following the Adapter Pattern.
"""

import logging
from typing import Dict, Any, Iterator, Optional

import requests

from .models import University, EventData

logger = logging.getLogger(__name__)

# GraphQL query for searching events
SEARCH_EVENTS_QUERY = """
query($filter: EventSearchFilter!, $first: Int, $after: String) {
  eventSearch(filter: $filter, first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        token
        title
        description
        dateTime
        createdTime
        endTime
        duration
        eventUrl
        status
        eventType
        howToFindUs
        isAttending
        isSaved
        guestLimit
        guestsAllowed
        numberOfAllowedGuests
        maxTickets
        waitlistMode
        zoomMeetingId
        featuredEventPhoto {
          id
          baseUrl
        }
        group {
          id
          name
          urlname
          keyGroupPhoto {
            id
            baseUrl
          }
        }
        venues {
          name
          lat
          lon
          city
          state
          country
        }
        eventHosts {
          memberId
          name
        }
        hostRsvps {
          edges {
            node {
              id
            }
          }
        }
        networkEvent {
          id
        }
        photoAlbum {
          id
        }
        rsvp {
          id
        }
        rsvpState
        rsvps(first: 5) {
          edges {
            node {
              id
              member {
                name
              }
            }
          }
        }
        series {
          id
        }
        speakerDetails {
          name
        }
      }
    }
  }
}
"""


class MeetupAPIClient:
    """
    Meetup API client using Adapter Pattern.
    
    Provides a clean interface to the Meetup GraphQL API,
    hiding implementation details and handling pagination automatically.
    """
    
    def __init__(
        self,
        api_token: str,
        api_endpoint: str = "https://api.meetup.com/gql-ext",
        events_per_page: int = 100,
        max_pages: int = 10,
        max_radius_miles: int = 100
    ):
        """
        Initialize Meetup API client.
        
        Args:
            api_token: Meetup OAuth 2.0 access token
            api_endpoint: GraphQL endpoint URL
            events_per_page: Number of events per page (max 100)
            max_pages: Maximum pages to fetch per university
            max_radius_miles: Maximum search radius in miles
        """
        self._api_token = api_token
        self._api_endpoint = api_endpoint
        self._events_per_page = events_per_page
        self._max_pages = max_pages
        self._max_radius_miles = max_radius_miles
        self._session: Optional[requests.Session] = None
    
    def _get_session(self) -> requests.Session:
        """
        Get or create HTTP session.
        
        Lazily creates session for connection pooling.
        
        Returns:
            Requests session instance
        """
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "Authorization": f"Bearer {self._api_token}",
                "Content-Type": "application/json",
            })
        return self._session
    
    def close(self) -> None:
        """Close HTTP session if open."""
        if self._session:
            self._session.close()
            self._session = None
    
    def __enter__(self) -> "MeetupAPIClient":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def _query_graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute GraphQL query.
        
        Args:
            query: GraphQL query string
            variables: Query variables
            
        Returns:
            JSON response data
            
        Raises:
            RuntimeError: If GraphQL errors occur
            requests.RequestException: If HTTP request fails
        """
        session = self._get_session()
        payload = {"query": query, "variables": variables}
        
        response = session.post(
            self._api_endpoint,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        
        if "errors" in data:
            error_msg = f"GraphQL errors: {data['errors']}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        return data
    
    def fetch_university_events(
        self,
        university: University,
        radius_miles: int,
        search_query: str = "events"
    ) -> Iterator[EventData]:
        """
        Fetch all events near a university with pagination.
        
        Uses iterator for memory efficiency. Automatically handles pagination.
        
        Args:
            university: University to search near
            radius_miles: Search radius in miles
            search_query: Search query string
            
        Yields:
            EventData instances for each found event
        """
        # Cap radius at maximum
        radius = min(radius_miles, self._max_radius_miles)
        
        if radius_miles > self._max_radius_miles:
            logger.warning(
                "Radius %d exceeds maximum %d, using %d",
                radius_miles,
                self._max_radius_miles,
                self._max_radius_miles
            )
        
        logger.info(
            "Fetching events university_id=%s name=%s lat=%s lon=%s radius=%s",
            university.id,
            university.name,
            university.latitude,
            university.longitude,
            radius
        )
        
        page_count = 0
        after_cursor = None
        
        while page_count < self._max_pages:
            variables = {
                "filter": {
                    "query": search_query,
                    "lat": university.latitude,
                    "lon": university.longitude,
                    "radius": radius,
                },
                "first": self._events_per_page,
                "after": after_cursor,
            }
            
            try:
                result = self._query_graphql(SEARCH_EVENTS_QUERY, variables)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch events university_id=%s page=%s error=%s",
                    university.id,
                    page_count + 1,
                    exc
                )
                break
            
            event_search = result.get("data", {}).get("eventSearch", {})
            edges = event_search.get("edges", [])
            
            logger.info(
                "Fetched %s events university_id=%s page=%s",
                len(edges),
                university.id,
                page_count + 1
            )
            
            # Yield events as they're fetched
            for edge in edges:
                node = edge.get("node", {})
                if node and node.get("id"):
                    yield EventData.from_meetup_event(
                        event=node,
                        feed_source_id=university.id,
                        university_id=university.id,
                        radius=radius,
                        university_latitude=university.latitude,
                        university_longitude=university.longitude,
                        api_endpoint=self._api_endpoint
                    )
            
            # Check for next page
            page_info = event_search.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")
            
            page_count += 1
            
            if not has_next_page or not after_cursor:
                logger.debug("Reached end of results for university_id=%s", university.id)
                break
        
        if page_count >= self._max_pages:
            logger.warning(
                "Reached maximum page limit (%s) for university_id=%s",
                self._max_pages,
                university.id
            )

