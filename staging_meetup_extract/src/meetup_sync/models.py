"""
Data models module.

Defines domain models using dataclasses for type safety and immutability.
Follows the Domain-Driven Design principle of having clear domain models.
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class University:
    """
    University domain model.
    
    Represents a university from the feed_source_university table.
    Frozen=True makes it immutable, following functional programming principles.
    
    Attributes:
        id: Database primary key
        name: University name
        latitude: Geographic latitude
        longitude: Geographic longitude
    """
    
    id: int
    name: str
    latitude: float
    longitude: float
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"{self.name} (ID: {self.id})"
    
    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"University(id={self.id}, name={self.name!r}, "
            f"lat={self.latitude}, lon={self.longitude})"
        )


@dataclass
class EventData:
    """
    Event data transfer object (DTO).
    
    Encapsulates all data needed to store an event in the database.
    Not frozen because it aggregates data from multiple sources.
    
    Attributes:
        source_event_id: Original source event ID from API
        source_api: API source endpoint URL
        data: Full event JSON data
        feed_source_id: Foreign key to feed_source_university
        university_id: Foreign key to university_new
        latitude: Event venue latitude (with university fallback)
        longitude: Event venue longitude (with university fallback)
        radius: Search radius used to find this event
    """
    
    source_event_id: str
    source_api: str
    data: Dict[str, Any]
    feed_source_id: int
    university_id: int
    latitude: float = None
    longitude: float = None
    radius: float = None
    
    @classmethod
    def from_meetup_event(
        cls,
        event: Dict[str, Any],
        feed_source_id: int,
        university_id: int,
        radius: float,
        university_latitude: float,
        university_longitude: float,
        api_endpoint: str
    ) -> "EventData":
        """
        Factory method to create EventData from Meetup API response.
        
        This method encapsulates the logic for extracting and transforming
        data from the Meetup API format to our internal format.
        
        Coordinates priority:
        1. Use venue coordinates if available from the event
        2. Fall back to university search coordinates if venue coords are missing
        
        Args:
            event: Raw event data from Meetup GraphQL API
            feed_source_id: ID of the feed source university
            university_id: ID of the university
            radius: Search radius used
            university_latitude: University latitude (fallback coordinates)
            university_longitude: University longitude (fallback coordinates)
            api_endpoint: Meetup API endpoint URL (used as source_api)
            
        Returns:
            EventData instance ready for database insertion
        """
        event_id = event.get("id")
        
        # Extract venue coordinates if available
        venues = event.get("venues", [])
        latitude = None
        longitude = None
        if venues and len(venues) > 0:
            latitude = venues[0].get("lat")
            longitude = venues[0].get("lon")
        
        # Fall back to university coordinates if venue coordinates not available
        # This ensures we always have coordinates for events
        if latitude is None or longitude is None:
            latitude = university_latitude
            longitude = university_longitude
        
        return cls(
            source_event_id=event_id,
            source_api=api_endpoint,
            data=event,
            feed_source_id=feed_source_id,
            university_id=university_id,
            latitude=latitude,
            longitude=longitude,
            radius=radius,
        )
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        title = self.data.get("title", "Untitled")
        return f"Event: {title} (ID: {self.source_event_id})"

