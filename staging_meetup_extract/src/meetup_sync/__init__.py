"""
Meetup Event Sync Package

A Python package for syncing Meetup events to PostgreSQL database.
Follows clean architecture principles with separation of concerns.
"""

__version__ = "1.0.0"
__author__ = "Capmus Team"

from .config import Config
from .database import DatabaseClient
from .meetup_api import MeetupAPIClient
from .models import University, EventData

__all__ = [
    "Config",
    "DatabaseClient",
    "MeetupAPIClient",
    "University",
    "EventData",
]

