"""
Configuration management module.

Handles environment variables and application settings with validation.
Follows the Single Responsibility Principle - only manages configuration.
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    """
    Immutable configuration container.
    
    Uses dataclass for clean, type-safe configuration management.
    Frozen=True ensures configuration cannot be modified after creation.
    """
    
    # Database configuration
    database_url: str
    
    # Meetup API configuration
    meetup_api_token: str
    meetup_api_endpoint: str = "https://api.meetup.com/gql-ext"
    
    # Logging configuration
    log_level: str = "INFO"
    
    # Batch processing configuration
    upsert_batch_size: int = 100
    events_per_page: int = 100
    max_pages_per_university: int = 10
    max_radius_miles: int = 100
    
    @classmethod
    def from_environment(cls) -> "Config":
        """
        Load configuration from environment variables.
        
        This factory method encapsulates the logic for loading configuration,
        following the Factory Pattern for object creation.
        
        Returns:
            Config instance with values from environment
            
        Raises:
            RuntimeError: If required environment variables are missing
        """
        if load_dotenv is not None:
            load_dotenv()
        
        # Required variables
        database_url = os.environ.get("SUPABASE_DB_URL")
        if not database_url:
            raise RuntimeError(
                "SUPABASE_DB_URL environment variable is required"
            )
        
        meetup_api_token = os.environ.get("MEETUP_API_TOKEN")
        if not meetup_api_token:
            raise RuntimeError(
                "MEETUP_API_TOKEN environment variable is required"
            )
        
        # Optional variables with defaults
        meetup_api_endpoint = os.environ.get(
            "MEETUP_API_ENDPOINT",
            "https://api.meetup.com/gql-ext"
        )
        
        log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
        
        upsert_batch_size = cls._parse_int_env(
            "UPSERT_BATCH_SIZE",
            default=100,
            min_value=1
        )
        
        return cls(
            database_url=database_url,
            meetup_api_token=meetup_api_token,
            meetup_api_endpoint=meetup_api_endpoint,
            log_level=log_level,
            upsert_batch_size=upsert_batch_size,
        )
    
    @staticmethod
    def _parse_int_env(
        key: str,
        default: int,
        min_value: Optional[int] = None
    ) -> int:
        """
        Parse integer environment variable with validation.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            min_value: Minimum allowed value (optional)
            
        Returns:
            Parsed integer value or default
        """
        raw_value = os.environ.get(key)
        if raw_value is None:
            return default
        
        try:
            value = int(raw_value)
        except ValueError:
            logger.warning(
                "Invalid %s value=%s, using default=%d",
                key, raw_value, default
            )
            return default
        
        if min_value is not None and value < min_value:
            logger.warning(
                "%s must be >= %d, value=%d, using default=%d",
                key, min_value, value, default
            )
            return default
        
        return value
    
    def configure_logging(self) -> None:
        """
        Configure application logging based on settings.
        
        This method applies the configuration, separating configuration
        loading from configuration application.
        """
        level = getattr(logging, self.log_level, logging.INFO)
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        logger.debug("Logging configured with level=%s", self.log_level)

