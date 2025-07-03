"""
Configuration management for Ring Attention Energy Pipeline

Uses Pydantic for validation and python-dotenv for environment loading.
Follows the 12-factor app methodology for configuration management.
"""
import os
from pathlib import Path
from typing import Optional
from pydantic import Field, field_validator, ConfigDict
from pydantic_settings import BaseSettings


class APIConfig(BaseSettings):
    """API configuration and rate limiting settings"""

    model_config = ConfigDict(env_file='.env', env_file_encoding='utf-8')

    # EIA Configuration
    eia_api_key: Optional[str] = Field(None, validation_alias='EIA_API_KEY')
    eia_base_url: str = Field("https://api.eia.gov/v2", validation_alias='EIA_BASE_URL')
    eia_rate_limit_delay: float = Field(0.72, validation_alias='EIA_RATE_LIMIT_DELAY')

    # CAISO Configuration
    caiso_base_url: str = Field(
        "http://oasis.caiso.com/oasisapi/SingleZip",
        validation_alias='CAISO_BASE_URL'
    )
    caiso_rate_limit_delay: float = Field(1.0, validation_alias='CAISO_RATE_LIMIT_DELAY')

    # ENTSO-E Configuration (optional for European data)
    entso_e_api_key: Optional[str] = Field(None, validation_alias='ENTSO_E_API_KEY')
    entso_e_base_url: str = Field(
        "https://web-api.tp.entsoe.eu",
        validation_alias='ENTSO_E_BASE_URL'
    )
    entso_e_rate_limit_delay: float = Field(0.15, validation_alias='ENTSO_E_RATE_LIMIT_DELAY')

    @field_validator("eia_api_key")
    @classmethod
    def validate_eia_key(cls, v):
        if v and len(v) < 20:
            raise ValueError("EIA API key appears to be invalid (too short)")
        return v

    @field_validator("entso_e_api_key")
    @classmethod
    def validate_entso_key(cls, v):
        if v and len(v) < 30:
            raise ValueError("ENTSO-E API key appears to be invalid (too short)")
        return v


class DataConfig(BaseSettings):
    """Data storage and caching configuration"""

    model_config = ConfigDict(env_file='.env', env_file_encoding='utf-8')

    data_cache_dir: Path = Field(Path("data/cache"), validation_alias='DATA_CACHE_DIR')
    max_cache_size_gb: int = Field(10, validation_alias='MAX_CACHE_SIZE_GB')

    # Data processing settings
    default_frequency: str = Field("1h", validation_alias='DEFAULT_FREQUENCY')
    missing_data_threshold: float = Field(0.1, validation_alias='MISSING_DATA_THRESHOLD')

    @field_validator("data_cache_dir")
    @classmethod
    def create_cache_dir(cls, v):
        if isinstance(v, str):
            v = Path(v)
        v.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("max_cache_size_gb")
    @classmethod
    def validate_cache_size(cls, v):
        if v < 1:
            raise ValueError("Cache size must be at least 1 GB")
        return v


class LoggingConfig(BaseSettings):
    """Logging configuration"""

    model_config = ConfigDict(env_file='.env', env_file_encoding='utf-8')

    log_level: str = Field("INFO", validation_alias='LOG_LEVEL')
    log_file: Optional[Path] = Field(None, validation_alias='LOG_FILE')
    log_format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        validation_alias='LOG_FORMAT'
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    @field_validator("log_file")
    @classmethod
    def create_log_dir(cls, v):
        if v:
            if isinstance(v, str):
                v = Path(v)
            v.parent.mkdir(parents=True, exist_ok=True)
        return v


class AppConfig:
    """Main application configuration - composition instead of inheritance"""

    def __init__(self,
                 debug: bool = False,
                 testing: bool = False,
                 api: APIConfig = None,
                 data: DataConfig = None,
                 logging: LoggingConfig = None):
        self.debug = debug or os.getenv('DEBUG', 'False').lower() == 'true'
        self.testing = testing or os.getenv('TESTING', 'False').lower() == 'true'

        # Component configurations - allow overrides for testing
        self.api = api or APIConfig()
        self.data = data or DataConfig()
        self.logging = logging or LoggingConfig()

    def setup_logging(self):
        """Configure logging based on settings"""
        import logging

        # Configure root logger
        logging.basicConfig(
            level=getattr(logging, self.logging.log_level),
            format=self.logging.log_format,
            handlers=[]
        )

        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, self.logging.log_level))
        console_formatter = logging.Formatter(self.logging.log_format)
        console_handler.setFormatter(console_formatter)

        logger = logging.getLogger()
        logger.addHandler(console_handler)

        # Add file handler if specified
        if self.logging.log_file:
            file_handler = logging.FileHandler(self.logging.log_file)
            file_handler.setLevel(getattr(logging, self.logging.log_level))
            file_handler.setFormatter(console_formatter)
            logger.addHandler(file_handler)

        return logger


# Global configuration instance
def get_config() -> AppConfig:
    """Get the application configuration instance"""
    return AppConfig()


# For testing - create a test configuration
def get_test_config() -> AppConfig:
    """Get a test configuration with appropriate overrides"""

    class TestAPIConfig:
        """Simple test config that doesn't inherit from BaseSettings"""
        def __init__(self):
            self.eia_api_key = "TEST_API_KEY_FOR_VCR"
            self.eia_base_url = "https://api.eia.gov/v2"
            self.eia_rate_limit_delay = 0.1
            self.caiso_base_url = "http://oasis.caiso.com/oasisapi/SingleZip"
            self.caiso_rate_limit_delay = 0.1
            self.entso_e_api_key = None
            self.entso_e_base_url = "https://web-api.tp.entsoe.eu"
            self.entso_e_rate_limit_delay = 0.1

    class TestDataConfig:
        """Simple test config for data settings"""
        def __init__(self):
            self.data_cache_dir = Path("tests/data/cache")
            self.data_cache_dir.mkdir(parents=True, exist_ok=True)
            self.max_cache_size_gb = 1
            self.default_frequency = "1h"
            self.missing_data_threshold = 0.1

    class TestLoggingConfig:
        """Simple test config for logging"""
        def __init__(self):
            self.log_level = "DEBUG"
            self.log_file = None
            self.log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    return AppConfig(
        testing=True,
        debug=True,
        api=TestAPIConfig(),
        data=TestDataConfig(),
        logging=TestLoggingConfig()
    )


if __name__ == "__main__":
    # Test configuration loading
    config = get_config()
    print("Configuration loaded successfully!")
    print(f"API config: {config.api}")
    print(f"Data config: {config.data}")
    print(f"Logging config: {config.logging}")
