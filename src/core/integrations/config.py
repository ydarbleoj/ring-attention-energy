"""
Configuration management for Ring Attention Energy Pipeline

Uses Pydantic for validation and python-dotenv for environment loading.
Follows the 12-factor app methodology for configuration management.
"""
import os
from pathlib import Path
from typing import Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings


class APIConfig(BaseSettings):
    """API configuration and rate limiting settings"""

    # EIA Configuration
    eia_api_key: Optional[str] = Field(None, env="EIA_API_KEY")
    eia_base_url: str = Field("https://api.eia.gov/v2", env="EIA_BASE_URL")
    eia_rate_limit_delay: float = Field(0.72, env="EIA_RATE_LIMIT_DELAY")

    # CAISO Configuration
    caiso_base_url: str = Field(
        "http://oasis.caiso.com/oasisapi/SingleZip",
        env="CAISO_BASE_URL"
    )
    caiso_rate_limit_delay: float = Field(1.0, env="CAISO_RATE_LIMIT_DELAY")

    # ENTSO-E Configuration (optional for European data)
    entso_e_api_key: Optional[str] = Field(None, env="ENTSO_E_API_KEY")
    entso_e_base_url: str = Field(
        "https://web-api.tp.entsoe.eu",
        env="ENTSO_E_BASE_URL"
    )
    entso_e_rate_limit_delay: float = Field(0.15, env="ENTSO_E_RATE_LIMIT_DELAY")

    @validator("eia_api_key")
    def validate_eia_key(cls, v):
        if v and len(v) < 20:
            raise ValueError("EIA API key appears to be invalid (too short)")
        return v

    @validator("entso_e_api_key")
    def validate_entso_key(cls, v):
        if v and len(v) < 30:
            raise ValueError("ENTSO-E API key appears to be invalid (too short)")
        return v


class DataConfig(BaseSettings):
    """Data storage and caching configuration"""

    data_cache_dir: Path = Field(Path("data/cache"), env="DATA_CACHE_DIR")
    max_cache_size_gb: int = Field(10, env="MAX_CACHE_SIZE_GB")

    # Data processing settings
    default_frequency: str = Field("1h", env="DEFAULT_FREQUENCY")
    missing_data_threshold: float = Field(0.1, env="MISSING_DATA_THRESHOLD")

    @validator("data_cache_dir")
    def create_cache_dir(cls, v):
        v.mkdir(parents=True, exist_ok=True)
        return v

    @validator("max_cache_size_gb")
    def validate_cache_size(cls, v):
        if v < 1:
            raise ValueError("Cache size must be at least 1 GB")
        return v


class LoggingConfig(BaseSettings):
    """Logging configuration"""

    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_file: Optional[Path] = Field(None, env="LOG_FILE")
    log_format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )

    @validator("log_level")
    def validate_log_level(cls, v):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    @validator("log_file")
    def create_log_dir(cls, v):
        if v:
            v.parent.mkdir(parents=True, exist_ok=True)
        return v


class AppConfig(BaseSettings):
    """Main application configuration"""

    debug: bool = Field(False, env="DEBUG")
    testing: bool = Field(False, env="TESTING")

    # Component configurations
    api: APIConfig = APIConfig()
    data: DataConfig = DataConfig()
    logging: LoggingConfig = LoggingConfig()

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

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
    return AppConfig(
        testing=True,
        debug=True,
        data=DataConfig(
            data_cache_dir=Path("tests/data/cache"),
            max_cache_size_gb=1
        ),
        logging=LoggingConfig(
            log_level="DEBUG"
        )
    )


if __name__ == "__main__":
    # Test configuration loading
    config = get_config()
    print("Configuration loaded successfully!")
    print(f"API config: {config.api}")
    print(f"Data config: {config.data}")
    print(f"Logging config: {config.logging}")
