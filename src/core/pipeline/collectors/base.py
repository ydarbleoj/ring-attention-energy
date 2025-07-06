"""Base collector interface for energy data sources."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import polars as pl
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataCollectionResult:
    """Result of a data collection operation."""

    def __init__(
        self,
        data: Optional[pl.DataFrame] = None,
        metadata: Dict[str, Any] = None,
        errors: List[str] = None,
        source: str = "unknown",
        collection_time: Optional[datetime] = None
    ):
        self.data = data if data is not None else pl.DataFrame()
        self.metadata = metadata or {}
        self.errors = errors or []
        self.source = source
        self.collection_time = collection_time or datetime.now()

    @property
    def success(self) -> bool:
        """Whether the collection was successful."""
        return not self.data.is_empty() and len(self.errors) == 0

    @property
    def records_collected(self) -> int:
        """Number of records collected."""
        return len(self.data) if not self.data.is_empty() else 0

    def __str__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        return f"DataCollectionResult({status}, {self.records_collected} records, {len(self.errors)} errors)"


class BaseEnergyDataCollector(ABC):
    """Abstract base class for energy data collectors."""

    def __init__(self, name: str, config: Dict[str, Any] = None):
        """Initialize collector.

        Args:
            name: Name of the collector
            config: Configuration dictionary
        """
        self.name = name
        self.config = config or {}
        self.logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    async def collect_demand_data(
        self,
        start_date: str,
        end_date: str,
        region: str,
        **kwargs
    ) -> DataCollectionResult:
        """Collect electricity demand data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            region: Region identifier
            **kwargs: Additional parameters

        Returns:
            DataCollectionResult with demand data
        """
        pass

    @abstractmethod
    async def collect_generation_data(
        self,
        start_date: str,
        end_date: str,
        region: str,
        **kwargs
    ) -> DataCollectionResult:
        """Collect electricity generation data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            region: Region identifier
            **kwargs: Additional parameters

        Returns:
            DataCollectionResult with generation data
        """
        pass

    async def collect_comprehensive_data(
        self,
        start_date: str,
        end_date: str,
        region: str,
        **kwargs
    ) -> Dict[str, DataCollectionResult]:
        """Collect both demand and generation data.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            region: Region identifier
            **kwargs: Additional parameters

        Returns:
            Dictionary with 'demand' and 'generation' results
        """
        self.logger.info(f"Collecting comprehensive data for {region} from {start_date} to {end_date}")

        # Collect both types of data
        demand_result = await self.collect_demand_data(start_date, end_date, region, **kwargs)
        generation_result = await self.collect_generation_data(start_date, end_date, region, **kwargs)

        return {
            "demand": demand_result,
            "generation": generation_result
        }

    def validate_date_range(self, start_date: str, end_date: str) -> None:
        """Validate date range parameters.

        Args:
            start_date: Start date string
            end_date: End date string

        Raises:
            ValueError: If dates are invalid
        """
        try:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)

            if start >= end:
                raise ValueError(f"Start date {start_date} must be before end date {end_date}")

            # Check if date range is reasonable (not too large)
            days = (end - start).days
            if days > 365 * 5:  # 5 years max
                self.logger.warning(f"Large date range requested: {days} days")

        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")

    def create_error_result(self, error_message: str, source: str = None) -> DataCollectionResult:
        """Create a result object for errors.

        Args:
            error_message: Error description
            source: Data source name

        Returns:
            DataCollectionResult with error
        """
        return DataCollectionResult(
            data=None,
            errors=[error_message],
            source=source or self.name
        )

    def get_collector_info(self) -> Dict[str, Any]:
        """Get information about this collector.

        Returns:
            Dictionary with collector information
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "config": self.config
        }
