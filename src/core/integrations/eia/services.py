"""
EIA Data Services

These services handle business logic for EIA data operations:
- Demand service: Handles electricity demand data
- Generation service: Handles electricity generation data

Services use the minimal EIA client for HTTP operations and
handle all data processing, multi-region logic, etc.
"""
import logging
from typing import Dict, List, Optional
from datetime import date

from .client_new import EIAClient
from .schema import EIAEndpoints

logger = logging.getLogger(__name__)


class EIADemandService:
    """Service for electricity demand data operations."""

    def __init__(self, client: EIAClient):
        """Initialize with EIA client."""
        self.client = client

    def get_raw_demand_data(
        self,
        regions: List[str],
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw demand data for multiple regions.

        Args:
            regions: List of region codes (e.g., ['PACW', 'ERCO'])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Raw JSON response from EIA API
        """
        logger.info(f"Fetching raw demand data for {len(regions)} regions: {regions}")

        # Build parameters using schema
        params = EIAEndpoints.get_demand_params(regions, start_date, end_date)
        endpoint_path = EIAEndpoints.get_endpoint_path('demand')

        # Make API request
        return self.client.make_paginated_request(endpoint_path, params)

    def get_raw_demand_data_single_region(
        self,
        region: str,
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw demand data for a single region.

        Args:
            region: Region code (e.g., 'PACW')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Raw JSON response from EIA API
        """
        return self.get_raw_demand_data([region], start_date, end_date)


class EIAGenerationService:
    """Service for electricity generation data operations."""

    def __init__(self, client: EIAClient):
        """Initialize with EIA client."""
        self.client = client

    def get_raw_generation_data(
        self,
        regions: List[str],
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw generation data for multiple regions.

        Args:
            regions: List of region codes (e.g., ['PACW', 'ERCO'])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Raw JSON response from EIA API
        """
        logger.info(f"Fetching raw generation data for {len(regions)} regions: {regions}")

        # Build parameters using schema
        params = EIAEndpoints.get_generation_params(regions, start_date, end_date)
        endpoint_path = EIAEndpoints.get_endpoint_path('generation')

        # Make API request
        return self.client.make_paginated_request(endpoint_path, params)

    def get_raw_generation_data_single_region(
        self,
        region: str,
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw generation data for a single region.

        Args:
            region: Region code (e.g., 'PACW')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Raw JSON response from EIA API
        """
        return self.get_raw_generation_data([region], start_date, end_date)


class EIADataService:
    """
    Combined service that provides access to both demand and generation services.

    This is the main service that should be used by collectors and orchestrators.
    """

    def __init__(self, api_key: str = None, config=None):
        """
        Initialize EIA data service.

        Args:
            api_key: EIA API key
            config: Configuration object
        """
        # Create the minimal client
        self.client = EIAClient(api_key=api_key, config=config)

        # Create specialized services
        self.demand = EIADemandService(self.client)
        self.generation = EIAGenerationService(self.client)

    def test_connection(self) -> bool:
        """Test if the EIA API connection works."""
        return self.client.test_connection()

    def get_raw_data(
        self,
        data_type: str,
        regions: List[str],
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw data for any data type and regions.

        Args:
            data_type: Either 'demand' or 'generation'
            regions: List of region codes
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Raw JSON response from EIA API
        """
        if data_type == 'demand':
            return self.demand.get_raw_demand_data(regions, start_date, end_date)
        elif data_type == 'generation':
            return self.generation.get_raw_generation_data(regions, start_date, end_date)
        else:
            raise ValueError(f"Unknown data type: {data_type}. Use 'demand' or 'generation'")

    def get_raw_data_single_region(
        self,
        data_type: str,
        region: str,
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw data for a single region (backwards compatibility).

        Args:
            data_type: Either 'demand' or 'generation'
            region: Region code
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Raw JSON response from EIA API
        """
        return self.get_raw_data(data_type, [region], start_date, end_date)
