import logging
from typing import Dict, List, Optional
from datetime import date

from .client import EIAClient
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
        params = EIAEndpoints.get_demand_params(regions, start_date, end_date)
        endpoint_path = EIAEndpoints.get_endpoint_path('demand')

        # Make API request
        return self.client.make_paginated_request(endpoint_path, params)

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
        params = EIAEndpoints.get_generation_params(regions, start_date, end_date)
        endpoint_path = EIAEndpoints.get_endpoint_path('generation')

        # Make API request
        return self.client.make_paginated_request(endpoint_path, params)


class EIAPriceService:
    """Service for retail electricity price data operations."""

    def __init__(self, client: EIAClient):
        """Initialize with EIA client."""
        self.client = client

    def get_raw_price_data(
        self,
        states: List[str],
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw retail price data for multiple states.

        Args:
            states: List of state codes (e.g., ['OR', 'WA'])
            start_date: Start date in YYYY-MM format (monthly data)
            end_date: End date in YYYY-MM format (monthly data)

        Returns:
            Raw JSON response from EIA API
        """
        params = EIAEndpoints.get_price_params(states, start_date, end_date)
        endpoint_path = EIAEndpoints.get_endpoint_path('price')

        # Make API request
        return self.client.make_paginated_request(endpoint_path, params)


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
        self.price = EIAPriceService(self.client)

    def get_raw_data(
        self,
        data_type: str,
        regions: List[str],
        start_date: str,
        end_date: str
    ) -> Dict:
        """
        Get raw data for any data type and regions/states.

        Args:
            data_type: Either 'demand', 'generation', or 'price'
            regions: List of region codes (for demand/generation) or state codes (for price)
            start_date: Start date in YYYY-MM-DD format (hourly) or YYYY-MM format (monthly)
            end_date: End date in YYYY-MM-DD format (hourly) or YYYY-MM format (monthly)

        Returns:
            Raw JSON response from EIA API
        """
        if data_type == 'demand':
            return self.demand.get_raw_demand_data(regions, start_date, end_date)
        elif data_type == 'generation':
            return self.generation.get_raw_generation_data(regions, start_date, end_date)
        elif data_type == 'price':
            # For price data, we need to convert regions to states and format dates properly
            # Convert PACW -> OR, etc. For now, assume OR for all regions
            states = ["OR"] if regions else ["OR"]  # Default to Oregon
            # Convert daily dates to monthly for price API
            start_month = start_date[:7]  # YYYY-MM-DD -> YYYY-MM
            end_month = end_date[:7]      # YYYY-MM-DD -> YYYY-MM
            return self.price.get_raw_price_data(states, start_month, end_month)
        else:
            raise ValueError(f"Unknown data type: {data_type}. Use 'demand', 'generation', or 'price'")
