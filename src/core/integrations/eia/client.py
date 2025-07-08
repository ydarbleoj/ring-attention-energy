import requests
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
import logging
import time
import sys
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Config import
try:
    from ..config import get_config
except ImportError:
    # Fallback for different import contexts
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import get_config

logger = logging.getLogger(__name__)


class EIAClient:
    """
    US Energy Information Administration API client
    Free tier: 5,000 requests/hour

    API Documentation: https://www.eia.gov/opendata/
    Get your free API key at: https://www.eia.gov/opendata/register.php
    """

    def __init__(self, api_key: str = None, config=None):
        """
        Initialize EIA client

        Args:
            api_key: EIA API key (if not provided, will load from config)
            config: Configuration instance (if not provided, will load default)
        """
        self.config = config or get_config()
        self.api_key = api_key or self.config.api.eia_api_key

        if not self.api_key:
            raise ValueError(
                "EIA API key is required. Either pass it directly or set EIA_API_KEY environment variable."
            )

        self.base_url = self.config.api.eia_base_url
        self.session = self._create_session()
        self.rate_limit_delay = self.config.api.eia_rate_limit_delay

    def _create_session(self) -> requests.Session:
        """Create session with optimized retry strategy and connection pooling"""
        session = requests.Session()

        # Optimized retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,  # Faster backoff
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Connection pooling for better performance
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # Connection pool size
            pool_maxsize=20,      # Max connections per pool
            pool_block=False      # Don't block on pool exhaustion
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _make_request(self, url: str, params: Dict) -> Dict:
        """Make API request with optimized rate limiting and error handling"""
        params['api_key'] = self.api_key

        try:
            # Optimized rate limiting - more aggressive for parallel requests
            # EIA allows 5000/hour = 1.39/sec, so 0.3s should be safe with 3 concurrent
            optimized_delay = max(0.3, self.rate_limit_delay * 0.4)
            time.sleep(optimized_delay)

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if 'response' not in data:
                raise ValueError(f"Unexpected API response structure: {data}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"EIA API request failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"EIA API response error: {e}")
            raise

    def get_electricity_demand(
        self,
        region: str = "US48",  # Lower 48 states
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01"
    ) -> pd.DataFrame:
        """
        Get hourly electricity demand data

        Args:
            region: Region code (US48, CAL, TEX, etc.)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with timestamp and demand columns
        """
        logger.info(f"Fetching electricity demand for {region} from {start_date} to {end_date}")

        url = f"{self.base_url}/electricity/rto/region-data/data/"
        params = {
            'frequency': 'hourly',
            'data[0]': 'value',
            'facets[respondent][]': region,
            'facets[type][]': 'D',  # Demand
            'start': start_date,
            'end': end_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'length': 5000
        }

        try:
            data = self._make_request(url, params)
            df = pd.DataFrame(data['response']['data'])

            if df.empty:
                logger.warning("No demand data returned from EIA API")
                return df

            # Process the data
            df['timestamp'] = pd.to_datetime(df['period'])
            df['demand'] = pd.to_numeric(df['value'], errors='coerce')

            # Keep only relevant columns
            result = df[['timestamp', 'demand']].copy()

            logger.info(f"Retrieved {len(result)} demand data points")
            return result

        except Exception as e:
            logger.error(f"Error fetching demand data: {e}")
            return pd.DataFrame(columns=['timestamp', 'demand'])

    def get_generation_mix(
        self,
        region: str = "US48",
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01"
    ) -> pd.DataFrame:
        """
        Get generation by fuel type

        Returns:
            DataFrame with timestamp and generation by fuel type
        """
        logger.info(f"Fetching generation mix for {region}")

        url = f"{self.base_url}/electricity/rto/fuel-type-data/data/"
        params = {
            'frequency': 'hourly',
            'data[0]': 'value',
            'facets[respondent][]': region,
            'start': start_date,
            'end': end_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'length': 5000
        }

        try:
            data = self._make_request(url, params)
            df = pd.DataFrame(data['response']['data'])

            if df.empty:
                logger.warning("No generation data returned from EIA API")
                return df

            # Process the data
            df['timestamp'] = pd.to_datetime(df['period'])
            df['generation'] = pd.to_numeric(df['value'], errors='coerce')

            # Pivot by fuel type
            result = df.pivot_table(
                index='timestamp',
                columns='fueltype',
                values='generation',
                aggfunc='first'
            ).reset_index()

            # Flatten column names
            result.columns.name = None

            # Rename columns to be more descriptive
            fuel_mapping = {
                'COL': 'coal_generation',
                'NG': 'natural_gas_generation',
                'NUC': 'nuclear_generation',
                'OIL': 'oil_generation',
                'SUN': 'solar_generation',
                'WAT': 'hydro_generation',
                'WND': 'wind_generation',
                'OTH': 'other_generation'
            }

            for old_name, new_name in fuel_mapping.items():
                if old_name in result.columns:
                    result = result.rename(columns={old_name: new_name})

            logger.info(f"Retrieved generation mix data with {len(result)} time points")
            return result

        except Exception as e:
            logger.error(f"Error fetching generation mix: {e}")
            return pd.DataFrame(columns=['timestamp'])

    def get_renewable_generation(
        self,
        region: str = "US48",
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01"
    ) -> pd.DataFrame:
        """
        Get renewable generation data (solar + wind)

        Returns:
            DataFrame with renewable generation time series
        """
        generation_mix = self.get_generation_mix(region, start_date, end_date)

        if generation_mix.empty:
            return pd.DataFrame(columns=['timestamp', 'solar_generation', 'wind_generation'])

        result = generation_mix[['timestamp']].copy()

        # Extract renewable sources
        if 'solar_generation' in generation_mix.columns:
            result['solar_generation'] = generation_mix['solar_generation'].fillna(0)
        else:
            result['solar_generation'] = 0

        if 'wind_generation' in generation_mix.columns:
            result['wind_generation'] = generation_mix['wind_generation'].fillna(0)
        else:
            result['wind_generation'] = 0

        result['total_renewable'] = result['solar_generation'] + result['wind_generation']

        return result

    def get_electricity_prices(
        self,
        region: str = "US48",
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01"
    ) -> pd.DataFrame:
        """
        Get electricity price data

        Note: Real-time pricing data may not be available for all regions
        """
        logger.info(f"Fetching electricity prices for {region}")

        # EIA doesn't always have real-time pricing, so this might return empty
        # In a real implementation, you'd use wholesale market APIs

        url = f"{self.base_url}/electricity/wholesale-prices/data/"
        params = {
            'frequency': 'hourly',
            'data[0]': 'value',
            'start': start_date,
            'end': end_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'length': 5000
        }

        try:
            data = self._make_request(url, params)
            df = pd.DataFrame(data['response']['data'])

            if df.empty:
                logger.warning("No price data available from EIA API")
                return pd.DataFrame(columns=['timestamp', 'price'])

            df['timestamp'] = pd.to_datetime(df['period'])
            df['price'] = pd.to_numeric(df['value'], errors='coerce')

            result = df[['timestamp', 'price']].copy()

            logger.info(f"Retrieved {len(result)} price data points")
            return result

        except Exception as e:
            logger.warning(f"Price data not available: {e}")
            return pd.DataFrame(columns=['timestamp', 'price'])

    def get_comprehensive_data(
        self,
        region: str = "US48",
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01"
    ) -> pd.DataFrame:
        """
        Get comprehensive energy data combining all available metrics

        Returns:
            DataFrame with all available energy metrics
        """
        logger.info(f"Fetching comprehensive energy data for {region}")

        # Collect all data types
        demand_data = self.get_electricity_demand(region, start_date, end_date)
        renewable_data = self.get_renewable_generation(region, start_date, end_date)
        price_data = self.get_electricity_prices(region, start_date, end_date)

        # Start with demand data as base
        if demand_data.empty:
            logger.error("No demand data available - cannot create comprehensive dataset")
            return pd.DataFrame()

        result = demand_data.copy()

        # Merge renewable data
        if not renewable_data.empty:
            result = pd.merge(result, renewable_data, on='timestamp', how='left')
        else:
            result['solar_generation'] = 0
            result['wind_generation'] = 0
            result['total_renewable'] = 0

        # Merge price data
        if not price_data.empty:
            result = pd.merge(result, price_data, on='timestamp', how='left')
        else:
            # Estimate prices based on demand if not available
            if 'demand' in result.columns:
                # Simple price model: higher demand = higher prices
                demand_normalized = (result['demand'] - result['demand'].min()) / (result['demand'].max() - result['demand'].min())
                result['price'] = 30 + 40 * demand_normalized  # $30-70/MWh range

        # Fill any remaining NaN values
        result = result.fillna(method='ffill').fillna(method='bfill')

        logger.info(f"Created comprehensive dataset with {len(result)} time points and {len(result.columns)} features")

        return result

    def test_api_connection(self) -> bool:
        """Test if the API key is valid and connection works"""
        try:
            # Simple test request
            url = f"{self.base_url}/electricity/rto/region-data/data/"
            params = {
                'frequency': 'hourly',
                'data[0]': 'value',
                'facets[respondent][]': 'US48',
                'facets[type][]': 'D',
                'start': '2023-01-01',
                'end': '2023-01-02',
                'length': 10
            }

            data = self._make_request(url, params)

            if 'response' in data and 'data' in data['response']:
                logger.info("EIA API connection successful")
                return True
            else:
                logger.error("EIA API connection failed - unexpected response")
                return False

        except Exception as e:
            logger.error(f"EIA API connection test failed: {e}")
            return False