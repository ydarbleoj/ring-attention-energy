import requests
import pandas as pd
import xml.etree.ElementTree as ET
import io
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import logging
import time

logger = logging.getLogger(__name__)


class CAISOClient:
    """
    California Independent System Operator (CAISO)
    Real-time grid data, no API key needed!

    CAISO OASIS API Documentation:
    http://www.caiso.com/Pages/documentsbygroup.aspx?GroupID=41B850C2-33A4-4B02-B87E-5F1FE0074A79
    """

    def __init__(self):
        self.base_url = "http://oasis.caiso.com/oasisapi/SingleZip"
        self.session = requests.Session()
        self.rate_limit_delay = 1.0  # Be respectful with requests

    def _make_request(self, params: Dict) -> str:
        """Make API request and return response text"""
        try:
            time.sleep(self.rate_limit_delay)
            response = self.session.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            return response.text

        except requests.exceptions.RequestException as e:
            logger.error(f"CAISO API request failed: {e}")
            raise

    def _parse_csv_response(self, response_text: str) -> pd.DataFrame:
        """Parse CSV response from CAISO API"""
        try:
            # CAISO returns data in a zip file containing CSV
            # First, try to read as direct CSV
            if response_text.startswith('data_item'):
                return pd.read_csv(io.StringIO(response_text))

            # If it's a zip file, extract and read
            try:
                zip_file = zipfile.ZipFile(io.BytesIO(response_text.encode('latin1')))
                for file_name in zip_file.namelist():
                    if file_name.endswith('.csv'):
                        with zip_file.open(file_name) as csv_file:
                            return pd.read_csv(csv_file)
            except:
                pass

            # Try direct CSV parsing
            return pd.read_csv(io.StringIO(response_text))

        except Exception as e:
            logger.error(f"Error parsing CAISO response: {e}")
            return pd.DataFrame()

    def get_real_time_demand(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        Get real-time system demand

        Args:
            start_date: Date in YYYYMMDD format (default: yesterday)
            end_date: Date in YYYYMMDD format (default: today)

        Returns:
            DataFrame with timestamp and demand columns
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"Fetching CAISO real-time demand from {start_date} to {end_date}")

        params = {
            'queryname': 'SLD_RTO',  # System Load Demand
            'startdatetime': f"{start_date}T00:00-0000",
            'enddatetime': f"{end_date}T23:59-0000",
            'version': '1',
            'market_run_id': 'RTM',  # Real-time market
            'resultformat': '6'  # CSV format
        }

        try:
            response_text = self._make_request(params)
            df = self._parse_csv_response(response_text)

            if df.empty:
                logger.warning("No demand data returned from CAISO API")
                return df

            # Process CAISO specific columns
            if 'INTERVALSTARTTIME_GMT' in df.columns:
                df['timestamp'] = pd.to_datetime(df['INTERVALSTARTTIME_GMT'])
            elif 'OPR_DT' in df.columns and 'OPR_HR' in df.columns:
                # Combine date and hour
                df['timestamp'] = pd.to_datetime(df['OPR_DT']) + pd.to_timedelta(df['OPR_HR'], unit='h')

            if 'MW' in df.columns:
                df['demand'] = pd.to_numeric(df['MW'], errors='coerce')
            elif 'VALUE' in df.columns:
                df['demand'] = pd.to_numeric(df['VALUE'], errors='coerce')

            result = df[['timestamp', 'demand']].copy()
            result = result.dropna()

            logger.info(f"Retrieved {len(result)} CAISO demand data points")
            return result

        except Exception as e:
            logger.error(f"Error fetching CAISO demand data: {e}")
            return pd.DataFrame(columns=['timestamp', 'demand'])

    def get_renewable_generation(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        Get renewable generation data from CAISO

        Returns:
            DataFrame with renewable generation by source
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"Fetching CAISO renewable generation from {start_date} to {end_date}")

        # CAISO renewable data query
        params = {
            'queryname': 'SLD_REN_FCST',  # Renewable forecast/actual
            'startdatetime': f"{start_date}T00:00-0000",
            'enddatetime': f"{end_date}T23:59-0000",
            'version': '1',
            'market_run_id': 'RTM',
            'resultformat': '6'
        }

        try:
            response_text = self._make_request(params)
            df = self._parse_csv_response(response_text)

            if df.empty:
                logger.warning("No renewable data returned from CAISO API")
                return pd.DataFrame(columns=['timestamp', 'solar_generation', 'wind_generation'])

            # Process renewable data
            if 'INTERVALSTARTTIME_GMT' in df.columns:
                df['timestamp'] = pd.to_datetime(df['INTERVALSTARTTIME_GMT'])

            # CAISO has different renewable types
            result = pd.DataFrame()

            if 'RENEWABLE_TYPE' in df.columns and 'MW' in df.columns:
                # Pivot by renewable type
                renewable_pivot = df.pivot_table(
                    index='timestamp',
                    columns='RENEWABLE_TYPE',
                    values='MW',
                    aggfunc='first'
                ).reset_index()

                result = renewable_pivot

                # Map CAISO renewable types to standard names
                type_mapping = {
                    'Solar': 'solar_generation',
                    'Wind': 'wind_generation',
                    'Geothermal': 'geothermal_generation',
                    'Biomass': 'biomass_generation',
                    'Biogas': 'biogas_generation',
                    'Small Hydro': 'small_hydro_generation'
                }

                for caiso_type, standard_name in type_mapping.items():
                    if caiso_type in result.columns:
                        result[standard_name] = pd.to_numeric(result[caiso_type], errors='coerce')
                        result = result.drop(columns=[caiso_type])

            # Ensure we have at least solar and wind columns
            if 'solar_generation' not in result.columns:
                result['solar_generation'] = 0
            if 'wind_generation' not in result.columns:
                result['wind_generation'] = 0

            # Fill NaN values
            result = result.fillna(0)

            logger.info(f"Retrieved CAISO renewable generation data with {len(result)} time points")
            return result

        except Exception as e:
            logger.error(f"Error fetching CAISO renewable data: {e}")
            return pd.DataFrame(columns=['timestamp', 'solar_generation', 'wind_generation'])

    def get_electricity_prices(
        self,
        start_date: str = None,
        end_date: str = None,
        location: str = "TH_NP15_GEN-APND"  # Default pricing node
    ) -> pd.DataFrame:
        """
        Get electricity prices from CAISO

        Args:
            location: CAISO pricing node (default: NP15 generation)

        Returns:
            DataFrame with timestamp and price columns
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"Fetching CAISO electricity prices from {start_date} to {end_date}")

        params = {
            'queryname': 'PRC_LMP',  # Locational Marginal Prices
            'startdatetime': f"{start_date}T00:00-0000",
            'enddatetime': f"{end_date}T23:59-0000",
            'version': '1',
            'market_run_id': 'RTM',
            'node': location,
            'resultformat': '6'
        }

        try:
            response_text = self._make_request(params)
            df = self._parse_csv_response(response_text)

            if df.empty:
                logger.warning("No price data returned from CAISO API")
                return pd.DataFrame(columns=['timestamp', 'price'])

            # Process price data
            if 'INTERVALSTARTTIME_GMT' in df.columns:
                df['timestamp'] = pd.to_datetime(df['INTERVALSTARTTIME_GMT'])

            if 'MW' in df.columns:
                df['price'] = pd.to_numeric(df['MW'], errors='coerce')
            elif 'LMP_PRC' in df.columns:
                df['price'] = pd.to_numeric(df['LMP_PRC'], errors='coerce')

            result = df[['timestamp', 'price']].copy()
            result = result.dropna()

            logger.info(f"Retrieved {len(result)} CAISO price data points")
            return result

        except Exception as e:
            logger.error(f"Error fetching CAISO price data: {e}")
            return pd.DataFrame(columns=['timestamp', 'price'])

    def get_comprehensive_data(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        """
        Get comprehensive CAISO data combining all available metrics

        Returns:
            DataFrame with all available CAISO metrics
        """
        logger.info("Fetching comprehensive CAISO data")

        # Collect all data types
        demand_data = self.get_real_time_demand(start_date, end_date)
        renewable_data = self.get_renewable_generation(start_date, end_date)
        price_data = self.get_electricity_prices(start_date, end_date)

        # Start with demand data as base
        if demand_data.empty:
            logger.warning("No CAISO demand data available")
            return pd.DataFrame()

        result = demand_data.copy()

        # Merge renewable data
        if not renewable_data.empty:
            result = pd.merge(result, renewable_data, on='timestamp', how='left')

        # Merge price data
        if not price_data.empty:
            result = pd.merge(result, price_data, on='timestamp', how='left')

        # Fill any NaN values
        result = result.fillna(method='ffill').fillna(0)

        logger.info(f"Created comprehensive CAISO dataset with {len(result)} time points")
        return result

    def test_api_connection(self) -> bool:
        """Test if the CAISO API connection works"""
        try:
            # Simple test request for recent demand data
            test_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

            params = {
                'queryname': 'SLD_RTO',
                'startdatetime': f"{test_date}T12:00-0000",
                'enddatetime': f"{test_date}T13:00-0000",
                'version': '1',
                'market_run_id': 'RTM',
                'resultformat': '6'
            }

            response_text = self._make_request(params)
            df = self._parse_csv_response(response_text)

            if not df.empty:
                logger.info("CAISO API connection successful")
                return True
            else:
                logger.warning("CAISO API connection test returned empty data")
                return False

        except Exception as e:
            logger.error(f"CAISO API connection test failed: {e}")
            return False

    def get_available_query_names(self) -> List[str]:
        """Get list of available CAISO query names for exploration"""
        return [
            'SLD_RTO',      # System Load Demand
            'SLD_REN_FCST', # Renewable Forecast
            'PRC_LMP',      # Locational Marginal Prices
            'PRC_INTVL_LMP', # Interval LMP
            'ENE_SLRS',     # Solar generation
            'ENE_WIND',     # Wind generation
            'AS_RESULTS',   # Ancillary Services
            'GEN_FUEL_TYPE' # Generation by fuel type
        ]