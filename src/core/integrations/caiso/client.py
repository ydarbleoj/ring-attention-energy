import requests
import pandas as pd
import xml.etree.ElementTree as ET
import io
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .schema import (
    CAISODemandResponse,
    CAISOGenerationResponse,
    CAISOSystemDemand,
    CAISOGeneration,
    CAISOAPIError,
    RegionalEnergyMetrics
)

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
        self.session = self._create_optimized_session()
        self.rate_limit_delay = 5.0   # More conservative for CAISO
        self.optimized_delay = 3.0    # Still conservative for parallel requests

    def _create_optimized_session(self) -> requests.Session:
        """Create session with optimized retry strategy and connection pooling for CAISO"""
        session = requests.Session()

        # Optimized retry strategy for CAISO API
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        # Connection pooling optimized for CAISO
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=5,   # Smaller pool for CAISO
            pool_maxsize=10,      # Conservative max connections
            pool_block=False
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _make_request(self, params: Dict) -> Union[str, bytes]:
        """Make optimized API request with improved error handling for CAISO"""
        max_retries = 3
        retry_delay = 5.0  # Start with longer delay

        for attempt in range(max_retries):
            try:
                # Use longer delays for CAISO to avoid rate limiting
                delay = self.optimized_delay if hasattr(self, 'optimized_delay') else self.rate_limit_delay
                logger.debug(f"Waiting {delay}s before CAISO API request (attempt {attempt + 1})")
                time.sleep(delay)

                response = self.session.get(self.base_url, params=params, timeout=90)  # Longer timeout
                response.raise_for_status()

                # Return raw content for ZIP files, text for CSV
                if response.headers.get('content-type', '').lower().startswith('application/zip') or \
                   response.content.startswith(b'PK'):  # ZIP file signature
                    logger.debug(f"CAISO API returned ZIP file, size: {len(response.content)} bytes")
                    return response.content
                else:
                    logger.debug(f"CAISO API returned text, length: {len(response.text)}")
                    return response.text

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Rate limited
                    if attempt < max_retries - 1:
                        retry_delay = min(retry_delay * 2, 60)  # Cap at 60 seconds
                        logger.warning(f"CAISO rate limited, retrying in {retry_delay} seconds (attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        continue
                    else:
                        logger.error(f"CAISO rate limited after {max_retries} attempts")
                        raise
                else:
                    logger.error(f"CAISO API HTTP error: {e.response.status_code} - {e}")
                    raise
            except requests.exceptions.RequestException as e:
                logger.error(f"CAISO API request failed: {e}")
                if attempt < max_retries - 1:
                    logger.warning(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                    continue
                raise

    def _parse_csv_response(self, response_data: Union[str, bytes]) -> pd.DataFrame:
        """Parse CSV response from CAISO API with improved error handling for both text and binary"""
        try:
            # Handle binary ZIP files
            if isinstance(response_data, bytes):
                try:
                    zip_file = zipfile.ZipFile(io.BytesIO(response_data))

                    for file_name in zip_file.namelist():
                        # Check for XML error files first
                        if file_name.endswith('.xml') and ('INVALID_REQUEST' in file_name or 'ERROR' in file_name):
                            logger.warning(f"CAISO API returned error file: {file_name}")
                            with zip_file.open(file_name) as xml_file:
                                xml_content = xml_file.read()
                                self._parse_caiso_error(xml_content)
                            return pd.DataFrame()

                        elif file_name.endswith('.xml'):
                            logger.warning(f"CAISO API returned XML file (possibly error): {file_name}")
                            with zip_file.open(file_name) as xml_file:
                                xml_content = xml_file.read()
                                if self._check_xml_for_errors(xml_content):
                                    return pd.DataFrame()

                        elif file_name.endswith('.csv'):
                            logger.debug(f"Found CSV file in ZIP: {file_name}")
                            with zip_file.open(file_name) as csv_file:
                                # Read with flexible options to handle CAISO format issues
                                df = pd.read_csv(
                                    csv_file,
                                    skipinitialspace=True,
                                    encoding='utf-8',
                                    on_bad_lines='skip'  # For pandas 1.3+
                                )
                                logger.debug(f"Successfully parsed CSV from ZIP: {len(df)} rows, columns: {list(df.columns)}")
                                return df

                    logger.warning("No CSV files found in ZIP")
                    return pd.DataFrame()

                except Exception as zip_error:
                    logger.error(f"ZIP parsing failed: {zip_error}")
                    return pd.DataFrame()

            # Handle text responses (string)
            response_text = response_data

            # Check if response is actually a ZIP file (common with CAISO)
            if len(response_text) > 100 and not response_text.startswith('data_item'):
                try:
                    # Try to parse as ZIP file first
                    zip_bytes = response_text.encode('latin1') if isinstance(response_text, str) else response_text
                    zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))

                    for file_name in zip_file.namelist():
                        if file_name.endswith('.csv'):
                            with zip_file.open(file_name) as csv_file:
                                # Read with flexible options to handle CAISO format issues
                                return pd.read_csv(
                                    csv_file,
                                    skipinitialspace=True,
                                    encoding='utf-8',
                                    on_bad_lines='skip'  # For pandas 1.3+
                                )
                except Exception as zip_error:
                    logger.debug(f"ZIP parsing failed, trying direct CSV: {zip_error}")

            # Try direct CSV parsing with robust options for different pandas versions
            try:
                # For pandas 1.3+
                return pd.read_csv(
                    io.StringIO(response_text),
                    skipinitialspace=True,
                    encoding='utf-8',
                    on_bad_lines='skip'
                )
            except (TypeError, ValueError) as e:
                # Fallback for older pandas or other issues
                logger.debug(f"Modern pandas options failed, trying basic parsing: {e}")
                return pd.read_csv(
                    io.StringIO(response_text),
                    skipinitialspace=True,
                    encoding='utf-8'
                )

        except Exception as e:
            logger.error(f"Error parsing CAISO response: {e}")
            logger.debug(f"Response preview: {response_text[:200]}...")
            return pd.DataFrame()

    def _check_xml_for_errors(self, xml_content: bytes) -> bool:
        """Check XML content for CAISO API errors and log them."""
        try:
            root = ET.fromstring(xml_content)

            # Look for error elements
            for elem in root.iter():
                if 'ERROR' in elem.tag or 'error' in elem.tag.lower():
                    for error_child in elem:
                        if 'ERR_CODE' in error_child.tag:
                            error_code = error_child.text
                        elif 'ERR_DESC' in error_child.tag:
                            error_desc = error_child.text
                            logger.error(f"CAISO API Error {error_code}: {error_desc}")
                            return True

            return False

        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            return True

    def _parse_caiso_error(self, xml_content: bytes) -> None:
        """Parse CAISO error XML and log detailed error information."""
        try:
            root = ET.fromstring(xml_content)

            error_code = None
            error_desc = None

            # Extract error details
            for elem in root.iter():
                if 'ERR_CODE' in elem.tag and elem.text:
                    error_code = elem.text.strip()
                elif 'ERR_DESC' in elem.tag and elem.text:
                    error_desc = elem.text.strip()

            if error_code and error_desc:
                logger.error(f"CAISO API Error {error_code}: {error_desc}")
            else:
                logger.error("CAISO API returned error XML but could not parse error details")

        except ET.ParseError as e:
            logger.error(f"Could not parse CAISO error XML: {e}")

    def get_real_time_demand(
        self,
        start_date: str = None,
        end_date: str = None,
        validate: bool = True
    ) -> Union[CAISODemandResponse, pd.DataFrame]:
        """
        Get real-time system demand with optional schema validation

        Args:
            start_date: Date in YYYYMMDD format (default: yesterday)
            end_date: Date in YYYYMMDD format (default: today)
            validate: Return validated Pydantic model if True, raw DataFrame if False

        Returns:
            CAISODemandResponse if validate=True, DataFrame if validate=False
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"Fetching CAISO real-time demand from {start_date} to {end_date}")

        # Use SLD_FCST (System Load and Demand Forecast) which has reliable data
        params = {
            'queryname': 'SLD_FCST',  # System Load and Demand Forecast
            'startdatetime': f"{start_date}T08:00-0000",  # UTC time (PST+8)
            'enddatetime': f"{end_date}T08:00-0000",
            'version': '1',
            'resultformat': '6'  # CSV format
        }

        try:
            response_data = self._make_request(params)
            df = self._parse_csv_response(response_data)

            if df.empty:
                logger.warning("No data returned from CAISO API")
                return CAISODemandResponse(data=[]) if validate else df

            logger.info(f"Retrieved {len(df)} demand records")

            if validate:
                # Return validated Pydantic model
                return CAISODemandResponse.from_dataframe(df)
            else:
                # Return raw DataFrame
                return df

        except Exception as e:
            logger.error(f"Error fetching CAISO demand data: {e}")
            if validate:
                return CAISODemandResponse(data=[])
            else:
                return pd.DataFrame()
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

    def get_generation_mix(
        self,
        start_date: str = None,
        end_date: str = None,
        validate: bool = True
    ) -> Union[CAISOGenerationResponse, pd.DataFrame]:
        """
        Get generation mix by fuel type

        Args:
            start_date: Date in YYYYMMDD format (default: yesterday)
            end_date: Date in YYYYMMDD format (default: today)
            validate: Return validated Pydantic model if True, raw DataFrame if False

        Returns:
            CAISOGenerationResponse if validate=True, DataFrame if validate=False
        """
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        logger.info(f"Fetching CAISO generation mix from {start_date} to {end_date}")

        params = {
            'queryname': 'ENE_SLRS',  # Energy Slrs (Generation by fuel type)
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
                logger.warning("No generation data returned from CAISO API")
                return CAISOGenerationResponse(data=[]) if validate else df

            logger.info(f"Retrieved {len(df)} generation records")

            if validate:
                return CAISOGenerationResponse.from_dataframe(df)
            else:
                return df

        except Exception as e:
            logger.error(f"Error fetching CAISO generation data: {e}")
            if validate:
                return CAISOGenerationResponse(data=[])
            else:
                return pd.DataFrame()

    def get_regional_metrics(
        self,
        start_date: str = None,
        end_date: str = None
    ) -> List[RegionalEnergyMetrics]:
        """
        Get comprehensive regional energy metrics for migration analysis

        Args:
            start_date: Date in YYYYMMDD format (default: yesterday)
            end_date: Date in YYYYMMDD format (default: today)

        Returns:
            List of RegionalEnergyMetrics objects
        """
        # Get demand and generation data
        demand_response = self.get_real_time_demand(start_date, end_date, validate=True)
        generation_response = self.get_generation_mix(start_date, end_date, validate=True)

        if not demand_response.data or not generation_response.data:
            logger.warning("Insufficient data for regional metrics")
            return []

        # Process data into regional metrics
        metrics = []

        # Group demand data by hour/day for aggregation
        demand_by_time = {}
        for demand in demand_response.data:
            time_key = demand.timestamp.replace(minute=0, second=0, microsecond=0)
            if time_key not in demand_by_time:
                demand_by_time[time_key] = []
            demand_by_time[time_key].append(demand.demand_mw)

        # Group generation data by hour/day and fuel type
        generation_by_time = {}
        for gen in generation_response.data:
            time_key = gen.timestamp.replace(minute=0, second=0, microsecond=0)
            if time_key not in generation_by_time:
                generation_by_time[time_key] = {'total': 0, 'renewable': 0, 'by_fuel': {}}

            generation_by_time[time_key]['total'] += gen.generation_mw
            generation_by_time[time_key]['by_fuel'][gen.fuel_type] = gen.generation_mw

            # Count renewable sources
            renewable_fuels = {'SOLAR', 'WIND', 'HYDRO', 'GEOTHERMAL', 'BIOGAS', 'BIOMASS'}
            if gen.fuel_type.upper() in renewable_fuels:
                generation_by_time[time_key]['renewable'] += gen.generation_mw

        # Create metrics for each time period
        for timestamp in sorted(demand_by_time.keys()):
            if timestamp in generation_by_time:
                demand_data = demand_by_time[timestamp]
                gen_data = generation_by_time[timestamp]

                avg_demand = sum(demand_data) / len(demand_data)
                peak_demand = max(demand_data)
                total_gen = gen_data['total']
                renewable_gen = gen_data['renewable']

                metrics.append(RegionalEnergyMetrics(
                    region="California",
                    timestamp=timestamp,
                    total_demand_mw=avg_demand,
                    total_generation_mw=total_gen,
                    renewable_generation_mw=renewable_gen,
                    renewable_percentage=(renewable_gen / total_gen * 100) if total_gen > 0 else 0,
                    peak_demand_mw=peak_demand,
                    load_factor=avg_demand / peak_demand if peak_demand > 0 else 0,
                    # TODO: Add more sophisticated economic and grid stress calculations
                    economic_activity_index=avg_demand / 1000,  # Simplified proxy
                    grid_stress_index=max(0, (peak_demand - avg_demand) / avg_demand) if avg_demand > 0 else 0
                ))

        logger.info(f"Generated {len(metrics)} regional metrics")
        return metrics