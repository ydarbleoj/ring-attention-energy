import requests
import logging
from typing import Dict, List, Optional, Tuple
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
    def __init__(self, api_key: str = None, config=None):
        """
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
        """Create HTTP session with optimized connection pooling."""
        session = requests.Session()

        # Retry strategy for reliability
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Connection pooling for performance
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=50,
            pool_block=False
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Standard headers
        session.headers.update({
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'EIA-Client/1.0',
            'Accept': 'application/json'
        })

        return session

    def make_request(self, endpoint: str, params: Dict) -> Dict:
        request_params = params.copy()
        request_params['api_key'] = self.api_key

        # Set default length if not specified
        if 'length' not in request_params:
            request_params['length'] = 5000

        # Construct full URL
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.get(url, params=request_params)
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

    def make_paginated_request(self, endpoint: str, params: Dict, max_records: int = 100000) -> Dict:
        all_data = []
        offset = 0
        page_size = 5000

        # Add pagination parameters
        paginated_params = params.copy()
        paginated_params['length'] = page_size

        while True:
            # Set offset for this page
            paginated_params['offset'] = offset

            logger.debug(f"Fetching page at offset {offset}")
            response = self.make_request(endpoint, paginated_params)

            # Extract data from response
            page_data = response.get('response', {}).get('data', [])

            if not page_data:
                # No more data available
                break

            all_data.extend(page_data)

            # Check if we got less than page_size records (last page)
            if len(page_data) < page_size:
                break

            # Move to next page
            offset += page_size

            # Safety check to prevent infinite loops
            if offset > max_records:
                logger.warning(f"Reached maximum record limit ({max_records}), stopping pagination")
                break

        # Return response in same format as original
        return {
            'response': {
                'data': all_data,
                'total': len(all_data)
            }
        }

    def test_connection(self) -> bool:
        """
        Test if the API key is valid and connection works.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Simple test request
            params = {
                'frequency': 'hourly',
                'data[0]': 'value',
                'facets[respondent][]': 'US48',
                'facets[type][]': 'D',
                'start': '2023-01-01',
                'end': '2023-01-02',
                'length': 10
            }

            data = self.make_request("/electricity/rto/region-data/data/", params)

            if 'response' in data and 'data' in data['response']:
                logger.info("EIA API connection successful")
                return True
            else:
                logger.error("EIA API connection failed - unexpected response")
                return False

        except Exception as e:
            logger.error(f"EIA API connection test failed: {e}")
            return False
