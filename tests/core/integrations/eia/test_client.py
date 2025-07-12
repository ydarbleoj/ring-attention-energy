import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

from src.core.integrations.eia.client import EIAClient
from src.core.integrations.config import get_test_config
from tests.vcr_config import VCRManager, validate_energy_data_response

OREGON_TEST_DATES = {
    "start": "2023-01-01",
    "end": "2023-01-08"  # One week for testing
}


class TestEIAClientBasic:

    def setup_method(self):
        """Set up test configuration and client"""
        self.config = get_test_config()
        self.config.api.eia_api_key = os.getenv('EIA_API_KEY', 'TEST_KEY_FOR_VCR')
        self.client = EIAClient(config=self.config)

    def test_client_initialization(self):
        """Test EIA client initializes correctly"""
        assert self.client.api_key == os.getenv('EIA_API_KEY', 'TEST_KEY_FOR_VCR')
        assert self.client.base_url == "https://api.eia.gov/v2"
        assert self.client.rate_limit_delay > 0
        assert self.client.session is not None

    def test_client_initialization_without_api_key(self):
        """Test that client raises error when no API key provided"""
        config = get_test_config()
        config.api.eia_api_key = None

        with pytest.raises(ValueError, match="EIA API key is required"):
            EIAClient(config=config)

    def test_connection(self) -> bool:
        """
        Test if the API key is valid and connection works.

        Returns:
            True if connection successful, False otherwise
        """
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

        data = self.client.make_request("/electricity/rto/region-data/data/", params)

        assert 'response' in data and 'data' in data['response']
        assert len(data['response']['data']) > 0, "No data returned from EIA API"