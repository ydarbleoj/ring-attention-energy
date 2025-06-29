import requests
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta

class EIAClient:
    """
    US Energy Information Administration API client
    Free tier: 5,000 requests/hour
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.eia.gov/v2"

    def get_electricity_demand(
        self,
        region: str = "US48",  # Lower 48 states
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01"
    ) -> pd.DataFrame:
        """Get hourly electricity demand data"""

        url = f"{self.base_url}/electricity/rto/region-data/data/"
        params = {
            'api_key': self.api_key,
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

        response = requests.get(url, params=params)
        data = response.json()

        return pd.DataFrame(data['response']['data'])

    def get_generation_mix(self, region: str = "US48") -> pd.DataFrame:
        """Get generation by fuel type"""
        url = f"{self.base_url}/electricity/rto/fuel-type-data/data/"
        # Similar pattern...
        pass

# Get your free API key at: https://www.eia.gov/opendata/register.php