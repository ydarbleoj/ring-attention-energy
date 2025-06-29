import requests
import xml.etree.ElementTree as ET
from datetime import datetime

class CAISOClient:
    """
    California Independent System Operator
    Real-time grid data, no API key needed!
    """

    def __init__(self):
        self.base_url = "http://oasis.caiso.com/oasisapi/SingleZip"

    def get_real_time_demand(self, date: str = None) -> pd.DataFrame:
        """Get real-time system demand"""
        if not date:
            date = datetime.now().strftime("%Y%m%d")

        params = {
            'queryname': 'SLD_RTO',  # System Load Demand
            'startdatetime': f"{date}T00:00-0000",
            'enddatetime': f"{date}T23:59-0000",
            'version': '1',
            'market_run_id': 'RTM',  # Real-time market
            'resultformat': '6'  # CSV format
        }

        response = requests.get(self.base_url, params=params)
        # Parse CSV response...
        return pd.read_csv(io.StringIO(response.text))