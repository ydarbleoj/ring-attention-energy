"""
Quick setup script to get energy data flowing
Run this first to test your data connections
"""

import os
from src.core.integrations.eia.client import EIAClient
from src.core.integrations.caiso.client import CAISOClient

def setup_eia():
    """Setup EIA client with your API key"""
    api_key = os.getenv('EIA_API_KEY')
    if not api_key:
        print("🔑 Get your free EIA API key at: https://www.eia.gov/opendata/register.php")
        print("Then set: export EIA_API_KEY=your_key_here")
        return None

    client = EIAClient(api_key)
    print("✅ EIA client ready")
    return client

def test_caiso():
    """Test CAISO connection (no API key needed)"""
    client = CAISOClient()
    try:
        data = client.get_real_time_demand()
        print(f"✅ CAISO data: {len(data)} records retrieved")
        return client
    except Exception as e:
        print(f"❌ CAISO error: {e}")
        return None

def download_sample_data():
    """Download a week of data for testing"""
    eia = setup_eia()
    caiso = test_caiso()

    if eia:
        # Get recent demand data
        demand_data = eia.get_electricity_demand(
            start_date="2024-01-01",
            end_date="2024-01-08"  # Just one week for testing
        )
        demand_data.to_csv('data/sample_demand.csv', index=False)
        print(f"💾 Saved {len(demand_data)} demand records")

if __name__ == "__main__":
    download_sample_data()