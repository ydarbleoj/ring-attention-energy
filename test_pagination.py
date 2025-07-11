#!/usr/bin/env python3
"""
Quick test to verify pagination is working correctly
"""
import sys
import os
from datetime import date

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from src.core.integrations.eia.client import EIAClient

def test_pagination():
    """Test that pagination is working correctly."""
    print("🔍 Testing EIA API Pagination...")

    client = EIAClient()

    # Test with a date range that should return > 5000 records
    print("📊 Testing demand data for CAL (2019-01-01 to 2019-03-31)")
    result = client.get_electricity_demand(
        region="CAL",
        start_date="2019-01-01",
        end_date="2019-03-31"
    )

    print(f"✅ Retrieved {len(result)} demand records")
    print(f"   Expected ~2160 records (90 days * 24 hours)")
    print(f"   Status: {'✅ Reasonable' if 2000 <= len(result) <= 3000 else '❌ Unexpected count'}")

    if len(result) > 0:
        print(f"   Date range: {result['timestamp'].min()} to {result['timestamp'].max()}")

    return len(result)

if __name__ == "__main__":
    test_pagination()
