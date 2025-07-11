#!/usr/bin/env python3
"""
Quick EIA data availability test to find actual data ranges.

This will test recent years first to validate the client works,
then search for the earliest available data systematically.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.integrations.eia.client import EIAClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_recent_data():
    """Test recent years to validate client functionality."""
    print("🔍 Testing recent data availability...")
    client = EIAClient()

    test_cases = [
        ('CAL', '2023-01-01', '2023-01-03'),
        ('TEX', '2022-01-01', '2022-01-03'),
        ('MISO', '2021-01-01', '2021-01-03')
    ]

    for region, start_date, end_date in test_cases:
        try:
            data = client.get_electricity_demand(region=region, start_date=start_date, end_date=end_date)
            print(f"✅ {region} {start_date[:4]}: {len(data)} records")
        except Exception as e:
            print(f"❌ {region} {start_date[:4]}: Error - {str(e)}")

def find_earliest_available_year(region, start_year=2010, end_year=2025):
    """Find the earliest year with available data for a region."""
    print(f"\n🔍 Finding earliest data for {region} ({start_year}-{end_year})...")
    client = EIAClient()

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-01-03"  # Small sample

        try:
            data = client.get_electricity_demand(region=region, start_date=start_date, end_date=end_date)
            if len(data) > 0:
                print(f"✅ {region}: First data found in {year} ({len(data)} records)")
                return year
            else:
                print(f"   {region} {year}: No data (empty response)")
        except Exception as e:
            print(f"   {region} {year}: Error - {str(e)}")

    print(f"❌ {region}: No data found in range {start_year}-{end_year}")
    return None

if __name__ == "__main__":
    print("🚀 EIA Data Availability Quick Test")
    print("=" * 50)

    # First test recent data to validate client
    test_recent_data()

    # Then find earliest available data for each major region
    regions = ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO']
    earliest_years = {}

    for region in regions:
        earliest_year = find_earliest_available_year(region, start_year=2010, end_year=2025)
        earliest_years[region] = earliest_year

    print(f"\n📊 Summary of Earliest Data Availability:")
    print("-" * 40)
    for region, year in earliest_years.items():
        if year:
            print(f"{region}: {year}")
        else:
            print(f"{region}: No data found")

    # Recommend strategy
    available_years = [year for year in earliest_years.values() if year is not None]
    if available_years:
        recommended_start = min(available_years)
        print(f"\n🎯 Recommended start year for historical loading: {recommended_start}")
        print(f"   Regions with data from {recommended_start}: {[region for region, year in earliest_years.items() if year and year <= recommended_start]}")
    else:
        print("\n❌ No data found for any region in tested range")
