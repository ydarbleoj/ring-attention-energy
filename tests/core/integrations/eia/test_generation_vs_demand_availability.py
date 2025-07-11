#!/usr/bin/env python3
"""
Test EIA generation data availability vs demand data.

Testing both endpoints:
- electricity/rto/region-data/data/ (demand)
- electricity/rto/fuel-type-data/data/ (generation)
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.integrations.eia.client import EIAClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_generation_data_availability(region, start_year=2012, end_year=2025):
    """Test generation data availability for a region across years."""
    print(f"\n🔍 Testing GENERATION data for {region} ({start_year}-{end_year})...")
    client = EIAClient()

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-01-03"  # Small sample

        try:
            data = client.get_generation_mix(region=region, start_date=start_date, end_date=end_date)
            if len(data) > 0:
                print(f"✅ {region} GENERATION {year}: {len(data)} records")
                return year
            else:
                print(f"   {region} GENERATION {year}: No data (empty response)")
        except Exception as e:
            print(f"   {region} GENERATION {year}: Error - {str(e)}")

    print(f"❌ {region}: No GENERATION data found in range {start_year}-{end_year}")
    return None

def test_demand_data_availability(region, start_year=2012, end_year=2025):
    """Test demand data availability for a region across years."""
    print(f"\n🔍 Testing DEMAND data for {region} ({start_year}-{end_year})...")
    client = EIAClient()

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-01-03"  # Small sample

        try:
            data = client.get_electricity_demand(region=region, start_date=start_date, end_date=end_date)
            if len(data) > 0:
                print(f"✅ {region} DEMAND {year}: {len(data)} records")
                return year
            else:
                print(f"   {region} DEMAND {year}: No data (empty response)")
        except Exception as e:
            print(f"   {region} DEMAND {year}: Error - {str(e)}")

    print(f"❌ {region}: No DEMAND data found in range {start_year}-{end_year}")
    return None

def compare_data_types():
    """Compare availability between demand and generation data."""
    print("🚀 Comparing EIA Data Type Availability")
    print("=" * 60)

    regions = ['CAL', 'TEX', 'MISO']  # Test subset first

    results = {}

    for region in regions:
        print(f"\n📊 Testing {region}...")

        # Test both data types
        demand_first_year = test_demand_data_availability(region, 2012, 2020)
        generation_first_year = test_generation_data_availability(region, 2012, 2020)

        results[region] = {
            'demand_first_year': demand_first_year,
            'generation_first_year': generation_first_year
        }

    # Summary
    print(f"\n📋 Data Availability Comparison:")
    print("-" * 50)
    print("Region  | Demand | Generation | Difference")
    print("-" * 50)

    for region, data in results.items():
        demand_year = data['demand_first_year'] or 'None'
        gen_year = data['generation_first_year'] or 'None'

        if data['demand_first_year'] and data['generation_first_year']:
            diff = data['generation_first_year'] - data['demand_first_year']
            diff_str = f"{diff:+d} years" if diff != 0 else "Same"
        else:
            diff_str = "N/A"

        print(f"{region:7} | {demand_year:6} | {gen_year:10} | {diff_str}")

    return results

def test_generation_fuel_types():
    """Test what fuel types are available in generation data."""
    print(f"\n🔍 Testing fuel types in generation data...")
    client = EIAClient()

    # Test recent data to see fuel type structure
    try:
        data = client.get_generation_mix(region='CAL', start_date='2023-01-01', end_date='2023-01-03')
        if len(data) > 0:
            print(f"✅ Generation data columns: {list(data.columns)}")
            print(f"   Sample data shape: {data.shape}")

            # Show fuel types
            fuel_cols = [col for col in data.columns if 'generation' in col.lower()]
            print(f"   Available fuel types: {fuel_cols}")

            return fuel_cols
        else:
            print("❌ No generation data to analyze fuel types")
            return []
    except Exception as e:
        print(f"❌ Error testing fuel types: {str(e)}")
        return []

if __name__ == "__main__":
    # Run comparison
    results = compare_data_types()

    # Test fuel types
    fuel_types = test_generation_fuel_types()

    print(f"\n🎯 Key Insights:")
    print("=" * 30)

    # Analyze results
    demand_years = [r['demand_first_year'] for r in results.values() if r['demand_first_year']]
    gen_years = [r['generation_first_year'] for r in results.values() if r['generation_first_year']]

    if demand_years:
        print(f"Demand data earliest: {min(demand_years)}")
    if gen_years:
        print(f"Generation data earliest: {min(gen_years)}")

    if demand_years and gen_years:
        if min(gen_years) < min(demand_years):
            print("🔥 Generation data available EARLIER than demand data!")
        elif min(gen_years) > min(demand_years):
            print("⚠️  Generation data available LATER than demand data")
        else:
            print("📊 Both data types available from same year")

    if fuel_types:
        print(f"Available fuel types: {len(fuel_types)} types detected")

    print(f"\nNext: Test comprehensive historical range if generation data shows different pattern")
