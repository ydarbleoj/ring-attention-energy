#!/usr/bin/env python3
"""Generate VCR cassettes for RawDataLoader tests."""

import os
from pathlib import Path
from datetime import date

from src.core.integrations.eia.client import EIAClient
from src.core.integrations.eia.service.raw_data_loader import RawDataLoader
from tests.vcr_config import create_vcr_config

def generate_cassettes():
    """Generate VCR cassettes for RawDataLoader tests."""

    print("🎬 Generating VCR cassettes for RawDataLoader tests...")

    # Get API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY not found in environment")
        return

    print(f"✅ Found EIA API key: {api_key[:10]}...")

    # Initialize client and raw loader
    client = EIAClient(api_key=api_key)
    raw_loader = RawDataLoader(client, raw_data_path="tests/data/raw")

    # Test parameters
    region = "PACW"
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 7)

    # Cassette directory
    cassette_dir = Path("tests/core/integrations/eia/cassettes")
    cassette_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n📊 Generating cassettes for:")
    print(f"   • Region: {region}")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Cassette Dir: {cassette_dir}")    # Generate demand data cassette
    print("\n🔄 Generating demand data cassette...")
    try:
        # Create VCR without API key filtering for successful recording
        import vcr
        vcr_instance = vcr.VCR(
            serializer='yaml',
            cassette_library_dir=str(cassette_dir),
            match_on=['method', 'uri', 'body'],
            record_mode='once',
            # Don't filter API key when recording
            filter_query_parameters=[],
            filter_headers=[],
            decode_compressed_response=True,
        )

        with vcr_instance.use_cassette("raw_loader_demand_test.yaml"):
            demand_file = raw_loader.extract_demand_data(region, start_date, end_date)
            print(f"✅ Demand cassette generated: {cassette_dir}/raw_loader_demand_test.yaml")
            print(f"   • File: {demand_file}")

            # Verify the extraction
            demand_package = raw_loader.load_raw_file(demand_file)
            demand_metadata = demand_package["metadata"]
            print(f"   • Records: {demand_metadata['record_count']}")
            print(f"   • Success: {demand_metadata['success']}")

    except Exception as e:
        print(f"❌ Failed to generate demand cassette: {e}")

    # Generate generation data cassette
    print("\n🔄 Generating generation data cassette...")
    try:
        # Create VCR without API key filtering for successful recording
        import vcr
        vcr_instance = vcr.VCR(
            serializer='yaml',
            cassette_library_dir=str(cassette_dir),
            match_on=['method', 'uri', 'body'],
            record_mode='once',
            # Don't filter API key when recording
            filter_query_parameters=[],
            filter_headers=[],
            decode_compressed_response=True,
        )

        with vcr_instance.use_cassette("raw_loader_generation_test.yaml"):
            generation_file = raw_loader.extract_generation_data(region, start_date, end_date)
            print(f"✅ Generation cassette generated: {cassette_dir}/raw_loader_generation_test.yaml")
            print(f"   • File: {generation_file}")

            # Verify the extraction
            generation_package = raw_loader.load_raw_file(generation_file)
            generation_metadata = generation_package["metadata"]
            print(f"   • Records: {generation_metadata['record_count']}")
            print(f"   • Success: {generation_metadata['success']}")

    except Exception as e:
        print(f"❌ Failed to generate generation cassette: {e}")

    print(f"\n🎉 Cassette generation completed!")
    print(f"📂 Check {cassette_dir} for generated cassettes")

if __name__ == "__main__":
    generate_cassettes()
