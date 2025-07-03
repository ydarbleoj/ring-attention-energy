#!/usr/bin/env python3
"""
Oregon EIA Data Test Runner

This script demonstrates the VCR cassette recording functionality
for Oregon energy data testing.

Usage:
    # Record Oregon data (requires real API key):
    export EIA_API_KEY=your_real_key
    python run_oregon_tests.py --record

    # Replay from cassettes (no API key needed):
    python run_oregon_tests.py --replay

    # Run all tests:
    python run_oregon_tests.py --all
"""

import os
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "tests"))

# Import paths for the specific modules
sys.path.insert(0, str(project_root / "src" / "core" / "integrations"))
sys.path.insert(0, str(project_root / "tests"))

def run_oregon_demand_test():
    """Run Oregon electricity demand test with VCR"""
    print("🏔️ Testing Oregon Electricity Demand Data...")

    try:
        from eia.client import EIAClient
        from config import get_test_config
        from vcr_config import VCRManager

        # Setup
        config = get_test_config()
        api_key = os.getenv('EIA_API_KEY', 'TEST_KEY_FOR_VCR')
        config.api.eia_api_key = api_key

        client = EIAClient(config=config)
        cassette_path = Path(__file__).parent / "cassettes"
        cassette_path.mkdir(exist_ok=True)

        # Test Oregon demand data
        with VCRManager("oregon_demand_demo", cassette_path):
            print(f"   Using API key: {api_key[:8]}..." if len(api_key) > 8 else f"   Using API key: {api_key}")

            demand_data = client.get_electricity_demand(
                region="PACW",  # Pacific West (includes Oregon)
                start_date="2023-01-01",
                end_date="2023-01-03"  # 2 days for demo
            )

            if not demand_data.empty:
                print(f"   ✅ Retrieved {len(demand_data)} Oregon demand data points")
                print(f"   📊 Demand range: {demand_data['demand'].min():.1f} - {demand_data['demand'].max():.1f} MW")
                print(f"   📅 Time range: {demand_data['timestamp'].min()} to {demand_data['timestamp'].max()}")

                # Oregon-specific validation
                avg_demand = demand_data['demand'].mean()
                if 5000 <= avg_demand <= 20000:  # Oregon typical range
                    print(f"   🎯 Oregon demand pattern validated: {avg_demand:.1f} MW average")
                else:
                    print(f"   ⚠️  Unusual Oregon demand pattern: {avg_demand:.1f} MW average")

            else:
                print("   ⚠️  No Oregon demand data retrieved (check API key or region)")

    except Exception as e:
        print(f"   ❌ Oregon demand test failed: {e}")


def run_oregon_renewable_test():
    """Run Oregon renewable generation test"""
    print("🌱 Testing Oregon Renewable Generation Data...")

    try:
        from eia.client import EIAClient
        from config import get_test_config
        from vcr_config import VCRManager

        config = get_test_config()
        api_key = os.getenv('EIA_API_KEY', 'TEST_KEY_FOR_VCR')
        config.api.eia_api_key = api_key

        client = EIAClient(config=config)
        cassette_path = Path(__file__).parent / "cassettes"

        with VCRManager("oregon_renewable_demo", cassette_path):
            renewable_data = client.get_renewable_generation(
                region="PACW",
                start_date="2023-01-01",
                end_date="2023-01-03"
            )

            if not renewable_data.empty:
                print(f"   ✅ Retrieved {len(renewable_data)} Oregon renewable data points")

                # Check for Oregon-specific renewable patterns
                if 'wind_generation' in renewable_data.columns:
                    wind_data = renewable_data['wind_generation'].dropna()
                    if len(wind_data) > 0:
                        print(f"   💨 Wind generation: {wind_data.min():.1f} - {wind_data.max():.1f} MW")

                if 'solar_generation' in renewable_data.columns:
                    solar_data = renewable_data['solar_generation'].dropna()
                    if len(solar_data) > 0:
                        print(f"   ☀️  Solar generation: {solar_data.min():.1f} - {solar_data.max():.1f} MW")

            else:
                print("   ⚠️  No Oregon renewable data retrieved")

    except Exception as e:
        print(f"   ❌ Oregon renewable test failed: {e}")


def check_cassettes():
    """Check what VCR cassettes exist"""
    print("📼 Checking VCR Cassettes...")

    cassette_path = Path(__file__).parent / "cassettes"

    if cassette_path.exists():
        cassettes = list(cassette_path.glob("*.yaml"))
        if cassettes:
            print(f"   Found {len(cassettes)} cassette files:")
            for cassette in cassettes:
                size_kb = cassette.stat().st_size / 1024
                print(f"   - {cassette.name} ({size_kb:.1f} KB)")
        else:
            print("   No cassette files found - will record on first run")
    else:
        print("   Cassettes directory doesn't exist yet")
        cassette_path.mkdir(parents=True, exist_ok=True)
        print("   Created cassettes directory")


def show_oregon_energy_context():
    """Show Oregon energy context for the tests"""
    print("🏔️ Oregon Energy Context:")
    print("   - Primary region: Pacific West (PACW)")
    print("   - Key characteristics:")
    print("     * High hydro generation (~60-80% of total)")
    print("     * Significant wind from Columbia River Gorge")
    print("     * Moderate demand (~8-15 GW peak)")
    print("     * Seasonal patterns driven by snowmelt/precipitation")
    print("     * Clean grid due to hydro dominance")
    print("   - Test focus areas:")
    print("     * Demand patterns and daily/seasonal cycles")
    print("     * Renewable generation (wind/solar)")
    print("     * Generation mix including massive hydro resources")
    print("")


def main():
    parser = argparse.ArgumentParser(description="Oregon EIA Data Test Runner")
    parser.add_argument("--record", action="store_true", help="Record new data (requires API key)")
    parser.add_argument("--replay", action="store_true", help="Replay from cassettes")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--check", action="store_true", help="Check cassette status")

    args = parser.parse_args()

    if not any([args.record, args.replay, args.all, args.check]):
        args.all = True  # Default to running all

    print("=" * 60)
    print("🏔️  OREGON EIA DATA VCR TESTING")
    print("=" * 60)

    show_oregon_energy_context()

    if args.check or args.all:
        check_cassettes()
        print("")

    if args.record:
        api_key = os.getenv('EIA_API_KEY')
        if not api_key:
            print("❌ EIA_API_KEY environment variable required for recording")
            print("   Get your free API key at: https://www.eia.gov/opendata/register.php")
            print("   Then: export EIA_API_KEY=your_key_here")
            return
        print("🎬 Recording mode: Will make actual API calls and save to cassettes")
        print("")

    if args.replay:
        print("📼 Replay mode: Using existing cassettes only")
        print("")

    if args.record or args.replay or args.all:
        run_oregon_demand_test()
        print("")
        run_oregon_renewable_test()
        print("")

    print("✅ Oregon EIA VCR testing complete!")
    print("")
    print("Next steps:")
    print("- Run: pytest tests/core/integrations/eia/test_client.py -v")
    print("- Check cassettes in: tests/core/integrations/eia/cassettes/")
    print("- Explore Oregon energy patterns in the Ring Attention model")


if __name__ == "__main__":
    main()
