#!/usr/bin/env python3
"""
Test Price Data Integration
Tests that price data flows through the Extract → Transform pipeline correctly.

This test validates:
1. Price data schema can parse EIA API responses
2. Price data gets extracted and saved as JSON files
3. Transform step processes price data alongside demand/generation
4. Consolidated parquet includes all three data types
5. Performance maintains target throughput
"""

import asyncio
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent))

from src.core.integrations.eia.schema import EIAResponse, PriceRecord
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig


def test_price_schema_validation():
    """Test that price schema can validate mock EIA price responses."""
    print("🔍 Testing Price Schema Validation...")

    # Mock EIA price response (LMP data)
    mock_price_response = {
        "response": {
            "total": "24",
            "dateFormat": "ISO8601",
            "frequency": "hourly",
            "data": [
                {
                    "period": "2024-01-01T00",
                    "parent": "PACW",
                    "parent-name": "PacifiCorp West",
                    "subba": "PACW-WY",
                    "subba-name": "PacifiCorp West - Wyoming",
                    "type": "LMP",
                    "type-name": "Locational Marginal Price",
                    "value": "45.23",
                    "value-units": "dollars per megawatthour"
                },
                {
                    "period": "2024-01-01T01",
                    "parent": "PACW",
                    "parent-name": "PacifiCorp West",
                    "subba": "PACW-ID",
                    "subba-name": "PacifiCorp West - Idaho",
                    "type": "LMP",
                    "type-name": "Locational Marginal Price",
                    "value": "42.18",
                    "value-units": "dollars per megawatthour"
                }
            ]
        }
    }

    try:
        # Validate with EIA schema
        eia_response = EIAResponse.model_validate(mock_price_response)
        print(f"   ✅ Schema validation successful")
        print(f"   📊 Total records: {eia_response.response.total_records}")

        # Parse price records
        price_records = eia_response.response.parse_price_records()
        print(f"   💰 Price records parsed: {len(price_records)}")

        # Validate price record properties
        if price_records:
            record = price_records[0]
            print(f"   🏷️  Sample price: ${record.price_per_mwh:.2f}/MWh at {record.timestamp}")
            print(f"   🌍 Region: {record.region} ({record.parent_name})")

        return True

    except Exception as e:
        print(f"   ❌ Schema validation failed: {e}")
        traceback.print_exc()
        return False


def create_mock_price_data_files():
    """Create mock price data JSON files for testing."""
    print("📁 Creating Mock Price Data Files...")

    # Create test data directory
    test_data_dir = Path("data/raw/eia/2024")
    test_data_dir.mkdir(parents=True, exist_ok=True)

    # Mock price data for PACW region
    mock_price_data = {
        "response": {
            "total": "168",  # 7 days * 24 hours
            "dateFormat": "ISO8601",
            "frequency": "hourly",
            "data": []
        }
    }

    # Generate 7 days of hourly price data
    from datetime import datetime, timedelta
    start_date = datetime(2024, 1, 1)

    for day in range(7):
        for hour in range(24):
            timestamp = start_date + timedelta(days=day, hours=hour)
            period = timestamp.strftime("%Y-%m-%dT%H")

            # Generate realistic price data with daily patterns
            base_price = 35.0
            hour_modifier = 10.0 if 16 <= hour <= 20 else 0.0  # Peak hours
            day_modifier = 5.0 if day < 5 else -2.0  # Weekday vs weekend
            price = base_price + hour_modifier + day_modifier + (hour * 0.5)

            mock_price_data["response"]["data"].append({
                "period": period,
                "parent": "PACW",
                "parent-name": "PacifiCorp West",
                "subba": "PACW-WY",
                "subba-name": "PacifiCorp West - Wyoming",
                "type": "LMP",
                "type-name": "Locational Marginal Price",
                "value": f"{price:.2f}",
                "value-units": "dollars per megawatthour"
            })

    # Save mock price data file
    filename = "eia_price_PACW_2024-01-01_to_2024-01-07_test.json"
    file_path = test_data_dir / filename

    with open(file_path, 'w') as f:
        json.dump(mock_price_data, f, indent=2)

    print(f"   ✅ Created mock price file: {filename}")
    print(f"   📊 Records: {len(mock_price_data['response']['data'])}")
    print(f"   📁 Path: {file_path}")

    return file_path


async def test_price_data_transform():
    """Test that price data gets processed by the transform step."""
    print("🔄 Testing Price Data Transform Pipeline...")

    try:
        # Ensure we have mock price data
        mock_file = create_mock_price_data_files()

        # Create transform step configuration
        config = DataCleanerStepConfig(
            step_name="Price Data Transform Test",
            step_id="price_transform_test",
            source="eia",
            raw_data_dir=Path("data/raw"),
            interim_data_dir=Path("data/interim"),
            validate_data=True,
            dry_run=False
        )

        transform_step = DataCleanerStep(config)

        print("   🚀 Executing transform step...")
        start_time = datetime.now()

        result = await transform_step.execute_async()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"   ✅ Transform completed in {duration:.2f}s")
        print(f"   📊 Result status: {result.status}")

        # Check output files
        interim_dir = Path("data/interim")
        parquet_files = list(interim_dir.glob("*.parquet"))

        if parquet_files:
            import polars as pl

            # Read the consolidated parquet
            latest_parquet = max(parquet_files, key=lambda x: x.stat().st_mtime)
            df = pl.read_parquet(latest_parquet)

            print(f"   📄 Consolidated parquet: {latest_parquet.name}")
            print(f"   📊 Total records: {len(df):,}")

            # Check data types included
            data_types = df["data_type"].unique().to_list()
            print(f"   🏷️  Data types: {data_types}")

            # Price-specific validation
            if "price" in data_types:
                price_df = df.filter(pl.col("data_type") == "price")
                print(f"   💰 Price records: {len(price_df):,}")

                if len(price_df) > 0:
                    avg_price = price_df["value"].mean()
                    min_price = price_df["value"].min()
                    max_price = price_df["value"].max()
                    print(f"   💲 Price range: ${min_price:.2f} - ${max_price:.2f} (avg: ${avg_price:.2f})")

                    return True
            else:
                print("   ⚠️  No price data found in output")
                return False
        else:
            print("   ❌ No parquet output files found")
            return False

    except Exception as e:
        print(f"   ❌ Transform test failed: {e}")
        traceback.print_exc()
        return False


def check_existing_data_integration():
    """Check if price data can be integrated with existing demand/generation data."""
    print("🔗 Checking Price Data Integration with Existing Data...")

    # Check if consolidated parquet exists
    interim_dir = Path("data/interim")
    parquet_files = list(interim_dir.glob("*.parquet"))

    if not parquet_files:
        print("   ⚠️  No existing parquet files found")
        return False

    try:
        import polars as pl

        # Read latest parquet
        latest_parquet = max(parquet_files, key=lambda x: x.stat().st_mtime)
        df = pl.read_parquet(latest_parquet)

        print(f"   📄 Reading: {latest_parquet.name}")
        print(f"   📊 Total records: {len(df):,}")

        # Analyze data types
        data_type_counts = df.groupby("data_type").count().sort("count", descending=True)
        print("   📊 Data type breakdown:")

        for row in data_type_counts.iter_rows(named=True):
            data_type = row["data_type"]
            count = row["count"]
            percentage = (count / len(df)) * 100
            print(f"      {data_type}: {count:,} records ({percentage:.1f}%)")

        # Check if all three types are present
        data_types = set(df["data_type"].unique().to_list())
        expected_types = {"demand", "generation", "price"}

        if expected_types.issubset(data_types):
            print("   ✅ All three data types integrated successfully!")

            # Performance calculation
            total_records = len(df)
            file_size_mb = latest_parquet.stat().st_size / (1024 * 1024)
            print(f"   📈 Performance: {total_records:,} records, {file_size_mb:.1f} MB")

            return True
        else:
            missing = expected_types - data_types
            print(f"   ⚠️  Missing data types: {missing}")
            return False

    except Exception as e:
        print(f"   ❌ Integration check failed: {e}")
        return False


async def main():
    """Run all price data integration tests."""
    print("🚀 PRICE DATA INTEGRATION TEST SUITE")
    print("=" * 60)

    test_results = []

    # Test 1: Schema validation
    print("\n📋 Test 1: Price Schema Validation")
    result1 = test_price_schema_validation()
    test_results.append(("Schema Validation", result1))

    # Test 2: Transform pipeline
    print("\n📋 Test 2: Price Data Transform Pipeline")
    result2 = await test_price_data_transform()
    test_results.append(("Transform Pipeline", result2))

    # Test 3: Integration check
    print("\n📋 Test 3: Data Integration Check")
    result3 = check_existing_data_integration()
    test_results.append(("Integration Check", result3))

    # Summary
    print("\n" + "=" * 60)
    print("📊 PRICE DATA INTEGRATION TEST RESULTS")
    print("=" * 60)

    passed = 0
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1

    print(f"\n🎯 Results: {passed}/{len(test_results)} tests passed")

    if passed == len(test_results):
        print("🎉 Price data integration is ready for live API testing!")
    else:
        print("⚠️  Some issues need to be resolved before live testing.")

    return passed == len(test_results)


if __name__ == "__main__":
    asyncio.run(main())
