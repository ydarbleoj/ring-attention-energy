#!/usr/bin/env python3
"""
Demo script for EIA Polars/Parquet service layer

This script demonstrates the new service layer that provides:
- Polars-based data processing for better performance
- Parquet storage for efficient data persistence
- Modular architecture for easy testing and extension

The service layer builds on our validated schema models and provides
a higher-level interface for data operations.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import polars as pl

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.core.integrations.config import get_config
from src.core.integrations.eia.service import DataLoader, StorageManager


def main():
    """Demonstrate the EIA Polars/Parquet service layer."""
    print("=== EIA Polars/Parquet Service Layer Demo ===\n")

    # Load configuration
    try:
        config = get_config()
        api_key = config.api.eia_api_key
    except:
        # Fallback to environment variable
        api_key = os.getenv('EIA_API_KEY')

    if not api_key:
        print("❌ EIA_API_KEY not found in environment variables")
        print("Please set your EIA API key in the .env file")
        return

    # Set up storage path
    storage_path = Path("data/processed")
    storage_path.mkdir(parents=True, exist_ok=True)

    # Create DataLoader with storage
    print("🔧 Setting up DataLoader with Polars/Parquet storage...")
    data_loader = DataLoader.create_with_storage(
        api_key=api_key,
        storage_path=storage_path
    )

    # Define date range for Oregon data (last 7 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)

    print(f"📅 Loading Oregon energy data from {start_date} to {end_date}")

    try:
        # Load demand data
        print("\n⚡ Loading demand data...")
        demand_df = data_loader.load_demand_data(
            start_date=start_date,
            end_date=end_date,
            region="PACW",  # PacifiCorp West (Oregon)
            save_to_storage=True,
            storage_filename="oregon_demand_7d",
            storage_subfolder="oregon"
        )

        print(f"✅ Loaded {len(demand_df)} demand records")
        print(f"📊 Demand DataFrame schema: {demand_df.schema}")
        print(f"🔢 Demand range: {demand_df['demand_mwh'].min():.1f} - {demand_df['demand_mwh'].max():.1f} MWh")

        # Load generation data
        print("\n🏭 Loading generation data...")
        generation_df = data_loader.load_generation_data(
            start_date=start_date,
            end_date=end_date,
            region="PACW",
            save_to_storage=True,
            storage_filename="oregon_generation_7d",
            storage_subfolder="oregon"
        )

        print(f"✅ Loaded generation data with {len(generation_df)} records")
        print(f"📊 Generation DataFrame schema: {generation_df.schema}")

        # Show renewable energy columns
        renewable_cols = [col for col in generation_df.columns if any(fuel in col for fuel in ["solar", "hydro", "wind"])]
        if renewable_cols:
            print(f"🌱 Renewable energy columns: {renewable_cols}")

        # Join demand and generation data
        print("\n🔗 Joining demand and generation data...")
        joined_df = data_loader.join_demand_generation(
            demand_df, generation_df,
            save_to_storage=True,
            storage_filename="oregon_comprehensive_7d",
            storage_subfolder="oregon"
        )

        print(f"✅ Created comprehensive dataset with {len(joined_df)} records")
        print(f"📊 Joined DataFrame has {len(joined_df.columns)} columns")

        # Demonstrate Polars operations
        print("\n🔍 Performing Polars analysis...")

        # Calculate daily averages
        daily_avg = joined_df.group_by_dynamic(
            "datetime",
            every="1d",
            group_by="demand_region"
        ).agg([
            pl.col("demand_demand_mwh").mean().alias("avg_demand_mwh"),
            pl.col("demand_demand_mwh").min().alias("min_demand_mwh"),
            pl.col("demand_demand_mwh").max().alias("max_demand_mwh")
        ])

        print(f"📈 Daily demand averages:")
        print(daily_avg)

        # Show renewable energy if available
        if renewable_cols:
            renewable_total = joined_df.select([
                "datetime",
                pl.sum_horizontal([f"generation_{col}" for col in renewable_cols]).alias("total_renewable_mwh")
            ])

            renewable_stats = renewable_total.select([
                pl.col("total_renewable_mwh").mean().alias("avg_renewable_mwh"),
                pl.col("total_renewable_mwh").max().alias("max_renewable_mwh")
            ])

            print(f"\n🌱 Renewable energy statistics:")
            print(renewable_stats)

        # Storage information
        print("\n💾 Storage information:")
        storage_info = data_loader.get_storage_info()
        print(f"📁 Total files stored: {storage_info['total_files']}")

        for file_info in storage_info['files']:
            print(f"   📄 {file_info['filename']}: {file_info['size_bytes']} bytes")

        # Demonstrate loading from storage
        print("\n🔄 Demonstrating storage reload...")
        reloaded_df = data_loader.load_from_storage("oregon_comprehensive_7d", subfolder="oregon")
        print(f"✅ Reloaded {len(reloaded_df)} records from storage")

        # Verify data integrity
        if reloaded_df.equals(joined_df):
            print("✅ Data integrity verified - stored and reloaded data match perfectly")
        else:
            print("⚠️  Data integrity check failed")

        print("\n=== Demo Complete ===")
        print("📊 Key Features Demonstrated:")
        print("   • Polars-based data processing for performance")
        print("   • Parquet storage for efficient persistence")
        print("   • Modular service architecture")
        print("   • Schema validation with Pydantic")
        print("   • Comprehensive data analysis capabilities")
        print("   • Data integrity verification")

    except Exception as e:
        print(f"❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()

        # Show partial storage info even on error
        try:
            storage_info = data_loader.get_storage_info()
            if storage_info['total_files'] > 0:
                print(f"\n💾 Partial data available: {storage_info['total_files']} files")
        except:
            pass


if __name__ == "__main__":
    main()
