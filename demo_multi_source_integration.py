#!/usr/bin/env python3
"""
Demo script for CAISO and multi-source integration testing
"""
import pandas as pd
import polars as pl
from datetime import datetime
from pathlib import Path

def main():
    print("🔥 Testing CAISO Multi-Source Integration")
    print("=" * 50)

    # Test 1: Basic Multi-Source Storage
    print("\n1. Testing Multi-Source Storage Manager...")
    from src.core.integrations.multi_source_storage import (
        MultiSourceStorageManager, DataSource, DataLayer
    )

    temp_dir = Path("./temp_multi_source_test")
    storage_manager = MultiSourceStorageManager(temp_dir)
    print(f"✅ Created storage manager at {temp_dir}")

    # Test 2: Create test CAISO data
    print("\n2. Creating test CAISO demand data...")
    caiso_data = pl.DataFrame({
        'timestamp': [datetime(2024, 7, 4, 12, 0), datetime(2024, 7, 4, 13, 0)],
        'demand_mw': [35000.0, 36000.0],
        'region': ['California', 'California']
    })
    print(f"✅ Created CAISO data: {len(caiso_data)} records")
    print(caiso_data)

    # Test 3: Save CAISO data
    print("\n3. Saving CAISO data...")
    caiso_path = storage_manager.save_raw_data(
        data=caiso_data,
        source=DataSource.CAISO,
        region="California",
        data_type="demand",
        timestamp=datetime(2024, 7, 4)
    )
    print(f"✅ Saved CAISO data to {caiso_path}")

    # Test 4: Create test EIA data for comparison
    print("\n4. Creating test EIA demand data...")
    eia_data = pl.DataFrame({
        'period': [datetime(2024, 7, 4, 12, 0), datetime(2024, 7, 4, 13, 0)],
        'value': [15000.0, 16000.0],
        'units': ['MWh', 'MWh']
    })
    print(f"✅ Created EIA data: {len(eia_data)} records")
    print(eia_data)

    # Test 5: Save EIA data
    print("\n5. Saving EIA data...")
    eia_path = storage_manager.save_raw_data(
        data=eia_data,
        source=DataSource.EIA,
        region="Oregon",
        data_type="demand",
        timestamp=datetime(2024, 7, 4)
    )
    print(f"✅ Saved EIA data to {eia_path}")

    # Test 6: Load and verify data
    print("\n6. Loading and verifying data...")
    loaded_caiso = storage_manager.load_raw_data(
        source=DataSource.CAISO,
        region="California",
        data_type="demand"
    )
    print(f"✅ Loaded CAISO data: {len(loaded_caiso)} records")

    loaded_eia = storage_manager.load_raw_data(
        source=DataSource.EIA,
        region="Oregon",
        data_type="demand"
    )
    print(f"✅ Loaded EIA data: {len(loaded_eia)} records")

    # Test 7: Data harmonization
    print("\n7. Testing data harmonization...")
    source_data = {
        DataSource.CAISO: caiso_data,
        DataSource.EIA: eia_data
    }

    try:
        harmonized_data = storage_manager.harmonize_data(
            source_data=source_data,
            region="Multi-region",
            data_type="demand"
        )
        print(f"✅ Harmonized data: {len(harmonized_data)} records")
        print(harmonized_data)

        # Check data sources are represented
        sources = harmonized_data['data_source'].unique().to_list()
        print(f"✅ Data sources in harmonized data: {sources}")

    except Exception as e:
        print(f"❌ Harmonization failed: {e}")
        import traceback
        traceback.print_exc()

    # Test 8: Migration features
    print("\n8. Testing migration features...")
    try:
        # First save harmonized data for each region
        ca_harmonized = harmonized_data.filter(pl.col('data_source') == 'CAISO')
        or_harmonized = harmonized_data.filter(pl.col('data_source') == 'EIA')

        if len(ca_harmonized) > 0 and len(or_harmonized) > 0:
            storage_manager.save_harmonized_data(
                data=ca_harmonized,
                region="California",
                data_type="demand"
            )
            storage_manager.save_harmonized_data(
                data=or_harmonized,
                region="Oregon",
                data_type="demand"
            )

            # Create migration features
            features = storage_manager.create_migration_features(
                regions=['California', 'Oregon'],
                start_date=datetime(2024, 7, 4),
                end_date=datetime(2024, 7, 5)
            )
            print(f"✅ Migration features: {len(features)} records")
            if len(features) > 0:
                print(features.head())
        else:
            print("⚠️ Insufficient harmonized data for migration features")

    except Exception as e:
        print(f"❌ Migration features failed: {e}")
        import traceback
        traceback.print_exc()

    # Test 9: Available datasets
    print("\n9. Checking available datasets...")
    datasets = storage_manager.get_available_datasets()
    print(f"✅ Available datasets: {len(datasets)}")
    for dataset in datasets:
        print(f"  - {dataset['source']}: {dataset['region']} {dataset['data_type']} ({dataset['record_count']} records)")

    print("\n🎉 Multi-source integration tests completed!")
    print("=" * 50)

    # Cleanup
    import shutil
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
        print("🧹 Cleaned up temporary directory")

if __name__ == "__main__":
    main()
