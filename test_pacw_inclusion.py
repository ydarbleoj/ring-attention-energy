#!/usr/bin/env python3
"""
Test Step 2 with PACW included and conservative settings
"""
import sys
import os
import time
from datetime import date

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator

def test_with_pacw():
    """Test concurrent extraction with PACW included."""
    print("🌟 Step 2 Test: Including PACW with Conservative Settings")
    print("=" * 60)

    orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    # Conservative test configuration including PACW
    test_config = {
        'start_date': date(2019, 1, 1),
        'end_date': date(2019, 1, 31),  # Just January 2019 for testing
        'regions': ['PACW', 'CAL', 'TEX'],  # PACW included as requested
        'data_types': ['demand'],  # Just demand for speed
        'max_workers': 3,  # 1 worker per region
        'batch_days': 30   # Smaller batches
    }

    print(f"📊 Test Configuration:")
    print(f"   Date range: {test_config['start_date']} to {test_config['end_date']}")
    print(f"   Regions: {test_config['regions']} (PACW included ✅)")
    print(f"   Data types: {test_config['data_types']}")
    print(f"   Workers: {test_config['max_workers']}")

    print(f"\n⏳ Starting extraction...")
    start_time = time.time()

    results = orchestrator.extract_historical_data_concurrent(**test_config)

    end_time = time.time()
    duration = end_time - start_time

    print(f"\n📋 Results:")
    if results['success']:
        print(f"✅ Status: SUCCESS")
        print(f"📁 Files: {results['total_files_created']}")
        print(f"📊 Records: {results['estimated_total_records']:,}")
        print(f"⏱️  Duration: {duration:.1f}s")
        print(f"🚀 RPS: {results['estimated_rps']:.1f}")

        # Verify PACW data was extracted
        if 'PACW' in str(results):
            print(f"✅ PACW data extracted successfully")

        # Projection for full load
        full_multiplier = 6 * 12 * 5  # 6 years, 12 months, 5 regions, 2 data types
        current_multiplier = 1 * 1 * 3 * 1  # 1 month, 3 regions, 1 data type

        projected_records = results['estimated_total_records'] * (full_multiplier / current_multiplier)
        projected_time = duration * (full_multiplier / current_multiplier)

        print(f"\n🔮 Full Historical Load Projection:")
        print(f"   Projected records: {projected_records:,.0f}")
        print(f"   Projected time: {projected_time/60:.1f} minutes")
        print(f"   Target achievement: {'✅' if projected_records >= 2500000 else '❌'}")

    else:
        print(f"❌ Status: FAILED")
        print(f"Error: {results.get('error', 'Unknown error')}")

    return results

if __name__ == "__main__":
    test_with_pacw()
