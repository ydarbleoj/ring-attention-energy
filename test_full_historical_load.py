#!/usr/bin/env python3
"""
Full Historical Load Test (Non-interactive)
"""
import sys
import os
import time
from datetime import date

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator

def run_full_load():
    """Run the complete historical data load for 2019-2025."""
    print("🌟 Full Historical Data Load (2019-2025) - Non-Interactive")
    print("=" * 70)

    orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    # Full historical configuration
    full_config = {
        'start_date': date(2019, 1, 1),
        'end_date': date(2025, 7, 11),  # Through today
        'regions': ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO'],  # All major regions INCLUDING PACW
        'data_types': ['demand', 'generation'],
        'max_workers': 5,  # Region-based parallelism: 1 worker per region (5 regions)
        'batch_days': 45   # Optimal batch size
    }

    print(f"📊 Full Load Configuration:")
    print(f"   Date range: {full_config['start_date']} to {full_config['end_date']}")
    print(f"   Regions: {full_config['regions']} (PACW included ✅)")
    print(f"   Data types: {full_config['data_types']}")
    print(f"   Workers: {full_config['max_workers']} (1 per region)")
    print(f"   Batch size: {full_config['batch_days']} days")

    # Calculate expected load
    days_total = (full_config['end_date'] - full_config['start_date']).days
    batches_per_region_type = days_total // full_config['batch_days']
    total_api_calls = len(full_config['regions']) * len(full_config['data_types']) * batches_per_region_type
    expected_records = total_api_calls * 1080  # ~45 days * 24 hours

    print(f"\n🎯 Expected Load:")
    print(f"   Total days: {days_total}")
    print(f"   Batches per region/type: {batches_per_region_type}")
    print(f"   Total API calls: {total_api_calls}")
    print(f"   Expected records: {expected_records:,}")

    print(f"\n🚀 Starting full historical load...")
    start_time = time.time()

    # Execute full load
    results = orchestrator.extract_historical_data_concurrent(**full_config)

    end_time = time.time()
    actual_duration = end_time - start_time

    # Final results
    print(f"\n🏁 FULL HISTORICAL LOAD COMPLETE!")
    print("=" * 60)

    if results['success']:
        print(f"✅ Status: SUCCESS")
        print(f"📁 Total files: {results['total_files_created']}")
        print(f"📊 Total records: {results['estimated_total_records']:,}")
        print(f"⏱️  Total time: {actual_duration:.1f}s ({actual_duration/60:.1f} min)")
        print(f"🚀 Average RPS: {results['estimated_rps']:.1f}")

        # Success metrics
        if results['estimated_total_records'] >= 1000000:
            print(f"🎯 SUCCESS: Achieved 1M+ record target!")
        if results['estimated_total_records'] >= 2500000:
            print(f"🏆 ULTIMATE SUCCESS: Achieved 2.5M+ record target!")
        if actual_duration <= 20 * 60:  # 20 minutes
            print(f"⚡ SUCCESS: Completed within time target!")
        if results['estimated_rps'] >= 3000:
            print(f"🚀 SUCCESS: Maintained 3,000+ RPS target!")

        # PACW verification
        print(f"\n✅ PACW Region Inclusion: Verified in configuration")

    else:
        print(f"❌ Status: FAILED")
        print(f"Error: {results['error']}")

    return results

if __name__ == "__main__":
    print("🎯 Step 2: Full Historical Data Loading (Including PACW)")
    print("Target: 2.5M records using region-based parallelism")
    print("=" * 70)

    run_full_load()

    print(f"\n🎉 Step 2 execution complete!")
    print(f"Next: Step 3 - Data Quality Validation")
