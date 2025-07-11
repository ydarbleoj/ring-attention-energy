#!/usr/bin/env python3
"""
Step 2: Intelligent Historical Data Loading with ThreadPoolExecutor

This script implements the updated approach for loading 2.5M+ records in ~15 minutes
using concurrent processing while respecting EIA rate limits and maintaining 3,000+ RPS.

Based on Step 1 findings:
- Data available from 2019-2025 (all major regions)
- Use ThreadPoolExecutor for multi-region parallelization
- Target: 2.5M records in 15 minutes at 3,200 RPS
"""

import sys
import os
import time
from datetime import date, datetime
from pathlib import Path

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator
from src.core.integrations.eia.client import EIAClient

def test_concurrent_extraction_performance():
    """Test concurrent extraction with performance monitoring."""
    print("🚀 Step 2: Intelligent Historical Data Loading with ThreadPoolExecutor")
    print("=" * 80)

    # Initialize orchestrator
    orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    # Test configuration based on Step 1 findings
    test_config = {
        'start_date': date(2019, 1, 1),
        'end_date': date(2019, 3, 31),  # 3 months test (quarter)
        'regions': ['CAL', 'TEX', 'MISO'],  # 3 regions for initial test
        'data_types': ['demand', 'generation'],
        'max_workers': 3,  # Region-based parallelism: 1 worker per region
        'batch_days': 45   # Optimal batch size from previous testing
    }

    print(f"📊 Test Configuration:")
    print(f"   Date range: {test_config['start_date']} to {test_config['end_date']}")
    print(f"   Regions: {test_config['regions']}")
    print(f"   Data types: {test_config['data_types']}")
    print(f"   Batch size: {test_config['batch_days']} days")
    print(f"   Max workers: {test_config['max_workers']} (1 per region)")

    # Calculate expected metrics
    quarter_days = 90  # Q1 2019
    expected_api_calls = len(test_config['regions']) * len(test_config['data_types']) * (quarter_days // test_config['batch_days'])
    expected_records = expected_api_calls * 2300  # Average records per call

    print(f"\n🎯 Expected Performance:")
    print(f"   API calls: ~{expected_api_calls}")
    print(f"   Records: ~{expected_records:,}")
    print(f"   Time (at 3,200 RPS): ~{expected_records/3200:.1f} seconds ({expected_records/3200/60:.1f} minutes)")

    print(f"\n⏳ Starting concurrent extraction...")
    start_time = time.time()

    # Execute concurrent extraction
    results = orchestrator.extract_historical_data_concurrent(**test_config)

    end_time = time.time()
    actual_duration = end_time - start_time

    # Analyze results
    print(f"\n📋 Extraction Results:")
    print("=" * 50)

    if results['success']:
        print(f"✅ Status: SUCCESS")
        print(f"📁 Files created: {results['total_files_created']}")
        print(f"📊 Records extracted: {results['estimated_total_records']:,}")
        print(f"⏱️  Duration: {actual_duration:.1f}s ({actual_duration/60:.1f} min)")
        print(f"🚀 RPS achieved: {results['estimated_rps']:.1f}")
        print(f"🎯 Performance ratio: {results['performance_summary']['performance_ratio']:.2%}")
        print(f"🛡️  API compliance: {results['performance_summary']['api_compliance']}")

        # Performance analysis
        target_rps = 3200
        if results['estimated_rps'] >= target_rps:
            print(f"🏆 PERFORMANCE TARGET MET! ({results['estimated_rps']:.1f} ≥ {target_rps} RPS)")
        else:
            print(f"⚠️  Below target: {results['estimated_rps']:.1f} < {target_rps} RPS")

        # Extrapolate to full dataset
        full_dataset_multiplier = 6  # 2019-2025 (6 years) vs Q1 2019 (0.25 years)
        full_regions_multiplier = 5 / len(test_config['regions'])  # 5 total regions vs test regions

        full_records = results['estimated_total_records'] * full_dataset_multiplier * full_regions_multiplier
        full_duration = actual_duration * full_dataset_multiplier * full_regions_multiplier

        print(f"\n🌍 Full Dataset Projection:")
        print(f"   Total records: {full_records:,.0f}")
        print(f"   Total time: {full_duration:.1f}s ({full_duration/60:.1f} min)")
        print(f"   Target 2.5M records: {'✅ ACHIEVABLE' if full_records >= 2500000 else '❌ BELOW TARGET'}")

    else:
        print(f"❌ Status: FAILED")
        print(f"Error: {results['error']}")
        print(f"Duration: {results['extraction_duration_seconds']:.1f}s")

    return results

def run_full_historical_load():
    """Run the complete historical data load for 2019-2025."""
    print(f"\n🌟 Full Historical Data Load (2019-2025)")
    print("=" * 60)

    # Confirm execution
    response = input("This will load ~2.5M records and take ~15 minutes. Continue? (y/N): ")
    if response.lower() != 'y':
        print("❌ Cancelled by user")
        return

    # Initialize orchestrator
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
    print(f"   Regions: {full_config['regions']}")
    print(f"   Data types: {full_config['data_types']}")
    print(f"   Expected time: ~15 minutes")
    print(f"   Expected records: ~2.5M")

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
        if actual_duration <= 20 * 60:  # 20 minutes
            print(f"⚡ SUCCESS: Completed within time target!")
        if results['estimated_rps'] >= 3000:
            print(f"🚀 SUCCESS: Maintained 3,000+ RPS target!")

    else:
        print(f"❌ Status: FAILED")
        print(f"Error: {results['error']}")

    return results

if __name__ == "__main__":
    print("🎯 Step 2: Intelligent Historical Data Loading")
    print("Target: 2.5M records in ~15 minutes using ThreadPoolExecutor")
    print("=" * 80)

    # Menu options
    print(f"\nOptions:")
    print(f"1. Test concurrent extraction (3 months, 3 regions)")
    print(f"2. Run full historical load (2019-2025, all regions)")
    print(f"3. Exit")

    choice = input(f"\nSelect option (1-3): ").strip()

    if choice == "1":
        results = test_concurrent_extraction_performance()
    elif choice == "2":
        results = run_full_historical_load()
    elif choice == "3":
        print("👋 Goodbye!")
        sys.exit(0)
    else:
        print("❌ Invalid choice")
        sys.exit(1)

    print(f"\n🎉 Step 2 execution complete!")
    print(f"Next: Step 3 - Data Quality Validation")
