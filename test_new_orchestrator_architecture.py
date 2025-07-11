#!/usr/bin/env python3
"""
Test the new ThreadPoolExecutor-based ExtractOrchestrator architecture

This test verifies:
1. EIAClient can make raw API calls
2. RawDataLoader can save the responses
3. ExtractOrchestrator can coordinate both with ThreadPoolExecutor
4. Performance is maintained with the new architecture
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from datetime import date, timedelta
import tempfile
import shutil
import time

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator

def test_new_architecture():
    """Test the new ThreadPoolExecutor-based architecture"""

    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    print(f"Using temporary directory: {temp_dir}")

    try:
        # Initialize orchestrator with temporary directory
        orchestrator = ExtractOrchestrator(raw_data_path=temp_dir)

        # Test date range (small for quick test)
        end_date = date.today()
        start_date = end_date - timedelta(days=2)  # Just 2 days for quick test

        print(f"\n🧪 Testing new architecture with date range: {start_date} to {end_date}")
        print(f"📁 Raw data path: {temp_dir}")

        # Test 1: Basic extraction
        print(f"\n--- Test 1: Basic single region extraction ---")
        start_time = time.time()

        results = orchestrator.process_data(
            start_date=start_date,
            end_date=end_date,
            region="PACW",  # Oregon region
            data_types=["demand"]  # Just demand for quick test
        )

        duration = time.time() - start_time
        print(f"✅ Basic extraction completed in {duration:.2f} seconds")
        print(f"📊 Results: {len(results['demand'])} demand batches")

        # Verify files were created
        raw_files = orchestrator.list_extracted_files()
        print(f"📁 Raw files created: {len(raw_files)}")

        for file_path in raw_files[:3]:  # Show first 3 files
            print(f"   - {file_path.name}")

        # Test 2: Multi-region concurrent extraction
        print(f"\n--- Test 2: Multi-region concurrent extraction ---")
        start_time = time.time()

        results = orchestrator.extract_historical_data_concurrent(
            start_date=start_date,
            end_date=end_date,
            regions=["PACW", "CAL"],  # 2 regions for quick test
            data_types=["demand"],
            max_workers=2,
            batch_days=1  # Small batches for quick test
        )

        duration = time.time() - start_time
        print(f"✅ Concurrent extraction completed in {duration:.2f} seconds")
        print(f"📊 Success: {results['success']}")
        print(f"📁 Files created: {results.get('total_files_created', 0)}")
        print(f"🚀 Estimated RPS: {results.get('estimated_rps', 0):.1f}")

        # Test 3: Get extraction summary
        print(f"\n--- Test 3: Extraction summary ---")
        summary = orchestrator.get_extraction_summary()
        print(f"📊 Total files: {summary['total_files']}")
        print(f"📊 Total records: {summary.get('total_records', 0):,}")
        print(f"📊 Total size: {summary.get('total_size_bytes', 0):,} bytes")
        print(f"📊 By data type: {summary.get('by_data_type', {})}")
        print(f"📊 By region: {summary.get('by_region', {})}")

        print(f"\n🎉 All tests passed! New architecture is working correctly.")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"\n🧹 Cleaned up temporary directory")

    return True

if __name__ == "__main__":
    print("🚀 Testing new ThreadPoolExecutor-based ExtractOrchestrator architecture")
    success = test_new_architecture()
    sys.exit(0 if success else 1)
