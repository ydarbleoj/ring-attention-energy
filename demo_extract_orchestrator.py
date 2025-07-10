#!/usr/bin/env python3
"""Demo script for Extract stage orchestrator with performance testing.

This script demonstrates Phase 1 of the ETL pipeline:
- Extract raw EIA API responses to data/raw/
- Monitor RPS (requests per second) and latency
- Test different batch configurations for optimization
"""

import asyncio
import os
from datetime import date
from pathlib import Path
import logging

from src.core.pipeline.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


def setup_logging():
    """Set up logging for the demo."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('extract_demo.log')
        ]
    )


async def test_extract_performance():
    """Test extract performance with different configurations."""

    print("🚀 Extract Orchestrator Performance Demo")
    print("="*60)

    # Check API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY not found in environment")
        print("   Please set your EIA API key to run this demo")
        return

    print(f"✅ Found EIA API key: {api_key[:10]}...")

    # Initialize orchestrator
    orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    # Test parameters for Oregon
    region = "PACW"  # Pacific West (includes Oregon)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 14)  # 2 weeks for testing

    print(f"\n📊 Test Parameters:")
    print(f"   • Region: {region}")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Duration: {(end_date - start_date).days + 1} days")

    # Test 1: Conservative configuration (baseline)
    print(f"\n🧪 Test 1: Conservative Configuration")
    print("-" * 40)

    conservative_config = ExtractBatchConfig(
        days_per_batch=7,  # 1 week batches
        max_concurrent_batches=1,  # Sequential processing
        requests_per_second_limit=2.0,  # Conservative RPS
        delay_between_requests=0.5,  # 500ms delay
    )

    orchestrator.configure_batching(conservative_config)

    try:
        results_conservative = await orchestrator.extract_oregon_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=["demand", "generation"]
        )

        conservative_metrics = orchestrator.get_performance_metrics()
        print(f"Conservative Results:")
        print(f"   • Duration: {conservative_metrics.duration_seconds:.2f}s")
        print(f"   • RPS: {conservative_metrics.requests_per_second:.2f}")
        print(f"   • Success Rate: {conservative_metrics.success_rate:.1f}%")
        print(f"   • Avg Latency: {conservative_metrics.average_latency_ms:.0f}ms")

    except Exception as e:
        print(f"❌ Conservative test failed: {e}")
        return

    # Test 2: Optimized configuration
    print(f"\n🧪 Test 2: Optimized Configuration")
    print("-" * 40)

    optimized_config = ExtractBatchConfig(
        days_per_batch=3,  # Smaller batches
        max_concurrent_batches=3,  # More concurrency
        requests_per_second_limit=5.0,  # Higher RPS
        delay_between_requests=0.2,  # 200ms delay
    )

    orchestrator.configure_batching(optimized_config)

    try:
        results_optimized = await orchestrator.extract_oregon_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=["demand"]  # Test with just demand first
        )

        optimized_metrics = orchestrator.get_performance_metrics()
        print(f"Optimized Results:")
        print(f"   • Duration: {optimized_metrics.duration_seconds:.2f}s")
        print(f"   • RPS: {optimized_metrics.requests_per_second:.2f}")
        print(f"   • Success Rate: {optimized_metrics.success_rate:.1f}%")
        print(f"   • Avg Latency: {optimized_metrics.average_latency_ms:.0f}ms")

        # Compare performance
        speedup = conservative_metrics.duration_seconds / optimized_metrics.duration_seconds
        print(f"\n📈 Performance Comparison:")
        print(f"   • Speedup: {speedup:.1f}x faster")
        print(f"   • RPS improvement: {optimized_metrics.requests_per_second / conservative_metrics.requests_per_second:.1f}x")

    except Exception as e:
        print(f"❌ Optimized test failed: {e}")

    # Test 3: Stress test (if previous tests succeeded)
    if conservative_metrics.success_rate > 90 and optimized_metrics.success_rate > 90:
        print(f"\n🧪 Test 3: Stress Test Configuration")
        print("-" * 40)

        stress_config = ExtractBatchConfig(
            days_per_batch=1,  # Daily batches
            max_concurrent_batches=5,  # High concurrency
            requests_per_second_limit=8.0,  # Push the limit
            delay_between_requests=0.1,  # 100ms delay
        )

        orchestrator.configure_batching(stress_config)

        try:
            # Smaller date range for stress test
            stress_start = date(2024, 1, 1)
            stress_end = date(2024, 1, 7)  # 1 week

            results_stress = await orchestrator.extract_oregon_data(
                start_date=stress_start,
                end_date=stress_end,
                region=region,
                data_types=["demand"]
            )

            stress_metrics = orchestrator.get_performance_metrics()
            print(f"Stress Test Results:")
            print(f"   • Duration: {stress_metrics.duration_seconds:.2f}s")
            print(f"   • RPS: {stress_metrics.requests_per_second:.2f}")
            print(f"   • Success Rate: {stress_metrics.success_rate:.1f}%")
            print(f"   • Rate limit hits: {stress_metrics.rate_limit_hits}")

        except Exception as e:
            print(f"⚠️  Stress test failed (expected): {e}")

    # Show final extraction summary
    print(f"\n📊 Final Extraction Summary:")
    summary = orchestrator.get_extraction_summary()
    print(f"   • Total files: {summary['total_files']}")
    print(f"   • Total records: {summary['total_records']:,}")
    print(f"   • Total size: {summary['total_size_bytes']:,} bytes")
    print(f"   • By data type: {summary['by_data_type']}")

    # List raw files created
    print(f"\n📁 Raw Files Created:")
    raw_data_path = Path("data/raw")
    if raw_data_path.exists():
        for file_path in sorted(raw_data_path.rglob("*.json")):
            size_mb = file_path.stat().st_size / (1024 * 1024)
            rel_path = file_path.relative_to(raw_data_path)
            print(f"   • {rel_path} ({size_mb:.2f} MB)")

    print(f"\n✅ Extract performance testing completed!")
    print(f"🔍 Check 'extract_demo.log' for detailed logs")
    print(f"📂 Check 'data/raw/' for extracted raw data files")


async def test_single_extract():
    """Test a single extract operation for debugging."""

    print("🔧 Single Extract Test")
    print("="*30)

    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY not found")
        return

    orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    # Simple single batch test
    simple_config = ExtractBatchConfig(
        days_per_batch=7,
        max_concurrent_batches=1,
        delay_between_requests=1.0,  # Very conservative
    )

    orchestrator.configure_batching(simple_config)

    try:
        results = await orchestrator.extract_oregon_data(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3),  # Just 3 days
            region="PACW",
            data_types=["demand"]  # Just demand
        )

        metrics = orchestrator.get_performance_metrics()
        print(f"✅ Single extract successful!")
        print(f"   • Records: {metrics.total_records_extracted:,}")
        print(f"   • Duration: {metrics.duration_seconds:.2f}s")

    except Exception as e:
        print(f"❌ Single extract failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    setup_logging()

    # Choose which test to run
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "single":
        asyncio.run(test_single_extract())
    else:
        asyncio.run(test_extract_performance())
