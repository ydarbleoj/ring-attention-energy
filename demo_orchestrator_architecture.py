#!/usr/bin/env python3
"""Demo of the new orchestrator architecture with performance testing.

This script demonstrates:
1. Extract stage: Raw data extraction with RPS/latency monitoring
2. Transform+Load stage: Data processing with Polars
3. Performance metrics and reporting across stages

Usage:
    python demo_orchestrator_architecture.py
"""

import asyncio
import os
from datetime import date, timedelta
from pathlib import Path

from src.core.orchestrator import ExtractOrchestrator, TransformLoadOrchestrator, BatchConfig


async def demo_extract_stage():
    """Demo the Extract stage with performance monitoring."""

    print("🚀 DEMO: Extract Stage Orchestrator")
    print("=" * 60)

    # Check for API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY not found - using VCR cassettes for demo")
        print("   Set EIA_API_KEY environment variable for live API testing")
    else:
        print(f"✅ Found EIA API key: {api_key[:10]}...")

    # Initialize orchestrator
    extract_orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    # Configure for demo (smaller batches, more aggressive)
    demo_config = BatchConfig(
        days_per_batch=3,  # 3-day batches for faster demo
        max_concurrent_batches=2,  # Conservative for demo
        operations_per_second_limit=8.0,  # Slightly more aggressive
        delay_between_operations=0.15,  # 150ms between requests
        enable_performance_monitoring=True
    )
    extract_orchestrator.configure_batching(demo_config)

    # Demo parameters
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 14)  # 2 weeks of data
    region = "PACW"  # Oregon region
    data_types = ["demand", "generation"]

    print(f"\n📊 Demo Parameters:")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Region: {region}")
    print(f"   • Data Types: {data_types}")
    print(f"   • Batch Size: {demo_config.days_per_batch} days")
    print(f"   • Max Concurrent: {demo_config.max_concurrent_batches}")
    print(f"   • Rate Limit: {demo_config.operations_per_second_limit} ops/sec")

    try:
        print(f"\n🔄 Starting extraction...")
        results = await extract_orchestrator.process_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=data_types
        )

        # Show extraction summary
        print(f"\n📋 Extraction Summary:")
        summary = extract_orchestrator.get_extraction_summary()
        print(f"   • Total Files: {summary['total_files']}")
        print(f"   • Total Records: {summary['total_records']:,}")
        print(f"   • Total Size: {summary['total_size_bytes']:,} bytes")
        print(f"   • By Data Type: {summary['by_data_type']}")

        # Show performance metrics
        metrics = extract_orchestrator.get_performance_metrics()
        print(f"\n⚡ Performance Metrics:")
        print(f"   • Duration: {metrics.duration_seconds:.2f} seconds")
        print(f"   • Operations/sec: {metrics.operations_per_second:.2f}")
        print(f"   • Success Rate: {metrics.success_rate:.1f}%")
        print(f"   • Avg Latency: {metrics.average_latency_ms:.0f}ms")
        print(f"   • Records/sec: {metrics.throughput_records_per_second:.0f}")
        print(f"   • MB/sec: {metrics.throughput_mb_per_second:.2f}")

        # List created files
        print(f"\n📁 Raw Files Created:")
        for file_path in extract_orchestrator.list_raw_files():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"   • {file_path.name} ({size_mb:.2f} MB)")

        print(f"\n✅ Extract stage completed successfully!")
        return True

    except Exception as e:
        print(f"\n❌ Extract stage failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def demo_transform_load_stage():
    """Demo the Transform+Load stage with performance monitoring."""

    print(f"\n\n🔄 DEMO: Transform+Load Stage Orchestrator")
    print("=" * 60)

    # Initialize orchestrator
    transform_load_orchestrator = TransformLoadOrchestrator(
        raw_data_path="data/raw",
        interim_data_path="data/interim",
        processed_data_path="data/processed"
    )

    # Configure for demo
    demo_config = BatchConfig(
        days_per_batch=7,  # Weekly batches for processing
        max_concurrent_batches=3,  # More concurrency for local processing
        operations_per_second_limit=15.0,  # Faster for file operations
        delay_between_operations=0.05,  # 50ms between operations
        enable_performance_monitoring=True
    )
    transform_load_orchestrator.configure_batching(demo_config)

    # Demo parameters (should match extract stage)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 14)
    region = "PACW"
    data_types = ["demand", "generation"]
    stages = ["transform", "load"]

    print(f"\n📊 Demo Parameters:")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Region: {region}")
    print(f"   • Data Types: {data_types}")
    print(f"   • Stages: {stages}")
    print(f"   • Batch Size: {demo_config.days_per_batch} days")
    print(f"   • Max Concurrent: {demo_config.max_concurrent_batches}")

    try:
        print(f"\n🔄 Starting transform+load...")
        results = await transform_load_orchestrator.process_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=data_types,
            stages=stages
        )

        # Show processing summary
        print(f"\n📋 Transform+Load Summary:")
        for stage_type, batch_results in results.items():
            successful = [r for r in batch_results if r.success]
            failed = [r for r in batch_results if not r.success]
            total_records = sum(r.records_processed for r in successful)

            print(f"   • {stage_type}: {len(successful)} successful, {len(failed)} failed")
            print(f"     Records processed: {total_records:,}")

        # Show performance metrics
        metrics = transform_load_orchestrator.get_performance_metrics()
        print(f"\n⚡ Performance Metrics:")
        print(f"   • Duration: {metrics.duration_seconds:.2f} seconds")
        print(f"   • Operations/sec: {metrics.operations_per_second:.2f}")
        print(f"   • Success Rate: {metrics.success_rate:.1f}%")
        print(f"   • Avg Latency: {metrics.average_latency_ms:.0f}ms")
        print(f"   • Records/sec: {metrics.throughput_records_per_second:.0f}")
        print(f"   • MB/sec: {metrics.throughput_mb_per_second:.2f}")

        # List created files
        interim_dir = Path("data/interim")
        processed_dir = Path("data/processed")

        if interim_dir.exists():
            print(f"\n📁 Interim Files Created:")
            for data_type in data_types:
                type_dir = interim_dir / data_type
                if type_dir.exists():
                    files = list(type_dir.glob("*.parquet"))
                    print(f"   • {data_type}: {len(files)} files")

        if processed_dir.exists():
            print(f"\n📁 Processed Files Created:")
            for file_path in processed_dir.rglob("*.parquet"):
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"   • {file_path.relative_to(processed_dir)} ({size_mb:.2f} MB)")

        print(f"\n✅ Transform+Load stage completed successfully!")
        return True

    except Exception as e:
        print(f"\n❌ Transform+Load stage failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def demo_full_pipeline():
    """Demo the complete ETL pipeline."""

    print("🎯 DEMO: Complete ETL Pipeline with Orchestrator Architecture")
    print("=" * 80)

    # Stage 1: Extract
    extract_success = await demo_extract_stage()

    if extract_success:
        # Stage 2: Transform + Load
        transform_load_success = await demo_transform_load_stage()

        if transform_load_success:
            print(f"\n\n🎉 COMPLETE ETL PIPELINE SUCCESS!")
            print("=" * 80)
            print("✅ Extract stage: Raw JSON files created")
            print("✅ Transform stage: Interim Parquet files created")
            print("✅ Load stage: Processed Parquet files created")
            print("\n📊 Full pipeline with performance monitoring operational!")
            print("🚀 Ready for production-scale energy data processing")
        else:
            print(f"\n❌ Pipeline failed at Transform+Load stage")
    else:
        print(f"\n❌ Pipeline failed at Extract stage")


def demo_configuration_examples():
    """Show different configuration examples for different scenarios."""

    print(f"\n\n📋 CONFIGURATION EXAMPLES")
    print("=" * 60)

    # High-throughput configuration
    print("🚀 High-Throughput Configuration (Production):")
    high_throughput = BatchConfig(
        days_per_batch=30,  # Monthly batches
        max_concurrent_batches=10,  # High concurrency
        operations_per_second_limit=20.0,  # Aggressive rate
        delay_between_operations=0.05,  # 50ms delay
        adaptive_batch_sizing=True
    )
    print(f"   • Batch size: {high_throughput.days_per_batch} days")
    print(f"   • Concurrency: {high_throughput.max_concurrent_batches}")
    print(f"   • Rate limit: {high_throughput.operations_per_second_limit} ops/sec")

    # Conservative configuration
    print(f"\n🐢 Conservative Configuration (Testing):")
    conservative = BatchConfig(
        days_per_batch=1,  # Daily batches
        max_concurrent_batches=1,  # Sequential processing
        operations_per_second_limit=2.0,  # Very conservative
        delay_between_operations=0.5,  # 500ms delay
        adaptive_batch_sizing=False
    )
    print(f"   • Batch size: {conservative.days_per_batch} days")
    print(f"   • Concurrency: {conservative.max_concurrent_batches}")
    print(f"   • Rate limit: {conservative.operations_per_second_limit} ops/sec")

    # Balanced configuration
    print(f"\n⚖️  Balanced Configuration (Default):")
    balanced = BatchConfig()  # Use defaults
    print(f"   • Batch size: {balanced.days_per_batch} days")
    print(f"   • Concurrency: {balanced.max_concurrent_batches}")
    print(f"   • Rate limit: {balanced.operations_per_second_limit} ops/sec")


if __name__ == "__main__":
    print("🏭 ETL Orchestrator Architecture Demo")
    print("=" * 80)
    print("This demo showcases the new modular orchestrator architecture:")
    print("• BaseOrchestrator: Shared performance metrics & batching")
    print("• ExtractOrchestrator: Raw data extraction with RPS monitoring")
    print("• TransformLoadOrchestrator: Polars-based data processing")
    print("• Comprehensive performance metrics across all stages")

    try:
        # Show configuration examples
        demo_configuration_examples()

        # Run the full pipeline demo
        asyncio.run(demo_full_pipeline())

    except KeyboardInterrupt:
        print(f"\n\n⏹️  Demo interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
