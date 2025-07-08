"""
Simple test to validate the orchestrator works with real collectors.
This is a lightweight test before running the full benchmark.
"""

import asyncio
import logging
import os
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
import polars as pl

from src.core.pipeline.orchestrator import DataLoadOrchestrator
from src.core.pipeline.config import EnergyPipelineConfig
from src.core.pipeline.collectors.eia_collector import EIACollector
from src.core.pipeline.collectors.caiso_collector import CAISOCollector

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_real_collectors():
    """Test real collectors with orchestrator."""

    print("🧪 Testing Real Collectors with Orchestrator")
    print("=" * 50)

    # Get EIA API key from environment or config
    eia_api_key = os.getenv("EIA_API_KEY")
    if not eia_api_key:
        print("❌ EIA_API_KEY not found in environment")
        print("   Please set it with: export EIA_API_KEY='your_key'")
        return False

    # Create orchestrator
    config = EnergyPipelineConfig()
    orchestrator = DataLoadOrchestrator(config)

    # Create and register collectors
    print("📋 Creating collectors...")

    try:
        # EIA Collector
        eia_collector = EIACollector(eia_api_key)
        orchestrator.register_collector("eia", eia_collector)
        print("✅ EIA collector registered")

        # CAISO Collector (needs config)
        caiso_config = {
            "storage_path": "data/cache",
            "timeout": 30,
            "retry": {
                "max_retries": 3,
                "initial_delay": 1,
                "max_delay": 60
            }
        }
        caiso_collector = CAISOCollector(caiso_config)
        orchestrator.register_collector("caiso", caiso_collector)
        print("✅ CAISO collector registered")

    except Exception as e:
        print(f"❌ Error creating collectors: {e}")
        return False

    # Test with a very small date range (2 days)
    test_start = date(2024, 1, 1)
    test_end = date(2024, 1, 2)
    region = "PACW"

    print(f"\n🔄 Testing data collection: {test_start} to {test_end}")

    try:
        # Test sequential mode first
        print("Testing sequential mode...")
        results = await orchestrator.load_historical_data(
            start_date=test_start,
            end_date=test_end,
            region=region,
            parallel=False,
            skip_completed=False
        )

        # Check results
        success = True
        for collector_name, collector_results in results.items():
            if collector_results:
                total_records = sum(r.records_collected for r in collector_results)
                success_count = len([r for r in collector_results if r.success])
                print(f"✅ {collector_name}: {success_count}/{len(collector_results)} successful, {total_records} total records")
            else:
                print(f"❌ {collector_name}: No results")
                success = False

        # Test parallel mode
        print("\nTesting parallel mode...")
        results_parallel = await orchestrator.load_historical_data(
            start_date=test_start,
            end_date=test_end,
            region=region,
            parallel=True,
            skip_completed=False
        )

        print("✅ Parallel mode completed")

        # Validate storage
        print("\n💾 Validating storage...")
        base_path = Path("data/cache")  # Changed from data/processed

        for collector_name in ["eia", "caiso"]:
            collector_path = base_path / collector_name / "2024"
            if collector_path.exists():
                files = list(collector_path.glob("*.parquet"))
                print(f"✅ {collector_name}: {len(files)} files created")

                # Show a sample file and validate contents
                if files:
                    sample_file = files[0]
                    print(f"   📄 Sample: {sample_file.name}")
                    try:
                        df = pl.read_parquet(sample_file)
                        print(f"      Records: {len(df)}, Columns: {df.columns}")
                    except Exception as e:
                        print(f"      ❌ Error reading file: {e}")
            else:
                print(f"❌ {collector_name}: No storage directory found")
                success = False

        return success

    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main test execution."""

    print("Starting orchestrator validation test...\n")

    success = await test_real_collectors()

    if success:
        print("\n🎉 SUCCESS! Real collectors working with orchestrator")
        print("\nReady to run full benchmark with:")
        print("   python benchmark_batch_sizes.py")
    else:
        print("\n❌ FAILED! Please check the errors above")
        print("\nTroubleshooting:")
        print("1. Ensure EIA_API_KEY is set")
        print("2. Check internet connectivity")
        print("3. Verify collector implementations")


if __name__ == "__main__":
    asyncio.run(main())
