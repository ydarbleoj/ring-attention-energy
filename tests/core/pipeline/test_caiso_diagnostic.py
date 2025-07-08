"""
CAISO Data Parsing Diagnostic Test

Focus on fixing the core data parsing issues before optimizing performance.
"""

import asyncio
import sys
from datetime import date
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.core.pipeline.orchestrator import DataLoadOrchestrator
from src.core.pipeline.config import EnergyPipelineConfig
from src.core.pipeline.collectors.caiso_collector import CAISOCollector
from src.core.integrations.caiso.client import CAISOClient

async def diagnose_caiso_parsing():
    """Diagnose CAISO data parsing issues."""

    print("🔍 CAISO Data Parsing Diagnostic")
    print("=" * 40)

    # Test with a very small date range first
    test_start = date(2024, 1, 1)
    test_end = date(2024, 1, 2)  # Just 1 day
    region = "CAISO"

    print(f"📅 Test period: {test_start} to {test_end} (1 day)")
    print(f"🌍 Region: {region}")

    try:
        # Test CAISO client directly first
        print("\n🧪 Testing CAISO client directly...")
        client = CAISOClient()

        # Test with a simple API call
        print("📡 Making direct API call...")
        demand_df = client.get_real_time_demand(
            start_date="20240101",
            end_date="20240102",
            validate=False  # Get raw DataFrame
        )

        print(f"✅ Direct API call successful")
        print(f"   Columns: {list(demand_df.columns) if not demand_df.empty else 'No data'}")
        print(f"   Rows: {len(demand_df)}")
        print(f"   Sample data:")
        if not demand_df.empty:
            print(f"   {demand_df.head(3)}")
        else:
            print("   No data returned")

    except Exception as e:
        print(f"❌ Direct client test failed: {e}")
        import traceback
        traceback.print_exc()
        return

    try:
        # Test collector
        print(f"\n🧪 Testing CAISO collector...")
        caiso_config = {
            "storage_path": "data/cache",
            "timeout": 30,
            "retry": {
                "max_retries": 3,
                "initial_delay": 1,
                "max_delay": 60
            }
        }
        collector = CAISOCollector(caiso_config)

        print("📡 Testing demand collection...")
        demand_result = await collector.collect_demand_data(
            start_date="2024-01-01",
            end_date="2024-01-02",
            region=region,
            save_to_storage=False  # Don't save for diagnostic
        )

        print(f"✅ Collector demand test:")
        print(f"   Success: {demand_result.success}")
        print(f"   Records: {demand_result.records_collected}")
        print(f"   Errors: {demand_result.errors}")

        print("📡 Testing generation collection...")
        generation_result = await collector.collect_generation_data(
            start_date="2024-01-01",
            end_date="2024-01-02",
            region=region,
            save_to_storage=False
        )

        print(f"✅ Collector generation test:")
        print(f"   Success: {generation_result.success}")
        print(f"   Records: {generation_result.records_collected}")
        print(f"   Errors: {generation_result.errors}")

    except Exception as e:
        print(f"❌ Collector test failed: {e}")
        import traceback
        traceback.print_exc()
        return

    try:
        # Test with orchestrator (minimal)
        print(f"\n🧪 Testing with orchestrator...")
        config = EnergyPipelineConfig()
        orchestrator = DataLoadOrchestrator(config)

        orchestrator.register_collector("caiso", collector)

        print("📡 Running orchestrator test...")
        results = await orchestrator.load_historical_data(
            start_date=test_start,
            end_date=test_end,
            region=region,
            collector_names=["caiso"],
            parallel=False,
            skip_completed=False
        )

        caiso_results = results["caiso"]
        total_records = sum(r.records_collected for r in caiso_results if r.success)
        success_count = len([r for r in caiso_results if r.success])

        print(f"✅ Orchestrator test:")
        print(f"   Results: {len(caiso_results)}")
        print(f"   Records: {total_records}")
        print(f"   Success: {success_count}/{len(caiso_results)}")

        for result in caiso_results:
            print(f"   • {result.data_type}: {result.success} ({result.records_collected} records)")
            if not result.success:
                print(f"     Errors: {result.errors}")

    except Exception as e:
        print(f"❌ Orchestrator test failed: {e}")
        import traceback
        traceback.print_exc()

    print(f"\n💡 DIAGNOSTIC SUMMARY")
    print("=" * 25)
    if demand_result and demand_result.success and generation_result and generation_result.success:
        print("✅ All tests passed - ready for optimization!")
    else:
        print("❌ Issues found - need to fix data parsing first")
        print("   Focus areas:")
        if not demand_result.success:
            print("   - Demand data collection")
        if not generation_result.success:
            print("   - Generation data collection")

if __name__ == "__main__":
    asyncio.run(diagnose_caiso_parsing())
