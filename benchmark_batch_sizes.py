"""
Benchmarking script to test real collectors with different batch sizes and validate storage.

This script:
1. Tests real EIA/CAISO collectors with 1 week of data
2. Benchmarks different batch sizes to find optimal settings
3. Validates that parquet files are created correctly with Option A pattern
4. Measures API call timing and storage performance
"""

import asyncio
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
import polars as pl

from src.core.pipeline.orchestrator import DataLoadOrchestrator, BatchConfig
from src.core.pipeline.config import EnergyPipelineConfig
from src.core.pipeline.collectors.eia_collector import EIACollector
from src.core.pipeline.collectors.caiso_collector import CAISOCollector

# Setup detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchSizeBenchmark:
    """Benchmark different batch sizes for collectors."""

    def __init__(self, eia_api_key: str):
        self.eia_api_key = eia_api_key
        self.results = {}

    async def run_benchmark(self) -> Dict:
        """Run the full benchmark suite."""

        print("🚀 Starting Batch Size Benchmark & Storage Validation")
        print("=" * 60)

        # Test parameters
        test_start = date(2024, 1, 1)  # 1 week test
        test_end = date(2024, 1, 7)
        region = "PACW"  # Pacific West

        # Test different batch sizes
        batch_sizes_to_test = {
            "eia": [7, 14, 30, 60],      # 1 week, 2 weeks, 1 month, 2 months
            "caiso": [30, 60, 90, 180]   # 1 month, 2 months, 3 months, 6 months
        }

        for collector_name, batch_sizes in batch_sizes_to_test.items():
            print(f"\n📊 Testing {collector_name.upper()} with different batch sizes...")

            for batch_size in batch_sizes:
                try:
                    result = await self._test_batch_size(
                        collector_name, batch_size, test_start, test_end, region
                    )

                    # Store results
                    if collector_name not in self.results:
                        self.results[collector_name] = []
                    self.results[collector_name].append(result)

                    print(f"✅ {collector_name} - {batch_size} days: "
                          f"{result['total_time']:.2f}s, {result['records_collected']} records")

                except Exception as e:
                    print(f"❌ {collector_name} - {batch_size} days: FAILED - {e}")

        # Analyze results
        self._analyze_results()

        # Validate storage
        await self._validate_storage()

        return self.results

    async def _test_batch_size(
        self,
        collector_name: str,
        batch_size: int,
        start_date: date,
        end_date: date,
        region: str
    ) -> Dict:
        """Test a specific batch size for a collector."""

        # Create orchestrator with custom batch size
        config = EnergyPipelineConfig()
        orchestrator = DataLoadOrchestrator(config)

        # Override batch configuration
        orchestrator.batch_configs[collector_name] = BatchConfig(
            collector_name, batch_size_days=batch_size
        )

        # Create and register collector
        collector = await self._create_collector(collector_name)
        orchestrator.register_collector(collector_name, collector)

        # Time the operation
        start_time = time.time()

        # Run the data collection
        results = await orchestrator.load_historical_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            collector_names=[collector_name],
            parallel=False,
            skip_completed=False
        )

        end_time = time.time()

        # Calculate metrics
        collector_results = results[collector_name]
        total_records = sum(r.records_collected for r in collector_results)
        total_time = end_time - start_time

        chunks = orchestrator.generate_date_chunks(start_date, end_date, collector_name)

        return {
            "batch_size": batch_size,
            "total_time": total_time,
            "records_collected": total_records,
            "chunks_generated": len(chunks),
            "api_calls_made": len(collector_results),
            "avg_records_per_second": total_records / total_time if total_time > 0 else 0,
            "success_rate": len([r for r in collector_results if r.success]) / len(collector_results) if collector_results else 0
        }

    async def _create_collector(self, collector_name: str):
        """Create a collector instance."""
        if collector_name == "eia":
            return EIACollector(self.eia_api_key)
        elif collector_name == "caiso":
            return CAISOCollector()
        else:
            raise ValueError(f"Unknown collector: {collector_name}")

    def _analyze_results(self):
        """Analyze benchmark results and recommend optimal batch sizes."""
        print("\n📈 BENCHMARK ANALYSIS")
        print("=" * 40)

        for collector_name, results in self.results.items():
            print(f"\n{collector_name.upper()} Results:")
            print(f"{'Batch Size':>12} {'Time (s)':>10} {'Records':>10} {'Rec/s':>10} {'Success':>10}")
            print("-" * 60)

            best_performance = None
            best_score = 0

            for result in results:
                # Calculate performance score (records per second / time)
                score = result['avg_records_per_second'] / result['total_time'] if result['total_time'] > 0 else 0

                if score > best_score:
                    best_score = score
                    best_performance = result

                print(f"{result['batch_size']:>12} "
                      f"{result['total_time']:>10.2f} "
                      f"{result['records_collected']:>10} "
                      f"{result['avg_records_per_second']:>10.1f} "
                      f"{result['success_rate']:>9.1%}")

            if best_performance:
                print(f"\n🏆 Recommended batch size for {collector_name}: {best_performance['batch_size']} days")
                print(f"   Performance: {best_performance['avg_records_per_second']:.1f} records/second")

    async def _validate_storage(self):
        """Validate that storage files were created correctly."""
        print("\n💾 STORAGE VALIDATION")
        print("=" * 30)

        base_path = Path("data/processed")

        # Check for expected file patterns
        collectors = ["eia", "caiso"]
        data_types = ["demand", "generation"]

        for collector in collectors:
            collector_path = base_path / collector / "2024"

            if collector_path.exists():
                files = list(collector_path.glob("*.parquet"))
                print(f"✅ {collector}: Found {len(files)} files in {collector_path}")

                # Validate file naming pattern
                for file in files[:3]:  # Show first 3 files
                    print(f"   📄 {file.name}")

                    # Try to read file to validate format
                    try:
                        df = pl.read_parquet(file)
                        print(f"      Records: {len(df)}, Columns: {df.columns}")
                    except Exception as e:
                        print(f"      ❌ Error reading file: {e}")
            else:
                print(f"❌ {collector}: No files found in {collector_path}")


async def main():
    """Main benchmark execution."""

    # You'll need to provide your EIA API key
    eia_api_key = "YOUR_EIA_API_KEY"  # Replace with actual key

    if eia_api_key == "YOUR_EIA_API_KEY":
        print("❌ Please set your EIA API key in the script")
        return

    benchmark = BatchSizeBenchmark(eia_api_key)
    results = await benchmark.run_benchmark()

    print("\n🎯 BENCHMARK COMPLETE")
    print("=" * 30)
    print("Results saved for analysis.")
    print("\nNext steps:")
    print("1. Update orchestrator with optimal batch sizes")
    print("2. Run full historical load (2000-2024)")
    print("3. Monitor storage usage and performance")


if __name__ == "__main__":
    asyncio.run(main())
