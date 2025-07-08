"""Comprehensive benchmarking script for DataLoadOrchestrator batch size optimization."""

import asyncio
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv
import polars as pl

from src.core.pipeline.orchestrator import DataLoadOrchestrator, BatchConfig
from src.core.pipeline.config import EnergyPipelineConfig
from src.core.pipeline.collectors.eia_collector import EIACollector
from src.core.pipeline.collectors.caiso_collector import CaisoCollector

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """Results of a batch size benchmark test."""
    collector_name: str
    batch_size_days: int
    total_chunks: int
    total_records: int
    elapsed_time: float
    success_rate: float
    avg_records_per_second: float
    storage_files_created: int
    error_messages: List[str]


class BatchSizeBenchmark:
    """Benchmark different batch sizes for optimal performance."""

    def __init__(self):
        self.eia_api_key = os.getenv("EIA_API_KEY")
        if not self.eia_api_key:
            raise ValueError("EIA_API_KEY not found in environment")

        self.config = EnergyPipelineConfig()
        self.results: List[BenchmarkResult] = []

    async def run_comprehensive_benchmark(self):
        """Run comprehensive benchmark tests."""
        logger.info("=== Comprehensive Batch Size Benchmark ===")

        # Test date range (1 week for quick testing)
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)
        region = "PACW"

        # EIA batch size tests: 7, 14, 30, 60, 90 days
        eia_batch_sizes = [7, 14, 30, 60, 90]
        logger.info(f"Testing EIA batch sizes: {eia_batch_sizes}")

        for batch_size in eia_batch_sizes:
            logger.info(f"\n--- Testing EIA with {batch_size}-day batches ---")
            result = await self._benchmark_collector(
                "eia", batch_size, start_date, end_date, region
            )
            self.results.append(result)

            # Wait between tests to respect rate limits
            await asyncio.sleep(2)

        # CAISO batch size tests: 90, 180, 365 days
        caiso_batch_sizes = [90, 180, 365]
        logger.info(f"\nTesting CAISO batch sizes: {caiso_batch_sizes}")

        for batch_size in caiso_batch_sizes:
            logger.info(f"\n--- Testing CAISO with {batch_size}-day batches ---")
            result = await self._benchmark_collector(
                "caiso", batch_size, start_date, end_date, region
            )
            self.results.append(result)

            # Short wait between tests
            await asyncio.sleep(1)

        # Generate comprehensive report
        self._generate_benchmark_report()

    async def _benchmark_collector(
        self,
        collector_name: str,
        batch_size_days: int,
        start_date: date,
        end_date: date,
        region: str
    ) -> BenchmarkResult:
        """Benchmark a single collector with a specific batch size."""

        # Create fresh orchestrator for each test
        orchestrator = DataLoadOrchestrator(self.config)

        # Override batch config for this test
        orchestrator.batch_configs[collector_name] = BatchConfig(
            collector_name, batch_size_days
        )

        # Create and register collector
        if collector_name == "eia":
            collector = EIACollector(self.eia_api_key)
        elif collector_name == "caiso":
            collector = CaisoCollector()
        else:
            raise ValueError(f"Unknown collector: {collector_name}")

        orchestrator.register_collector(collector_name, collector)

        # Clear any existing storage for clean test
        self._clear_test_storage(collector_name, start_date.year)

        # Run the benchmark
        start_time = time.time()
        error_messages = []

        try:
            results = await orchestrator.load_historical_data(
                start_date=start_date,
                end_date=end_date,
                region=region,
                collector_names=[collector_name],
                parallel=False
            )

            collector_results = results.get(collector_name, [])

            # Calculate metrics
            total_chunks = len(orchestrator.generate_date_chunks(start_date, end_date, collector_name))
            total_records = sum(r.records_collected for r in collector_results)
            successful_results = len([r for r in collector_results if r.success])
            success_rate = successful_results / len(collector_results) if collector_results else 0

            # Count storage files created
            storage_files = self._count_storage_files(collector_name, start_date.year)

        except Exception as e:
            error_messages.append(str(e))
            total_chunks = 0
            total_records = 0
            success_rate = 0
            storage_files = 0

        elapsed_time = time.time() - start_time
        avg_records_per_second = total_records / elapsed_time if elapsed_time > 0 else 0

        result = BenchmarkResult(
            collector_name=collector_name,
            batch_size_days=batch_size_days,
            total_chunks=total_chunks,
            total_records=total_records,
            elapsed_time=elapsed_time,
            success_rate=success_rate,
            avg_records_per_second=avg_records_per_second,
            storage_files_created=storage_files,
            error_messages=error_messages
        )

        logger.info(f"Benchmark result: {result}")
        return result

    def _clear_test_storage(self, collector_name: str, year: int):
        """Clear test storage for clean benchmark."""
        storage_path = Path("data/processed") / collector_name / str(year)
        if storage_path.exists():
            for file in storage_path.glob("*.parquet"):
                file.unlink()
            logger.info(f"Cleared existing storage for {collector_name}/{year}")

    def _count_storage_files(self, collector_name: str, year: int) -> int:
        """Count parquet files created during benchmark."""
        storage_path = Path("data/processed") / collector_name / str(year)
        if storage_path.exists():
            return len(list(storage_path.glob("*.parquet")))
        return 0

    def _generate_benchmark_report(self):
        """Generate comprehensive benchmark report."""
        logger.info("\n" + "="*60)
        logger.info("COMPREHENSIVE BENCHMARK REPORT")
        logger.info("="*60)

        # Group results by collector
        eia_results = [r for r in self.results if r.collector_name == "eia"]
        caiso_results = [r for r in self.results if r.collector_name == "caiso"]

        # EIA Analysis
        if eia_results:
            logger.info("\n📊 EIA BATCH SIZE ANALYSIS")
            logger.info("-" * 40)

            best_eia = max(eia_results, key=lambda x: x.avg_records_per_second)
            logger.info(f"🏆 Best EIA batch size: {best_eia.batch_size_days} days")
            logger.info(f"   - Records/second: {best_eia.avg_records_per_second:.2f}")
            logger.info(f"   - Success rate: {best_eia.success_rate:.2%}")
            logger.info(f"   - Total time: {best_eia.elapsed_time:.2f}s")

            logger.info("\n📈 EIA Performance by Batch Size:")
            for result in sorted(eia_results, key=lambda x: x.batch_size_days):
                status = "✅" if result.success_rate > 0.8 else "⚠️" if result.success_rate > 0.5 else "❌"
                logger.info(
                    f"   {status} {result.batch_size_days:2d} days: "
                    f"{result.avg_records_per_second:6.2f} rec/s, "
                    f"{result.success_rate:.1%} success, "
                    f"{result.elapsed_time:5.2f}s"
                )

        # CAISO Analysis
        if caiso_results:
            logger.info("\n📊 CAISO BATCH SIZE ANALYSIS")
            logger.info("-" * 40)

            best_caiso = max(caiso_results, key=lambda x: x.avg_records_per_second)
            logger.info(f"🏆 Best CAISO batch size: {best_caiso.batch_size_days} days")
            logger.info(f"   - Records/second: {best_caiso.avg_records_per_second:.2f}")
            logger.info(f"   - Success rate: {best_caiso.success_rate:.2%}")
            logger.info(f"   - Total time: {best_caiso.elapsed_time:.2f}s")

            logger.info("\n📈 CAISO Performance by Batch Size:")
            for result in sorted(caiso_results, key=lambda x: x.batch_size_days):
                status = "✅" if result.success_rate > 0.8 else "⚠️" if result.success_rate > 0.5 else "❌"
                logger.info(
                    f"   {status} {result.batch_size_days:3d} days: "
                    f"{result.avg_records_per_second:6.2f} rec/s, "
                    f"{result.success_rate:.1%} success, "
                    f"{result.elapsed_time:5.2f}s"
                )

        # Storage Validation
        logger.info("\n💾 STORAGE VALIDATION")
        logger.info("-" * 40)

        total_files = sum(r.storage_files_created for r in self.results)
        logger.info(f"Total parquet files created: {total_files}")

        # Check file structure
        self._validate_storage_structure()

        # Recommendations
        logger.info("\n🎯 RECOMMENDATIONS")
        logger.info("-" * 40)

        if eia_results:
            best_eia = max(eia_results, key=lambda x: x.avg_records_per_second)
            logger.info(f"✅ Use {best_eia.batch_size_days}-day batches for EIA")

            # Estimate 2000-2024 load time
            chunks_2000_2024 = (25 * 365) // best_eia.batch_size_days
            estimated_time = chunks_2000_2024 * (best_eia.elapsed_time / best_eia.total_chunks)
            logger.info(f"📅 Estimated 2000-2024 load time: {estimated_time/60:.1f} minutes")

        if caiso_results:
            best_caiso = max(caiso_results, key=lambda x: x.avg_records_per_second)
            logger.info(f"✅ Use {best_caiso.batch_size_days}-day batches for CAISO")

            # Estimate 2000-2024 load time
            chunks_2000_2024 = (25 * 365) // best_caiso.batch_size_days
            estimated_time = chunks_2000_2024 * (best_caiso.elapsed_time / best_caiso.total_chunks)
            logger.info(f"📅 Estimated 2000-2024 load time: {estimated_time/60:.1f} minutes")

        logger.info("\n" + "="*60)

        # Save results to file
        self._save_benchmark_results()

    def _validate_storage_structure(self):
        """Validate that storage files follow Option A pattern."""
        storage_base = Path("data/processed")

        for collector_name in ["eia", "caiso"]:
            collector_path = storage_base / collector_name / "2024"
            if collector_path.exists():
                files = list(collector_path.glob("*.parquet"))
                logger.info(f"{collector_name}: {len(files)} files in correct location")

                # Check filename pattern
                for file in files[:3]:  # Show first 3 files
                    logger.info(f"  📄 {file.name}")

                # Validate file contents
                if files:
                    try:
                        sample_df = pl.read_parquet(files[0])
                        logger.info(f"  📊 Sample file: {len(sample_df)} rows, {len(sample_df.columns)} columns")
                        logger.info(f"      Columns: {list(sample_df.columns)}")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Could not read sample file: {e}")

    def _save_benchmark_results(self):
        """Save benchmark results to CSV for analysis."""
        results_data = []
        for result in self.results:
            results_data.append({
                "collector": result.collector_name,
                "batch_size_days": result.batch_size_days,
                "total_chunks": result.total_chunks,
                "total_records": result.total_records,
                "elapsed_time": result.elapsed_time,
                "success_rate": result.success_rate,
                "records_per_second": result.avg_records_per_second,
                "storage_files": result.storage_files_created,
                "has_errors": len(result.error_messages) > 0
            })

        df = pl.DataFrame(results_data)
        output_path = Path("benchmark_results.csv")
        df.write_csv(output_path)
        logger.info(f"📊 Benchmark results saved to: {output_path}")


async def main():
    """Run the comprehensive benchmark."""
    benchmark = BatchSizeBenchmark()
    await benchmark.run_comprehensive_benchmark()


if __name__ == "__main__":
    asyncio.run(main())
