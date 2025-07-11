#!/usr/bin/env python3
"""
Comprehensive 60-day batch performance benchmark for Extract Orchestrator.

This script runs multiple test configurations to benchmark towards 2000-3000 RPS:
1. 3 sequential benchmark tests (baseline)
2. Multiple parallel configurations (concurrency scaling)
3. Performance analysis and optimization recommendations

Based on llm-guide.yaml requirements for scalable energy data pipeline.
"""

import asyncio
import time
from datetime import date, timedelta
from pathlib import Path
import json
import statistics
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass, asdict

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""
    test_name: str
    config: Dict[str, Any]
    start_time: float
    end_time: float
    duration_seconds: float
    total_records: int
    records_per_second: float
    total_batches: int
    successful_batches: int
    failed_batches: int
    avg_batch_latency: float
    total_api_calls: int
    success_rate: float
    memory_usage_mb: float = 0.0


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results with analysis."""
    sequential_results: List[BenchmarkResult]
    parallel_results: List[BenchmarkResult]
    best_performance: BenchmarkResult
    recommendations: Dict[str, Any]


class PerformanceBenchmark:
    """Advanced performance benchmarking for Extract Orchestrator."""

    def __init__(self, base_output_dir: str = "data/benchmarks"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

        # Test configuration - 60 days as optimal batch size
        self.test_start_date = date(2024, 1, 1)
        self.test_end_date = date(2024, 3, 1)  # 60 days
        self.region = "PACW"
        self.data_types = ["demand", "generation"]

        print(f"🚀 Performance Benchmark Suite Initialized")
        print(f"📅 Date Range: {self.test_start_date} to {self.test_end_date} (60 days)")
        print(f"🌎 Region: {self.region}")
        print(f"📊 Data Types: {self.data_types}")
        print(f"🎯 Target: 2000-3000 RPS\n")

    async def run_comprehensive_benchmark(self) -> BenchmarkSuite:
        """Run complete benchmark suite with sequential and parallel tests."""

        print("=" * 80)
        print("🧪 COMPREHENSIVE PERFORMANCE BENCHMARK SUITE")
        print("=" * 80)

        # Phase 1: Sequential Baseline Tests (3 runs)
        print("\n📈 PHASE 1: Sequential Baseline Tests")
        print("-" * 50)
        sequential_results = await self._run_sequential_tests()

        # Phase 2: Parallel Configuration Tests
        print("\n🚀 PHASE 2: Parallel Configuration Tests")
        print("-" * 50)
        parallel_results = await self._run_parallel_tests()

        # Phase 3: Analysis and Recommendations
        print("\n📊 PHASE 3: Performance Analysis")
        print("-" * 50)
        analysis = self._analyze_results(sequential_results, parallel_results)

        # Create benchmark suite
        suite = BenchmarkSuite(
            sequential_results=sequential_results,
            parallel_results=parallel_results,
            best_performance=analysis["best_result"],
            recommendations=analysis["recommendations"]
        )

        # Save results
        await self._save_benchmark_results(suite)

        # Print summary
        self._print_final_summary(suite)

        return suite

    async def _run_sequential_tests(self) -> List[BenchmarkResult]:
        """Run 3 sequential benchmark tests for baseline measurement."""

        results = []

        for run_num in range(1, 4):
            print(f"\n🔄 Sequential Test {run_num}/3")

            # Standard configuration for sequential testing
            config = {
                "days_per_batch": 60,
                "max_concurrent_batches": 1,  # Sequential
                "delay_between_operations": 0.3,
                "max_operations_per_second": 5.0,
                "adaptive_batch_sizing": False
            }

            result = await self._run_single_benchmark(
                test_name=f"Sequential_Run_{run_num}",
                config=config
            )

            results.append(result)

            # Print immediate results
            print(f"   ✅ {result.records_per_second:.1f} RPS | "
                  f"{result.total_records:,} records | "
                  f"{result.duration_seconds:.2f}s | "
                  f"{result.success_rate:.1%} success")

            # Brief pause between runs
            await asyncio.sleep(2)

        # Calculate sequential baseline
        avg_rps = statistics.mean([r.records_per_second for r in results])
        print(f"\n📊 Sequential Baseline: {avg_rps:.1f} RPS (avg of 3 runs)")

        return results

    async def _run_parallel_tests(self) -> List[BenchmarkResult]:
        """Run parallel configuration tests with increasing concurrency."""

        results = []

        # Test configurations with increasing parallelism
        test_configs = [
            {
                "name": "Parallel_2_Batches",
                "days_per_batch": 60,
                "max_concurrent_batches": 2,
                "delay_between_operations": 0.25,
                "max_operations_per_second": 6.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "Parallel_3_Batches",
                "days_per_batch": 60,
                "max_concurrent_batches": 3,
                "delay_between_operations": 0.2,
                "max_operations_per_second": 7.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "Parallel_4_Batches",
                "days_per_batch": 60,
                "max_concurrent_batches": 4,
                "delay_between_operations": 0.15,
                "max_operations_per_second": 8.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "Parallel_5_Batches_Aggressive",
                "days_per_batch": 60,
                "max_concurrent_batches": 5,
                "delay_between_operations": 0.1,
                "max_operations_per_second": 10.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "Parallel_6_Batches_Maximum",
                "days_per_batch": 60,
                "max_concurrent_batches": 6,
                "delay_between_operations": 0.08,
                "max_operations_per_second": 12.0,
                "adaptive_batch_sizing": True
            }
        ]

        for i, test_config in enumerate(test_configs, 1):
            print(f"\n🚀 Parallel Test {i}/{len(test_configs)}: {test_config['name']}")
            print(f"   ⚡ {test_config['max_concurrent_batches']} concurrent batches, "
                  f"{test_config['delay_between_operations']}s delay, "
                  f"{test_config['max_operations_per_second']} RPS limit")

            # Extract config for orchestrator
            name = test_config.pop("name")
            result = await self._run_single_benchmark(
                test_name=name,
                config=test_config
            )

            results.append(result)

            # Print immediate results with comparison
            if i == 1:
                baseline = results[0].records_per_second
            else:
                improvement = ((result.records_per_second - baseline) / baseline) * 100
                print(f"   ✅ {result.records_per_second:.1f} RPS | "
                      f"{result.total_records:,} records | "
                      f"{result.duration_seconds:.2f}s | "
                      f"{result.success_rate:.1%} success | "
                      f"{improvement:+.1f}% vs baseline")

            if i == 1:
                print(f"   ✅ {result.records_per_second:.1f} RPS | "
                      f"{result.total_records:,} records | "
                      f"{result.duration_seconds:.2f}s | "
                      f"{result.success_rate:.1%} success")
                baseline = result.records_per_second

            # Pause between tests
            await asyncio.sleep(3)

        return results

    async def _run_single_benchmark(
        self,
        test_name: str,
        config: Dict[str, Any]
    ) -> BenchmarkResult:
        """Run a single benchmark test with specified configuration."""

        start_time = time.time()

        try:
            # Create orchestrator with custom config
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/raw")

            # Apply custom batch config
            batch_config = ExtractBatchConfig(**config)
            orchestrator.batch_config = batch_config

            # Run extraction
            results = await orchestrator.process_data(
                start_date=self.test_start_date,
                end_date=self.test_end_date,
                region=self.region,
                data_types=self.data_types
            )

            end_time = time.time()
            duration = end_time - start_time

            # Calculate metrics
            total_records = 0
            total_batches = 0
            successful_batches = 0
            failed_batches = 0
            total_latencies = []
            total_api_calls = 0

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    total_batches += 1
                    total_api_calls += 1  # Each batch = 1 API call

                    if batch_result.success:
                        successful_batches += 1
                        total_records += batch_result.records_processed or 0
                        if hasattr(batch_result, 'operation_latency_seconds'):
                            total_latencies.append(batch_result.operation_latency_seconds)
                    else:
                        failed_batches += 1

            # Calculate performance metrics
            records_per_second = total_records / duration if duration > 0 else 0
            success_rate = successful_batches / total_batches if total_batches > 0 else 0
            avg_batch_latency = statistics.mean(total_latencies) if total_latencies else 0

            return BenchmarkResult(
                test_name=test_name,
                config=config.copy(),
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                total_records=total_records,
                records_per_second=records_per_second,
                total_batches=total_batches,
                successful_batches=successful_batches,
                failed_batches=failed_batches,
                avg_batch_latency=avg_batch_latency,
                total_api_calls=total_api_calls,
                success_rate=success_rate
            )

        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time

            print(f"   ❌ Test failed: {e}")

            return BenchmarkResult(
                test_name=test_name,
                config=config.copy(),
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                total_records=0,
                records_per_second=0,
                total_batches=0,
                successful_batches=0,
                failed_batches=1,
                avg_batch_latency=0,
                total_api_calls=0,
                success_rate=0
            )

    def _analyze_results(
        self,
        sequential_results: List[BenchmarkResult],
        parallel_results: List[BenchmarkResult]
    ) -> Dict[str, Any]:
        """Analyze benchmark results and generate recommendations."""

        all_results = sequential_results + parallel_results

        # Find best performing configuration
        best_result = max(all_results, key=lambda r: r.records_per_second)

        # Calculate improvements
        sequential_baseline = statistics.mean([r.records_per_second for r in sequential_results])

        # Analyze scaling efficiency
        scaling_analysis = []
        for result in parallel_results:
            concurrency = result.config.get("max_concurrent_batches", 1)
            theoretical_max = sequential_baseline * concurrency
            efficiency = (result.records_per_second / theoretical_max) * 100 if theoretical_max > 0 else 0

            scaling_analysis.append({
                "concurrency": concurrency,
                "actual_rps": result.records_per_second,
                "theoretical_max": theoretical_max,
                "efficiency": efficiency,
                "success_rate": result.success_rate
            })

        # Generate recommendations
        recommendations = {
            "optimal_config": best_result.config,
            "performance_improvement": (best_result.records_per_second / sequential_baseline - 1) * 100,
            "target_progress": {
                "current_best": best_result.records_per_second,
                "target_min": 2000,
                "target_max": 3000,
                "gap_to_min": 2000 - best_result.records_per_second,
                "progress_to_min": (best_result.records_per_second / 2000) * 100
            },
            "scaling_efficiency": scaling_analysis,
            "next_steps": self._generate_optimization_recommendations(best_result, scaling_analysis)
        }

        return {
            "best_result": best_result,
            "recommendations": recommendations
        }

    def _generate_optimization_recommendations(
        self,
        best_result: BenchmarkResult,
        scaling_analysis: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate specific optimization recommendations."""

        recommendations = []

        current_rps = best_result.records_per_second
        target_min = 2000

        if current_rps < target_min:
            gap = target_min - current_rps
            gap_percentage = (gap / target_min) * 100

            recommendations.append(f"Performance gap: {gap:.0f} RPS ({gap_percentage:.1f}% below target)")

            # API optimization recommendations
            if best_result.config.get("delay_between_operations", 0.3) > 0.05:
                recommendations.append("Reduce delay_between_operations to 0.05s (aggressive rate limiting)")

            # Concurrency recommendations
            max_concurrency = best_result.config.get("max_concurrent_batches", 1)
            if max_concurrency < 10:
                recommendations.append(f"Test higher concurrency: {max_concurrency + 2}-{max_concurrency + 5} batches")

            # Connection pooling recommendations
            recommendations.append("Implement HTTP connection pooling with 15-20 connections")
            recommendations.append("Add request pipelining for multiple concurrent requests per connection")

            # Batch optimization
            recommendations.append("Test 30-day and 90-day batch sizes for optimal API call efficiency")

            # Infrastructure recommendations
            if current_rps < 1000:
                recommendations.append("Consider async request batching (multiple API calls per batch)")
                recommendations.append("Implement request queuing and burst processing")

            # Advanced optimizations
            if current_rps < 1500:
                recommendations.append("Multi-region parallel processing (PACW + other regions)")
                recommendations.append("Implement response streaming and incremental processing")

        else:
            recommendations.append("🎯 Target performance achieved! Focus on stability and reliability")
            recommendations.append("Implement production monitoring and alerting")
            recommendations.append("Add performance regression testing")

        return recommendations

    async def _save_benchmark_results(self, suite: BenchmarkSuite):
        """Save benchmark results to JSON file."""

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_60day_performance_{timestamp}.json"
        file_path = self.base_output_dir / filename

        # Convert to serializable format
        suite_data = {
            "timestamp": timestamp,
            "test_configuration": {
                "date_range": f"{self.test_start_date} to {self.test_end_date}",
                "region": self.region,
                "data_types": self.data_types,
                "batch_size_days": 60
            },
            "sequential_results": [asdict(r) for r in suite.sequential_results],
            "parallel_results": [asdict(r) for r in suite.parallel_results],
            "best_performance": asdict(suite.best_performance),
            "recommendations": suite.recommendations
        }

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(suite_data, f, indent=2, default=str)

        print(f"\n💾 Results saved to: {file_path}")

    def _print_final_summary(self, suite: BenchmarkSuite):
        """Print comprehensive benchmark summary."""

        print("\n" + "=" * 80)
        print("🏆 BENCHMARK RESULTS SUMMARY")
        print("=" * 80)

        # Sequential baseline
        sequential_avg = statistics.mean([r.records_per_second for r in suite.sequential_results])
        print(f"\n📊 Sequential Baseline (3 runs):")
        print(f"   Average: {sequential_avg:.1f} RPS")
        print(f"   Range: {min(r.records_per_second for r in suite.sequential_results):.1f} - "
              f"{max(r.records_per_second for r in suite.sequential_results):.1f} RPS")

        # Best performance
        best = suite.best_performance
        improvement = ((best.records_per_second / sequential_avg) - 1) * 100
        print(f"\n🚀 Best Performance: {best.test_name}")
        print(f"   Records/Second: {best.records_per_second:.1f} RPS")
        print(f"   Improvement: +{improvement:.1f}% vs sequential baseline")
        print(f"   Success Rate: {best.success_rate:.1%}")
        print(f"   Configuration: {best.config}")

        # Target progress
        target_progress = suite.recommendations["target_progress"]
        print(f"\n🎯 Target Progress:")
        print(f"   Current Best: {target_progress['current_best']:.1f} RPS")
        print(f"   Target Range: {target_progress['target_min']:,} - {target_progress['target_max']:,} RPS")
        print(f"   Progress to Minimum: {target_progress['progress_to_min']:.1f}%")

        if target_progress['gap_to_min'] > 0:
            print(f"   Gap to Target: {target_progress['gap_to_min']:.0f} RPS remaining")
        else:
            print(f"   🎉 TARGET ACHIEVED!")

        # Top recommendations
        print(f"\n💡 Top Optimization Recommendations:")
        for i, rec in enumerate(suite.recommendations["next_steps"][:5], 1):
            print(f"   {i}. {rec}")

        # Scaling efficiency
        print(f"\n📈 Parallel Scaling Efficiency:")
        for analysis in suite.recommendations["scaling_efficiency"]:
            print(f"   {analysis['concurrency']} batches: "
                  f"{analysis['actual_rps']:.1f} RPS "
                  f"({analysis['efficiency']:.1f}% efficiency)")

        print("\n" + "=" * 80)
        print("✅ Benchmark Complete!")
        print("=" * 80)


async def main():
    """Run the comprehensive 60-day performance benchmark."""

    benchmark = PerformanceBenchmark()

    try:
        results = await benchmark.run_comprehensive_benchmark()

        # Additional analysis
        best_rps = results.best_performance.records_per_second

        if best_rps >= 2000:
            print(f"\n🎉 SUCCESS! Achieved {best_rps:.1f} RPS - Target reached!")
        elif best_rps >= 1000:
            print(f"\n🚀 GOOD PROGRESS! {best_rps:.1f} RPS - Halfway to target")
        else:
            print(f"\n⚡ BASELINE ESTABLISHED! {best_rps:.1f} RPS - Foundation for optimization")

        return results

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
