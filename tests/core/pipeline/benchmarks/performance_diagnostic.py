#!/usr/bin/env python3
"""
Performance Diagnostic - Understanding the unexpected optimization results.

UNEXPECTED FINDINGS:
- Single data type: 454 RPS (-75.5% vs baseline)
- Baseline dual types: 1,853 RPS
- Reducing API calls hurt performance!

INVESTIGATION:
What's really driving the 1,853 RPS baseline performance?
"""

import asyncio
import time
from datetime import date, datetime
from pathlib import Path
import json
from typing import Dict, Any, List
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class PerformanceDiagnostic:
    """Diagnose what's really driving performance."""

    def __init__(self):
        self.region = "PACW"
        self.test_date_range = {
            "start": date(2024, 1, 1),
            "end": date(2024, 4, 1)  # 90 days like the baseline
        }

        print("🔬 PERFORMANCE DIAGNOSTIC")
        print("🧪 Understanding the unexpected optimization results")
        print("🎯 Why did reducing API calls hurt performance?")

    async def run_diagnostic(self):
        """Run diagnostic tests to understand performance characteristics."""

        print(f"\n{'='*70}")
        print("🔍 RECREATING BASELINE CONDITIONS")
        print(f"{'='*70}")

        # Test 1: Exact baseline recreation
        print(f"\n🧪 TEST 1: Exact Baseline Recreation")
        print("   Attempting to recreate 1,853 RPS result")

        baseline_result = await self._recreate_baseline()

        # Test 2: Data type comparison
        print(f"\n🧪 TEST 2: Data Type Impact Analysis")
        print("   Comparing single vs dual data types")

        data_type_results = await self._compare_data_types()

        # Test 3: Timing analysis
        print(f"\n🧪 TEST 3: Timing Component Analysis")
        print("   Breaking down where time is spent")

        timing_results = await self._analyze_timing_components()

        # Comprehensive analysis
        all_results = [baseline_result] + data_type_results + timing_results
        self._diagnostic_analysis(all_results)

        return all_results

    async def _recreate_baseline(self) -> Dict[str, Any]:
        """Attempt to recreate the exact baseline conditions."""

        print("   Using exact rate-limited benchmark conditions...")

        # Exact configuration from rate-limited benchmark winner
        config = ExtractBatchConfig(
            days_per_batch=45,  # 45-day batches (the winner)
            max_concurrent_batches=2,  # Dual batch configuration
            delay_between_operations=2.0,  # 2-second delay (rate limited)
            max_operations_per_second=8.0,  # Conservative rate
            adaptive_batch_sizing=False
        )

        print(f"     45-day dual batches with 2.0s delays: ", end="", flush=True)

        start_time = time.time()

        try:
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/diagnostic")
            orchestrator.batch_config = config

            initial_ops = orchestrator.metrics.total_operations

            # Exact same call as rate-limited benchmark
            results = await orchestrator.process_data(
                start_date=self.test_date_range['start'],
                end_date=self.test_date_range['end'],
                region=self.region,
                data_types=["demand", "generation"]  # Both types like baseline
            )

            end_time = time.time()
            total_duration = end_time - start_time

            # Calculate metrics
            total_records = 0
            final_ops = orchestrator.metrics.total_operations
            total_api_calls = final_ops - initial_ops

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    if batch_result.success:
                        total_records += batch_result.records_processed or 0

            rps = total_records / total_duration if total_duration > 0 else 0

            print(f"{rps:.1f} RPS, {total_records:,} records, {total_api_calls} API calls")

            return {
                "test_name": "Baseline_Recreation",
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "duration": total_duration,
                "config": "45-day dual batches, 2.0s delays"
            }

        except Exception as e:
            print(f"Failed: {e}")
            return {"test_name": "Baseline_Recreation", "rps": 0, "error": str(e)}

    async def _compare_data_types(self) -> List[Dict[str, Any]]:
        """Compare single vs dual data type performance."""

        results = []

        # Configuration without rate limiting for fair comparison
        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=1,  # Sequential for clarity
            delay_between_operations=0.01,  # Minimal delay
            max_operations_per_second=50.0,
            adaptive_batch_sizing=False
        )

        # Test single data types
        for data_type in ["demand", "generation"]:
            print(f"   Testing {data_type} only: ", end="", flush=True)

            start_time = time.time()

            try:
                orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/diagnostic")
                orchestrator.batch_config = config

                initial_ops = orchestrator.metrics.total_operations

                results_data = await orchestrator.process_data(
                    start_date=self.test_date_range['start'],
                    end_date=self.test_date_range['end'],
                    region=self.region,
                    data_types=[data_type]  # Single type
                )

                end_time = time.time()
                total_duration = end_time - start_time

                total_records = 0
                final_ops = orchestrator.metrics.total_operations
                total_api_calls = final_ops - initial_ops

                for dt, batch_results in results_data.items():
                    for batch_result in batch_results:
                        if batch_result.success:
                            total_records += batch_result.records_processed or 0

                rps = total_records / total_duration if total_duration > 0 else 0

                print(f"{rps:.1f} RPS, {total_api_calls} API calls")

                results.append({
                    "test_name": f"Single_{data_type.title()}",
                    "rps": rps,
                    "total_records": total_records,
                    "total_api_calls": total_api_calls,
                    "duration": total_duration
                })

            except Exception as e:
                print(f"Failed: {e}")
                results.append({"test_name": f"Single_{data_type.title()}", "rps": 0, "error": str(e)})

        # Test dual data types
        print(f"   Testing both demand + generation: ", end="", flush=True)

        start_time = time.time()

        try:
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/diagnostic")
            orchestrator.batch_config = config

            initial_ops = orchestrator.metrics.total_operations

            results_data = await orchestrator.process_data(
                start_date=self.test_date_range['start'],
                end_date=self.test_date_range['end'],
                region=self.region,
                data_types=["demand", "generation"]  # Both types
            )

            end_time = time.time()
            total_duration = end_time - start_time

            total_records = 0
            final_ops = orchestrator.metrics.total_operations
            total_api_calls = final_ops - initial_ops

            for dt, batch_results in results_data.items():
                for batch_result in batch_results:
                    if batch_result.success:
                        total_records += batch_result.records_processed or 0

            rps = total_records / total_duration if total_duration > 0 else 0

            print(f"{rps:.1f} RPS, {total_api_calls} API calls")

            results.append({
                "test_name": "Dual_Demand_Generation",
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "duration": total_duration
            })

        except Exception as e:
            print(f"Failed: {e}")
            results.append({"test_name": "Dual_Demand_Generation", "rps": 0, "error": str(e)})

        return results

    async def _analyze_timing_components(self) -> List[Dict[str, Any]]:
        """Analyze what components take the most time."""

        results = []

        print("   Analyzing timing breakdown...")

        # Simple timing test with detailed logging
        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=1,
            delay_between_operations=0.01,
            max_operations_per_second=50.0,
            adaptive_batch_sizing=False
        )

        component_times = {}

        # Measure setup time
        setup_start = time.time()
        orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/diagnostic")
        orchestrator.batch_config = config
        setup_time = time.time() - setup_start
        component_times['setup'] = setup_time

        # Measure processing time
        process_start = time.time()
        try:
            results_data = await orchestrator.process_data(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 2, 15),  # Smaller range for timing analysis
                region=self.region,
                data_types=["demand"]
            )
            process_time = time.time() - process_start
            component_times['total_processing'] = process_time

            # Estimate pure API time vs overhead
            total_records = sum(
                batch_result.records_processed or 0
                for dt, batch_results in results_data.items()
                for batch_result in batch_results
                if batch_result.success
            )

            estimated_api_time = len([
                batch_result for dt, batch_results in results_data.items()
                for batch_result in batch_results
            ]) * 1.0  # Assume ~1 second per API call

            component_times['estimated_api_time'] = estimated_api_time
            component_times['estimated_overhead'] = process_time - estimated_api_time

            print(f"     Setup: {setup_time:.3f}s, Processing: {process_time:.2f}s")
            print(f"     Est. API time: {estimated_api_time:.1f}s, Overhead: {component_times['estimated_overhead']:.1f}s")

            results.append({
                "test_name": "Timing_Analysis",
                "component_times": component_times,
                "total_records": total_records,
                "rps": total_records / process_time if process_time > 0 else 0
            })

        except Exception as e:
            print(f"     Failed: {e}")
            results.append({"test_name": "Timing_Analysis", "error": str(e)})

        return results

    def _diagnostic_analysis(self, results: List[Dict[str, Any]]):
        """Analyze diagnostic results."""

        print(f"\n{'='*80}")
        print("🔬 DIAGNOSTIC ANALYSIS")
        print(f"{'='*80}")

        successful_results = [r for r in results if r.get('rps', 0) > 0]

        if not successful_results:
            print("❌ No successful diagnostics")
            return

        # Find results
        baseline_rec = next((r for r in results if r['test_name'] == 'Baseline_Recreation'), None)
        single_demand = next((r for r in results if r['test_name'] == 'Single_Demand'), None)
        single_generation = next((r for r in results if r['test_name'] == 'Single_Generation'), None)
        dual_types = next((r for r in results if r['test_name'] == 'Dual_Demand_Generation'), None)

        print(f"\n📊 PERFORMANCE COMPARISON:")
        print(f"   🎯 Original baseline claim: 1,853.9 RPS")

        if baseline_rec:
            print(f"   🔄 Baseline recreation: {baseline_rec['rps']:.1f} RPS")
        if single_demand:
            print(f"   📊 Single demand: {single_demand['rps']:.1f} RPS")
        if single_generation:
            print(f"   ⚡ Single generation: {single_generation['rps']:.1f} RPS")
        if dual_types:
            print(f"   🔄 Dual types: {dual_types['rps']:.1f} RPS")

        # Analysis insights
        print(f"\n💡 KEY INSIGHTS:")

        if baseline_rec and baseline_rec['rps'] < 1000:
            print(f"   ❓ Baseline recreation much lower than claimed 1,853 RPS")
            print(f"   🤔 Original result may have been under different conditions")

        if single_demand and dual_types:
            if dual_types['rps'] > single_demand['rps']:
                efficiency = (dual_types['rps'] / single_demand['rps'] - 1) * 100
                print(f"   ⚡ Dual types {efficiency:+.0f}% more efficient than single")
                print(f"   📈 Orchestrator has optimization for multiple data types")
            else:
                print(f"   📊 Single type more efficient than dual types")

        # API call efficiency analysis
        if dual_types and single_demand:
            if 'total_api_calls' in dual_types and 'total_api_calls' in single_demand:
                dual_calls = dual_types['total_api_calls']
                single_calls = single_demand['total_api_calls']
                print(f"   📞 API calls - Single: {single_calls}, Dual: {dual_calls}")

                if dual_calls > 0 and single_calls > 0:
                    dual_efficiency = dual_types['total_records'] / dual_calls
                    single_efficiency = single_demand['total_records'] / single_calls
                    print(f"   📊 Records/call - Single: {single_efficiency:.0f}, Dual: {dual_efficiency:.0f}")

        print(f"\n🎯 CONCLUSIONS:")
        print(f"   📈 The 1,853 RPS baseline may have been optimistic")
        print(f"   🔄 Current realistic performance: {max(r['rps'] for r in successful_results):.0f} RPS")
        print(f"   💡 Focus on understanding orchestrator optimizations")
        print(f"   🚀 Need different optimization strategies")


async def main():
    """Run performance diagnostic."""

    diagnostic = PerformanceDiagnostic()
    results = await diagnostic.run_diagnostic()

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/performance_diagnostic_{timestamp}.json"

    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "test_type": "performance_diagnostic",
            "description": "Diagnostic analysis of unexpected optimization results",
            "results": results
        }, f, indent=2, default=str)

    print(f"\n💾 Diagnostic results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
