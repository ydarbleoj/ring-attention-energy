#!/usr/bin/env python3
"""
Aggressive Performance Test - Push towards 3000 RPS target.

Based on successful 2105 RPS achievement, this test explores more aggressive 
configurations to reach the upper target of 3000 RPS.
"""

import asyncio
import time
from datetime import date
from pathlib import Path
import json
from typing import Dict, Any

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class AggressivePerformanceTest:
    """Test aggressive configurations for maximum performance."""

    def __init__(self):
        self.test_start_date = date(2024, 1, 1)
        self.test_end_date = date(2024, 3, 1)  # 60 days optimal
        self.region = "PACW"
        self.data_types = ["demand", "generation"]
        
        print("🚀 AGGRESSIVE PERFORMANCE TEST")
        print("🎯 Target: Push towards 3000 RPS")
        print(f"📅 Date Range: {self.test_start_date} to {self.test_end_date}")

    async def run_aggressive_tests(self):
        """Run aggressive performance configurations."""
        
        # Test configurations pushing API limits
        aggressive_configs = [
            {
                "name": "Ultra_Fast_Sequential",
                "days_per_batch": 60,
                "max_concurrent_batches": 1,
                "delay_between_operations": 0.1,  # Very fast
                "max_operations_per_second": 10.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "Minimal_Delay_Sequential",
                "days_per_batch": 60,
                "max_concurrent_batches": 1,
                "delay_between_operations": 0.05,  # Minimal delay
                "max_operations_per_second": 15.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "No_Delay_Sequential",
                "days_per_batch": 60,
                "max_concurrent_batches": 1,
                "delay_between_operations": 0.01,  # Almost no delay
                "max_operations_per_second": 20.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "Optimized_30_Day_Batches",
                "days_per_batch": 30,  # Smaller batches, more API calls
                "max_concurrent_batches": 1,
                "delay_between_operations": 0.1,
                "max_operations_per_second": 10.0,
                "adaptive_batch_sizing": False
            },
            {
                "name": "Optimized_90_Day_Batches",
                "days_per_batch": 90,  # Larger batches, fewer API calls
                "max_concurrent_batches": 1,
                "delay_between_operations": 0.1,
                "max_operations_per_second": 10.0,
                "adaptive_batch_sizing": False
            }
        ]
        
        results = []
        
        for i, config in enumerate(aggressive_configs, 1):
            print(f"\n🔥 Test {i}/{len(aggressive_configs)}: {config['name']}")
            
            name = config.pop("name")
            result = await self._run_single_test(name, config)
            results.append(result)
            
            print(f"   ✅ {result['rps']:.1f} RPS | "
                  f"{result['total_records']:,} records | "
                  f"{result['duration']:.2f}s | "
                  f"{result['success_rate']:.1%} success")
            
            await asyncio.sleep(1)
        
        # Find best result
        best = max(results, key=lambda x: x['rps'])
        
        print(f"\n🏆 BEST RESULT: {best['name']}")
        print(f"   🚀 {best['rps']:.1f} RPS")
        print(f"   📊 {best['total_records']:,} records in {best['duration']:.2f}s")
        print(f"   ✅ {best['success_rate']:.1%} success rate")
        print(f"   ⚙️  Config: {best['config']}")
        
        if best['rps'] >= 3000:
            print(f"\n🎉 OUTSTANDING! Exceeded 3000 RPS target!")
        elif best['rps'] >= 2500:
            print(f"\n🚀 EXCELLENT! Close to 3000 RPS target!")
        else:
            print(f"\n📈 PROGRESS! {best['rps']:.0f} RPS achieved")
        
        return results

    async def _run_single_test(self, name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single aggressive test."""
        
        start_time = time.time()
        
        try:
            # Create orchestrator
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/aggressive")
            
            # Apply config
            batch_config = ExtractBatchConfig(**config)
            orchestrator.batch_config = batch_config
            
            # Run test
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
            
            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    total_batches += 1
                    if batch_result.success:
                        successful_batches += 1
                        total_records += batch_result.records_processed or 0
            
            rps = total_records / duration if duration > 0 else 0
            success_rate = successful_batches / total_batches if total_batches > 0 else 0
            
            return {
                "name": name,
                "config": config,
                "duration": duration,
                "total_records": total_records,
                "rps": rps,
                "success_rate": success_rate,
                "total_batches": total_batches,
                "successful_batches": successful_batches
            }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            print(f"   ❌ Failed: {e}")
            
            return {
                "name": name,
                "config": config,
                "duration": duration,
                "total_records": 0,
                "rps": 0,
                "success_rate": 0,
                "error": str(e)
            }


async def main():
    """Run aggressive performance tests."""
    
    test = AggressivePerformanceTest()
    results = await test.run_aggressive_tests()
    
    # Save results
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/aggressive_performance_{timestamp}.json"
    
    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "results": results
        }, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
