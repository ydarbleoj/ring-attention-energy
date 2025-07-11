"""
Benchmark Runner - Execute performance benchmarks with proper organization.

This runner provides a clean interface for executing different benchmark types
and managing results in an organized way.
"""

import asyncio
import sys
from pathlib import Path

# Add current directory to path for local imports
sys.path.append(str(Path(__file__).parent))

from rate_limited_benchmark import RateLimitedBenchmark
from api_call_benchmark import APICallBenchmark


async def run_api_call_benchmark():
    """Run the pure API call performance benchmark."""

    print("⚡ Starting API Call Performance Benchmark")
    print("=" * 60)

    benchmark = APICallBenchmark()
    results = await benchmark.run_benchmark()

    print("\n✅ API Call Benchmark Complete!")
    return results


async def run_rate_limited_benchmark():
    """Run the rate-limited benchmark that respects EIA API limits."""

    print("🚀 Starting Rate-Limited Benchmark")
    print("=" * 60)

    benchmark = RateLimitedBenchmark()
    results = await benchmark.run_benchmark()

    print("\n✅ Rate-Limited Benchmark Complete!")
    return results


async def main():
    """Main benchmark runner."""

    print("📊 ENERGY DATA EXTRACTION BENCHMARKS")
    print("🤝 Respecting API rate limits and being good neighbors")
    print("=" * 70)    # Available benchmarks
    benchmarks = {
        "1": ("API Call Performance Benchmark", run_api_call_benchmark),
        "2": ("Rate-Limited Benchmark", run_rate_limited_benchmark),
    }

    print("\nAvailable Benchmarks:")
    for key, (name, _) in benchmarks.items():
        print(f"  {key}. {name}")

    print("\nRunning API Call Performance Benchmark (pure measurement)...")
    print("-" * 50)

    # Run the API call benchmark first
    await run_api_call_benchmark()

    print("\n" + "="*50)
    print("Running Rate-Limited Benchmark (production simulation)...")
    print("-" * 50)

    # Then run the rate-limited benchmark
    await run_rate_limited_benchmark()

    print("\n🎯 All benchmarks complete!")
    print("📁 Results saved in data/benchmarks/")


if __name__ == "__main__":
    asyncio.run(main())
