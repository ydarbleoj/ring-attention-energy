"""
Performance Benchmarks for Energy Data Pipeline.

This module contains comprehensive benchmarks for optimizing API data ingestion
in real-world scenarios. Focus areas:

1. Rate Limit Compliance - Respecting EIA's 5,000 requests/hour limit
2. Concurrent I/O Optimization - Maximizing throughput within constraints
3. Batch Size Optimization - Finding optimal data chunk sizes
4. Error Handling & Resilience - Production-ready error recovery
5. Performance Monitoring - Detailed metrics and analysis

Goal: Build a methodical framework for understanding and maximizing
real-world API data ingestion performance.
"""

__version__ = "1.0.0"
__author__ = "Energy Data Pipeline Team"

from .rate_limited_benchmark import RateLimitedBenchmark

__all__ = [
    "RateLimitedBenchmark"
]
