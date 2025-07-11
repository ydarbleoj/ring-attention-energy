#!/usr/bin/env python3
"""
Test script demonstrating the clean, simplified ExtractOrchestrator API.

This shows the two main methods you should use:
1. extract_historical_data() - For batch historical extraction
2. extract_latest_data() - For getting recent data
"""

from datetime import date, timedelta
from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator


def demo_simplified_api():
    """Demonstrate the simplified ExtractOrchestrator API."""

    # Initialize orchestrator
    orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    print("🧪 ExtractOrchestrator Simplified API Demo")
    print("="*50)

    # Example 1: Extract latest data (last 2 days)
    print("\n📊 Example 1: Extract Latest Data")
    print("-" * 30)

    latest_results = orchestrator.extract_latest_data(
        regions=['PACW'],  # Just Oregon for demo
        data_types=['demand'],
        days_back=2
    )

    print(f"✅ Latest data extraction result:")
    print(f"   Success: {latest_results.get('success', False)}")
    print(f"   Files created: {latest_results.get('total_files_created', 0)}")
    print(f"   Duration: {latest_results.get('extraction_duration_seconds', 0):.1f}s")

    # Example 2: Extract historical data (1 week)
    print("\n📈 Example 2: Extract Historical Data")
    print("-" * 30)

    end_date = date.today()
    start_date = end_date - timedelta(days=7)

    historical_results = orchestrator.extract_historical_data(
        start_date=start_date,
        end_date=end_date,
        regions=['PACW'],  # Just Oregon for demo
        data_types=['demand'],
        max_workers=2,
        batch_days=7
    )

    print(f"✅ Historical data extraction result:")
    print(f"   Success: {historical_results.get('success', False)}")
    print(f"   Files created: {historical_results.get('total_files_created', 0)}")
    print(f"   Duration: {historical_results.get('extraction_duration_seconds', 0):.1f}s")
    print(f"   Estimated RPS: {historical_results.get('estimated_rps', 0):.1f}")

    # Show summary
    print("\n📋 Extraction Summary")
    print("-" * 20)
    summary = orchestrator.get_extraction_summary()
    print(f"   Total files: {summary.get('total_files', 0)}")
    print(f"   Total records: {summary.get('total_records', 0):,}")
    print(f"   Total size: {summary.get('total_size_bytes', 0):,} bytes")


if __name__ == "__main__":
    demo_simplified_api()
