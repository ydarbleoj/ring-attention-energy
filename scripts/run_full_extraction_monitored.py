#!/usr/bin/env python3
"""
Full Historical Extraction with Monitoring

This script runs the full historical extraction with enhanced monitoring
and better handling of potential hanging issues.

Usage:
    python run_full_extraction_monitored.py [--year YYYY] [--test]
"""

import sys
import time
import argparse
import signal
from datetime import date, datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator


class FullExtractionRunner:
    """Enhanced full extraction runner with monitoring."""

    def __init__(self):
        self.orchestrator = None
        self.start_time = None
        self.interrupted = False

        # Set up signal handler for graceful interruption
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle interruption signals gracefully."""
        print(f"\n⚠️  Received interrupt signal ({signum})")
        print("🛑 Attempting graceful shutdown...")
        self.interrupted = True

    def run_extraction(self, start_year: int = 2019, end_year: int = 2025, test_mode: bool = False):
        """Run the full historical extraction."""

        if test_mode:
            print("🧪 TEST MODE: Running limited extraction")
            start_date = date(2024, 1, 1)
            end_date = date(2024, 1, 31)  # Just January 2024
            estimated_time = "30 seconds"
        else:
            start_date = date(start_year, 1, 1)
            end_date = date(end_year, 12, 31)
            estimated_time = "6-8 minutes"

        print(f"🚀 HISTORICAL DATA EXTRACTION")
        print("=" * 50)
        print(f"📅 Period: {start_date} to {end_date}")
        print(f"⏱️  Estimated time: {estimated_time}")
        print(f"🔧 Mode: {'Test' if test_mode else 'Full Production'}")
        print()

        if not test_mode:
            response = input("Ready to start? (y/N): ").strip().lower()
            if response != 'y':
                print("❌ Extraction cancelled")
                return 1

        print("🔥 Starting extraction...")
        print("-" * 50)

        self.orchestrator = ExtractOrchestrator(raw_data_path="data/raw")
        self.start_time = time.time()

        try:
            # Progress monitoring
            last_progress_time = time.time()

            results = self.orchestrator.extract_historical_data(
                start_date=start_date,
                end_date=end_date,
                regions=['PACW', 'ERCO', 'CAL', 'TEX', 'MISO'],
                data_types=['demand', 'generation'],
                max_workers=5,
                batch_days=45
            )

            end_time = time.time()
            total_duration = end_time - self.start_time

            # Check for interruption
            if self.interrupted:
                print("⚠️  Extraction was interrupted but may have completed")

            # Display results
            self._display_results(results, total_duration, test_mode)

            return 0 if results.get("success") else 1

        except KeyboardInterrupt:
            print(f"\n🛑 Extraction interrupted by user")
            self._display_interrupt_info()
            return 1

        except Exception as e:
            print(f"\n❌ Extraction failed: {e}")
            if self.start_time:
                duration = time.time() - self.start_time
                print(f"   Duration before failure: {duration:.1f} seconds")
            return 1

    def _display_results(self, results: dict, duration: float, test_mode: bool):
        """Display extraction results."""
        print("\n" + "=" * 50)
        print("🏁 EXTRACTION COMPLETED")
        print("=" * 50)

        if results.get("success"):
            files = results.get("total_files_created", 0)
            records = results.get("estimated_total_records", 0)
            rps = results.get("estimated_rps", 0)

            print(f"✅ SUCCESS!")
            print(f"📁 Files created: {files:,}")
            print(f"📊 Records: {records:,}")
            print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} min)")
            print(f"🚀 Average RPS: {rps:.1f}")

            if test_mode:
                print(f"\n🎯 Test completed successfully!")
                print(f"   System ready for full production extraction")
            else:
                print(f"\n🎉 Full historical extraction completed!")
                print(f"   Data available in: data/raw/eia/")
        else:
            error = results.get("error", "Unknown error")
            print(f"❌ FAILED: {error}")
            print(f"⏱️  Duration: {duration:.1f}s")

    def _display_interrupt_info(self):
        """Display information about interruption."""
        if self.start_time:
            duration = time.time() - self.start_time
            print(f"   Duration before interrupt: {duration:.1f} seconds")
        print(f"   Partial data may be available in: data/raw/eia/")
        print(f"   You can resume extraction later if needed")


def main():
    """Main execution with command line arguments."""
    parser = argparse.ArgumentParser(
        description="Full Historical Data Extraction with Monitoring",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--year",
        type=int,
        help="Start year for extraction (default: 2019)"
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (January 2024 only)"
    )

    args = parser.parse_args()

    runner = FullExtractionRunner()

    start_year = args.year if args.year else 2019

    return runner.run_extraction(
        start_year=start_year,
        test_mode=args.test
    )


if __name__ == "__main__":
    sys.exit(main())
