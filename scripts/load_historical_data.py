#!/usr/bin/env python3
"""
Historical Data Loader: 2012-2025

Loads complete historical EIA data from 2012 to present across all regions.
Processes year by year to manage API limits and create organized data chunks.

Usage:
    python scripts/load_historical_data.py [--start-year 2012] [--end-year 2024] [--continue-on-error]

Performance expectations based on single year test:
- Per year: ~375k records in ~29 seconds (12,881 RPS)
- 13 years: ~4.9M records in ~6.3 minutes
- Storage: ~500MB raw JSON + ~200MB Parquet per year
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
import time

# Add src to Python path
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig
from src.core.pipeline.runners.step_runner import StepRunner


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"logs/historical_data_load_{timestamp}.log"

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file)
        ]
    )
    return log_file


def create_extract_step_for_year(year: int, api_key: str) -> ApiExtractStep:
    """Create extract step for a specific year."""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # All EIA regions
    all_regions = ["PACW", "ERCO", "NYIS", "ISNE", "PJM", "MISO", "SPP", "CARO"]

    config = ApiExtractStepConfig(
        step_name=f"EIA Extract {year}",
        step_id=f"eia_extract_{year}",
        source="eia",
        start_date=start_date,
        end_date=end_date,
        regions=all_regions,
        data_types=["demand", "generation"],
        api_key=api_key,
        dry_run=False
    )

    return ApiExtractStep(config)


def create_transform_step_for_year(year: int) -> DataCleanerStep:
    """Create transform step for a specific year."""
    config = DataCleanerStepConfig(
        step_name=f"EIA Transform {year}",
        step_id=f"eia_transform_{year}",
        source="eia",
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path(f"data/interim"),
        validate_data=True,
        dry_run=False
    )

    return DataCleanerStep(config)


def process_year(year: int, api_key: str, step_runner: StepRunner, logger) -> dict:
    """Process a single year of data."""
    logger.info("="*100)
    logger.info(f"🚀 PROCESSING YEAR {year}")
    logger.info("="*100)

    year_start_time = time.time()

    # Step 1: Extract
    logger.info(f"📥 Step 1: Extracting {year} data...")
    extract_step = create_extract_step_for_year(year, api_key)

    extract_start = time.time()
    extract_result = step_runner.run_step(extract_step, save_metrics=True)
    extract_duration = time.time() - extract_start

    if not extract_result.success:
        logger.error(f"❌ Extract failed for {year}")
        return {"year": year, "success": False, "error": "Extract failed", "extract_result": extract_result}

    extract_records = extract_result.metrics.records_processed
    extract_rps = extract_records / extract_duration if extract_duration > 0 else 0

    logger.info(f"✅ Extract complete: {extract_records:,} records in {extract_duration:.1f}s ({extract_rps:.0f} RPS)")

    # Step 2: Transform
    logger.info(f"🔄 Step 2: Transforming {year} data...")
    transform_step = create_transform_step_for_year(year)

    transform_start = time.time()
    transform_result = step_runner.run_step(transform_step, save_metrics=True)
    transform_duration = time.time() - transform_start

    if not transform_result.success:
        logger.error(f"❌ Transform failed for {year}")
        return {"year": year, "success": False, "error": "Transform failed", "transform_result": transform_result}

    transform_records = transform_result.metrics.records_processed
    transform_rps = transform_records / transform_duration if transform_duration > 0 else 0

    # Handle empty datasets (normal for historical data gaps)
    if transform_records == 0:
        logger.info(f"ℹ️  No data found for {year} (normal for historical gaps)")
    else:
        logger.info(f"✅ Transform complete: {transform_records:,} records in {transform_duration:.1f}s ({transform_rps:.0f} RPS)")

    # Year summary
    total_duration = time.time() - year_start_time
    overall_rps = extract_records / total_duration if total_duration > 0 else 0

    logger.info(f"🏆 Year {year} Summary:")
    logger.info(f"   Records: {extract_records:,}")
    logger.info(f"   Duration: {total_duration:.1f}s")
    logger.info(f"   Throughput: {overall_rps:.0f} RPS")
    logger.info(f"   Extract: {extract_duration:.1f}s ({extract_rps:.0f} RPS)")
    logger.info(f"   Transform: {transform_duration:.1f}s ({transform_rps:.0f} RPS)")
    if transform_records == 0:
        logger.info(f"   Note: Empty dataset (normal for historical data gaps)")

    return {
        "year": year,
        "success": True,
        "extract_records": extract_records,
        "transform_records": transform_records,
        "total_duration": total_duration,
        "extract_duration": extract_duration,
        "transform_duration": transform_duration,
        "overall_rps": overall_rps,
        "extract_rps": extract_rps,
        "transform_rps": transform_rps,
        "extract_result": extract_result,
        "transform_result": transform_result
    }


def load_historical_data(start_year: int, end_year: int, continue_on_error: bool = False):
    """Load historical data from start_year to end_year."""
    logger = logging.getLogger("HistoricalDataLoader")

    # Check API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise ValueError("EIA_API_KEY environment variable required")

    # Setup runner
    runner_output_dir = Path(f"data/pipeline_runs/historical_{start_year}_{end_year}")
    step_runner = StepRunner(output_dir=runner_output_dir)

    # Calculate scope
    years_to_process = list(range(start_year, end_year + 1))
    estimated_records = len(years_to_process) * 375000  # Based on single year test
    estimated_duration = len(years_to_process) * 29  # seconds

    logger.info("🚀 HISTORICAL DATA LOADER STARTING")
    logger.info("="*100)
    logger.info(f"   Years: {start_year}-{end_year} ({len(years_to_process)} years)")
    logger.info(f"   Regions: All 8 EIA regions")
    logger.info(f"   Data Types: demand, generation")
    logger.info(f"   Estimated Records: {estimated_records:,}")
    logger.info(f"   Estimated Duration: {estimated_duration/60:.1f} minutes")
    logger.info(f"   Continue on Error: {continue_on_error}")
    logger.info("="*100)

    # Process each year
    results = []
    total_start_time = time.time()
    total_records = 0
    successful_years = 0
    failed_years = []

    for year in years_to_process:
        try:
            result = process_year(year, api_key, step_runner, logger)
            results.append(result)

            if result["success"]:
                successful_years += 1
                total_records += result["extract_records"]
                logger.info(f"✅ Year {year} completed successfully")
            else:
                failed_years.append(year)
                logger.error(f"❌ Year {year} failed: {result.get('error', 'Unknown error')}")
                if not continue_on_error:
                    logger.error("Stopping due to error (use --continue-on-error to continue)")
                    break

        except Exception as e:
            logger.error(f"❌ Exception processing year {year}: {e}")
            failed_years.append(year)
            if not continue_on_error:
                logger.error("Stopping due to exception (use --continue-on-error to continue)")
                break

    # Final summary
    total_duration = time.time() - total_start_time
    overall_rps = total_records / total_duration if total_duration > 0 else 0

    logger.info("="*100)
    logger.info("🏁 HISTORICAL DATA LOADER COMPLETED")
    logger.info("="*100)
    logger.info(f"📊 Final Results:")
    logger.info(f"   Years Processed: {successful_years}/{len(years_to_process)}")
    logger.info(f"   Total Records: {total_records:,}")
    logger.info(f"   Total Duration: {total_duration/60:.1f} minutes ({total_duration:.1f}s)")
    logger.info(f"   Overall Throughput: {overall_rps:.0f} records/second")
    logger.info(f"   Average per Year: {total_records/successful_years:,.0f} records" if successful_years > 0 else "   No successful years")

    if failed_years:
        logger.warning(f"⚠️  Failed Years: {failed_years}")
    else:
        logger.info("✅ All years processed successfully!")

    # Performance evaluation
    target_rps = 5000
    performance_rating = "🔥 EXCELLENT" if overall_rps >= target_rps else \
                        "✅ GOOD" if overall_rps >= target_rps * 0.8 else \
                        "⚠️  NEEDS IMPROVEMENT"

    logger.info(f"🎯 Performance: {overall_rps:.0f} RPS - {performance_rating}")

    # Data validation summary
    logger.info("📁 Data Organization:")
    logger.info(f"   Raw JSON: data/raw/eia/ (by year)")
    logger.info(f"   Parquet: data/interim/ (consolidated by year)")
    logger.info(f"   Metrics: {runner_output_dir}/")

    return {
        "success": len(failed_years) == 0,
        "years_processed": successful_years,
        "years_failed": len(failed_years),
        "failed_years": failed_years,
        "total_records": total_records,
        "total_duration": total_duration,
        "overall_rps": overall_rps,
        "results": results
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Historical EIA Data Loader (2012-2025)")
    parser.add_argument("--start-year", type=int, default=2012, help="Start year (default: 2012)")
    parser.add_argument("--end-year", type=int, default=2024, help="End year (default: 2024)")
    parser.add_argument("--continue-on-error", action="store_true",
                       help="Continue processing even if a year fails")
    parser.add_argument("--log-level", type=str, default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")

    args = parser.parse_args()

    # Validate year range
    current_year = datetime.now().year
    if args.start_year < 2012:
        print("❌ Start year cannot be before 2012 (EIA data availability)")
        sys.exit(1)
    if args.end_year > current_year:
        print(f"❌ End year cannot be after {current_year}")
        sys.exit(1)
    if args.start_year > args.end_year:
        print("❌ Start year must be <= end year")
        sys.exit(1)

    # Setup logging
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    log_file = setup_logging(args.log_level)

    print(f"📝 Logging to: {log_file}")

    try:
        result = load_historical_data(
            start_year=args.start_year,
            end_year=args.end_year,
            continue_on_error=args.continue_on_error
        )

        # Exit with success/failure code
        sys.exit(0 if result['success'] else 1)

    except Exception as e:
        logging.error(f"❌ Historical data load failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
