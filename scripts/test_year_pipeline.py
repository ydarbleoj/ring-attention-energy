#!/usr/bin/env python3
"""
Year-Long Pipeline Performance Test

Tests the complete pipeline (ApiExtractStep → DataCleanerStep) with a full year
of data across ALL EIA regions to validate performance and achieve 5k+ records/second throughput.

Usage:
    python scripts/test_year_pipeline.py [--year 2024] [--dry-run]

Performance targets:
- Extract: 500+ records/second (API limited)
- Transform: 10k+ records/second (Polars optimized)
- Overall: 5k+ records/second end-to-end

All EIA regions tested: PACW, ERCO, NYIS, ISNE, PJM, MISO, SPP, CARO
Expected records: ~140k+ (8 regions × 2 data types × 365 days × 24 hours)
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add src to Python path
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig
from src.core.pipeline.runners.step_runner import StepRunner, SequentialRunner


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"logs/year_pipeline_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        ]
    )


def create_extract_step(year: int, api_key: str, dry_run: bool = False) -> ApiExtractStep:
    """Create optimized extract step for year-long data across all EIA regions."""

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # All EIA regions for comprehensive testing
    all_regions = ["PACW", "ERCO", "NYIS", "ISNE", "PJM", "MISO", "SPP", "CARO"]

    config = ApiExtractStepConfig(
        step_name=f"EIA Extract {year} All Regions",
        step_id=f"eia_extract_{year}_all_regions",
        source="eia",
        start_date=start_date,
        end_date=end_date,
        regions=all_regions,  # All regions for proper performance testing
        data_types=["demand", "generation"],
        api_key=api_key,
        dry_run=dry_run
    )

    return ApiExtractStep(config)


def create_transform_step(year: int, dry_run: bool = False) -> DataCleanerStep:
    """Create optimized transform step for year-long data across all regions."""

    config = DataCleanerStepConfig(
        step_name=f"EIA Transform {year} All Regions",
        step_id=f"eia_transform_{year}_all_regions",
        source="eia",
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path(f"data/interim/{year}"),
        validate_data=True,
        dry_run=dry_run
    )

    return DataCleanerStep(config)


def run_year_pipeline_test(year: int, dry_run: bool = False):
    """Run the complete year-long pipeline test with all EIA regions."""

    logger = logging.getLogger("YearPipelineTest")

    # Check API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key and not dry_run:
        raise ValueError("EIA_API_KEY environment variable required for real execution")

    # All EIA regions for comprehensive testing
    all_regions = ["PACW", "ERCO", "NYIS", "ISNE", "PJM", "MISO", "SPP", "CARO"]

    logger.info("🚀 Starting Year-Long Pipeline Performance Test")
    logger.info(f"   Year: {year}")
    logger.info(f"   Regions: {', '.join(all_regions)} (All EIA regions)")
    logger.info(f"   Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
    logger.info(f"   Target: 5k+ records/second overall throughput")
    logger.info(f"   Expected records: ~140k+ (8 regions × 2 data types × 365 days × 24 hours)")
    logger.info("")

    # Create extract step
    extract_step = create_extract_step(year, api_key or "", dry_run)

    # Setup runners
    runner_output_dir = Path(f"data/pipeline_runs/{year}_all_regions")
    step_runner = StepRunner(output_dir=runner_output_dir)

    # Option 1: Run steps individually with detailed monitoring
    logger.info("📋 Execution Strategy: Individual steps with detailed monitoring")

    # Step 1: Extract
    logger.info("="*80)
    logger.info("📥 STEP 1: EXTRACT")
    logger.info("="*80)

    extract_start = datetime.now()
    extract_result = step_runner.run_step(extract_step, save_metrics=True)
    extract_duration = (datetime.now() - extract_start).total_seconds()

    if not extract_result.success:
        logger.error("❌ Extract step failed - aborting pipeline")
        return extract_result

    # Extract performance analysis
    extract_records = extract_result.metrics.records_processed
    extract_rps = extract_records / extract_duration if extract_duration > 0 else 0

    logger.info(f"✅ Extract Performance Summary:")
    logger.info(f"   Records: {extract_records:,}")
    logger.info(f"   Duration: {extract_duration:.2f}s")
    logger.info(f"   Throughput: {extract_rps:.1f} records/second")
    logger.info(f"   API calls: {extract_result.metrics.api_calls_made}")
    logger.info(f"   Files created: {extract_result.metrics.files_created}")

    # Performance check
    if extract_rps < 100:
        logger.warning(f"⚠️  Extract throughput below expected minimum (100 RPS)")

    # Step 2: Transform - CREATE ONLY AFTER EXTRACT IS COMPLETE
    logger.info("="*80)
    logger.info("🔄 STEP 2: TRANSFORM")
    logger.info("="*80)

    # NOW create the transform step after extract has completed
    transform_step = create_transform_step(year, dry_run)

    transform_start = datetime.now()
    transform_result = step_runner.run_step(transform_step, save_metrics=True)
    transform_duration = (datetime.now() - transform_start).total_seconds()

    if not transform_result.success:
        logger.error("❌ Transform step failed")
        return transform_result

    # Transform performance analysis
    transform_records = transform_result.metrics.records_processed
    transform_rps = transform_records / transform_duration if transform_duration > 0 else 0

    logger.info(f"✅ Transform Performance Summary:")
    logger.info(f"   Records: {transform_records:,}")
    logger.info(f"   Duration: {transform_duration:.2f}s")
    logger.info(f"   Throughput: {transform_rps:.1f} records/second")
    logger.info(f"   Files created: {transform_result.metrics.files_created}")

    # Performance check
    if transform_rps < 1000:
        logger.warning(f"⚠️  Transform throughput below expected minimum (1000 RPS)")

    # Overall pipeline analysis
    logger.info("="*80)
    logger.info("📊 OVERALL PIPELINE PERFORMANCE")
    logger.info("="*80)

    total_duration = extract_duration + transform_duration
    total_records = extract_records
    overall_rps = total_records / total_duration if total_duration > 0 else 0

    logger.info(f"🏆 Pipeline Summary:")
    logger.info(f"   Total Records: {total_records:,}")
    logger.info(f"   Total Duration: {total_duration:.2f}s")
    logger.info(f"   Overall Throughput: {overall_rps:.1f} records/second")
    logger.info(f"   Extract: {extract_duration:.2f}s ({extract_rps:.1f} RPS)")
    logger.info(f"   Transform: {transform_duration:.2f}s ({transform_rps:.1f} RPS)")

    # Performance evaluation
    logger.info("="*80)
    logger.info("🎯 PERFORMANCE EVALUATION")
    logger.info("="*80)

    target_rps = 5000
    performance_rating = "🔥 EXCELLENT" if overall_rps >= target_rps else \
                        "✅ GOOD" if overall_rps >= target_rps * 0.8 else \
                        "⚠️  NEEDS IMPROVEMENT" if overall_rps >= target_rps * 0.5 else \
                        "❌ POOR"

    logger.info(f"Target: {target_rps:,} records/second")
    logger.info(f"Achieved: {overall_rps:.1f} records/second")
    logger.info(f"Rating: {performance_rating}")

    if overall_rps >= target_rps:
        logger.info("🎉 Performance target achieved!")
    else:
        logger.info("💡 Performance optimization suggestions:")
        if extract_rps < 500:
            logger.info("   • Increase API concurrency in EIA config")
            logger.info("   • Optimize batch size for better API usage")
        if transform_rps < 5000:
            logger.info("   • Enable Polars optimizations")
            logger.info("   • Increase parallel processing workers")

    # Data validation
    if not dry_run and transform_result.output_paths:
        logger.info("="*80)
        logger.info("🔍 DATA VALIDATION")
        logger.info("="*80)

        try:
            import polars as pl
            output_file = transform_result.output_paths[0]
            df = pl.read_parquet(output_file)

            logger.info(f"✅ Output validation:")
            logger.info(f"   File: {output_file.name}")
            logger.info(f"   Size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
            logger.info(f"   Shape: {df.shape}")
            logger.info(f"   Columns: {df.columns}")
            logger.info(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            logger.info(f"   Data types: {df['data_type'].unique().to_list()}")
            logger.info(f"   Regions: {df['region'].unique().to_list()}")
            logger.info(f"   Value range: {df['value'].min()} to {df['value'].max()}")

            # Check for expected year coverage
            year_coverage = df['timestamp'].dt.year().unique().to_list()
            if year in year_coverage:
                logger.info(f"✅ Year {year} data coverage confirmed")
            else:
                logger.warning(f"⚠️  Expected year {year} not found in data")

            # Check for expected region coverage
            expected_regions = set(all_regions)
            actual_regions = set(df['region'].unique().to_list())
            if expected_regions.issubset(actual_regions):
                logger.info(f"✅ All expected regions found in data")
            else:
                missing_regions = expected_regions - actual_regions
                logger.warning(f"⚠️  Missing regions: {missing_regions}")

        except Exception as e:
            logger.error(f"❌ Data validation failed: {e}")

    # Final results
    result_summary = {
        'success': extract_result.success and transform_result.success,
        'year': year,
        'regions': all_regions,
        'total_records': total_records,
        'total_duration': total_duration,
        'overall_rps': overall_rps,
        'extract_rps': extract_rps,
        'transform_rps': transform_rps,
        'target_achieved': overall_rps >= target_rps,
        'performance_rating': performance_rating,
        'extract_result': extract_result,
        'transform_result': transform_result
    }

    logger.info("="*80)
    logger.info("✅ YEAR-LONG PIPELINE TEST COMPLETED")
    logger.info("="*80)

    return result_summary


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Year-Long Pipeline Performance Test")
    parser.add_argument("--year", type=int, default=2024, help="Year to extract data for")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode")
    parser.add_argument("--log-level", type=str, default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")

    args = parser.parse_args()

    # Setup logging
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    setup_logging(args.log_level)

    try:
        result = run_year_pipeline_test(
            year=args.year,
            dry_run=args.dry_run
        )

        # Exit with success/failure code
        sys.exit(0 if result['success'] else 1)

    except Exception as e:
        logging.error(f"❌ Pipeline test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
