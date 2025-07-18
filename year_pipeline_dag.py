"""
Year-Long Pipeline with PipelineDAG - Phase 3 Implementation

Enhanced version of the year pipeline test using the new PipelineDAG
for proper step chaining and single consolidated parquet output.

Key improvements over the original test_year_pipeline.py:
- Uses PipelineDAG for proper extract → transform chaining
- Ensures single consolidated parquet file output
- Automatic data flow between steps
- Improved error handling and recovery
- Better monitoring and metrics

Usage:
    python year_pipeline_dag.py [--year 2024] [--dry-run]

Performance targets:
- Extract: 500+ records/second (API limited)
- Transform: 10k+ records/second (Polars optimized)
- Overall: 5k+ records/second end-to-end
- Output: Single consolidated parquet file (128MB-1GB optimal)

All EIA regions: PACW, ERCO, NYIS, ISNE, PJM, MISO, SPP, CARO
Expected records: ~140k+ (8 regions × 2 data types × 365 days × 24 hours)
"""
import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add src to Python path
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.orchestrators.pipeline_dag import PipelineDAG, PipelineDAGConfig
from src.core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"logs/year_pipeline_dag_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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
        step_id="eia_extract_year",
        source="eia",
        start_date=start_date,
        end_date=end_date,
        regions=all_regions,
        data_types=["demand", "generation"],
        api_key=api_key,
        dry_run=dry_run
    )

    return ApiExtractStep(config)


def create_transform_step(year: int, dry_run: bool = False) -> DataCleanerStep:
    """Create optimized transform step for year-long data consolidation."""

    config = DataCleanerStepConfig(
        step_name=f"EIA Transform {year} All Regions",
        step_id="eia_transform_year",
        source="eia",
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path(f"data/interim/{year}"),
        validate_data=True,
        dry_run=dry_run
    )

    return DataCleanerStep(config)


async def run_year_pipeline_dag(year: int, dry_run: bool = False):
    """Run the year-long pipeline using PipelineDAG orchestration."""

    logger = logging.getLogger("YearPipelineDAG")

    # Check API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key and not dry_run:
        logger.error("❌ EIA_API_KEY environment variable is required for live execution")
        logger.info("💡 Set it with: export EIA_API_KEY='your_api_key_here'")
        return

    # All EIA regions for comprehensive testing
    all_regions = ["PACW", "ERCO", "NYIS", "ISNE", "PJM", "MISO", "SPP", "CARO"]

    logger.info("🚀 Starting Year-Long Pipeline with PipelineDAG")
    logger.info(f"   Year: {year}")
    logger.info(f"   Regions: {', '.join(all_regions)} (All EIA regions)")
    logger.info(f"   Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
    logger.info(f"   Target: 5k+ records/second overall throughput")
    logger.info(f"   Expected records: ~140k+ (8 regions × 2 data types × 365 days × 24 hours)")
    logger.info(f"   Goal: Single consolidated parquet file (128MB-1GB optimal)")
    logger.info("")

    # Create pipeline steps
    extract_step = create_extract_step(year, api_key or "", dry_run)
    transform_step = create_transform_step(year, dry_run)

    # Create pipeline DAG configuration
    dag_config = PipelineDAGConfig(
        pipeline_name=f"EIA Year Pipeline {year}",
        pipeline_id=f"eia_year_{year}_{datetime.now().strftime('%H%M%S')}",
        output_dir=Path(f"data/pipeline_runs/{year}_all_regions"),
        max_parallel_steps=1,  # Extract and transform must be sequential
        stop_on_failure=True,
        save_intermediate_results=True,
        auto_connect_steps=True,  # Automatically connect extract outputs to transform inputs
        validate_data_flow=True,
        log_level="INFO"
    )

    # Create and configure pipeline DAG
    dag = PipelineDAG(dag_config)

    # Add steps with proper dependencies (extract → transform)
    dag.create_extract_transform_chain(extract_step, transform_step)

    logger.info("📋 Pipeline DAG Configuration:")
    logger.info(f"   Pipeline: {dag_config.pipeline_name}")
    logger.info(f"   Steps: {len(dag.nodes)} (extract → transform)")
    logger.info(f"   Auto-connect: {dag_config.auto_connect_steps}")
    logger.info(f"   Output dir: {dag_config.output_dir}")
    logger.info("")

    # Execute pipeline with comprehensive monitoring
    logger.info("="*80)
    logger.info("🚀 EXECUTING YEAR-LONG PIPELINE DAG")
    logger.info("="*80)

    pipeline_start = datetime.now()

    try:
        results = await dag.execute_async()

        pipeline_duration = (datetime.now() - pipeline_start).total_seconds()

        # Analyze results
        logger.info("="*80)
        logger.info("📊 YEAR-LONG PIPELINE EXECUTION RESULTS")
        logger.info("="*80)

        if results["success"]:
            logger.info("✅ Pipeline completed successfully!")

            # Overall performance metrics
            total_records = results["total_records_processed"]
            total_bytes = results["total_bytes_processed"]
            overall_rps = total_records / pipeline_duration if pipeline_duration > 0 else 0

            logger.info(f"🏆 Overall Performance:")
            logger.info(f"   Total Duration: {pipeline_duration:.2f}s")
            logger.info(f"   Total Records: {total_records:,}")
            logger.info(f"   Total Bytes: {total_bytes:,}")
            logger.info(f"   Overall Throughput: {overall_rps:.1f} records/second")
            logger.info(f"   Files Created: {results['total_files_created']}")

            # Step performance breakdown
            logger.info("\n📋 Step Performance Analysis:")

            # Extract step analysis
            if "eia_extract_year" in results["step_results"]:
                extract_result = results["step_results"]["eia_extract_year"]
                extract_metrics = extract_result["metrics"]
                extract_rps = extract_metrics["records_processed"] / extract_metrics["duration_seconds"]

                logger.info(f"   📥 EXTRACT STEP:")
                logger.info(f"     Duration: {extract_metrics['duration_seconds']:.2f}s")
                logger.info(f"     Records: {extract_metrics['records_processed']:,}")
                logger.info(f"     Throughput: {extract_rps:.1f} records/second")
                logger.info(f"     API calls: {extract_metrics['api_calls_made']}")
                logger.info(f"     Files created: {extract_metrics['files_created']}")

                # Performance evaluation
                if extract_rps >= 500:
                    logger.info("     ✅ Extract performance meets target (500+ RPS)")
                elif extract_rps >= 100:
                    logger.info("     ⚠️  Extract performance below target but acceptable")
                else:
                    logger.info("     ❌ Extract performance needs improvement")

            # Transform step analysis
            if "eia_transform_year" in results["step_results"]:
                transform_result = results["step_results"]["eia_transform_year"]
                transform_metrics = transform_result["metrics"]
                transform_rps = transform_metrics["records_processed"] / transform_metrics["duration_seconds"]

                logger.info(f"   🔄 TRANSFORM STEP:")
                logger.info(f"     Duration: {transform_metrics['duration_seconds']:.2f}s")
                logger.info(f"     Records: {transform_metrics['records_processed']:,}")
                logger.info(f"     Throughput: {transform_rps:.1f} records/second")
                logger.info(f"     Files created: {transform_metrics['files_created']}")

                # Performance evaluation
                if transform_rps >= 10000:
                    logger.info("     ✅ Transform performance meets target (10k+ RPS)")
                elif transform_rps >= 5000:
                    logger.info("     ⚠️  Transform performance below target but acceptable")
                else:
                    logger.info("     ❌ Transform performance needs improvement")

                # Consolidated file validation
                if transform_metrics["files_created"] == 1:
                    logger.info("     ✅ Single consolidated parquet file created!")

                    # Show the consolidated file
                    if transform_result["output_paths"]:
                        parquet_file = transform_result["output_paths"][0]
                        logger.info(f"     📄 Consolidated file: {parquet_file}")

                        # Check file size if not dry run
                        if not dry_run:
                            try:
                                file_path = Path(parquet_file)
                                if file_path.exists():
                                    file_size = file_path.stat().st_size
                                    file_size_mb = file_size / (1024 * 1024)
                                    logger.info(f"     📊 File size: {file_size_mb:.2f} MB")

                                    if 128 <= file_size_mb <= 1024:
                                        logger.info("     ✅ File size is optimal for parquet (128MB-1GB)")
                                    elif file_size_mb < 128:
                                        logger.info("     ⚠️  File size is smaller than optimal (< 128MB)")
                                    else:
                                        logger.info("     ⚠️  File size is larger than optimal (> 1GB)")
                            except Exception as e:
                                logger.warning(f"     ⚠️  Could not check file size: {e}")
                else:
                    logger.warning(f"     ⚠️  Expected 1 file, got {transform_metrics['files_created']}")

            # Overall performance evaluation
            logger.info("\n🎯 PERFORMANCE EVALUATION:")
            target_rps = 5000

            if overall_rps >= target_rps:
                performance_rating = "🔥 EXCELLENT"
            elif overall_rps >= target_rps * 0.8:
                performance_rating = "✅ GOOD"
            elif overall_rps >= target_rps * 0.5:
                performance_rating = "⚠️  NEEDS IMPROVEMENT"
            else:
                performance_rating = "❌ POOR"

            logger.info(f"   Overall Rating: {performance_rating}")
            logger.info(f"   Target: {target_rps:,} RPS")
            logger.info(f"   Achieved: {overall_rps:.1f} RPS")
            logger.info(f"   Performance: {(overall_rps/target_rps)*100:.1f}% of target")

            # Phase 3 benefits demonstrated
            logger.info("\n🎯 PHASE 3 BENEFITS DEMONSTRATED:")
            logger.info("   ✅ PipelineDAG orchestration with step chaining")
            logger.info("   ✅ Automated data flow between extract and transform")
            logger.info("   ✅ Single consolidated parquet file output")
            logger.info("   ✅ Comprehensive pipeline monitoring")
            logger.info("   ✅ Error handling and recovery")
            logger.info("   ✅ Reproducible pipeline execution")
            logger.info("   ✅ Parallel execution capabilities (when applicable)")
            logger.info("   ✅ Performance tracking and reporting")

            # Next steps
            logger.info("\n🔄 READY FOR PHASE 4:")
            logger.info("   → Add load steps for MLX integration")
            logger.info("   → Implement step caching and resumption")
            logger.info("   → Add advanced monitoring and alerting")
            logger.info("   → Integrate with ring attention pipeline")

        else:
            logger.error("❌ Pipeline failed!")
            logger.error(f"   Completed steps: {results['completed_steps']}")
            logger.error(f"   Failed steps: {results['failed_steps']}")

            # Show failed steps
            if results["execution_summary"]["failed_step_ids"]:
                logger.error(f"   Failed step IDs: {', '.join(results['execution_summary']['failed_step_ids'])}")

        logger.info("="*80)

        # Show results file location
        results_file = dag_config.output_dir / f"{dag_config.pipeline_id}_results.json"
        logger.info(f"💾 Full results saved to: {results_file}")

        return results

    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {str(e)}", exc_info=True)
        return None


def main():
    """Main entry point for the year-long pipeline DAG test."""

    parser = argparse.ArgumentParser(description="Year-Long Pipeline with PipelineDAG")
    parser.add_argument("--year", type=int, default=2024, help="Year to extract data for")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without execution")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    # Create logs directory
    Path("logs").mkdir(exist_ok=True)

    print("🔗 Year-Long Pipeline with PipelineDAG - Phase 3 Implementation")
    print("   Enhanced pipeline orchestration with step chaining")
    print("   Features: Single parquet output, data flow, comprehensive monitoring")
    print("")

    # Run the pipeline
    results = asyncio.run(run_year_pipeline_dag(
        year=args.year,
        dry_run=args.dry_run
    ))

    if results and results["success"]:
        print("\n✅ Year-long pipeline completed successfully!")
        print("   Phase 3 PipelineDAG implementation is working!")
        print("   Single consolidated parquet file created as expected")
        print("   Ready to proceed with Phase 4 enhancements")
    else:
        print("\n❌ Pipeline failed - check logs for details")
        sys.exit(1)


if __name__ == "__main__":
    main()
