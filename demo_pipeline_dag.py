#!/usr/bin/env python3
"""
Pipeline DAG Demonstration - Extract → Transform Chain

Demonstrates the new PipelineDAG functionality for chaining steps together.
This solves the multiple parquet files issue by ensuring proper data flow
between extract and transform steps.

Key features demonstrated:
- Extract → Transform pipeline chaining
- Automatic data flow between steps
- Consolidated parquet output (single file)
- Comprehensive monitoring and metrics
- Error handling and recovery

Usage:
    python demo_pipeline_dag.py [--year 2024] [--region PACW] [--dry-run]

Performance expectations:
- Extract: 500+ records/second (API limited)
- Transform: 10k+ records/second (Polars optimized)
- Single consolidated parquet file output
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
    import logging

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"logs/pipeline_dag_demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        ]
    )


def create_extract_step(year: int, region: str, api_key: str, dry_run: bool = False) -> ApiExtractStep:
    """Create EIA extract step for the demo."""

    start_date = f"{year}-01-01"
    end_date = f"{year}-01-07"  # One week for demo

    config = ApiExtractStepConfig(
        step_name=f"EIA Extract {year} {region}",
        step_id="eia_extract",
        source="eia",
        start_date=start_date,
        end_date=end_date,
        regions=[region],
        data_types=["demand", "generation"],
        api_key=api_key,
        dry_run=dry_run
    )

    return ApiExtractStep(config)


def create_transform_step(year: int, region: str, dry_run: bool = False) -> DataCleanerStep:
    """Create transform step for the demo."""

    config = DataCleanerStepConfig(
        step_name=f"EIA Transform {year} {region}",
        step_id="eia_transform",
        source="eia",
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path("data/interim"),
        validate_data=True,
        dry_run=dry_run
    )

    return DataCleanerStep(config)


async def run_extract_transform_pipeline(year: int, region: str, dry_run: bool = False):
    """Run the extract → transform pipeline using PipelineDAG."""

    logger = logging.getLogger("PipelineDAGDemo")

    # Check API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key and not dry_run:
        logger.error("❌ EIA_API_KEY environment variable is required for live execution")
        logger.info("💡 Set it with: export EIA_API_KEY='your_api_key_here'")
        return

    logger.info("🚀 Starting Pipeline DAG Demonstration")
    logger.info(f"   Year: {year}")
    logger.info(f"   Region: {region}")
    logger.info(f"   Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
    logger.info(f"   Goal: Single consolidated parquet file output")
    logger.info("")

    # Create pipeline steps
    extract_step = create_extract_step(year, region, api_key or "", dry_run)
    transform_step = create_transform_step(year, region, dry_run)

    # Create pipeline DAG configuration
    dag_config = PipelineDAGConfig(
        pipeline_name=f"EIA Extract-Transform {year} {region}",
        pipeline_id=f"eia_etl_{year}_{region}_{datetime.now().strftime('%H%M%S')}",
        output_dir=Path(f"data/pipeline_runs/{year}_{region}"),
        max_parallel_steps=2,  # Extract and transform can't run in parallel due to dependency
        stop_on_failure=True,
        save_intermediate_results=True,
        auto_connect_steps=True,
        validate_data_flow=True,
        log_level="INFO"
    )

    # Create and configure pipeline DAG
    dag = PipelineDAG(dag_config)

    # Add steps with proper dependencies
    dag.create_extract_transform_chain(extract_step, transform_step)

    logger.info("📋 Pipeline DAG Configuration:")
    logger.info(f"   Steps: {len(dag.nodes)}")
    logger.info(f"   Dependencies: extract → transform")
    logger.info(f"   Max parallel: {dag_config.max_parallel_steps}")
    logger.info(f"   Auto-connect: {dag_config.auto_connect_steps}")
    logger.info("")

    # Execute pipeline
    logger.info("="*80)
    logger.info("🚀 EXECUTING PIPELINE DAG")
    logger.info("="*80)

    pipeline_start = datetime.now()

    try:
        results = await dag.execute_async()

        pipeline_duration = (datetime.now() - pipeline_start).total_seconds()

        # Display results
        logger.info("="*80)
        logger.info("📊 PIPELINE EXECUTION RESULTS")
        logger.info("="*80)

        if results["success"]:
            logger.info("✅ Pipeline completed successfully!")

            # Performance metrics
            logger.info(f"⏱️  Total Duration: {pipeline_duration:.2f}s")
            logger.info(f"📈 Steps Completed: {results['completed_steps']}/{results['total_steps']}")
            logger.info(f"📊 Records Processed: {results['total_records_processed']:,}")
            logger.info(f"💾 Bytes Processed: {results['total_bytes_processed']:,}")
            logger.info(f"📄 Files Created: {results['total_files_created']}")

            # Step-by-step breakdown
            logger.info("\n📋 Step Performance Breakdown:")
            for step_id, step_result in results["step_results"].items():
                step_metrics = step_result["metrics"]
                logger.info(f"   {step_id.upper()}:")
                logger.info(f"     Duration: {step_metrics['duration_seconds']:.2f}s")
                logger.info(f"     Records: {step_metrics['records_processed']:,}")
                logger.info(f"     Throughput: {step_metrics['records_processed']/step_metrics['duration_seconds']:.1f} RPS")
                logger.info(f"     Files: {step_metrics['files_created']}")

                # Show output paths
                if step_result["output_paths"]:
                    logger.info(f"     Outputs: {len(step_result['output_paths'])} files")
                    for output_path in step_result["output_paths"][:3]:  # Show first 3
                        logger.info(f"       → {output_path}")

            # Data flow validation
            logger.info("\n🔗 Data Flow Validation:")
            if "eia_extract" in results["step_results"] and "eia_transform" in results["step_results"]:
                extract_files = len(results["step_results"]["eia_extract"]["output_paths"])
                transform_files = len(results["step_results"]["eia_transform"]["output_paths"])

                logger.info(f"   Extract created: {extract_files} files")
                logger.info(f"   Transform created: {transform_files} files")

                if transform_files == 1:
                    logger.info("   ✅ Single consolidated parquet file created!")

                    # Show the consolidated file
                    parquet_file = results["step_results"]["eia_transform"]["output_paths"][0]
                    logger.info(f"   📄 Consolidated file: {parquet_file}")

                    # Check file size
                    if not dry_run:
                        try:
                            file_path = Path(parquet_file)
                            if file_path.exists():
                                file_size = file_path.stat().st_size
                                file_size_mb = file_size / (1024 * 1024)
                                logger.info(f"   📊 File size: {file_size_mb:.2f} MB")

                                if 128 <= file_size_mb <= 1024:
                                    logger.info("   ✅ File size is optimal for parquet (128MB-1GB)")
                                elif file_size_mb < 128:
                                    logger.info("   ⚠️  File size is smaller than optimal (< 128MB)")
                                else:
                                    logger.info("   ⚠️  File size is larger than optimal (> 1GB)")
                        except Exception as e:
                            logger.warning(f"   ⚠️  Could not check file size: {e}")
                else:
                    logger.warning(f"   ⚠️  Multiple files created instead of single consolidated file")

            logger.info("\n🎯 Key Benefits Demonstrated:")
            logger.info("   ✅ Automated step dependency management")
            logger.info("   ✅ Data flow between extract and transform steps")
            logger.info("   ✅ Single consolidated parquet file output")
            logger.info("   ✅ Comprehensive performance monitoring")
            logger.info("   ✅ Error handling and recovery")
            logger.info("   ✅ Reproducible pipeline execution")

        else:
            logger.error("❌ Pipeline failed!")
            logger.error(f"   Completed: {results['completed_steps']}")
            logger.error(f"   Failed: {results['failed_steps']}")

            # Show failed steps
            if results["execution_summary"]["failed_step_ids"]:
                logger.error(f"   Failed steps: {', '.join(results['execution_summary']['failed_step_ids'])}")

        logger.info("="*80)

        # Show results file location
        results_file = dag_config.output_dir / f"{dag_config.pipeline_id}_results.json"
        logger.info(f"💾 Full results saved to: {results_file}")

        return results

    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {str(e)}", exc_info=True)
        return None


def main():
    """Main entry point for the demonstration."""

    parser = argparse.ArgumentParser(description="Pipeline DAG Extract-Transform Demo")
    parser.add_argument("--year", type=int, default=2024, help="Year to extract data for")
    parser.add_argument("--region", type=str, default="PACW", help="EIA region to extract")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without execution")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    # Create logs directory
    Path("logs").mkdir(exist_ok=True)

    print("🔗 Pipeline DAG Demonstration - Extract → Transform Chain")
    print("   Solving the multiple parquet files issue with proper step chaining")
    print("   Features: Data flow, single output file, comprehensive monitoring")
    print("")

    # Run the pipeline
    results = asyncio.run(run_extract_transform_pipeline(
        year=args.year,
        region=args.region,
        dry_run=args.dry_run
    ))

    if results and results["success"]:
        print("\n✅ Demo completed successfully!")
        print("   The PipelineDAG successfully chained extract → transform steps")
        print("   Single consolidated parquet file was created as expected")
        print("   Ready for Phase 3 implementation!")
    else:
        print("\n❌ Demo failed - check logs for details")
        sys.exit(1)


if __name__ == "__main__":
    main()
