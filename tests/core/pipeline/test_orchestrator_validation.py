"""Validation test for the DataLoadOrchestrator with real collectors."""

import asyncio
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.core.pipeline.orchestrator import DataLoadOrchestrator
from src.core.pipeline.config import EnergyPipelineConfig
from src.core.pipeline.collectors.eia_collector import EIACollector
from src.core.pipeline.collectors.caiso_collector import CAISOCollector

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_orchestrator_with_real_collectors():
    """Test the orchestrator with real EIA and CAISO collectors."""

    logger.info("=== Orchestrator Validation Test ===")

    # Get API key
    eia_api_key = os.getenv("EIA_API_KEY")
    if not eia_api_key:
        raise ValueError("EIA_API_KEY not found in environment")

    # Setup config and orchestrator
    config = EnergyPipelineConfig()
    orchestrator = DataLoadOrchestrator(config)

    # Create collectors
    eia_collector = EIACollector(eia_api_key)
    # caiso_collector = CAISOCollector()  # Skip CAISO for today

    # Register collectors
    orchestrator.register_collector("eia", eia_collector)
    # orchestrator.register_collector("caiso", caiso_collector)  # Skip CAISO for today

    # Test with a small date range (1 week)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 7)
    region = "PACW"

    logger.info(f"Testing data collection from {start_date} to {end_date} for region {region}")

    # Test sequential loading
    logger.info("--- Testing Sequential Loading ---")
    results_sequential = await orchestrator.load_historical_data(
        start_date=start_date,
        end_date=end_date,
        region=region,
        collector_names=["eia"],  # Start with just EIA
        parallel=False
    )

    # Check results
    logger.info("--- Results Summary ---")
    for collector_name, results in results_sequential.items():
        logger.info(f"{collector_name}: {len(results)} results")
        for result in results:
            logger.info(f"  - {result.metadata.get('data_type', 'unknown')}: {result.records_collected} records")

    # Check if files were created
    logger.info("--- Storage Validation ---")
    storage_base = Path("data/processed")

    for collector_name in ["eia"]:
        collector_path = storage_base / collector_name / "2024"
        if collector_path.exists():
            files = list(collector_path.glob("*.parquet"))
            logger.info(f"{collector_name} created {len(files)} files:")
            for file in files:
                logger.info(f"  - {file.name}")
        else:
            logger.warning(f"No files found for {collector_name}")

    # Show progress summary
    progress = orchestrator.get_progress_summary()
    logger.info(f"--- Progress Summary ---")
    for collector, stats in progress.items():
        logger.info(f"{collector}: {stats}")

    logger.info("=== Validation Test Complete ===")
    return results_sequential


if __name__ == "__main__":
    asyncio.run(test_orchestrator_with_real_collectors())
