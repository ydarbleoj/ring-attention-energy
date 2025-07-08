"""Pipeline orchestrator for coordinating data collection across multiple sources."""

import asyncio
import logging
from datetime import date, timedelta
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from pathlib import Path

from .config import EnergyPipelineConfig
from .collectors.base import BaseEnergyDataCollector, DataCollectionResult


@dataclass
class BatchConfig:
    """Configuration for collector-specific batch sizes."""

    collector_name: str
    batch_size_days: int
    max_parallel_batches: int = 1  # Start conservative


@dataclass
class LoadProgress:
    """Track loading progress per collector."""

    collector_name: str
    completed_ranges: List[Tuple[date, date]]
    failed_ranges: List[Tuple[date, date]]
    in_progress_ranges: Set[Tuple[date, date]]

    def is_range_completed(self, start_date: date, end_date: date) -> bool:
        """Check if a date range has been successfully loaded."""
        return (start_date, end_date) in self.completed_ranges

    def mark_completed(self, start_date: date, end_date: date) -> None:
        """Mark a date range as successfully completed."""
        range_tuple = (start_date, end_date)
        if range_tuple in self.in_progress_ranges:
            self.in_progress_ranges.remove(range_tuple)
        if range_tuple not in self.completed_ranges:
            self.completed_ranges.append(range_tuple)

    def mark_failed(self, start_date: date, end_date: date) -> None:
        """Mark a date range as failed."""
        range_tuple = (start_date, end_date)
        if range_tuple in self.in_progress_ranges:
            self.in_progress_ranges.remove(range_tuple)
        if range_tuple not in self.failed_ranges:
            self.failed_ranges.append(range_tuple)


class DataLoadOrchestrator:
    """Orchestrates data loading across multiple collectors with different batch sizes."""

    def __init__(self, config: EnergyPipelineConfig):
        self.config = config
        self.collectors: Dict[str, BaseEnergyDataCollector] = {}
        self.batch_configs: Dict[str, BatchConfig] = {}
        self.progress: Dict[str, LoadProgress] = {}
        self.logger = logging.getLogger(__name__)

        # Default batch configurations
        self._setup_default_batch_configs()

    def _setup_default_batch_configs(self) -> None:
        """Setup default batch configurations for known collectors."""
        self.batch_configs = {
            "eia": BatchConfig("eia", batch_size_days=60),      # 60 days optimal (202 recs/sec)
            "caiso": BatchConfig("caiso", batch_size_days=90),  # 3 months for CAISO (data issues)
            "synthetic": BatchConfig("synthetic", batch_size_days=365),  # 1 year for synthetic
        }

    def register_collector(self, name: str, collector: BaseEnergyDataCollector) -> None:
        """Register a collector with the orchestrator."""
        self.collectors[name] = collector
        if name not in self.progress:
            self.progress[name] = LoadProgress(
                collector_name=name,
                completed_ranges=[],
                failed_ranges=[],
                in_progress_ranges=set()
            )
        self.logger.info(f"Registered collector: {name}")

    def get_batch_config(self, collector_name: str) -> BatchConfig:
        """Get batch configuration for a collector."""
        return self.batch_configs.get(
            collector_name,
            BatchConfig(collector_name, batch_size_days=30)  # Default to 1 month
        )

    def generate_date_chunks(
        self,
        start_date: date,
        end_date: date,
        collector_name: str
    ) -> List[Tuple[date, date]]:
        """Generate date chunks for a collector based on its batch configuration."""
        batch_config = self.get_batch_config(collector_name)
        chunks = []

        current_date = start_date
        while current_date < end_date:
            chunk_end = min(
                current_date + timedelta(days=batch_config.batch_size_days - 1),
                end_date
            )
            chunks.append((current_date, chunk_end))
            current_date = chunk_end + timedelta(days=1)

        self.logger.info(
            f"Generated {len(chunks)} chunks for {collector_name} "
            f"({batch_config.batch_size_days}-day batches)"
        )
        return chunks

    async def load_historical_data(
        self,
        start_date: date,
        end_date: date,
        region: str,
        collector_names: Optional[List[str]] = None,
        parallel: bool = False,
        skip_completed: bool = True
    ) -> Dict[str, List[DataCollectionResult]]:
        """
        Load historical data from multiple collectors.

        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            region: Region identifier for data collection
            collector_names: List of collector names to use (None = all registered)
            parallel: Whether to run collectors in parallel
            skip_completed: Skip date ranges that have already been completed

        Returns:
            Dictionary mapping collector names to their collection results
        """
        if collector_names is None:
            collector_names = list(self.collectors.keys())

        self.logger.info(
            f"Starting historical data load: {start_date} to {end_date}, "
            f"region={region}, collectors={collector_names}, parallel={parallel}"
        )

        if parallel:
            return await self._load_parallel(start_date, end_date, region, collector_names, skip_completed)
        else:
            return await self._load_sequential(start_date, end_date, region, collector_names, skip_completed)

    async def _load_sequential(
        self,
        start_date: date,
        end_date: date,
        region: str,
        collector_names: List[str],
        skip_completed: bool
    ) -> Dict[str, List[DataCollectionResult]]:
        """Load data sequentially from each collector."""
        results = {}

        for collector_name in collector_names:
            self.logger.info(f"Loading data from {collector_name}")
            results[collector_name] = await self._load_collector_data(
                collector_name, start_date, end_date, region, skip_completed
            )

        return results

    async def _load_parallel(
        self,
        start_date: date,
        end_date: date,
        region: str,
        collector_names: List[str],
        skip_completed: bool
    ) -> Dict[str, List[DataCollectionResult]]:
        """Load data in parallel from all collectors."""
        tasks = [
            self._load_collector_data(name, start_date, end_date, region, skip_completed)
            for name in collector_names
        ]

        results_list = await asyncio.gather(*tasks, return_exceptions=True)

        results = {}
        for i, collector_name in enumerate(collector_names):
            if isinstance(results_list[i], Exception):
                self.logger.error(f"Collector {collector_name} failed: {results_list[i]}")
                results[collector_name] = []
            else:
                results[collector_name] = results_list[i]

        return results

    async def _load_collector_data(
        self,
        collector_name: str,
        start_date: date,
        end_date: date,
        region: str,
        skip_completed: bool
    ) -> List[DataCollectionResult]:
        """Load data for a single collector across all its date chunks.

        For Option A: Creates separate files for demand and generation data.
        Storage pattern: data/processed/{collector_name}/{year}/{collector_name}_{data_type}_{year}_{month}.parquet
        """
        if collector_name not in self.collectors:
            raise ValueError(f"Collector {collector_name} not registered")

        collector = self.collectors[collector_name]
        chunks = self.generate_date_chunks(start_date, end_date, collector_name)
        progress = self.progress[collector_name]

        results = []
        for chunk_start, chunk_end in chunks:
            # Skip if already completed
            if skip_completed and progress.is_range_completed(chunk_start, chunk_end):
                self.logger.info(f"Skipping completed range: {chunk_start} to {chunk_end}")
                continue

            # Mark as in progress
            progress.in_progress_ranges.add((chunk_start, chunk_end))

            try:
                self.logger.info(f"Loading {collector_name}: {chunk_start} to {chunk_end}")

                # Use separate demand and generation collection for Option A
                demand_result = await collector.collect_demand_data(
                    start_date=chunk_start.strftime("%Y-%m-%d"),
                    end_date=chunk_end.strftime("%Y-%m-%d"),
                    region=region,
                    save_to_storage=True,
                    storage_filename=self._generate_storage_filename(
                        collector_name, "demand", chunk_start, chunk_end, region
                    ),
                    storage_subfolder=self._generate_storage_subfolder(
                        collector_name, "demand", chunk_start
                    )
                )

                generation_result = await collector.collect_generation_data(
                    start_date=chunk_start.strftime("%Y-%m-%d"),
                    end_date=chunk_end.strftime("%Y-%m-%d"),
                    region=region,
                    save_to_storage=True,
                    storage_filename=self._generate_storage_filename(
                        collector_name, "generation", chunk_start, chunk_end, region
                    ),
                    storage_subfolder=self._generate_storage_subfolder(
                        collector_name, "generation", chunk_start
                    )
                )

                # Check if both succeeded
                if demand_result.success and generation_result.success:
                    results.extend([demand_result, generation_result])
                    progress.mark_completed(chunk_start, chunk_end)
                    self.logger.info(
                        f"Successfully loaded {collector_name}: {chunk_start} to {chunk_end} "
                        f"(demand: {demand_result.records_collected}, generation: {generation_result.records_collected})"
                    )
                else:
                    progress.mark_failed(chunk_start, chunk_end)
                    error_details = []
                    if not demand_result.success:
                        error_details.append(f"demand: {demand_result.errors}")
                    if not generation_result.success:
                        error_details.append(f"generation: {generation_result.errors}")
                    self.logger.error(f"Failed to load {collector_name}: {'; '.join(error_details)}")

            except Exception as e:
                progress.mark_failed(chunk_start, chunk_end)
                self.logger.error(f"Exception loading {collector_name}: {e}")

        return results

    def _generate_storage_filename(
        self,
        collector_name: str,
        data_type: str,
        start_date: date,
        end_date: date,
        region: str
    ) -> str:
        """Generate storage filename following Option A pattern.

        Pattern: {collector_name}_{data_type}_{region}_{year}_{month}
        """
        year = start_date.year
        month = start_date.strftime("%m")
        return f"{collector_name}_{data_type}_{region}_{year}_{month}"

    def _generate_storage_subfolder(
        self,
        collector_name: str,
        data_type: str,
        start_date: date
    ) -> str:
        """Generate storage subfolder following Option A pattern.

        Pattern: {collector_name}/{year}
        """
        year = start_date.year
        return f"{collector_name}/{year}"

    def get_progress_summary(self) -> Dict[str, Dict[str, int]]:
        """Get a summary of loading progress for all collectors."""
        summary = {}
        for name, progress in self.progress.items():
            summary[name] = {
                "completed": len(progress.completed_ranges),
                "failed": len(progress.failed_ranges),
                "in_progress": len(progress.in_progress_ranges)
            }
        return summary
