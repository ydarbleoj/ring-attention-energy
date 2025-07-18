"""
Base step interface for Kubeflow-style pipeline components.

This module defines the core abstractions for pipeline steps that can be
composed into workflows. Each step has a clear contract for input validation,
execution, and output metadata.

Follows the LLM guide principles:
- "Build modular components that can be tested and benchmarked independently"
- "Think like a systems engineer: reliability, fault tolerance, monitoring"
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator


class StepMetrics(BaseModel):
    """Standard metrics collected from step execution."""

    duration_seconds: float = Field(..., description="Step execution time in seconds")
    start_time: datetime = Field(..., description="Step start timestamp")
    end_time: datetime = Field(..., description="Step end timestamp")
    memory_usage_mb: Optional[float] = Field(None, description="Peak memory usage in MB")
    records_processed: int = Field(0, description="Number of records processed")
    bytes_processed: int = Field(0, description="Number of bytes processed")
    api_calls_made: int = Field(0, description="Number of API calls made")
    files_created: int = Field(0, description="Number of output files created")

    @field_validator("duration_seconds")
    @classmethod
    def validate_duration(cls, v):
        if v < 0:
            raise ValueError("Duration must be non-negative")
        return v


class StepConfig(BaseModel):
    """Base configuration for pipeline steps."""

    model_config = {"arbitrary_types_allowed": True}

    step_name: str = Field(..., description="Human-readable step name")
    step_id: str = Field(..., description="Unique step identifier")
    dry_run: bool = Field(False, description="If True, validate inputs but don't execute")
    output_dir: Path = Field(Path("data"), description="Base output directory")
    log_level: str = Field("INFO", description="Logging level for this step")


class StepOutput(BaseModel):
    """Standard output from step execution."""

    model_config = {"arbitrary_types_allowed": True}

    step_id: str = Field(..., description="Step identifier")
    success: bool = Field(..., description="Whether step completed successfully")
    metrics: StepMetrics = Field(..., description="Execution metrics")
    output_paths: List[Path] = Field(default_factory=list, description="Paths to output files")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Step-specific metadata")
    errors: List[str] = Field(default_factory=list, description="Error messages if any")


class BaseStep(ABC):
    """
    Abstract base class for pipeline steps.

    Each step implements:
    - validate_input(config): Check configuration is valid
    - run(): Execute the step and return output metadata

    This follows Kubeflow component patterns with clear I/O contracts.
    """

    def __init__(self, config: StepConfig):
        """Initialize step with configuration."""
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{config.step_id}")
        self.logger.setLevel(getattr(logging, config.log_level.upper()))

        # Validate configuration
        self.validate_input(config)

        # Ensure output directory exists
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Initialized {self.__class__.__name__} with ID: {config.step_id}")

    @abstractmethod
    def validate_input(self, config: StepConfig) -> None:
        """
        Validate step configuration.

        Args:
            config: Step configuration to validate

        Raises:
            ValueError: If configuration is invalid
        """
        pass

    @abstractmethod
    def _execute(self) -> Dict[str, Any]:
        """
        Execute the core step logic.

        Returns:
            Dict with step-specific results that will be included in metadata

        Raises:
            Exception: Any execution errors
        """
        pass

    def run(self) -> StepOutput:
        """
        Execute the step with full monitoring and error handling.

        Returns:
            StepOutput with execution results and metrics
        """
        start_time = datetime.now()
        start_timestamp = time.time()

        # self.logger.info(f"Starting step: {self.config.step_name}")

        if self.config.dry_run:
            self.logger.info("DRY RUN: Validating inputs only")
            return StepOutput(
                step_id=self.config.step_id,
                success=True,
                metrics=StepMetrics(
                    duration_seconds=0.0,
                    start_time=start_time,
                    end_time=start_time
                ),
                metadata={"dry_run": True}
            )

        try:
            # Execute core step logic
            step_results = self._execute()

            end_time = datetime.now()
            duration = time.time() - start_timestamp

            # Extract metrics from results
            metrics = StepMetrics(
                duration_seconds=duration,
                start_time=start_time,
                end_time=end_time,
                records_processed=step_results.get("records_processed", 0),
                bytes_processed=step_results.get("bytes_processed", 0),
                api_calls_made=step_results.get("api_calls_made", 0),
                files_created=step_results.get("files_created", 0),
                memory_usage_mb=step_results.get("memory_usage_mb")
            )

            # Extract output paths
            output_paths = step_results.get("output_paths", [])
            if isinstance(output_paths, (str, Path)):
                output_paths = [Path(output_paths)]
            output_paths = [Path(p) for p in output_paths]

            # Create success output
            output = StepOutput(
                step_id=self.config.step_id,
                success=True,
                metrics=metrics,
                output_paths=output_paths,
                metadata=step_results
            )

            # self.logger.info(
            #     f"Step completed successfully: {metrics.records_processed:,} records, "
            #     f"{metrics.files_created} files, {duration:.1f}s"
            # )

            return output

        except Exception as e:
            end_time = datetime.now()
            duration = time.time() - start_timestamp
            error_msg = f"Step failed: {str(e)}"

            self.logger.error(error_msg, exc_info=True)

            # Create failure output
            return StepOutput(
                step_id=self.config.step_id,
                success=False,
                metrics=StepMetrics(
                    duration_seconds=duration,
                    start_time=start_time,
                    end_time=end_time
                ),
                errors=[error_msg],
                metadata={"exception": str(e)}
            )


class ExtractStepConfig(StepConfig):
    """Base configuration for data extraction steps."""

    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format")
    regions: List[str] = Field(default_factory=list, description="Regions to extract data for")
    data_types: List[str] = Field(default_factory=list, description="Types of data to extract")

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v):
        """Validate date is in YYYY-MM-DD format."""
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")
        return v


class TransformStepConfig(StepConfig):
    """Base configuration for data transformation steps."""

    input_paths: List[Path] = Field(..., description="Input file paths to transform")

    @field_validator("input_paths")
    @classmethod
    def validate_input_paths(cls, v):
        """Validate all input paths exist."""
        for path in v:
            if not Path(path).exists():
                raise ValueError(f"Input path does not exist: {path}")
        return v


class LoadStepConfig(StepConfig):
    """Base configuration for data loading steps."""

    input_paths: List[Path] = Field(..., description="Input file paths to load")
    target_format: str = Field(..., description="Target format for loaded data")
