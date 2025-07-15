"""
Test pipeline foundation components.

Tests the basic step interface, benchmarking, and runner functionality
to ensure the foundation is working correctly.
"""

import pytest
from pathlib import Path
from datetime import datetime
import json

from src.core.pipeline.steps.base import (
    BaseStep, StepConfig, StepOutput, StepMetrics, ExtractStepConfig
)
from src.core.pipeline.benchmarks import PipelineBenchmark
from src.core.pipeline.runners import StepRunner


class MockStepConfig(ExtractStepConfig):
    """Mock step configuration for testing."""
    test_value: int = 42


class MockStep(BaseStep):
    """Simple mock step for validation."""

    def validate_input(self, config: MockStepConfig) -> None:
        """Validate mock configuration."""
        if config.test_value < 0:
            raise ValueError("test_value must be non-negative")

    def _execute(self) -> dict:
        """Execute mock step."""
        import time

        # Simulate work
        time.sleep(0.01)  # Shorter for tests

        # Create test output file
        output_file = self.config.output_dir / f"test_output_{self.config.step_id}.json"
        test_data = {
            "step_id": self.config.step_id,
            "test_value": self.config.test_value,
            "timestamp": datetime.now().isoformat()
        }

        with open(output_file, 'w') as f:
            json.dump(test_data, f, indent=2)

        return {
            "records_processed": 100,
            "bytes_processed": output_file.stat().st_size,
            "files_created": 1,
            "api_calls_made": 0,
            "output_paths": [output_file],
            "test_completed": True
        }


class TestPipelineFoundation:
    """Test suite for pipeline foundation components."""

    def test_step_config_validation(self):
        """Test step configuration validation."""
        # Valid config
        config = MockStepConfig(
            step_name="Test Step",
            step_id="test_001",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["TEST"],
            data_types=["mock"],
            test_value=42
        )
        assert config.test_value == 42

        # Invalid date format
        with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
            MockStepConfig(
                step_name="Test",
                step_id="test",
                start_date="invalid-date",
                end_date="2024-01-02",
                test_value=42
            )

    def test_step_execution_success(self, tmp_path):
        """Test successful step execution."""
        config = MockStepConfig(
            step_name="Test Step",
            step_id="test_success",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["TEST"],
            data_types=["mock"],
            output_dir=tmp_path,
            test_value=42
        )

        step = MockStep(config)
        result = step.run()

        # Verify result
        assert result.success
        assert result.step_id == "test_success"
        assert result.metrics.records_processed == 100
        assert result.metrics.files_created == 1
        assert len(result.output_paths) == 1
        assert result.output_paths[0].exists()

        # Verify file content
        with open(result.output_paths[0]) as f:
            data = json.load(f)
        assert data["step_id"] == "test_success"
        assert data["test_value"] == 42

    def test_step_execution_failure(self, tmp_path):
        """Test step execution failure handling."""
        config = MockStepConfig(
            step_name="Test Step",
            step_id="test_failure",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["TEST"],
            data_types=["mock"],
            output_dir=tmp_path,
            test_value=-1  # Invalid value
        )

        # Should raise validation error
        with pytest.raises(ValueError, match="test_value must be non-negative"):
            MockStep(config)

    def test_dry_run_mode(self, tmp_path):
        """Test dry run mode."""
        config = MockStepConfig(
            step_name="Test Step",
            step_id="test_dry_run",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["TEST"],
            data_types=["mock"],
            output_dir=tmp_path,
            test_value=42,
            dry_run=True
        )

        step = MockStep(config)
        result = step.run()

        # Verify dry run
        assert result.success
        assert result.metadata["dry_run"] is True
        assert result.metrics.duration_seconds == 0.0
        assert len(result.output_paths) == 0

    def test_benchmark_functionality(self):
        """Test benchmark metrics collection."""
        benchmark = PipelineBenchmark("test_benchmark", enable_system_monitoring=False)
        benchmark.start()

        # Simulate operations
        for i in range(5):
            benchmark.record_operation(
                success=True,
                latency_seconds=0.01,
                records=20,
                bytes_processed=1024
            )

        benchmark.record_api_call()
        benchmark.record_file_created()

        benchmark.end()

        metrics = benchmark.get_metrics()

        # Verify metrics
        assert metrics.total_operations == 5
        assert metrics.successful_operations == 5
        assert metrics.failed_operations == 0
        assert metrics.success_rate == 100.0
        assert metrics.total_records_processed == 100
        assert metrics.total_bytes_processed == 5120
        assert metrics.api_calls_made == 1
        assert metrics.files_created == 1

    def test_step_runner(self, tmp_path):
        """Test step runner functionality."""
        runner = StepRunner(tmp_path / "runs")

        config = MockStepConfig(
            step_name="Test Step",
            step_id="test_runner",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["TEST"],
            data_types=["mock"],
            output_dir=tmp_path,
            test_value=42
        )

        step = MockStep(config)
        result = runner.run_step(step, save_metrics=True)

        # Verify runner result
        assert result.success
        assert result.step_id == "test_runner"

        # Verify metrics file was created
        metrics_file = tmp_path / "runs" / "MockStep.test_runner_metrics.json"
        assert metrics_file.exists()

        # Verify metrics content
        with open(metrics_file) as f:
            metrics_data = json.load(f)
        assert metrics_data["operation_name"] == "MockStep.test_runner"
        assert metrics_data["total_operations"] == 1


def test_integration_foundation():
    """Integration test for complete foundation."""
    # This can be run manually or as part of a larger test suite
    # Uses real filesystem paths for integration testing
    output_dir = Path("data/test_pipeline_integration")

    config = MockStepConfig(
        step_name="Integration Test Step",
        step_id="integration_001",
        start_date="2024-01-01",
        end_date="2024-01-02",
        regions=["TEST"],
        data_types=["mock"],
        output_dir=output_dir,
        test_value=100
    )

    # Test step execution
    step = MockStep(config)
    result = step.run()

    assert result.success
    assert result.metrics.records_processed == 100

    # Test with runner
    runner = StepRunner(output_dir / "runs")
    runner_result = runner.run_step(step, save_metrics=True)

    assert runner_result.success

    # Cleanup
    import shutil
    if output_dir.exists():
        shutil.rmtree(output_dir)

    print("✅ Foundation integration test passed!")


if __name__ == "__main__":
    # Run integration test when executed directly
    test_integration_foundation()
