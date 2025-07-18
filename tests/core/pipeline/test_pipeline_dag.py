"""
Comprehensive tests for PipelineDAG orchestration.

Tests the pipeline DAG functionality including:
- Step dependency management
- Parallel execution
- Data flow between steps
- Error handling and recovery
- Performance monitoring
"""
import asyncio
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import json

from src.core.pipeline.orchestrators.pipeline_dag import PipelineDAG, PipelineDAGConfig, StepNode
from src.core.pipeline.steps.base import BaseStep, StepConfig, StepOutput, StepMetrics


class MockStepConfig(StepConfig):
    """Mock step configuration for testing."""
    mock_data: str = "test"


class MockStep(BaseStep):
    """Mock step for testing pipeline orchestration."""

    def __init__(self, config: MockStepConfig, execution_time: float = 0.1,
                 should_fail: bool = False, records_processed: int = 100):
        super().__init__(config)
        self.execution_time = execution_time
        self.should_fail = should_fail
        self.records_processed = records_processed
        self.execution_count = 0

    def validate_input(self, config: MockStepConfig) -> None:
        """Validate mock step configuration."""
        pass

    def _execute(self) -> dict:
        """Execute mock step with configurable behavior."""
        import time

        self.execution_count += 1

        # Simulate execution time
        time.sleep(self.execution_time)

        if self.should_fail:
            raise RuntimeError(f"Mock step {self.config.step_id} failed")

        output_path = Path(f"mock_output_{self.config.step_id}.json")

        return {
            "records_processed": self.records_processed,
            "bytes_processed": 1024,
            "files_created": 1,
            "output_paths": [output_path],
            "execution_count": self.execution_count
        }


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def dag_config(temp_output_dir):
    """Create test DAG configuration."""
    return PipelineDAGConfig(
        pipeline_name="Test Pipeline",
        pipeline_id="test_pipeline",
        output_dir=temp_output_dir,
        max_parallel_steps=2,
        stop_on_failure=True,
        log_level="DEBUG"
    )


@pytest.fixture
def simple_steps():
    """Create simple test steps."""
    step1 = MockStep(MockStepConfig(
        step_name="Extract",
        step_id="extract",
        mock_data="extract_data"
    ), execution_time=0.1, records_processed=1000)

    step2 = MockStep(MockStepConfig(
        step_name="Transform",
        step_id="transform",
        mock_data="transform_data"
    ), execution_time=0.2, records_processed=800)

    step3 = MockStep(MockStepConfig(
        step_name="Load",
        step_id="load",
        mock_data="load_data"
    ), execution_time=0.1, records_processed=800)

    return step1, step2, step3


class TestPipelineDAG:
    """Test suite for PipelineDAG functionality."""

    def test_dag_initialization(self, dag_config):
        """Test DAG initialization."""
        dag = PipelineDAG(dag_config)

        assert dag.config.pipeline_name == "Test Pipeline"
        assert dag.config.pipeline_id == "test_pipeline"
        assert len(dag.nodes) == 0
        assert len(dag.completed_steps) == 0
        assert len(dag.failed_steps) == 0

    def test_add_single_step(self, dag_config, simple_steps):
        """Test adding a single step to the DAG."""
        dag = PipelineDAG(dag_config)
        step1, _, _ = simple_steps

        dag.add_step(step1)

        assert len(dag.nodes) == 1
        assert "extract" in dag.nodes
        assert dag.nodes["extract"].step == step1
        assert len(dag.nodes["extract"].dependencies) == 0

    def test_add_step_with_dependencies(self, dag_config, simple_steps):
        """Test adding steps with dependencies."""
        dag = PipelineDAG(dag_config)
        step1, step2, step3 = simple_steps

        dag.add_step(step1)
        dag.add_step(step2, dependencies=["extract"])
        dag.add_step(step3, dependencies=["transform"])

        assert len(dag.nodes) == 3
        assert dag.nodes["transform"].dependencies == {"extract"}
        assert dag.nodes["load"].dependencies == {"transform"}
        assert dag.nodes["extract"].dependents == {"transform"}
        assert dag.nodes["transform"].dependents == {"load"}

    def test_add_duplicate_step_fails(self, dag_config, simple_steps):
        """Test that adding duplicate step fails."""
        dag = PipelineDAG(dag_config)
        step1, _, _ = simple_steps

        dag.add_step(step1)

        with pytest.raises(ValueError, match="already exists"):
            dag.add_step(step1)

    def test_add_step_with_invalid_dependency_fails(self, dag_config, simple_steps):
        """Test that adding step with invalid dependency fails."""
        dag = PipelineDAG(dag_config)
        _, step2, _ = simple_steps

        with pytest.raises(ValueError, match="Dependency 'extract' not found"):
            dag.add_step(step2, dependencies=["extract"])

    def test_validate_empty_dag_fails(self, dag_config):
        """Test that validating empty DAG fails."""
        dag = PipelineDAG(dag_config)

        with pytest.raises(ValueError, match="Pipeline DAG is empty"):
            dag.validate_dag()

    def test_validate_dag_with_cycle_fails(self, dag_config):
        """Test that validating DAG with cycle fails."""
        dag = PipelineDAG(dag_config)

        # Create steps that form a cycle
        step1 = MockStep(MockStepConfig(step_name="Step1", step_id="step1"))
        step2 = MockStep(MockStepConfig(step_name="Step2", step_id="step2"))
        step3 = MockStep(MockStepConfig(step_name="Step3", step_id="step3"))

        dag.add_step(step1)
        dag.add_step(step2, dependencies=["step1"])
        dag.add_step(step3, dependencies=["step2"])

        # Manually create cycle for testing
        dag.nodes["step1"].dependencies.add("step3")
        dag.nodes["step3"].dependents.add("step1")

        with pytest.raises(ValueError, match="Cycle detected"):
            dag.validate_dag()

    def test_compute_execution_order(self, dag_config, simple_steps):
        """Test computation of execution order."""
        dag = PipelineDAG(dag_config)
        step1, step2, step3 = simple_steps

        dag.add_step(step1)
        dag.add_step(step2, dependencies=["extract"])
        dag.add_step(step3, dependencies=["transform"])

        execution_order = dag._compute_execution_order()

        assert execution_order == ["extract", "transform", "load"]

    def test_get_ready_steps(self, dag_config, simple_steps):
        """Test getting ready steps for execution."""
        dag = PipelineDAG(dag_config)
        step1, step2, step3 = simple_steps

        dag.add_step(step1)
        dag.add_step(step2, dependencies=["extract"])
        dag.add_step(step3, dependencies=["transform"])

        # Initially, only the first step should be ready
        ready_steps = dag._get_ready_steps()
        assert ready_steps == ["extract"]

        # After completing the first step
        dag.nodes["extract"].status = "completed"
        ready_steps = dag._get_ready_steps()
        assert ready_steps == ["transform"]

        # After completing the second step
        dag.nodes["transform"].status = "completed"
        ready_steps = dag._get_ready_steps()
        assert ready_steps == ["load"]

    def test_create_extract_transform_chain(self, dag_config, simple_steps):
        """Test creating extract-transform chain."""
        dag = PipelineDAG(dag_config)
        step1, step2, _ = simple_steps

        dag.create_extract_transform_chain(step1, step2)

        assert len(dag.nodes) == 2
        assert dag.nodes["transform"].dependencies == {"extract"}

    def test_create_etl_pipeline(self, dag_config, simple_steps):
        """Test creating full ETL pipeline."""
        dag = PipelineDAG(dag_config)
        step1, step2, step3 = simple_steps

        dag.create_etl_pipeline(step1, step2, step3)

        assert len(dag.nodes) == 3
        assert dag.nodes["transform"].dependencies == {"extract"}
        assert dag.nodes["load"].dependencies == {"transform"}

    @pytest.mark.asyncio
    async def test_execute_single_step(self, dag_config, simple_steps):
        """Test executing a single step."""
        dag = PipelineDAG(dag_config)
        step1, _, _ = simple_steps

        dag.add_step(step1)

        results = await dag.execute_async()

        assert results["success"] is True
        assert results["completed_steps"] == 1
        assert results["failed_steps"] == 0
        assert results["total_records_processed"] == 1000
        assert "extract" in dag.completed_steps

    @pytest.mark.asyncio
    async def test_execute_linear_pipeline(self, dag_config, simple_steps):
        """Test executing a linear pipeline."""
        dag = PipelineDAG(dag_config)
        step1, step2, step3 = simple_steps

        dag.create_etl_pipeline(step1, step2, step3)

        results = await dag.execute_async()

        assert results["success"] is True
        assert results["completed_steps"] == 3
        assert results["failed_steps"] == 0
        assert results["total_records_processed"] == 2600  # 1000 + 800 + 800

        # Check execution order
        assert all(step_id in dag.completed_steps for step_id in ["extract", "transform", "load"])

    @pytest.mark.asyncio
    async def test_execute_parallel_steps(self, dag_config):
        """Test executing independent steps in parallel."""
        dag = PipelineDAG(dag_config)

        # Create two independent steps
        step1 = MockStep(MockStepConfig(step_name="Step1", step_id="step1"), execution_time=0.2)
        step2 = MockStep(MockStepConfig(step_name="Step2", step_id="step2"), execution_time=0.2)

        dag.add_step(step1)
        dag.add_step(step2)

        start_time = datetime.now()
        results = await dag.execute_async()
        execution_time = (datetime.now() - start_time).total_seconds()

        assert results["success"] is True
        assert results["completed_steps"] == 2

        # Should execute in parallel, so total time should be less than sequential
        assert execution_time < 0.35  # Less than 0.2 + 0.2 (with some margin)

    @pytest.mark.asyncio
    async def test_execute_with_step_failure_stop_on_failure(self, dag_config, simple_steps):
        """Test pipeline execution with step failure and stop_on_failure=True."""
        dag = PipelineDAG(dag_config)
        step1, step2, step3 = simple_steps

        # Make the transform step fail
        step2.should_fail = True

        dag.create_etl_pipeline(step1, step2, step3)

        results = await dag.execute_async()

        assert results["success"] is False
        assert results["completed_steps"] == 1  # Only extract should complete
        assert results["failed_steps"] == 1  # Transform should fail
        assert "extract" in dag.completed_steps
        assert "transform" in dag.failed_steps
        assert "load" not in dag.completed_steps  # Should not run due to failure

    @pytest.mark.asyncio
    async def test_execute_with_step_failure_continue_on_failure(self, dag_config, simple_steps):
        """Test pipeline execution with step failure and stop_on_failure=False."""
        # Modify config to continue on failure
        dag_config.stop_on_failure = False
        dag = PipelineDAG(dag_config)

        step1, step2, step3 = simple_steps

        # Make the transform step fail
        step2.should_fail = True

        dag.create_etl_pipeline(step1, step2, step3)

        results = await dag.execute_async()

        assert results["success"] is False
        assert results["completed_steps"] == 1  # Only extract should complete
        assert results["failed_steps"] == 1  # Transform should fail
        # Load still won't run because it depends on transform

    def test_get_step_status(self, dag_config, simple_steps):
        """Test getting step status."""
        dag = PipelineDAG(dag_config)
        step1, _, _ = simple_steps

        dag.add_step(step1)

        assert dag.get_step_status("extract") == "pending"
        assert dag.get_step_status("nonexistent") is None

        dag.nodes["extract"].status = "completed"
        assert dag.get_step_status("extract") == "completed"

    def test_get_pipeline_summary(self, dag_config, simple_steps):
        """Test getting pipeline summary."""
        dag = PipelineDAG(dag_config)
        step1, step2, _ = simple_steps

        dag.add_step(step1)
        dag.add_step(step2, dependencies=["extract"])

        summary = dag.get_pipeline_summary()

        assert summary["pipeline_name"] == "Test Pipeline"
        assert summary["total_steps"] == 2
        assert summary["completed_steps"] == 0
        assert summary["failed_steps"] == 0
        assert summary["step_status"]["extract"] == "pending"
        assert summary["step_status"]["transform"] == "pending"
        assert summary["is_complete"] is False

        # Mark one step as completed
        dag.completed_steps.add("extract")
        summary = dag.get_pipeline_summary()
        assert summary["completed_steps"] == 1

    @pytest.mark.asyncio
    async def test_pipeline_results_saved(self, dag_config, simple_steps):
        """Test that pipeline results are saved to file."""
        dag = PipelineDAG(dag_config)
        step1, _, _ = simple_steps

        dag.add_step(step1)

        await dag.execute_async()

        # Check that results file was created
        results_file = dag_config.output_dir / f"{dag_config.pipeline_id}_results.json"
        assert results_file.exists()

        # Check results content
        with open(results_file) as f:
            results = json.load(f)

        assert results["pipeline_name"] == "Test Pipeline"
        assert results["success"] is True
        assert results["completed_steps"] == 1
        assert "step_results" in results
        assert "extract" in results["step_results"]

    def test_synchronous_execute(self, dag_config, simple_steps):
        """Test synchronous pipeline execution."""
        dag = PipelineDAG(dag_config)
        step1, _, _ = simple_steps

        dag.add_step(step1)

        results = dag.execute()

        assert results["success"] is True
        assert results["completed_steps"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
