"""
Test script to validate pipeline foundation.

This script tests the basic step interface, benchmarking, and runner
to ensure our foundation is working before proceeding to EIA step migration.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# Test imports
try:
    from src.core.pipeline.steps.base import (
        BaseStep, StepConfig, StepOutput, StepMetrics, ExtractStepConfig
    )
    from src.core.pipeline.benchmarks import PipelineBenchmark
    from src.core.pipeline.runners import StepRunner
    print("✅ All imports successful")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)


# Mock step for testing
class TestStepConfig(ExtractStepConfig):
    """Test step configuration."""
    test_value: int = 42


class TestStep(BaseStep):
    """Simple test step."""

    def validate_input(self, config: TestStepConfig) -> None:
        """Validate test configuration."""
        if config.test_value < 0:
            raise ValueError("test_value must be non-negative")

    def _execute(self) -> dict:
        """Execute test step."""
        import time
        import json

        # Simulate work
        time.sleep(0.1)

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


def test_foundation():
    """Test pipeline foundation components."""
    print("\n🧪 Testing Pipeline Foundation")
    print("="*50)

    # Test 1: Basic step execution
    print("\n1. Testing basic step execution...")

    config = TestStepConfig(
        step_name="Test Step",
        step_id="test_001",
        start_date="2024-01-01",
        end_date="2024-01-02",
        regions=["TEST"],
        data_types=["mock"],
        output_dir=Path("data/test_pipeline"),
        test_value=42
    )

    step = TestStep(config)
    result = step.run()

    print(f"   Step success: {result.success}")
    print(f"   Duration: {result.metrics.duration_seconds:.3f}s")
    print(f"   Records: {result.metrics.records_processed}")
    print(f"   Files: {result.metrics.files_created}")
    print(f"   Output paths: {len(result.output_paths)}")

    assert result.success, "Step should succeed"
    assert result.metrics.records_processed == 100, "Should process 100 records"
    assert len(result.output_paths) == 1, "Should create 1 output file"

    # Test 2: Benchmark functionality
    print("\n2. Testing benchmark functionality...")

    benchmark = PipelineBenchmark("test_benchmark")
    benchmark.start()

    # Simulate operations
    for i in range(5):
        benchmark.record_operation(
            success=True,
            latency_seconds=0.01,
            records=20,
            bytes_processed=1024
        )

    benchmark.end()

    metrics = benchmark.get_metrics()
    print(f"   Operations: {metrics.total_operations}")
    print(f"   Success rate: {metrics.success_rate}%")
    print(f"   Records/sec: {metrics.records_per_second:.1f}")

    assert metrics.total_operations == 5, "Should record 5 operations"
    assert metrics.success_rate == 100.0, "Should have 100% success rate"

    # Test 3: Step runner
    print("\n3. Testing step runner...")

    runner = StepRunner(Path("data/test_pipeline/runs"))

    # Create another test step
    config2 = TestStepConfig(
        step_name="Test Step 2",
        step_id="test_002",
        start_date="2024-01-01",
        end_date="2024-01-02",
        regions=["TEST"],
        data_types=["mock"],
        output_dir=Path("data/test_pipeline"),
        test_value=100
    )

    step2 = TestStep(config2)
    runner_result = runner.run_step(step2, save_metrics=True)

    print(f"   Runner success: {runner_result.success}")
    print(f"   Step ID: {runner_result.step_id}")

    assert runner_result.success, "Runner should execute step successfully"

    print("\n🎉 All foundation tests passed!")
    print("\nReady to proceed with EIA step migration.")


if __name__ == "__main__":
    test_foundation()
