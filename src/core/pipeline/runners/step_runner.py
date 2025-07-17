import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..steps import BaseStep, StepOutput
from ..benchmarks import PipelineBenchmark


class StepRunner:
    """
    Executes individual pipeline steps with monitoring and error handling.

    Provides a clean execution environment for steps while collecting
    comprehensive metrics and handling failures gracefully.
    """

    def __init__(self, output_dir: Path = Path("data/pipeline_runs")):
        """
        Initialize step runner.

        Args:
            output_dir: Directory to store run outputs and metrics
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("StepRunner")

        self.logger.info(f"StepRunner initialized with output dir: {self.output_dir}")

    def run_step(self, step: BaseStep, save_metrics: bool = True) -> StepOutput:
        """
        Execute a single pipeline step with full monitoring.

        Args:
            step: Step instance to execute
            save_metrics: Whether to save benchmark metrics to file

        Returns:
            StepOutput with execution results
        """
        step_name = f"{step.__class__.__name__}.{step.config.step_id}"
        benchmark = PipelineBenchmark(step_name, enable_system_monitoring=True)

        self.logger.info(f"🚀 Executing step: {step_name}")
        benchmark.start()

        try:
            # Execute the step
            result = step.run()

            # Record benchmark metrics
            benchmark.record_operation(
                success=result.success,
                latency_seconds=result.metrics.duration_seconds,
                records=result.metrics.records_processed,
                bytes_processed=result.metrics.bytes_processed
            )

            # Record additional metrics
            for _ in range(result.metrics.api_calls_made):
                benchmark.record_api_call()

            for _ in range(result.metrics.files_created):
                benchmark.record_file_created()

            benchmark.end()

            # Log summary
            additional_info = {
                "Step ID": result.step_id,
                "Output Files": len(result.output_paths),
                "Output Paths": [str(p) for p in result.output_paths[:3]]  # Show first 3
            }
            if len(result.output_paths) > 3:
                additional_info["Output Paths"].append(f"... and {len(result.output_paths) - 3} more")

            benchmark.log_summary(additional_info)

            # Save metrics if requested
            if save_metrics:
                metrics_path = self.output_dir / f"{step_name}_metrics.json"
                benchmark.save_metrics(metrics_path)

            if result.success:
                self.logger.info(f"✅ Step completed successfully: {step_name}")
            else:
                self.logger.error(f"❌ Step failed: {step_name}")
                for error in result.errors:
                    self.logger.error(f"   Error: {error}")

            return result

        except Exception as e:
            benchmark.end()
            error_msg = f"Step execution failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            # Create failure result
            from datetime import datetime
            from ..steps import StepMetrics

            return StepOutput(
                step_id=step.config.step_id,
                success=False,
                metrics=StepMetrics(
                    duration_seconds=benchmark.metrics.duration_seconds,
                    start_time=benchmark.metrics.start_time or datetime.now(),
                    end_time=benchmark.metrics.end_time or datetime.now()
                ),
                errors=[error_msg],
                metadata={"runner_exception": str(e)}
            )


class SequentialRunner:
    """
    Executes a sequence of pipeline steps in order.

    Provides orchestration for multi-step workflows with dependency
    management and error handling.
    """

    def __init__(self, output_dir: Path = Path("data/pipeline_runs")):
        """
        Initialize sequential runner.

        Args:
            output_dir: Directory to store run outputs and metrics
        """
        self.step_runner = StepRunner(output_dir)
        self.logger = logging.getLogger("SequentialRunner")

    def run_sequence(self, steps: List[BaseStep],
                    stop_on_failure: bool = True) -> Dict[str, Any]:
        """
        Execute a sequence of steps in order.

        Args:
            steps: List of steps to execute in order
            stop_on_failure: Whether to stop execution if a step fails

        Returns:
            Dict with sequence execution results
        """
        sequence_name = f"sequence_{len(steps)}_steps"
        self.logger.info(f"🚀 Starting sequence execution: {sequence_name}")
        self.logger.info(f"   Steps: {[s.__class__.__name__ for s in steps]}")

        benchmark = PipelineBenchmark(sequence_name)
        benchmark.start()

        results = []
        all_output_paths = []
        total_errors = []

        for i, step in enumerate(steps, 1):
            step_name = f"{step.__class__.__name__}.{step.config.step_id}"
            self.logger.info(f"📋 Step {i}/{len(steps)}: {step_name}")

            # Execute step
            result = self.step_runner.run_step(step, save_metrics=True)
            results.append(result)

            # Accumulate results
            all_output_paths.extend(result.output_paths)
            total_errors.extend(result.errors)

            # Record in sequence benchmark
            benchmark.record_operation(
                success=result.success,
                latency_seconds=result.metrics.duration_seconds,
                records=result.metrics.records_processed,
                bytes_processed=result.metrics.bytes_processed
            )

            # Handle failure
            if not result.success:
                self.logger.error(f"❌ Step {i} failed: {step_name}")
                if stop_on_failure:
                    self.logger.error("🛑 Stopping sequence due to failure")
                    break
            else:
                self.logger.info(f"✅ Step {i} completed: {step_name}")

        benchmark.end()

        # Calculate summary
        successful_steps = sum(1 for r in results if r.success)
        failed_steps = len(results) - successful_steps
        sequence_success = failed_steps == 0

        # Log sequence summary
        additional_info = {
            "Total Steps": len(steps),
            "Steps Executed": len(results),
            "Successful Steps": successful_steps,
            "Failed Steps": failed_steps,
            "Total Output Files": len(all_output_paths),
            "Stop on Failure": stop_on_failure
        }

        benchmark.log_summary(additional_info)

        # Save sequence metrics
        sequence_metrics_path = self.step_runner.output_dir / f"{sequence_name}_metrics.json"
        benchmark.save_metrics(sequence_metrics_path)

        sequence_result = {
            'success': sequence_success,
            'sequence_name': sequence_name,
            'total_steps': len(steps),
            'steps_executed': len(results),
            'successful_steps': successful_steps,
            'failed_steps': failed_steps,
            'duration_seconds': benchmark.metrics.duration_seconds,
            'total_records': benchmark.metrics.total_records_processed,
            'total_bytes': benchmark.metrics.total_bytes_processed,
            'total_output_files': len(all_output_paths),
            'output_paths': all_output_paths,
            'errors': total_errors,
            'step_results': results,
            'metrics_path': sequence_metrics_path
        }

        if sequence_success:
            self.logger.info(f"🏆 Sequence completed successfully: {sequence_name}")
        else:
            self.logger.error(f"❌ Sequence failed: {sequence_name}")

        return sequence_result
