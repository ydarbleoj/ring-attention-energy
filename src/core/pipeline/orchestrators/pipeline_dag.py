"""
Pipeline DAG - Kubeflow-style step chaining and orchestration.

Provides a directed acyclic graph (DAG) for composing pipeline steps into
complex workflows with dependency management, parallel execution, and
comprehensive monitoring.

Key features:
- Step dependency management with topological sorting
- Parallel execution of independent steps
- Data flow between steps with output → input passing
- Comprehensive error handling and recovery
- Pipeline-wide metrics and monitoring
- Configuration management for complex workflows

Follows the LLM guide principles:
- "Build modular components that can be tested and benchmarked independently"
- "Think like a systems engineer: reliability, fault tolerance, monitoring"
"""
import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

from pydantic import BaseModel, Field, ConfigDict

from ..steps.base import BaseStep, StepOutput, StepMetrics
from ..benchmarks.pipeline_benchmark import PipelineBenchmark
from ..runners.step_runner import StepRunner


@dataclass
class StepNode:
    """Represents a step in the pipeline DAG."""

    step_id: str
    step: BaseStep
    dependencies: Set[str] = field(default_factory=set)
    dependents: Set[str] = field(default_factory=set)
    status: str = "pending"  # pending, running, completed, failed
    result: Optional[StepOutput] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class PipelineDAGConfig(BaseModel):
    """Configuration for pipeline DAG execution."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    pipeline_name: str = Field(..., description="Name of the pipeline")
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    output_dir: Path = Field(default=Path("data/pipeline_runs"), description="Output directory")
    max_parallel_steps: int = Field(default=4, description="Maximum parallel step execution")
    stop_on_failure: bool = Field(default=True, description="Stop pipeline on step failure")
    save_intermediate_results: bool = Field(default=True, description="Save step outputs")
    log_level: str = Field(default="INFO", description="Pipeline logging level")

    # Data flow configuration
    auto_connect_steps: bool = Field(default=True, description="Auto-connect step outputs to inputs")
    validate_data_flow: bool = Field(default=True, description="Validate data passes between steps")


class PipelineDAG:
    """
    Pipeline DAG orchestrator for chaining steps into complex workflows.

    Provides dependency management, parallel execution, and comprehensive
    monitoring for multi-step ML pipelines.
    """

    def __init__(self, config: PipelineDAGConfig):
        """Initialize pipeline DAG with configuration."""
        self.config = config
        self.logger = logging.getLogger(f"PipelineDAG.{config.pipeline_id}")
        self.logger.setLevel(getattr(logging, config.log_level.upper()))

        # DAG structure
        self.nodes: Dict[str, StepNode] = {}
        self.execution_order: List[str] = []

        # Execution state
        self.step_runner = StepRunner(config.output_dir)
        self.pipeline_benchmark = PipelineBenchmark(f"pipeline_{config.pipeline_id}")

        # Results tracking
        self.completed_steps: Set[str] = set()
        self.failed_steps: Set[str] = set()
        self.step_outputs: Dict[str, StepOutput] = {}

        # Ensure output directory exists
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Initialized PipelineDAG: {config.pipeline_name}")

    def add_step(self, step: BaseStep, dependencies: Optional[List[str]] = None) -> 'PipelineDAG':
        """
        Add a step to the pipeline DAG.

        Args:
            step: Step instance to add
            dependencies: List of step IDs this step depends on

        Returns:
            Self for method chaining
        """
        step_id = step.config.step_id
        dependencies = dependencies or []

        if step_id in self.nodes:
            raise ValueError(f"Step '{step_id}' already exists in pipeline")

        # Create node
        node = StepNode(
            step_id=step_id,
            step=step,
            dependencies=set(dependencies)
        )

        # Validate dependencies exist
        for dep_id in dependencies:
            if dep_id not in self.nodes:
                raise ValueError(f"Dependency '{dep_id}' not found for step '{step_id}'")

            # Add this step as dependent of the dependency
            self.nodes[dep_id].dependents.add(step_id)

        self.nodes[step_id] = node
        self.logger.info(f"Added step '{step_id}' with dependencies: {dependencies}")

        return self

    def add_steps(self, steps: List[Tuple[BaseStep, Optional[List[str]]]]) -> 'PipelineDAG':
        """
        Add multiple steps to the pipeline DAG.

        Args:
            steps: List of (step, dependencies) tuples

        Returns:
            Self for method chaining
        """
        for step, dependencies in steps:
            self.add_step(step, dependencies)
        return self

    def create_extract_transform_chain(self, extract_step: BaseStep,
                                     transform_step: BaseStep) -> 'PipelineDAG':
        """
        Create a common extract → transform chain.

        Args:
            extract_step: Data extraction step
            transform_step: Data transformation step

        Returns:
            Self for method chaining
        """
        self.add_step(extract_step)
        self.add_step(transform_step, dependencies=[extract_step.config.step_id])
        return self

    def create_etl_pipeline(self, extract_step: BaseStep,
                          transform_step: BaseStep,
                          load_step: BaseStep) -> 'PipelineDAG':
        """
        Create a complete ETL pipeline.

        Args:
            extract_step: Data extraction step
            transform_step: Data transformation step
            load_step: Data loading step

        Returns:
            Self for method chaining
        """
        extract_id = extract_step.config.step_id
        transform_id = transform_step.config.step_id

        self.add_step(extract_step)
        self.add_step(transform_step, dependencies=[extract_id])
        self.add_step(load_step, dependencies=[transform_id])

        return self

    def validate_dag(self) -> None:
        """Validate the DAG structure and detect cycles."""
        if not self.nodes:
            raise ValueError("Pipeline DAG is empty")

        # Check for cycles using DFS
        visited = set()
        rec_stack = set()

        def has_cycle(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)

            for dependent in self.nodes[node_id].dependents:
                if dependent not in visited:
                    if has_cycle(dependent):
                        return True
                elif dependent in rec_stack:
                    return True

            rec_stack.remove(node_id)
            return False

        for node_id in self.nodes:
            if node_id not in visited:
                if has_cycle(node_id):
                    raise ValueError(f"Cycle detected in pipeline DAG involving step '{node_id}'")

        self.logger.info("DAG validation passed - no cycles detected")

    def _compute_execution_order(self) -> List[str]:
        """Compute topological execution order for steps."""
        # Kahn's algorithm for topological sorting
        in_degree = {node_id: len(node.dependencies) for node_id, node in self.nodes.items()}
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        execution_order = []

        while queue:
            current = queue.popleft()
            execution_order.append(current)

            # Reduce in-degree for all dependents
            for dependent in self.nodes[current].dependents:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(execution_order) != len(self.nodes):
            raise ValueError("Cannot create execution order - DAG has cycles")

        return execution_order

    def _get_ready_steps(self) -> List[str]:
        """Get steps that are ready to execute (all dependencies completed)."""
        ready_steps = []

        for node_id, node in self.nodes.items():
            if node.status == "pending":
                # Check if all dependencies are completed
                if all(self.nodes[dep].status == "completed" for dep in node.dependencies):
                    ready_steps.append(node_id)

        return ready_steps

    def _connect_step_data_flow(self, step_id: str) -> None:
        """Connect data flow from dependencies to current step."""
        if not self.config.auto_connect_steps:
            return

        node = self.nodes[step_id]

        # If step has dependencies, pass their outputs as inputs
        if node.dependencies:
            dependency_outputs = []
            for dep_id in node.dependencies:
                if dep_id in self.step_outputs:
                    dependency_outputs.extend(self.step_outputs[dep_id].output_paths)

            # Update step configuration with input paths if supported
            if hasattr(node.step.config, 'input_paths') and dependency_outputs:
                node.step.config.input_paths = dependency_outputs
                self.logger.info(f"Connected {len(dependency_outputs)} output paths to step '{step_id}'")

    async def _execute_step_async(self, step_id: str) -> StepOutput:
        """Execute a single step asynchronously."""
        node = self.nodes[step_id]

        # Update status
        node.status = "running"
        node.start_time = datetime.now()

        self.logger.info(f"🚀 Executing step: {step_id}")

        try:
            # Connect data flow
            self._connect_step_data_flow(step_id)

            # Execute step in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                self.step_runner.run_step,
                node.step,
                self.config.save_intermediate_results
            )

            # Update status
            node.status = "completed" if result.success else "failed"
            node.end_time = datetime.now()
            node.result = result

            # Track results
            self.step_outputs[step_id] = result

            if result.success:
                self.completed_steps.add(step_id)
                self.logger.info(f"✅ Step completed: {step_id}")
            else:
                self.failed_steps.add(step_id)
                self.logger.error(f"❌ Step failed: {step_id} - {result.errors}")

            return result

        except Exception as e:
            node.status = "failed"
            node.end_time = datetime.now()

            error_msg = f"Step execution failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            # Create failure result
            from ..steps.base import StepMetrics

            result = StepOutput(
                step_id=step_id,
                success=False,
                metrics=StepMetrics(
                    duration_seconds=0.0,
                    start_time=node.start_time or datetime.now(),
                    end_time=node.end_time or datetime.now()
                ),
                errors=[error_msg]
            )

            node.result = result
            self.step_outputs[step_id] = result
            self.failed_steps.add(step_id)

            return result

    def _log_pipeline_progress(self) -> None:
        """Log current pipeline progress."""
        total_steps = len(self.nodes)
        completed = len(self.completed_steps)
        failed = len(self.failed_steps)
        running = len([n for n in self.nodes.values() if n.status == "running"])
        pending = total_steps - completed - failed - running

        self.logger.info(
            f"📊 Pipeline Progress: {completed}/{total_steps} completed, "
            f"{running} running, {pending} pending, {failed} failed"
        )

    async def execute_async(self) -> Dict[str, Any]:
        """Execute the pipeline DAG asynchronously with parallel step execution."""
        self.logger.info(f"🚀 Starting pipeline execution: {self.config.pipeline_name}")

        # Validate DAG
        self.validate_dag()

        # Start pipeline benchmark
        self.pipeline_benchmark.start()

        # Track execution
        total_steps = len(self.nodes)
        currently_running = set()

        try:
            while len(self.completed_steps) + len(self.failed_steps) < total_steps:
                # Get ready steps
                ready_steps = self._get_ready_steps()

                # Check if we can start new steps
                available_slots = self.config.max_parallel_steps - len(currently_running)
                steps_to_start = ready_steps[:available_slots]

                # Start new steps
                if steps_to_start:
                    tasks = []
                    for step_id in steps_to_start:
                        task = asyncio.create_task(self._execute_step_async(step_id))
                        tasks.append((step_id, task))
                        currently_running.add(step_id)

                    # Wait for at least one step to complete
                    if tasks:
                        done, pending = await asyncio.wait(
                            [task for _, task in tasks],
                            return_when=asyncio.FIRST_COMPLETED
                        )

                        # Process completed tasks
                        for step_id, task in tasks:
                            if task in done:
                                currently_running.remove(step_id)
                                result = await task

                                # Log progress
                                self._log_pipeline_progress()

                                # Check for failure
                                if not result.success and self.config.stop_on_failure:
                                    self.logger.error(f"Pipeline stopped due to step failure: {step_id}")

                                    # Cancel remaining tasks
                                    for _, remaining_task in tasks:
                                        if remaining_task in pending:
                                            remaining_task.cancel()

                                    raise RuntimeError(f"Pipeline failed at step: {step_id}")

                # If no steps can run and none are running, we're stuck
                if not ready_steps and not currently_running:
                    waiting_steps = [
                        step_id for step_id, node in self.nodes.items()
                        if node.status == "pending"
                    ]

                    if waiting_steps:
                        raise RuntimeError(f"Pipeline deadlock - steps waiting: {waiting_steps}")
                    break

                # Small delay to prevent busy waiting
                if not steps_to_start:
                    await asyncio.sleep(0.1)

        finally:
            self.pipeline_benchmark.end()

        # Calculate final results
        pipeline_success = len(self.failed_steps) == 0
        duration = self.pipeline_benchmark.metrics.duration_seconds

        # Aggregate metrics
        total_records = sum(r.metrics.records_processed for r in self.step_outputs.values())
        total_bytes = sum(r.metrics.bytes_processed for r in self.step_outputs.values())
        total_files = sum(len(r.output_paths) for r in self.step_outputs.values())

        # Log final summary
        self.logger.info("="*80)
        self.logger.info(f"📊 PIPELINE EXECUTION SUMMARY: {self.config.pipeline_name}")
        self.logger.info("="*80)
        self.logger.info(f"✅ Success: {pipeline_success}")
        self.logger.info(f"⏱️  Duration: {duration:.2f}s")
        self.logger.info(f"📈 Steps: {len(self.completed_steps)}/{total_steps} completed")
        self.logger.info(f"📊 Records: {total_records:,} processed")
        self.logger.info(f"💾 Data: {total_bytes:,} bytes processed")
        self.logger.info(f"📄 Files: {total_files} created")

        if self.failed_steps:
            self.logger.error(f"❌ Failed steps: {', '.join(self.failed_steps)}")

        self.logger.info("="*80)

        # Save pipeline results
        results = {
            'pipeline_name': self.config.pipeline_name,
            'pipeline_id': self.config.pipeline_id,
            'success': pipeline_success,
            'duration_seconds': duration,
            'total_steps': total_steps,
            'completed_steps': len(self.completed_steps),
            'failed_steps': len(self.failed_steps),
            'total_records_processed': total_records,
            'total_bytes_processed': total_bytes,
            'total_files_created': total_files,
            'step_results': {step_id: result.model_dump() for step_id, result in self.step_outputs.items()},
            'execution_summary': {
                'completed_step_ids': list(self.completed_steps),
                'failed_step_ids': list(self.failed_steps),
                'step_execution_order': [
                    step_id for step_id in self.execution_order
                    if step_id in self.completed_steps or step_id in self.failed_steps
                ]
            }
        }

        # Save results to file
        results_path = self.config.output_dir / f"{self.config.pipeline_id}_results.json"
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)

        self.logger.info(f"💾 Pipeline results saved to: {results_path}")

        return results

    def execute(self) -> Dict[str, Any]:
        """Execute the pipeline DAG synchronously."""
        return asyncio.run(self.execute_async())

    def get_step_status(self, step_id: str) -> Optional[str]:
        """Get the status of a specific step."""
        return self.nodes[step_id].status if step_id in self.nodes else None

    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get a summary of the current pipeline state."""
        return {
            'pipeline_name': self.config.pipeline_name,
            'total_steps': len(self.nodes),
            'completed_steps': len(self.completed_steps),
            'failed_steps': len(self.failed_steps),
            'step_status': {step_id: node.status for step_id, node in self.nodes.items()},
            'execution_order': self.execution_order,
            'is_complete': len(self.completed_steps) + len(self.failed_steps) == len(self.nodes)
        }


# Example usage and testing
if __name__ == "__main__":
    print("🔗 Pipeline DAG - Kubeflow-style step chaining")
    print("   Features: Dependency management, parallel execution, monitoring")
    print("   Usage: Create PipelineDAG, add steps with dependencies, execute")
