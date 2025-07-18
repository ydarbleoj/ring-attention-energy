# Pipeline Refactor: Kubeflow-Style Steps

## Overview

Refactoring the current pipeline architecture to follow Kubeflow-inspired patterns with standardized step interfaces. This addresses mixed responsibilities and enables better testing, monitoring, and composition.

## Current Problems

1. **Mixed Responsibilities**: `EIACollector` handles API calls, file I/O, orchestration, and error handling
2. **Tight Coupling**: Hard to test individual components or swap implementations
3. **No Clear Contracts**: Each component has different interfaces and return formats
4. **Complex Orchestration**: Orchestrators managing too many concerns
5. **Benchmarking Scattered**: Performance metrics mixed into collectors instead of dedicated components

## Inspiration: Kubeflow Components

- **Single Responsibility**: Each step does one thing well
- **JSON Contracts**: Pydantic configs for type safety and validation
- **Composable**: Steps can be chained into DAGs for different workflows
- **Observable**: Standard metrics and logging from each step
- **Testable**: Mock configs, validate outputs independently

## New Architecture

### Base Step Interface

```python
class BaseStep:
    def __init__(self, config: StepConfig):
        self.config = config
        self.validate_input(config)

    def validate_input(self, config: StepConfig) -> None:
        """Validate step configuration using Pydantic"""
        raise NotImplementedError

    def run(self) -> StepOutput:
        """Execute step and return standardized output metadata"""
        raise NotImplementedError
```

### Step Categories

#### Extract Steps

- **`EIAExtractStep`**: EIA API calls → raw JSON files
- **`CAISOExtractStep`**: CAISO API → raw files
- **`WeatherExtractStep`**: Weather data → raw files

#### Transform Steps

- **`TimeSeriesAlignStep`**: Raw files → aligned timestamps
- **`MissingDataImputeStep`**: Gaps → filled data
- **`FeatureEngineerStep`**: Raw data → ML features

#### Load Steps

- **`MLXDatasetStep`**: Features → MLX-optimized tensors
- **`RingAttentionPrepStep`**: Long sequences → segmented data

### Supporting Components

#### Performance Monitoring

- **`PipelineBenchmark`**: Dedicated class for performance tracking
- **`StepMetrics`**: Standard metrics collection (duration, memory, throughput)
- **`PerformanceProfiler`**: System resource monitoring

#### Pipeline Orchestration

- **`StepRunner`**: Execute individual steps with monitoring
- **`PipelineDAG`**: Chain steps into workflows
- **`PipelineConfig`**: Overall pipeline configuration management

## Implementation Checklist

### Phase 1: Foundation ✅ **COMPLETED**

- [x] Create `BaseStep` interface with Pydantic models
- [x] Implement `StepConfig` and `StepOutput` base models
- [x] Create `PipelineBenchmark` class (extracted from BaseCollector)
- [x] Basic `StepRunner` for executing individual steps
- [x] Comprehensive test suite with pytest integration
- [x] Fix system monitoring compatibility for macOS
- [x] Update Pydantic models to use modern ConfigDict syntax

### Phase 2: Extract Step Migration ✅ **COMPLETED**

- [x] Create `EIAExtractStep` from `EIACollector.collect_batch_data_sync`
- [x] Move raw data handling to step-specific logic
- [x] Update type-safe configuration with Pydantic validation
- [x] Preserve all existing functionality (API calls, data saving, error handling)
- [x] Maintain efficient single API call for multiple regions
- [x] Comprehensive testing of extract step (14 test cases, 100% pass rate)
- [x] Create migration examples and documentation
- [x] Verify compatibility with StepRunner and benchmarking

### Phase 3: Pipeline Composition ✅ **COMPLETED**

- [x] Implement `PipelineDAG` for step chaining
- [x] Create extract → transform pipeline chains
- [x] Add automatic data flow between steps
- [x] Implement parallel execution with dependency management
- [x] Add comprehensive error handling and recovery
- [x] Create end-to-end pipeline testing
- [x] Solve multiple parquet files issue with proper step chaining
- [x] Add async execution with configurable concurrency
- [x] Implement pipeline-wide metrics and monitoring

### Phase 4: Advanced Features

- [ ] Parallel step execution where possible
- [ ] Step caching and resumption
- [ ] Advanced monitoring and alerting
- [ ] Integration with MLX ring attention pipeline

## Benefits

### Technical

- **Modularity**: Each step independently testable and swappable
- **Performance**: Dedicated benchmarking and profiling
- **Reliability**: Better error isolation and recovery
- **Scalability**: Steps can be distributed or parallelized

### Research & Development

- **Experimentation**: Easy to swap different implementations
- **Debugging**: Clear data lineage through pipeline
- **Reproducibility**: Standardized configs and outputs
- **Monitoring**: Built-in metrics for research validation

### Production Ready

- **Fault Tolerance**: Failures isolated to individual steps
- **Observability**: Standard logging and metrics
- **Configuration Management**: Type-safe, validated configs
- **Integration**: Compatible with MLflow, Kubeflow, etc.

## Alignment with LLM Guide

- ✅ "Build modular components that can be tested and benchmarked independently"
- ✅ "Think like a systems engineer: reliability, fault tolerance, monitoring"
- ✅ "Production-ready ML systems with clear interfaces"
- ✅ "Connect research contributions to practical energy grid challenges"

## Files to Create/Modify

### New Files

- `src/core/pipeline/steps/base.py` - BaseStep interface
- `src/core/pipeline/steps/extract/eia_extract.py` - EIA extract step
- `src/core/pipeline/benchmarks/pipeline_benchmark.py` - Performance monitoring
- `src/core/pipeline/runners/step_runner.py` - Step execution

### Modified Files

- `src/core/pipeline/collectors/eia_collector.py` - Simplified to focus on API logic
- `src/core/pipeline/orchestrators/extract_orchestrator.py` - Use step interface
- Tests for all new components

## Success Metrics

- [x] Each step can be tested in isolation
- [x] Performance benchmarking separated from business logic
- [ ] End-to-end pipeline reproducible with config files
- [x] Memory usage and execution time clearly tracked per step
- [ ] Easy to add new data sources or processing steps

## Foundation Implementation Summary

**Phase 1 Complete! ✅**

We have successfully implemented the foundation for Kubeflow-style pipeline steps:

### ✅ What's Working:

- **BaseStep Interface**: Abstract base class with clear `validate_input()` and `_execute()` contracts
- **Pydantic Models**: Type-safe `StepConfig`, `StepOutput`, and `StepMetrics` with modern ConfigDict syntax
- **Performance Monitoring**: Dedicated `PipelineBenchmark` class with system resource tracking
- **Step Execution**: `StepRunner` and `SequentialRunner` for executing steps with full monitoring
- **Error Handling**: Graceful failure handling with detailed error reporting
- **Testing**: Comprehensive test suite with pytest integration
- **macOS Compatibility**: System monitoring works across different environments

### 📁 Files Created:

- `src/core/pipeline/steps/base.py` - Core step interface and models
- `src/core/pipeline/benchmarks/pipeline_benchmark.py` - Performance monitoring
- `src/core/pipeline/runners/step_runner.py` - Step execution framework
- `tests/core/pipeline/test_pipeline_foundation.py` - Complete test coverage

### 🔄 Ready for Phase 2:

The foundation is solid and tested. We can now proceed with confidence to migrate the `EIACollector` to an `EIAExtractStep` that follows our new standardized interface.

**Next Steps**: Start Phase 2 by creating `EIAExtractStep` that uses the existing `EIADataService` but follows the new step interface pattern.

## Phase 2 Implementation Summary ✅ **COMPLETED**

**Phase 2 Complete! ✅**

We have successfully migrated the `EIACollector.collect_batch_data_sync()` functionality to the new `EIAExtractStep` that follows the BaseStep interface pattern.

### ✅ What's Working:

- **EIAExtractStep**: Complete implementation following BaseStep interface
- **Type-Safe Configuration**: `EIAExtractStepConfig` with Pydantic validation
- **Preserved Functionality**: All existing API calls, data saving, and error handling maintained
- **Efficient Performance**: Single API call for multiple regions preserved
- **Comprehensive Testing**: 14 test cases covering all scenarios with 100% pass rate
- **Migration Examples**: Clear documentation showing before/after patterns
- **Integration Ready**: Compatible with StepRunner and pipeline benchmarking

### 📁 Files Created:

- `src/core/pipeline/steps/extract/eia_extract.py` - Main EIAExtractStep implementation
- `tests/core/pipeline/steps/test_eia_extract_step.py` - Comprehensive test suite
- `demo_eia_extract_step.py` - Practical usage demonstration
- `examples/eia_collector_migration.py` - Migration guide and comparison
- `docs/phase2_implementation_summary.md` - Detailed implementation documentation

### 🔄 Migration Pattern:

```python
# OLD: EIACollector.collect_batch_data_sync()
result = collector.collect_batch_data_sync(
    data_type="demand", start_date=start, end_date=end, regions=regions
)

# NEW: EIAExtractStep with standardized interface
config = EIAExtractStepConfig(
    step_name="EIA Extract", step_id="extract", api_key=key,
    data_type="demand", start_date="2024-01-01", end_date="2024-01-02"
)
step = EIAExtractStep(config)
result = step.run()  # Returns standardized StepOutput
```

### 🔄 Ready for Phase 3:

The EIAExtractStep is now ready for pipeline composition. We can proceed with confidence to implement:

- Transform steps (time series alignment, missing data imputation)
- Load steps (MLX-compatible data preparation)
- PipelineDAG for step chaining and workflow orchestration

## Phase 3 Implementation Summary ✅ **COMPLETED**

**Phase 3 Complete! ✅**

We have successfully implemented the PipelineDAG orchestrator for Kubeflow-style step chaining and composition.

### ✅ What's Working:

- **PipelineDAG Class**: Complete implementation with dependency management, parallel execution, and comprehensive monitoring
- **Step Chaining**: Extract → Transform chains with automatic data flow between steps
- **Parallel Execution**: Configurable concurrency with async execution and dependency respect
- **Data Flow Automation**: Automatic connection of step outputs to step inputs
- **Error Handling**: Graceful failure handling with stop-on-failure configuration
- **Performance Monitoring**: Pipeline-wide metrics collection and reporting
- **Single Parquet Output**: Solves the multiple parquet files issue through proper step orchestration

### 📁 Files Created:

- `src/core/pipeline/orchestrators/pipeline_dag.py` - Main PipelineDAG implementation
- `tests/core/pipeline/test_pipeline_dag.py` - Comprehensive test suite (15+ test cases)
- `demo_pipeline_dag.py` - Working demonstration script
- `year_pipeline_dag.py` - Year-long pipeline with PipelineDAG orchestration

### 🔄 Key Features Implemented:

```python
# Pipeline DAG with step chaining
dag = PipelineDAG(config)
dag.create_extract_transform_chain(extract_step, transform_step)
results = await dag.execute_async()

# Automatic data flow
extract_outputs → transform_inputs (automatic)

# Parallel execution with dependencies
max_parallel_steps=4  # Configurable concurrency
dependency_respect=True  # Dependencies always respected

# Comprehensive monitoring
pipeline_metrics, step_metrics, performance_reports
```

### 🎯 Problems Solved:

1. **Multiple Parquet Files Issue**: ✅ Solved through proper step chaining
2. **Data Flow Management**: ✅ Automatic output → input connection
3. **Step Orchestration**: ✅ Dependency management with topological sorting
4. **Performance Monitoring**: ✅ Pipeline-wide and step-specific metrics
5. **Error Handling**: ✅ Graceful failure handling and recovery
6. **Parallel Execution**: ✅ Async execution with configurable concurrency

### 🔄 Ready for Phase 4:

The PipelineDAG foundation is solid and tested. We can now proceed with confidence to implement Phase 4 advanced features:

- **Step Caching**: Resume pipelines from checkpoints
- **MLX Integration**: Load steps for ring attention pipeline
- **Advanced Monitoring**: External monitoring system integration
- **Performance Optimization**: Further optimize transform step to 10k+ RPS

**Next Steps**: Start Phase 4 by creating MLX load steps and implementing step caching capabilities.
