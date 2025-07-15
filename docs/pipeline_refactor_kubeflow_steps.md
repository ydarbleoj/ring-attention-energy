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

### Phase 2: Extract Step Migration

- [ ] Create `EIAExtractStep` from `EIACollector.collect_batch_data_sync`
- [ ] Move raw data handling to step-specific logic
- [ ] Update orchestrators to use new step interface
- [ ] Comprehensive testing of extract step

### Phase 3: Pipeline Composition

- [ ] Implement `PipelineDAG` for step chaining
- [ ] Create standard transform steps
- [ ] Add load steps for MLX integration
- [ ] End-to-end pipeline testing

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
