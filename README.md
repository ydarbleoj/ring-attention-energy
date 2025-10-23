# Hamiltonian Neural Networks for Energy Systems

A physics-informed machine learning framework for modeling power grid dynamics using Hamiltonian mechanics and symplectic integration.

## Overview

This project applies Hamiltonian mechanics to energy system modeling, treating power grids as conservative dynamical systems. By representing grid states in phase space (energy storage and power flows), we enforce physical constraints like energy conservation and respect transmission limits automatically.

## Core Components

### Hamiltonian Mechanics Module (`src/core/hamiltonian/`)

Physics foundation implementing:

- Phase space representation (q: stored energy, p: power flows)
- Symplectic integrators (Leapfrog, Yoshida, Adaptive)
- Constraint enforcement via Lagrange multipliers
- Energy conservation guarantees

**Key benefit**: Long-term stability in predictions by preserving geometric structure of phase space.

### Data Pipeline (`src/core/pipeline/`)

Production-ready data collection achieving 326 records/second:

- EIA integration (optimized, validated)
- CAISO integration (pending API parameter fix)
- Synthetic data generation for testing
- Parallel chunk processing with orchestration

**Performance**: 60-day optimal batch size, 100% reliability on benchmarks.

### Planned: Hamiltonian Neural Network

Integration of learned Hamiltonians with symplectic structure:

- Neural network learns H(q, p) from historical data
- Automatic differentiation for Hamilton's equations
- Physical constraints preserved during training
- Interpretable energy functions

## Technical Approach

**Phase Space Formulation**:

```
State: (q, p, t)
  q = energy stored at nodes (batteries, reservoirs)
  p = power flows (transmission lines, generation)

Dynamics: Hamilton's equations
  dq/dt = ∂H/∂p
  dp/dt = -∂H/∂q
```

**Symplectic Integration**:
Preserves phase space volume and bounds energy error over long trajectories, critical for multi-day forecasting.

## Current Status

**Completed**:

- Hamiltonian mechanics core module with multiple integrator options
- EIA data pipeline (production-ready, benchmarked)
- Orchestrator for parallel data collection
- Comprehensive test suite (100% coverage on core modules)

**In Progress**:

- CAISO API parameter corrections
- MLX integration for neural network training
- Feature engineering pipeline

**Next Steps**:

- Train Hamiltonian Neural Network on historical grid data
- Validate against physical baselines
- Scale to multi-year datasets (2000-2024)

## Architecture Decisions

1. **Symplectic Integration**: Guarantees energy conservation, unlike standard RK4/Euler methods
2. **Async Pipeline**: Efficient I/O for large-scale data collection
3. **Modular Design**: Separate collectors for each data source
4. **Type Safety**: Comprehensive type hints throughout

## Background

**Motivation**: LLMs and traditional neural networks for energy systems lack physical constraints, leading to unphysical predictions. Hamiltonian mechanics provides the mathematical structure to ensure models respect conservation laws.

## References

- Greydanus et al. "Hamiltonian Neural Networks" (NeurIPS 2019)
- Cranmer et al. "Lagrangian Neural Networks" (ICLR 2020)
- Zhong et al. "Symplectic ODE-Net" (NeurIPS 2020)

## License

MIT
