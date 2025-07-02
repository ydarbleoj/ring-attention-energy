import os
import asyncio
import pytest
import pytest_asyncio
import vcr
import mlx.core as mx
import numpy as np
from typing import AsyncGenerator
from pathlib import Path
from unittest.mock import patch, MagicMock

# Test fixtures for MLX and Ring Attention testing
@pytest.fixture
def test_data_dir():
    """Directory for test data and VCR cassettes"""
    return Path(__file__).parent / "data"

@pytest.fixture
def vcr_config():
    """VCR configuration for recording HTTP requests"""
    return {
        'serializer': 'yaml',
        'cassette_library_dir': str(Path(__file__).parent / "vcr_cassettes"),
        'record_mode': 'once',
        'match_on': ['uri', 'method'],
        'filter_headers': ['authorization', 'x-api-key'],
    }

@pytest.fixture
def vcr_cassette(vcr_config):
    """VCR cassette fixture for energy API requests"""
    def _cassette(cassette_name):
        cassette_path = Path(vcr_config['cassette_library_dir']) / f"{cassette_name}.yaml"
        cassette_path.parent.mkdir(parents=True, exist_ok=True)
        return vcr.use_cassette(str(cassette_path), **{k: v for k, v in vcr_config.items() if k != 'cassette_library_dir'})
    return _cassette

@pytest.fixture
def sample_energy_sequence():
    """Generate sample energy time series data for testing"""
    def _generate(seq_len=8760, d_model=256, batch_size=1):
        # Generate realistic energy consumption patterns
        # Simulate daily and seasonal patterns
        hours = np.arange(seq_len)
        daily_pattern = np.sin(2 * np.pi * hours / 24)  # Daily cycle
        seasonal_pattern = np.sin(2 * np.pi * hours / (24 * 365))  # Yearly cycle
        noise = np.random.normal(0, 0.1, seq_len)

        # Base energy consumption with patterns
        base_consumption = 100 + 20 * daily_pattern + 10 * seasonal_pattern + noise

        # Create MLX array with additional features
        sequence_data = mx.random.normal((batch_size, seq_len, d_model))
        # Set the first feature to be our realistic energy pattern - MLX compatible way
        energy_feature = mx.array(base_consumption.reshape(1, -1))
        if batch_size == 1:
            # Create a new array with the energy pattern in the first feature
            new_sequence = mx.concatenate([
                energy_feature[:, :, None],  # Energy pattern as first feature
                sequence_data[:, :, 1:]  # Rest of the features
            ], axis=2)
            sequence_data = new_sequence

        return sequence_data
    return _generate

@pytest.fixture
def ring_attention_params():
    """Standard parameters for ring attention testing"""
    return {
        'd_model': 256,
        'n_heads': 8,
        'ring_size': 4,
        'causal': True
    }

@pytest.fixture
def numerical_tolerance():
    """Tolerance levels for numerical comparisons"""
    return {
        'rtol': 1e-5,
        'atol': 1e-8
    }
