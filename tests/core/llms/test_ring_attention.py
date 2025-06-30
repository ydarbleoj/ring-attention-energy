import mlx.core as mx
import numpy as np
from src.ring_attention import RingAttention, EnergyTransformerBlock

def test_ring_attention_basic():
    """Test basic functionality with small sequences"""
    batch_size = 2
    seq_len = 128  # Small for testing
    d_model = 256
    n_heads = 8

    # Create test data (simulating hourly energy data)
    x = mx.random.normal((batch_size, seq_len, d_model))

    # Initialize ring attention
    ring_attn = RingAttention(d_model, n_heads, ring_size=4)

    # Forward pass
    output = ring_attn(x)

    # Basic shape check
    assert output.shape == (batch_size, seq_len, d_model)
    print(f"✅ Ring attention output shape: {output.shape}")

    # Test with longer sequence (more realistic for energy data)
    long_seq_len = 8760  # One year of hourly data
    x_long = mx.random.normal((1, long_seq_len, d_model))

    output_long = ring_attn(x_long)
    assert output_long.shape == (1, long_seq_len, d_model)
    print(f"✅ Long sequence output shape: {output_long.shape}")

if __name__ == "__main__":
    test_ring_attention_basic()
    print("🎉 Ring attention tests passed!")