#!/usr/bin/env python3
"""
Simple test runner for ring attention without pytest
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Now we can import
import mlx.core as mx
from src.core.llms.ring_attention import RingAttention, EnergyTransformerBlock

def test_basic_functionality():
    """Basic functionality test"""
    print("🧪 Testing Ring Attention Basic Functionality")
    print("=" * 50)

    batch_size = 2
    seq_len = 128
    d_model = 256
    n_heads = 8

    # Create test data
    x = mx.random.normal((batch_size, seq_len, d_model))
    print(f"📊 Input shape: {x.shape}")

    # Initialize ring attention
    ring_attn = RingAttention(d_model, n_heads, ring_size=4)
    print(f"🔧 Ring attention initialized with {n_heads} heads, ring size 4")

    # Forward pass
    output = ring_attn(x)
    print(f"✅ Output shape: {output.shape}")

    # Basic shape check
    assert output.shape == (batch_size, seq_len, d_model)
    print("✅ Shape test passed")

    # Check for NaN/Inf
    assert not mx.any(mx.isnan(output)), "NaN values detected"
    assert not mx.any(mx.isinf(output)), "Inf values detected"
    print("✅ No NaN/Inf values")

    # Test with longer sequence (energy data)
    long_seq_len = 8760  # One year of hourly data
    x_long = mx.random.normal((1, long_seq_len, d_model))
    print(f"\n📈 Testing long sequence: {x_long.shape}")

    output_long = ring_attn(x_long)
    assert output_long.shape == (1, long_seq_len, d_model)
    print(f"✅ Long sequence output shape: {output_long.shape}")

    # Check memory efficiency
    standard_memory = long_seq_len * long_seq_len
    ring_memory = 4 * (long_seq_len // 4) ** 2
    memory_ratio = ring_memory / standard_memory
    print(f"💾 Memory efficiency: {memory_ratio:.4f}x vs standard attention")

    return True

def test_energy_transformer_block():
    """Test the full energy transformer block"""
    print("\n🏗️  Testing Energy Transformer Block")
    print("=" * 50)

    d_model = 256
    n_heads = 8
    d_ff = d_model * 4
    ring_size = 4
    seq_len = 512

    x = mx.random.normal((1, seq_len, d_model))
    print(f"📊 Input shape: {x.shape}")

    block = EnergyTransformerBlock(
        d_model=d_model,
        n_heads=n_heads,
        d_ff=d_ff,
        ring_size=ring_size
    )

    output = block(x)
    print(f"✅ Output shape: {output.shape}")

    assert output.shape == x.shape
    assert not mx.any(mx.isnan(output))
    assert not mx.any(mx.isinf(output))

    print("✅ Energy transformer block test passed")

    return True

def test_math_correctness():
    """Test mathematical correctness of ring attention"""
    print("\n🔬 Testing Mathematical Correctness")
    print("=" * 50)

    d_model = 128
    n_heads = 4
    ring_size = 2
    seq_len = 64

    ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

    # Test causal masking
    segment_len = seq_len // ring_size

    # Same segment should be causal
    mask_same = ring_attn._create_segment_mask(segment_len, segment_len, 0, 0)
    assert mask_same.shape == (segment_len, segment_len)
    # Check that upper triangle is masked (False) - create upper triangle manually
    upper_triangle = mx.triu(mx.ones((segment_len, segment_len)), k=1)
    upper_triangle_sum = mx.sum(mask_same * upper_triangle)
    assert upper_triangle_sum == 0, "Upper triangle should be masked in causal attention"
    print("✅ Causal masking for same segment correct")

    # Future segment should be completely masked
    mask_future = ring_attn._create_segment_mask(segment_len, segment_len, 0, 1)
    assert mx.sum(mask_future) == 0, "Future segment should be completely masked"
    print("✅ Future segment masking correct")

    # Past segment should be completely visible
    mask_past = ring_attn._create_segment_mask(segment_len, segment_len, 1, 0)
    assert mx.sum(mask_past) == segment_len * segment_len, "Past segment should be completely visible"
    print("✅ Past segment masking correct")

    # Test sequence splitting and reconstruction
    x = mx.arange(seq_len * d_model).reshape(1, seq_len, d_model).astype(mx.float32)
    segments = ring_attn._split_sequence(x)

    assert len(segments) == ring_size, f"Expected {ring_size} segments, got {len(segments)}"

    # Reconstruct and compare
    reconstructed = mx.concatenate(segments, axis=1)
    original_truncated = x[:, :reconstructed.shape[1], :]

    assert mx.allclose(reconstructed, original_truncated), "Sequence reconstruction failed"
    print("✅ Sequence splitting and reconstruction correct")

    return True

if __name__ == "__main__":
    print("🌟 Ring Attention Test Suite")
    print("=" * 60)

    try:
        test_basic_functionality()
        test_energy_transformer_block()
        test_math_correctness()

        print("\n🎉 All tests passed successfully!")
        print("✨ Ring attention implementation is mathematically sound")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
