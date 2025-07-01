#!/usr/bin/env python3
"""
Ring Attention Energy Optimization - Main Entry Point

This module provides the main entry point for the ring attention energy optimization system.
Currently focused on testing and validating the ring attention mathematical implementation.

Usage:
    python -m src.main --mode test
    python -m src.main --mode demo
    python -m src.main --mode benchmark
"""

import argparse
import sys
from pathlib import Path
import mlx.core as mx
import time
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from core.llms.ring_attention import RingAttention, EnergyTransformerBlock


def run_ring_attention_demo():
    """Demonstrate ring attention with energy-like time series data"""
    print("🔄 Running Ring Attention Demo...")
    print("=" * 60)

    # Parameters for energy time series
    d_model = 256
    n_heads = 8
    ring_size = 4

    # Test with different sequence lengths representing different time horizons
    test_cases = [
        ("Daily (24 hours)", 24),
        ("Weekly (168 hours)", 168),
        ("Monthly (720 hours)", 720),
        ("Yearly (8760 hours)", 8760)
    ]

    for case_name, seq_len in test_cases:
        print(f"\n📊 Testing {case_name}:")
        print(f"   Sequence length: {seq_len} timesteps")

        # Create sample energy data
        x = mx.random.normal((1, seq_len, d_model))

        # Initialize ring attention
        ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

        # Time the forward pass
        start_time = time.time()
        output = ring_attn(x)
        end_time = time.time()

        # Calculate memory efficiency
        standard_attention_memory = seq_len * seq_len
        ring_attention_memory = ring_size * (seq_len // ring_size) ** 2
        memory_ratio = ring_attention_memory / standard_attention_memory

        print(f"   ✅ Output shape: {output.shape}")
        print(f"   ⏱️  Processing time: {(end_time - start_time)*1000:.2f}ms")
        print(f"   💾 Memory efficiency: {memory_ratio:.4f}x (vs standard attention)")

        # Validate output
        assert not mx.any(mx.isnan(output)), f"NaN detected in {case_name}"
        assert not mx.any(mx.isinf(output)), f"Inf detected in {case_name}"


def run_transformer_block_demo():
    """Demonstrate the full energy transformer block"""
    print("\n🏗️  Running Energy Transformer Block Demo...")
    print("=" * 60)

    # Energy transformer parameters
    d_model = 256
    n_heads = 8
    d_ff = d_model * 4
    ring_size = 4
    seq_len = 8760  # Full year of hourly data

    print(f"🔧 Configuration:")
    print(f"   Model dimension: {d_model}")
    print(f"   Number of heads: {n_heads}")
    print(f"   Feed-forward dimension: {d_ff}")
    print(f"   Ring size: {ring_size}")
    print(f"   Sequence length: {seq_len} (1 year hourly)")

    # Create energy time series
    x = mx.random.normal((1, seq_len, d_model))

    # Initialize transformer block
    transformer_block = EnergyTransformerBlock(
        d_model=d_model,
        n_heads=n_heads,
        d_ff=d_ff,
        ring_size=ring_size
    )

    # Process the sequence
    start_time = time.time()
    output = transformer_block(x)
    end_time = time.time()

    print(f"\n📈 Results:")
    print(f"   ✅ Input shape: {x.shape}")
    print(f"   ✅ Output shape: {output.shape}")
    print(f"   ⏱️  Processing time: {(end_time - start_time)*1000:.2f}ms")
    print(f"   📊 Output statistics:")
    print(f"      Mean: {mx.mean(output):.6f}")
    print(f"      Std:  {mx.std(output):.6f}")
    print(f"      Min:  {mx.min(output):.6f}")
    print(f"      Max:  {mx.max(output):.6f}")


def run_memory_benchmark():
    """Benchmark memory usage across different sequence lengths"""
    print("\n🚀 Running Memory Benchmark...")
    print("=" * 60)

    d_model = 256
    n_heads = 8
    ring_size = 4

    # Different sequence lengths to test
    seq_lengths = [128, 512, 1024, 2048, 4096, 8760]

    print(f"{'Seq Length':<12} {'Processing Time':<16} {'Memory Ratio':<14} {'Status'}")
    print("-" * 60)

    ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

    for seq_len in seq_lengths:
        try:
            x = mx.random.normal((1, seq_len, d_model))

            start_time = time.time()
            output = ring_attn(x)
            end_time = time.time()

            processing_time = (end_time - start_time) * 1000

            # Calculate theoretical memory savings
            standard_memory = seq_len * seq_len
            ring_memory = ring_size * (seq_len // ring_size) ** 2
            memory_ratio = ring_memory / standard_memory

            status = "✅ Success"
            print(f"{seq_len:<12} {processing_time:<14.2f}ms {memory_ratio:<13.4f} {status}")

        except Exception as e:
            status = f"❌ Failed: {str(e)[:20]}..."
            print(f"{seq_len:<12} {'N/A':<14} {'N/A':<13} {status}")


def run_mathematical_validation():
    """Run mathematical validation tests"""
    print("\n🔬 Running Mathematical Validation...")
    print("=" * 60)

    d_model = 128  # Smaller for detailed analysis
    n_heads = 4
    ring_size = 2
    seq_len = 64

    ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

    # Test 1: Attention score computation
    print("🧮 Test 1: Attention Score Computation")
    x = mx.random.normal((1, seq_len, d_model), key=mx.random.key(42))

    # Get Q, K, V projections
    q = ring_attn.w_q(x).reshape(1, seq_len, n_heads, d_model // n_heads)
    k = ring_attn.w_k(x).reshape(1, seq_len, n_heads, d_model // n_heads)
    v = ring_attn.w_v(x).reshape(1, seq_len, n_heads, d_model // n_heads)

    print(f"   Q shape: {q.shape}")
    print(f"   K shape: {k.shape}")
    print(f"   V shape: {v.shape}")

    # Test 2: Segment masking
    print("\n🎭 Test 2: Segment Masking")
    segment_len = seq_len // ring_size

    # Test different mask types
    mask_types = [
        ("Same segment (causal)", 0, 0),
        ("Future segment (blocked)", 0, 1),
        ("Past segment (visible)", 1, 0)
    ]

    for mask_name, q_idx, kv_idx in mask_types:
        mask = ring_attn._create_segment_mask(segment_len, segment_len, q_idx, kv_idx)
        visible_positions = mx.sum(mask)
        total_positions = segment_len * segment_len

        print(f"   {mask_name}: {visible_positions}/{total_positions} positions visible")

    # Test 3: Full forward pass
    print("\n🔄 Test 3: Full Forward Pass")
    output = ring_attn(x)

    print(f"   Input shape: {x.shape}")
    print(f"   Output shape: {output.shape}")
    print(f"   Shape preservation: {'✅' if output.shape == x.shape else '❌'}")
    print(f"   No NaN values: {'✅' if not mx.any(mx.isnan(output)) else '❌'}")
    print(f"   No Inf values: {'✅' if not mx.any(mx.isinf(output)) else '❌'}")

    # Test 4: Gradient flow (conceptual)
    print("\n🌊 Test 4: Output Sensitivity Analysis")

    # Test with slightly different input
    x_perturbed = x + mx.random.normal(x.shape) * 0.01
    output_perturbed = ring_attn(x_perturbed)

    output_diff = mx.mean(mx.abs(output - output_perturbed))
    print(f"   Input perturbation sensitivity: {output_diff:.6f}")
    print(f"   Model is responsive: {'✅' if output_diff > 1e-6 else '❌'}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Ring Attention Energy Optimization")
    parser.add_argument(
        "--mode",
        choices=["demo", "benchmark", "test", "validate"],
        default="demo",
        help="Mode to run the application in"
    )

    args = parser.parse_args()

    print("🌟 Ring Attention Energy Optimization System")
    print("=" * 60)
    print(f"🎯 Mode: {args.mode.upper()}")
    print(f"🖥️  MLX Backend: {'Available' if mx.default_device() else 'Not Available'}")
    print(f"🔧 Default Device: {mx.default_device()}")

    if args.mode == "demo":
        run_ring_attention_demo()
        run_transformer_block_demo()

    elif args.mode == "benchmark":
        run_memory_benchmark()

    elif args.mode == "test":
        print("\n🧪 Running basic functionality tests...")
        run_ring_attention_demo()
        print("\n✅ All basic tests completed!")

    elif args.mode == "validate":
        run_mathematical_validation()

    print("\n🎉 Execution completed successfully!")


if __name__ == "__main__":
    main()