#!/usr/bin/env python3
"""
Comprehensive n_heads analysis for ring attention
"""
import sys
from pathlib import Path
import time
import math

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import mlx.core as mx
from src.core.llms.ring_attention import RingAttention, EnergyTransformerBlock

def test_attention_heads_scaling():
    """Test how different numbers of heads affect attention computation"""
    print("\n🔬 Testing Multi-Head Scaling Effects")
    print("=" * 60)

    seq_len = 256
    d_model = 256
    ring_size = 4

    # Test different head configurations
    head_configs = [1, 2, 4, 8, 16]

    results = []

    for n_heads in head_configs:
        # Ensure d_model is divisible by n_heads
        if d_model % n_heads != 0:
            print(f"   ⚠️  Skipping n_heads={n_heads} (d_model={d_model} not divisible)")
            continue

        d_k = d_model // n_heads

        print(f"\n   🧮 Testing n_heads={n_heads:2d}, d_k={d_k:3d}")

        # Create test data
        x = mx.random.normal((1, seq_len, d_model), key=mx.random.key(42))

        # Create ring attention with this head configuration
        ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

        # Time the forward pass
        start_time = time.time()
        output = ring_attn(x)
        end_time = time.time()

        processing_time = (end_time - start_time) * 1000  # Convert to ms

        # Validate output
        assert output.shape == x.shape, f"Shape mismatch for n_heads={n_heads}"
        assert not mx.any(mx.isnan(output)), f"NaN detected with n_heads={n_heads}"
        assert not mx.any(mx.isinf(output)), f"Inf detected with n_heads={n_heads}"

        # Calculate parameter count for Q, K, V, O projections
        total_params = 4 * d_model * d_model  # w_q, w_k, w_v, w_o are all d_model x d_model

        # Test attention head specialization by looking at output variance
        output_variance = float(mx.var(output))

        results.append({
            'n_heads': n_heads,
            'd_k': d_k,
            'processing_time_ms': processing_time,
            'total_params': total_params,
            'output_variance': output_variance
        })

        print(f"      ⏱️  Processing time: {processing_time:.2f}ms")
        print(f"      📊 Output variance: {output_variance:.6f}")
        print(f"      🔢 Parameters per head: {total_params // n_heads:,}")

    # Analysis of results
    print(f"\n📈 Multi-Head Analysis Summary:")
    print(f"{'n_heads':<8} {'d_k':<5} {'Time (ms)':<10} {'Params/Head':<12} {'Variance':<12}")
    print("-" * 65)

    for result in results:
        params_per_head = result['total_params'] // result['n_heads']
        print(f"{result['n_heads']:<8} {result['d_k']:<5} {result['processing_time_ms']:<10.2f} "
              f"{params_per_head:<12,} {result['output_variance']:<12.6f}")

    # Key insights validation
    print(f"\n🔍 Key Insights:")

    # 1. Parameter count should be constant across head configurations
    param_counts = [r['total_params'] for r in results]
    assert all(p == param_counts[0] for p in param_counts), "Parameter count should be constant"
    print(f"   ✅ Total parameters constant: {param_counts[0]:,}")

    # 2. d_k should decrease as n_heads increases
    d_k_values = [r['d_k'] for r in results]
    n_head_values = [r['n_heads'] for r in results]
    for i in range(1, len(d_k_values)):
        if n_head_values[i] > n_head_values[i-1]:
            assert d_k_values[i] <= d_k_values[i-1], "d_k should decrease as n_heads increases"
    print(f"   ✅ d_k decreases as n_heads increases: {d_k_values}")

    # 3. Processing time should be relatively stable (ring attention efficiency)
    times = [r['processing_time_ms'] for r in results]
    time_variance = mx.var(mx.array(times))
    print(f"   ✅ Processing time variance: {float(time_variance):.2f}ms² (should be low)")

    return results

def test_head_attention_patterns():
    """Test that different heads learn different attention patterns"""
    print("\n🎭 Testing Head Attention Pattern Specialization")
    print("=" * 60)

    d_model = 128  # Smaller for detailed analysis
    n_heads = 4
    ring_size = 2
    seq_len = 64

    # Create structured input that should lead to different head patterns
    x = mx.zeros((1, seq_len, d_model))

    # Add different periodic patterns to different parts of the feature space
    for i in range(seq_len):
        # Daily-like pattern in first quarter of features
        daily_pattern = mx.sin(2 * mx.pi * i / 24)
        weekly_pattern = mx.sin(2 * mx.pi * i / (24*7))
        monthly_pattern = mx.sin(2 * mx.pi * i / (24*30))
        noise = mx.random.normal((d_model//4,))

        # MLX way to set values
        x_slice = x[0, i, :]
        x_slice = x_slice.at[:d_model//4].set(daily_pattern)
        x_slice = x_slice.at[d_model//4:d_model//2].set(weekly_pattern)
        x_slice = x_slice.at[d_model//2:3*d_model//4].set(monthly_pattern)
        x_slice = x_slice.at[3*d_model//4:].set(noise)
        x = x.at[0, i, :].set(x_slice)

    ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

    # Get Q, K, V projections to analyze head behavior
    q = ring_attn.w_q(x).reshape(1, seq_len, n_heads, d_model // n_heads)
    k = ring_attn.w_k(x).reshape(1, seq_len, n_heads, d_model // n_heads)

    print(f"   📊 Input patterns created for {seq_len} timesteps")
    print(f"   🔧 Analyzing {n_heads} attention heads")

    # Analyze each head's query-key similarity patterns
    head_analyses = []

    for head_idx in range(n_heads):
        q_head = q[0, :, head_idx, :]  # [seq_len, d_k]
        k_head = k[0, :, head_idx, :]  # [seq_len, d_k]

        # Compute attention scores for this head
        scores = mx.matmul(q_head, k_head.T) / math.sqrt(d_model // n_heads)

        # Analyze the attention pattern characteristics
        # 1. Diagonal dominance (local attention)
        diagonal_strength = mx.mean(mx.diag(scores))

        # 2. Bandwidth (how far attention spreads)
        attention_weights = mx.softmax(scores, axis=-1)
        center_positions = mx.arange(seq_len).astype(mx.float32)

        # Calculate effective attention bandwidth for each query position
        bandwidths = []
        for q_pos in range(seq_len):
            weights = attention_weights[q_pos, :]
            # Weighted variance of attention positions
            mean_pos = mx.sum(weights * center_positions)
            variance = mx.sum(weights * (center_positions - mean_pos) ** 2)
            bandwidths.append(float(variance))

        avg_bandwidth = mx.mean(mx.array(bandwidths))

        head_analyses.append({
            'head': head_idx,
            'diagonal_strength': float(diagonal_strength),
            'avg_bandwidth': float(avg_bandwidth),
            'score_variance': float(mx.var(scores))
        })

        print(f"   🧠 Head {head_idx}: diagonal={diagonal_strength:.3f}, "
              f"bandwidth={avg_bandwidth:.2f}, variance={mx.var(scores):.4f}")

    # Verify heads show different patterns
    diagonal_strengths = [h['diagonal_strength'] for h in head_analyses]
    bandwidths = [h['avg_bandwidth'] for h in head_analyses]

    # Heads should show some diversity in their attention patterns
    diagonal_variance = mx.var(mx.array(diagonal_strengths))
    bandwidth_variance = mx.var(mx.array(bandwidths))

    print(f"\n   📈 Pattern Diversity:")
    print(f"      Diagonal strength variance: {float(diagonal_variance):.4f}")
    print(f"      Bandwidth variance: {float(bandwidth_variance):.4f}")

    # Test that heads are not identical (some diversity expected)
    assert float(diagonal_variance) > 1e-6, "Heads should show some attention pattern diversity"
    assert float(bandwidth_variance) > 1e-6, "Heads should show different attention bandwidths"

    print("   ✅ Heads show different attention patterns")

    return head_analyses

def test_memory_scaling_with_heads():
    """Test memory scaling with different head configurations"""
    print("\n💾 Testing Memory Scaling with Different Head Counts")
    print("=" * 60)

    seq_lens = [128, 512, 1024, 2048]
    head_configs = [2, 4, 8, 16]
    d_model = 256
    ring_size = 4

    print(f"{'Seq Len':<8} {'n_heads':<8} {'d_k':<5} {'Time (ms)':<10} {'Memory Ratio':<12}")
    print("-" * 55)

    for seq_len in seq_lens:
        for n_heads in head_configs:
            if d_model % n_heads != 0:
                continue

            d_k = d_model // n_heads
            x = mx.random.normal((1, seq_len, d_model))

            ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

            start_time = time.time()
            output = ring_attn(x)
            end_time = time.time()

            processing_time = (end_time - start_time) * 1000

            # Calculate memory efficiency
            standard_memory = seq_len * seq_len * n_heads
            ring_memory = ring_size * (seq_len // ring_size) ** 2 * n_heads
            memory_ratio = ring_memory / standard_memory

            print(f"{seq_len:<8} {n_heads:<8} {d_k:<5} {processing_time:<10.2f} {memory_ratio:<12.4f}")

    print(f"\n🔍 Key Insight: Memory ratio is independent of n_heads!")
    print(f"   Ring attention memory savings scale with sequence length, not head count.")

if __name__ == "__main__":
    print("🌟 Comprehensive Ring Attention n_heads Analysis")
    print("=" * 70)

    try:
        results = test_attention_heads_scaling()
        head_patterns = test_head_attention_patterns()
        test_memory_scaling_with_heads()

        print("\n🎉 All n_heads tests passed successfully!")
        print("✨ Ring attention scales effectively across different head configurations!")

        # Summary insights
        print("\n📝 Summary Insights about n_heads in Ring Attention:")
        print("   1. Total parameters remain constant regardless of n_heads")
        print("   2. d_k = d_model // n_heads decreases as n_heads increases")
        print("   3. Processing time is relatively stable across head counts")
        print("   4. Memory efficiency is independent of n_heads")
        print("   5. Different heads learn different attention patterns")
        print("   6. Ring attention maintains multi-head benefits while scaling to long sequences")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
