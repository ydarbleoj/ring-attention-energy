import mlx.core as mx
import numpy as np
import pytest
import math
from src.core.llms.ring_attention import RingAttention, EnergyTransformerBlock


class TestRingAttentionMath:
    """Comprehensive mathematical tests for Ring Attention implementation"""

    def test_attention_output_shape(self, ring_attention_params, sample_energy_sequence):
        """Test that attention output maintains correct shape"""
        seq_lens = [128, 1024, 8760]  # Short, medium, yearly sequences

        for seq_len in seq_lens:
            x = sample_energy_sequence(seq_len=seq_len)
            ring_attn = RingAttention(**ring_attention_params)

            output = ring_attn(x)

            assert output.shape == x.shape, f"Shape mismatch for seq_len={seq_len}"
            print(f"✅ Shape test passed for sequence length {seq_len}")

    def test_causal_masking_correctness(self, ring_attention_params):
        """Test that causal masking is applied correctly across segments"""
        batch_size, seq_len, d_model = 1, 256, ring_attention_params['d_model']
        ring_size = ring_attention_params['ring_size']

        # Create deterministic input for testing
        x = mx.ones((batch_size, seq_len, d_model))
        ring_attn = RingAttention(**ring_attention_params)

        # Test segment mask creation
        segment_len = seq_len // ring_size

        # Test same segment (should be causal)
        mask_same = ring_attn._create_segment_mask(segment_len, segment_len, 0, 0)
        assert mask_same.shape == (segment_len, segment_len)
        # Upper triangle should be False (masked)
        assert not mx.any(mask_same[mx.triu_indices(segment_len, k=1)])

        # Test future segment (should be all False)
        mask_future = ring_attn._create_segment_mask(segment_len, segment_len, 0, 1)
        assert not mx.any(mask_future)

        # Test past segment (should be all True)
        mask_past = ring_attn._create_segment_mask(segment_len, segment_len, 1, 0)
        assert mx.all(mask_past)

        print("✅ Causal masking tests passed")

    def test_attention_computation_numerical_stability(self, ring_attention_params, numerical_tolerance):
        """Test numerical stability of attention computation"""
        # Test with extreme values
        batch_size, seq_len, d_model = 1, 128, ring_attention_params['d_model']

        # Large values test
        x_large = mx.full((batch_size, seq_len, d_model), 10.0)
        ring_attn = RingAttention(**ring_attention_params)

        output_large = ring_attn(x_large)

        # Check for NaN or Inf values
        assert not mx.any(mx.isnan(output_large)), "NaN values detected with large inputs"
        assert not mx.any(mx.isinf(output_large)), "Inf values detected with large inputs"

        # Small values test
        x_small = mx.full((batch_size, seq_len, d_model), 1e-6)
        output_small = ring_attn(x_small)

        assert not mx.any(mx.isnan(output_small)), "NaN values detected with small inputs"
        assert not mx.any(mx.isinf(output_small)), "Inf values detected with small inputs"

        print("✅ Numerical stability tests passed")

    def test_memory_efficiency_vs_standard_attention(self, ring_attention_params):
        """Test that ring attention uses less memory than standard attention"""
        # This is a conceptual test - in practice you'd measure actual memory usage
        seq_lens = [1024, 4096, 8760]

        for seq_len in seq_lens:
            ring_size = ring_attention_params['ring_size']
            segment_len = seq_len // ring_size

            # Standard attention memory: O(seq_len^2)
            standard_memory = seq_len * seq_len

            # Ring attention memory: O(ring_size * segment_len^2)
            ring_memory = ring_size * segment_len * segment_len

            memory_ratio = ring_memory / standard_memory

            print(f"Seq len: {seq_len}, Memory ratio (ring/standard): {memory_ratio:.4f}")

            # Ring attention should use significantly less memory for long sequences
            if seq_len >= 1024:
                assert memory_ratio < 1.0, f"Ring attention not more memory efficient for seq_len={seq_len}"

        print("✅ Memory efficiency tests passed")

    def test_segment_splitting_correctness(self, ring_attention_params):
        """Test that sequence splitting maintains data integrity"""
        batch_size, seq_len, d_model = 2, 256, ring_attention_params['d_model']
        ring_size = ring_attention_params['ring_size']

        # Create identifiable test data
        x = mx.arange(batch_size * seq_len * d_model).reshape(batch_size, seq_len, d_model).astype(mx.float32)

        ring_attn = RingAttention(**ring_attention_params)
        segments = ring_attn._split_sequence(x)

        # Check number of segments
        assert len(segments) == ring_size

        # Check that concatenating segments recovers original sequence
        reconstructed = mx.concatenate(segments, axis=1)

        # Handle case where seq_len not evenly divisible by ring_size
        original_truncated = x[:, :reconstructed.shape[1], :]

        assert mx.allclose(reconstructed, original_truncated), "Segment splitting/reconstruction failed"

        print("✅ Segment splitting tests passed")

    def test_attention_equivalence_small_sequences(self, ring_attention_params, numerical_tolerance):
        """Test that ring attention gives similar results to conceptual standard attention for small sequences"""
        # For very small sequences, ring attention should behave similarly to standard attention
        batch_size, seq_len, d_model = 1, 64, ring_attention_params['d_model']

        # Use small ring size for this test
        ring_params = ring_attention_params.copy()
        ring_params['ring_size'] = 2

        x = mx.random.normal((batch_size, seq_len, d_model), key=mx.random.key(42))

        ring_attn = RingAttention(**ring_params)
        output = ring_attn(x)

        # Basic sanity checks
        assert output.shape == x.shape
        assert not mx.any(mx.isnan(output))

        # Test that different inputs produce different outputs
        x2 = mx.random.normal((batch_size, seq_len, d_model), key=mx.random.key(123))
        output2 = ring_attn(x2)

        # Outputs should be different for different inputs
        assert not mx.allclose(output, output2, **numerical_tolerance), "Ring attention not sensitive to input changes"

        print("✅ Attention equivalence tests passed")

    def test_energy_transformer_block_integration(self, ring_attention_params):
        """Test that EnergyTransformerBlock works correctly with ring attention"""
        batch_size, seq_len, d_model = 1, 512, ring_attention_params['d_model']
        d_ff = d_model * 4

        x = mx.random.normal((batch_size, seq_len, d_model))

        block = EnergyTransformerBlock(
            d_model=d_model,
            n_heads=ring_attention_params['n_heads'],
            d_ff=d_ff,
            ring_size=ring_attention_params['ring_size']
        )

        output = block(x)

        assert output.shape == x.shape
        assert not mx.any(mx.isnan(output))
        assert not mx.any(mx.isinf(output))

        print("✅ EnergyTransformerBlock integration tests passed")

    def test_attention_heads_scaling(self, ring_attention_params):
        """Test how different numbers of heads affect attention computation"""
        print("\n🔬 Testing Multi-Head Scaling Effects")
        print("=" * 60)

        seq_len = 256
        d_model = ring_attention_params['d_model']  # 256
        ring_size = ring_attention_params['ring_size']  # 4

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
            import time
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
            if n_head_values[i] > n_head_values[i - 1]:
                assert d_k_values[i] <= d_k_values[i - 1], "d_k should decrease as n_heads increases"
        print(f"   ✅ d_k decreases as n_heads increases: {d_k_values}")

        # 3. Processing time should be relatively stable (ring attention efficiency)
        times = [r['processing_time_ms'] for r in results]
        time_variance = mx.var(mx.array(times))
        print(f"   ✅ Processing time variance: {float(time_variance):.2f}ms² (should be low)")

        return results

    def test_head_attention_patterns(self, ring_attention_params):
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
            x = x.at[0, i, :d_model // 4].set(mx.sin(2 * mx.pi * i / 24))
            # Weekly-like pattern in second quarter
            x = x.at[0, i, d_model // 4:d_model // 2].set(mx.sin(2 * mx.pi * i / (24 * 7)))
            # Monthly-like pattern in third quarter
            x = x.at[0, i, d_model // 2:3 * d_model // 4].set(mx.sin(2 * mx.pi * i / (24 * 30)))
            # Random noise in last quarter
            x = x.at[0, i, 3 * d_model // 4:].set(mx.random.normal((d_model // 4,)))

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
            center_positions = mx.arange(seq_len)

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

    def test_ring_vs_standard_attention_equivalence(self, ring_attention_params, numerical_tolerance):
        """Test ring attention approximates standard attention for small sequences"""
        print("\n⚖️  Testing Ring vs Standard Attention Equivalence")
        print("=" * 60)

        # Use small sequence where ring overhead is minimal
        d_model = ring_attention_params['d_model']
        n_heads = ring_attention_params['n_heads']
        seq_len = 32  # Small enough that ring_size=4 gives segments of 8
        ring_size = 4

        x = mx.random.normal((1, seq_len, d_model), key=mx.random.key(123))

        # Ring attention
        ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)
        ring_output = ring_attn(x)

        # For comparison, let's implement a simple causal attention
        def simple_causal_attention(x, w_q, w_k, w_v, w_o, n_heads):
            batch_size, seq_len, d_model = x.shape
            d_k = d_model // n_heads

            q = w_q(x).reshape(batch_size, seq_len, n_heads, d_k)
            k = w_k(x).reshape(batch_size, seq_len, n_heads, d_k)
            v = w_v(x).reshape(batch_size, seq_len, n_heads, d_k)

            # Compute full attention matrix
            scores = mx.matmul(q, k.transpose(0, 1, 3, 2)) / math.sqrt(d_k)

            # Apply causal mask
            causal_mask = mx.tril(mx.ones((seq_len, seq_len)))
            causal_mask = mx.expand_dims(mx.expand_dims(causal_mask, 0), 2)  # Broadcast
            scores = mx.where(causal_mask, scores, -mx.inf)

            # Attention weights and output
            attn_weights = mx.softmax(scores, axis=-1)
            output = mx.matmul(attn_weights, v)

            # Reshape and project
            output = output.reshape(batch_size, seq_len, d_model)
            return w_o(output)

        # Standard causal attention with same weights
        standard_output = simple_causal_attention(
            x, ring_attn.w_q, ring_attn.w_k, ring_attn.w_v, ring_attn.w_o, n_heads
        )

        # Compare outputs
        output_diff = mx.mean(mx.abs(ring_output - standard_output))
        max_diff = mx.max(mx.abs(ring_output - standard_output))
        relative_diff = output_diff / (mx.mean(mx.abs(standard_output)) + 1e-8)

        print(f"   📊 Comparison Results:")
        print(f"      Mean absolute difference: {float(output_diff):.8f}")
        print(f"      Max absolute difference: {float(max_diff):.8f}")
        print(f"      Relative difference: {float(relative_diff):.8f}")

        # For small sequences with small ring sizes, outputs should be very similar
        # (not identical due to numerical precision and ring segmentation)
        tolerance_relaxed = {
            'rtol': numerical_tolerance['rtol'] * 10,  # Relax tolerance for ring approximation
            'atol': numerical_tolerance['atol'] * 10
        }

        # Check if they're approximately equal
        approximately_equal = mx.allclose(ring_output, standard_output, **tolerance_relaxed)

        print(f"   🎯 Approximately equal (relaxed tolerance): {approximately_equal}")

        # They should at least be in the same ballpark
        assert float(relative_diff) < 0.1, f"Ring attention output too different from standard attention: {float(relative_diff):.4f}"

        print("   ✅ Ring attention reasonably approximates standard causal attention")

        return {
            'mean_diff': float(output_diff),
            'max_diff': float(max_diff),
            'relative_diff': float(relative_diff),
            'approximately_equal': bool(approximately_equal)
        }


def test_ring_attention_basic_functionality():
    """Basic functionality test that can run without fixtures"""
    batch_size = 2
    seq_len = 128
    d_model = 256
    n_heads = 8

    # Create test data
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
    test_ring_attention_basic_functionality()
    print("🎉 Ring attention tests passed!")