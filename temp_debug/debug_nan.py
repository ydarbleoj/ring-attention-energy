#!/usr/bin/env python3
"""
Debug NaN values in ring attention
"""
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import mlx.core as mx
from src.core.llms.ring_attention import RingAttention

def debug_nan_values():
    print("🔍 Debugging NaN Values in Ring Attention")
    print("=" * 50)

    batch_size = 1
    seq_len = 32  # Small for debugging
    d_model = 64
    n_heads = 4
    ring_size = 2

    x = mx.random.normal((batch_size, seq_len, d_model))
    print(f"Input shape: {x.shape}")
    print(f"Input has NaN: {mx.any(mx.isnan(x))}")
    print(f"Input stats: min={mx.min(x):.4f}, max={mx.max(x):.4f}, mean={mx.mean(x):.4f}")

    ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

    # Check each step
    print("\n📊 Checking Q, K, V projections:")
    q = ring_attn.w_q(x)
    k = ring_attn.w_k(x)
    v = ring_attn.w_v(x)

    print(f"Q has NaN: {mx.any(mx.isnan(q))}, stats: min={mx.min(q):.4f}, max={mx.max(q):.4f}")
    print(f"K has NaN: {mx.any(mx.isnan(k))}, stats: min={mx.min(k):.4f}, max={mx.max(k):.4f}")
    print(f"V has NaN: {mx.any(mx.isnan(v))}, stats: min={mx.min(v):.4f}, max={mx.max(v):.4f}")

    # Check reshaping
    print("\n🔄 Checking reshaping:")
    q_reshaped = q.reshape(batch_size, seq_len, n_heads, d_model // n_heads)
    k_reshaped = k.reshape(batch_size, seq_len, n_heads, d_model // n_heads)
    v_reshaped = v.reshape(batch_size, seq_len, n_heads, d_model // n_heads)

    print(f"Q reshaped shape: {q_reshaped.shape}")
    print(f"Q reshaped has NaN: {mx.any(mx.isnan(q_reshaped))}")

    # Try the full forward pass
    try:
        output = ring_attn(x)
        print(f"\n✅ Output shape: {output.shape}")
        print(f"Output has NaN: {mx.any(mx.isnan(output))}")
        print(f"Output has Inf: {mx.any(mx.isinf(output))}")
        if mx.any(mx.isnan(output)) or mx.any(mx.isinf(output)):
            print(f"Output stats: min={mx.min(output):.4f}, max={mx.max(output):.4f}")
    except Exception as e:
        print(f"❌ Forward pass failed: {e}")

if __name__ == "__main__":
    debug_nan_values()
