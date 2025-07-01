#!/usr/bin/env python3
"""
Debug ring attention shapes
"""
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import mlx.core as mx
from src.core.llms.ring_attention import RingAttention

def debug_shapes():
    print("🔍 Debugging Ring Attention Shapes")
    print("=" * 50)

    batch_size = 2
    seq_len = 128
    d_model = 256
    n_heads = 8
    ring_size = 4

    x = mx.random.normal((batch_size, seq_len, d_model))
    print(f"Input shape: {x.shape}")

    ring_attn = RingAttention(d_model, n_heads, ring_size)

    try:
        output = ring_attn(x)
        print(f"✅ Success! Output shape: {output.shape}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    debug_shapes()
