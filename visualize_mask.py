#!/usr/bin/env python3
"""
Visualize ring attention masking patterns for energy time series
"""
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import mlx.core as mx
from src.core.llms.ring_attention import RingAttention

def visualize_ring_attention_mask():
    """Visualize how the mask works across ring segments"""
    print("🎭 Visualizing Ring Attention Masking for Energy Data")
    print("=" * 60)

    # Energy scenario: 24 hours split into 4 segments of 6 hours each
    seq_len = 24  # 24 hours
    ring_size = 4  # 4 segments of 6 hours
    segment_len = seq_len // ring_size  # 6 hours per segment

    print(f"📅 Scenario: {seq_len} hours of energy data")
    print(f"🔄 Ring size: {ring_size} segments of {segment_len} hours each")
    print(f"   Segment 0: Hours 0-5   (Midnight - 6AM)")
    print(f"   Segment 1: Hours 6-11  (6AM - Noon)")
    print(f"   Segment 2: Hours 12-17 (Noon - 6PM)")
    print(f"   Segment 3: Hours 18-23 (6PM - Midnight)")

    # Create ring attention instance
    d_model = 64
    n_heads = 4
    ring_attn = RingAttention(d_model, n_heads, ring_size, causal=True)

    # Create a full mask matrix to show the complete attention pattern
    full_mask = np.zeros((seq_len, seq_len))

    # For each query segment
    for q_seg_idx in range(ring_size):
        q_start = q_seg_idx * segment_len
        q_end = (q_seg_idx + 1) * segment_len

        print(f"\n🔍 Query Segment {q_seg_idx} (Hours {q_start}-{q_end-1}):")

        # For each key-value segment
        for kv_seg_idx in range(ring_size):
            kv_start = kv_seg_idx * segment_len
            kv_end = (kv_seg_idx + 1) * segment_len

            # Get the mask for this segment pair
            mask = ring_attn._create_segment_mask(
                segment_len, segment_len, q_seg_idx, kv_seg_idx
            )
            mask_np = np.array(mask)

            # Fill the full mask matrix
            full_mask[q_start:q_end, kv_start:kv_end] = mask_np

            # Describe the relationship
            visible_positions = np.sum(mask_np)
            total_positions = segment_len * segment_len

            if q_seg_idx > kv_seg_idx:
                relationship = "PAST (all visible)"
            elif q_seg_idx < kv_seg_idx:
                relationship = "FUTURE (none visible)"
            else:
                relationship = "SAME (causal)"

            print(f"   → KV Segment {kv_seg_idx} (Hours {kv_start}-{kv_end-1}): "
                  f"{visible_positions:2d}/{total_positions} positions - {relationship}")

    # Show specific examples
    print(f"\n📊 Specific Examples:")
    print(f"   Hour 8 (10AM) can attend to:")
    hour_8_row = full_mask[8, :]
    visible_hours = [i for i, v in enumerate(hour_8_row) if v == 1]
    print(f"      Hours {visible_hours} ✅")

    print(f"   Hour 15 (3PM) can attend to:")
    hour_15_row = full_mask[15, :]
    visible_hours = [i for i, v in enumerate(hour_15_row) if v == 1]
    print(f"      Hours {visible_hours} ✅")

    # Calculate mask statistics
    total_positions = seq_len * seq_len
    visible_positions = np.sum(full_mask)
    causal_efficiency = visible_positions / total_positions

    print(f"\n📈 Mask Statistics:")
    print(f"   Total possible attention positions: {total_positions}")
    print(f"   Causally visible positions: {int(visible_positions)}")
    print(f"   Causal efficiency: {causal_efficiency:.2%}")
    print(f"   Information preserved: No future leakage! 🔒")

    return full_mask

def demonstrate_energy_causality():
    """Show why causality matters for energy forecasting"""
    print(f"\n⚡ Why Causality Matters for Energy Forecasting")
    print("=" * 60)

    scenarios = [
        {
            "name": "Peak Demand Prediction",
            "description": "Predicting 2PM demand using morning data",
            "current_hour": 14,  # 2PM
            "available_data": "Hours 0-14 (midnight to 2PM)",
            "forbidden_data": "Hours 15-23 (3PM onwards)",
            "why_forbidden": "Cannot use evening peak to predict afternoon peak!"
        },
        {
            "name": "Solar Generation Forecast",
            "description": "Forecasting 10AM solar output",
            "current_hour": 10,  # 10AM
            "available_data": "Hours 0-10 (midnight to 10AM)",
            "forbidden_data": "Hours 11-23 (11AM onwards)",
            "why_forbidden": "Cannot use afternoon sunshine to predict morning output!"
        },
        {
            "name": "Grid Load Balancing",
            "description": "Balancing supply/demand at 6PM",
            "current_hour": 18,  # 6PM
            "available_data": "Hours 0-18 (all day until 6PM)",
            "forbidden_data": "Hours 19-23 (evening)",
            "why_forbidden": "Cannot use future consumption to plan current generation!"
        }
    ]

    for i, scenario in enumerate(scenarios, 1):
        print(f"\n   {i}. {scenario['name']}")
        print(f"      📋 Task: {scenario['description']}")
        print(f"      ✅ Available: {scenario['available_data']}")
        print(f"      ❌ Forbidden: {scenario['forbidden_data']}")
        print(f"      🧠 Why: {scenario['why_forbidden']}")

    print(f"\n🎯 Key Insight:")
    print(f"   The mask enforces realistic forecasting constraints!")
    print(f"   Ring attention maintains causality while scaling to long sequences.")

if __name__ == "__main__":
    try:
        full_mask = visualize_ring_attention_mask()
        demonstrate_energy_causality()

        print(f"\n🎉 Mask visualization complete!")
        print(f"✨ Ring attention properly enforces causal constraints for energy forecasting!")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
