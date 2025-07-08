#!/usr/bin/env python3
"""
Test softmax with -inf values
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import mlx.core as mx

def test_softmax_inf():
    print("🧪 Testing Softmax with -inf Values")
    print("=" * 40)

    # Test case 1: Normal scores
    scores_normal = mx.array([[1.0, 2.0, 3.0], [0.5, 1.5, 2.5]])
    print(f"Normal scores: {scores_normal}")
    softmax_normal = mx.softmax(scores_normal, axis=-1)
    print(f"Normal softmax: {softmax_normal}")
    print(f"Normal has NaN: {mx.any(mx.isnan(softmax_normal))}")

    # Test case 2: Some -inf values
    scores_inf = mx.array([[1.0, -mx.inf, 3.0], [0.5, 1.5, -mx.inf]])
    print(f"\nScores with -inf: {scores_inf}")
    softmax_inf = mx.softmax(scores_inf, axis=-1)
    print(f"Softmax with -inf: {softmax_inf}")
    print(f"With -inf has NaN: {mx.any(mx.isnan(softmax_inf))}")

    # Test case 3: All -inf values (this might cause NaN)
    scores_all_inf = mx.array([[-mx.inf, -mx.inf, -mx.inf], [-mx.inf, -mx.inf, -mx.inf]])
    print(f"\nAll -inf scores: {scores_all_inf}")
    softmax_all_inf = mx.softmax(scores_all_inf, axis=-1)
    print(f"All -inf softmax: {softmax_all_inf}")
    print(f"All -inf has NaN: {mx.any(mx.isnan(softmax_all_inf))}")

if __name__ == "__main__":
    test_softmax_inf()
