import mlx.core as mx
import mlx.nn as nn
import math
from typing import Optional, Tuple, List
import logging

class RingAttention(nn.Module):
    """
    Ring Attention implementation optimized for MLX

    Key insight: Instead of computing full N x N attention matrix,
    split sequence across devices and pass KV states in a ring
    """

    def __init__(
        self,
        d_model: int,
        n_heads: int,
        ring_size: int = 4,
        segment_size: Optional[int] = None,
        causal: bool = True
    ):
        super().__init__()
        self.d_model = d_model
        self.n_heads = n_heads
        self.ring_size = ring_size
        self.causal = causal
        self.d_k = d_model // n_heads

        # Projection layers
        self.w_q = nn.Linear(d_model, d_model, bias=False)
        self.w_k = nn.Linear(d_model, d_model, bias=False)
        self.w_v = nn.Linear(d_model, d_model, bias=False)
        self.w_o = nn.Linear(d_model, d_model, bias=False)

        # For energy time series, we'll often want fixed segment sizes
        self.segment_size = segment_size

    def _split_sequence(self, x: mx.array) -> List[mx.array]:
        """Split sequence into ring segments"""
        logging.info(f"Splitting sequence of shape {x.shape} into {self.ring_size} segments")
        if len(x.shape) == 3:
            batch_size, seq_len, d_model = x.shape
        elif len(x.shape) == 4:
            batch_size, seq_len, n_heads, d_k = x.shape
        else:
            raise ValueError(f"Expected 3D or 4D tensor, got {len(x.shape)}D tensor with shape {x.shape}")

        if self.segment_size:
            segment_len = self.segment_size
        else:
            segment_len = seq_len // self.ring_size

        segments = []
        for i in range(self.ring_size):
            start_idx = i * segment_len
            end_idx = min((i + 1) * segment_len, seq_len)
            segments.append(x[:, start_idx:end_idx])

        return segments

    def _ring_attention_step(
        self,
        q_local: mx.array,
        k_remote: mx.array,
        v_remote: mx.array,
        segment_idx: int,
        remote_idx: int
    ) -> Tuple[mx.array, mx.array]:
        """
        Single step of ring attention between local queries and remote K,V
        This is where your distributed systems thinking really applies!
        """
        batch_size, q_len, n_heads, d_k = q_local.shape
        _, kv_len, _, _ = k_remote.shape

        # Debug: print shapes to understand the issue
        logging.info(f"Ring attention step - q_local: {q_local.shape}, k_remote: {k_remote.shape}, v_remote: {v_remote.shape}")

        # Compute attention scores
        # q_local: (batch, q_len, n_heads, d_k)
        # k_remote: (batch, kv_len, n_heads, d_k)
        # We want: (batch, n_heads, q_len, kv_len)

        # Rearrange for efficient computation
        q_reshaped = q_local.transpose(0, 2, 1, 3)  # (batch, n_heads, q_len, d_k)
        k_reshaped = k_remote.transpose(0, 2, 1, 3)  # (batch, n_heads, kv_len, d_k)
        v_reshaped = v_remote.transpose(0, 2, 1, 3)  # (batch, n_heads, kv_len, d_k)

        # Compute attention scores: (batch, n_heads, q_len, d_k) @ (batch, n_heads, d_k, kv_len)
        scores = mx.matmul(q_reshaped, k_reshaped.transpose(0, 1, 3, 2))  # (batch, n_heads, q_len, kv_len)
        scores = scores / math.sqrt(d_k)

        logging.info(f"Scores shape: {scores.shape}")        # Apply causal mask if needed
        if self.causal and segment_idx <= remote_idx:
            # Create causal mask for this segment pair
            mask = self._create_segment_mask(q_len, kv_len, segment_idx, remote_idx)
            logging.info(f"Base mask shape: {mask.shape}")

            # Expand mask to match scores dimensions
            # scores shape: (batch, n_heads, q_len, kv_len)
            # mask shape: (q_len, kv_len) -> (1, 1, q_len, kv_len)
            mask = mask[None, None, :, :]  # Add batch and head dimensions
            logging.info(f"Expanded mask shape: {mask.shape}")
            logging.info(f"Target scores shape: {scores.shape}")

            mask = mx.broadcast_to(mask, scores.shape)  # Broadcast to full shape

            # Check if mask is all False (would cause all -inf and NaN in softmax)
            if not mx.any(mask):
                # If no positions are visible, skip this attention step
                # Return zeros with the right shape
                output_shape = (batch_size, q_len, n_heads, d_k)
                output = mx.zeros(output_shape)
                attn_weights = mx.zeros(scores.shape)
                return output, attn_weights

            scores = mx.where(mask, scores, -mx.inf)

        # Softmax and apply to values
        attn_weights = mx.softmax(scores, axis=-1)  # (batch, n_heads, q_len, kv_len)
        output = mx.matmul(attn_weights, v_reshaped)  # (batch, n_heads, q_len, d_k)

        # Transpose back to (batch, q_len, n_heads, d_k) to match expected output format
        output = output.transpose(0, 2, 1, 3)

        return output, attn_weights

    def _create_segment_mask(
        self,
        q_len: int,
        kv_len: int,
        q_segment_idx: int,
        kv_segment_idx: int
    ) -> mx.array:
        """Create causal mask for segment pairs"""
        if q_segment_idx > kv_segment_idx:
            # Query segment is after KV segment - all positions visible
            return mx.ones((q_len, kv_len), dtype=mx.bool_)
        elif q_segment_idx < kv_segment_idx:
            # Query segment is before KV segment - no positions visible
            return mx.zeros((q_len, kv_len), dtype=mx.bool_)
        else:
            # Same segment - standard causal mask
            mask = mx.tril(mx.ones((q_len, kv_len)))
            return mask.astype(mx.bool_)

    def __call__(self, x: mx.array, mask: Optional[mx.array] = None) -> mx.array:
        """
        Forward pass implementing ring attention
        Perfect for long energy time series!
        """
        batch_size, seq_len, d_model = x.shape

        # Project to Q, K, V
        q = self.w_q(x)  # [batch, seq_len, d_model]
        k = self.w_k(x)
        v = self.w_v(x)

        # Reshape for multi-head attention
        q = q.reshape(batch_size, seq_len, self.n_heads, self.d_k)
        k = k.reshape(batch_size, seq_len, self.n_heads, self.d_k)
        v = v.reshape(batch_size, seq_len, self.n_heads, self.d_k)

        # Split into ring segments
        q_segments = self._split_sequence(q)
        k_segments = self._split_sequence(k)
        v_segments = self._split_sequence(v)

        # Initialize output segments
        output_segments = []

        # For each query segment, attend to all key-value segments
        for i, q_seg in enumerate(q_segments):
            segment_outputs = []

            # Ring communication pattern
            for j in range(self.ring_size):
                k_seg = k_segments[j]
                v_seg = v_segments[j]

                output, _ = self._ring_attention_step(q_seg, k_seg, v_seg, i, j)
                segment_outputs.append(output)

            # Combine outputs from all KV segments for this Q segment
            combined_output = mx.sum(mx.stack(segment_outputs, axis=0), axis=0)
            output_segments.append(combined_output)

        # Concatenate all segments back together
        full_output = mx.concatenate(output_segments, axis=1)

        # Reshape and project
        full_output = full_output.reshape(batch_size, seq_len, d_model)
        return self.w_o(full_output)


class EnergyTransformerBlock(nn.Module):
    """Transformer block optimized for energy time series data"""

    def __init__(
        self,
        d_model: int,
        n_heads: int,
        d_ff: int,
        ring_size: int = 4,
        dropout_rate: float = 0.1
    ):
        super().__init__()
        self.ring_attention = RingAttention(d_model, n_heads, ring_size)
        self.feed_forward = nn.Sequential(
            nn.Linear(d_model, d_ff),
            nn.GELU(),
            nn.Linear(d_ff, d_model)
        )
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)

    def __call__(self, x: mx.array) -> mx.array:
        # Ring attention with residual connection
        attn_out = self.ring_attention(self.norm1(x))
        x = x + attn_out

        # Feed forward with residual connection
        ff_out = self.feed_forward(self.norm2(x))
        x = x + ff_out

        return x