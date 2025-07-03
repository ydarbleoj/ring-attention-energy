"""
Ring Attention Energy Optimization - Main Demo

This demo showcases the complete Ring Attention implementation with real energy data processing.

Features demonstrated:
1. Energy data collection and processing pipeline
2. Ring Attention processing of long energy time series (8760+ hours)
3. Memory efficiency compared to standard attention
4. Integration with realistic energy data patterns
5. Performance benchmarks and analysis

Usage:
    python src/main.py
"""

import asyncio
import mlx.core as mx
import numpy as np
import time
import logging
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from core.llms.ring_attention import RingAttention, EnergyTransformerBlock
from core.integrations.energy_data import EnergyDataConfig, EnergyDataPipeline


class RingAttentionEnergyDemo:
    """Complete demo of Ring Attention with energy data"""

    def __init__(self):
        self.config = EnergyDataConfig(
            sources=["synthetic"],
            sequence_length=8760,  # Full year of hourly data
            start_date="2022-01-01",
            end_date="2023-01-01",
            features=["demand", "solar_generation", "wind_generation",
                     "temperature", "price", "grid_stability"]
        )

        # Model configuration
        self.d_model = 128
        self.n_heads = 8
        self.ring_size = 4  # Number of ring segments

    async def load_energy_data(self):
        """Load and process energy data"""
        logger.info("🔋 Loading energy data...")

        pipeline = EnergyDataPipeline(self.config)
        sequences, metadata = await pipeline.run_full_pipeline()

        logger.info(f"✅ Loaded {metadata['num_sequences']} sequences")
        logger.info(f"📊 Data shape: {metadata['data_shape']}")
        logger.info(f"📈 Features: {metadata['feature_names']}")
        logger.info(f"📅 Time range: {metadata['time_range']}")

        return sequences, metadata

    def initialize_models(self, num_features):
        """Initialize Ring Attention and comparison models"""
        logger.info("🧠 Initializing models...")        # Ring Attention model
        self.ring_attention = RingAttention(
            d_model=self.d_model,
            n_heads=self.n_heads,
            ring_size=self.ring_size
        )

        # Energy Transformer Block
        self.energy_transformer = EnergyTransformerBlock(
            d_model=self.d_model,
            n_heads=self.n_heads,
            ring_size=self.ring_size,
            d_ff=self.d_model * 4
        )

        # Feature projection matrix
        self.feature_projection = mx.random.normal((num_features, self.d_model)) * 0.1

        logger.info(f"✅ Models initialized with d_model={self.d_model}, n_heads={self.n_heads}")

    def project_energy_features(self, energy_data):
        """Project energy features to model dimension"""
        # Take first sequence for demo
        sequence = energy_data[0]  # Shape: (seq_len, num_features)

        # Project to model dimension
        projected = mx.matmul(sequence, self.feature_projection)

        # Add batch dimension for Ring Attention
        projected = mx.expand_dims(projected, axis=0)  # Shape: (1, seq_len, d_model)

        return projected

    def run_ring_attention_demo(self, input_data):
        """Demonstrate Ring Attention processing"""
        logger.info("⚡ Running Ring Attention demo...")

        seq_len = input_data.shape[1]  # Now it's (batch, seq_len, d_model)

        # Time Ring Attention
        start_time = time.time()
        ring_output = self.ring_attention(input_data)
        ring_time = time.time() - start_time

        logger.info(f"✅ Ring Attention processed {seq_len} timesteps in {ring_time:.3f}s")
        logger.info(f"📊 Output shape: {ring_output.shape}")

        # Test causal masking
        self.verify_causal_masking(input_data, ring_output)

        return ring_output, ring_time

    def verify_causal_masking(self, input_data, output):
        """Verify that causal masking is working correctly"""
        logger.info("🔒 Verifying causal masking...")

        # The output at position t should only depend on inputs up to position t
        # We can test this by modifying future inputs and checking output doesn't change

        seq_len = input_data.shape[1]  # (batch, seq_len, d_model)
        test_position = seq_len // 2

        # Get output at test position
        original_output = output[0, test_position]  # Remove batch dimension

        # Modify future inputs (should not affect output at test position)
        modified_input = input_data.copy()
        modified_input[0, test_position + 1:] = mx.random.normal(modified_input[0, test_position + 1:].shape)

        # Recompute output
        modified_output = self.ring_attention(modified_input)
        modified_test_output = modified_output[0, test_position]

        # Outputs should be very similar (within numerical precision)
        diff = mx.mean(mx.abs(original_output - modified_test_output))

        if diff < 1e-5:
            logger.info("✅ Causal masking verified - future inputs don't affect past outputs")
        else:
            logger.warning(f"⚠️ Causal masking may have issues - difference: {diff}")

    def run_energy_transformer_demo(self, input_data):
        """Demonstrate Energy Transformer Block"""
        logger.info("🏭 Running Energy Transformer Block demo...")

        start_time = time.time()
        transformer_output = self.energy_transformer(input_data)
        transformer_time = time.time() - start_time

        logger.info(f"✅ Energy Transformer processed sequence in {transformer_time:.3f}s")
        logger.info(f"📊 Output shape: {transformer_output.shape}")

        return transformer_output, transformer_time    def memory_efficiency_analysis(self, input_data):
        """Analyze memory efficiency compared to standard attention"""
        logger.info("💾 Running memory efficiency analysis...")

        seq_len = input_data.shape[1]  # (batch, seq_len, d_model)
        d_model = input_data.shape[2]

        # Ring Attention memory (linear in sequence length)
        ring_memory = seq_len * d_model * 4  # Approximate bytes

        # Standard attention memory (quadratic in sequence length)
        standard_memory = seq_len * seq_len * 4  # Attention matrix

        memory_ratio = standard_memory / ring_memory

        logger.info(f"📈 Sequence length: {seq_len}")
        logger.info(f"💾 Ring Attention memory: ~{ring_memory / 1024**2:.1f} MB")
        logger.info(f"💾 Standard Attention memory: ~{standard_memory / 1024**2:.1f} MB")
        logger.info(f"⚡ Memory efficiency: {memory_ratio:.1f}x better")

        return ring_memory, standard_memory, memory_ratio

    def analyze_attention_patterns(self, input_data):
        """Analyze what the attention heads are learning"""
        logger.info("🔍 Analyzing attention patterns...")

        # Get attention weights from each head
        seq_len = input_data.shape[0]

        # Since Ring Attention processes segments, let's analyze a segment
        segment_start = 0
        segment_end = min(seq_len // self.ring_size, seq_len)
        segment_input = input_data[segment_start:segment_end]

        # Run through Ring Attention and capture intermediate states
        output = self.ring_attention(segment_input)

        # For demo purposes, we'll analyze the output patterns
        # In a full implementation, you'd capture attention weights

        # Analyze temporal dependencies
        output_diff = mx.diff(output, axis=0)
        temporal_complexity = mx.mean(mx.abs(output_diff))

        logger.info(f"🕐 Temporal complexity score: {temporal_complexity:.6f}")
        logger.info("💡 Lower scores indicate smoother temporal patterns")

        return temporal_complexity

    def energy_pattern_analysis(self, energy_data, metadata):
        """Analyze energy-specific patterns in the data"""
        logger.info("⚡ Analyzing energy patterns...")

        # Take first sequence for analysis
        sequence = energy_data[0]  # Shape: (seq_len, num_features)
        feature_names = metadata['feature_names']

        logger.info(f"📊 Analyzing {len(feature_names)} energy features over {sequence.shape[0]} hours")

        # Compute basic statistics
        means = mx.mean(sequence, axis=0)
        stds = mx.std(sequence, axis=0)

        for i, feature in enumerate(feature_names):
            logger.info(f"📈 {feature}: mean={means[i]:.3f}, std={stds[i]:.3f}")

        # Analyze correlations between features (simplified)
        if len(feature_names) >= 2:
            demand_idx = feature_names.index('demand') if 'demand' in feature_names else 0
            price_idx = feature_names.index('price') if 'price' in feature_names else 1

            demand_vals = sequence[:, demand_idx]
            price_vals = sequence[:, price_idx]

            # Compute correlation (simplified)
            demand_norm = (demand_vals - mx.mean(demand_vals)) / mx.std(demand_vals)
            price_norm = (price_vals - mx.mean(price_vals)) / mx.std(price_vals)
            correlation = mx.mean(demand_norm * price_norm)

            logger.info(f"🔗 Demand-Price correlation: {correlation:.3f}")

    def performance_benchmarks(self, input_data):
        """Run comprehensive performance benchmarks"""
        logger.info("🏃 Running performance benchmarks...")

        seq_len = input_data.shape[0]

        # Benchmark Ring Attention
        ring_times = []
        for _ in range(5):
            start_time = time.time()
            _ = self.ring_attention(input_data)
            ring_times.append(time.time() - start_time)

        avg_ring_time = np.mean(ring_times)
        std_ring_time = np.std(ring_times)

        # Benchmark Energy Transformer
        transformer_times = []
        for _ in range(5):
            start_time = time.time()
            _ = self.energy_transformer(input_data)
            transformer_times.append(time.time() - start_time)

        avg_transformer_time = np.mean(transformer_times)
        std_transformer_time = np.std(transformer_times)

        # Throughput calculations
        ring_throughput = seq_len / avg_ring_time
        transformer_throughput = seq_len / avg_transformer_time

        logger.info("🎯 Performance Results:")
        logger.info(f"⚡ Ring Attention: {avg_ring_time:.3f}±{std_ring_time:.3f}s ({ring_throughput:.0f} timesteps/s)")
        logger.info(f"🏭 Energy Transformer: {avg_transformer_time:.3f}±{std_transformer_time:.3f}s ({transformer_throughput:.0f} timesteps/s)")

        return {
            'ring_attention': {'time': avg_ring_time, 'throughput': ring_throughput},
            'energy_transformer': {'time': avg_transformer_time, 'throughput': transformer_throughput}
        }

    async def run_complete_demo(self):
        """Run the complete Ring Attention energy demo"""
        logger.info("🚀 Starting Ring Attention Energy Demo")
        logger.info("="*60)

        try:
            # 1. Load energy data
            energy_data, metadata = await self.load_energy_data()

            # 2. Initialize models
            num_features = len(metadata['feature_names'])
            self.initialize_models(num_features)

            # 3. Project energy features to model space
            input_data = self.project_energy_features(energy_data)
            logger.info(f"🔄 Projected to model space: {input_data.shape}")

            # 4. Run Ring Attention demo
            ring_output, ring_time = self.run_ring_attention_demo(input_data)

            # 5. Run Energy Transformer demo
            transformer_output, transformer_time = self.run_energy_transformer_demo(input_data)

            # 6. Memory efficiency analysis
            self.memory_efficiency_analysis(input_data)

            # 7. Analyze attention patterns
            self.analyze_attention_patterns(input_data)

            # 8. Energy-specific pattern analysis
            self.energy_pattern_analysis(energy_data, metadata)

            # 9. Performance benchmarks
            performance = self.performance_benchmarks(input_data)

            # 10. Summary
            logger.info("="*60)
            logger.info("🎉 Demo Summary:")
            logger.info(f"✅ Processed {input_data.shape[0]} hours of energy data")
            logger.info(f"⚡ Ring Attention: {ring_time:.3f}s")
            logger.info(f"🏭 Energy Transformer: {transformer_time:.3f}s")
            logger.info(f"💾 Memory efficiency: ~{input_data.shape[0]**2 / (input_data.shape[0] * input_data.shape[1]):.0f}x better than standard attention")
            logger.info("🚀 Ring Attention successfully demonstrated for energy time series!")

            return {
                'energy_data': energy_data,
                'metadata': metadata,
                'ring_output': ring_output,
                'transformer_output': transformer_output,
                'performance': performance
            }

        except Exception as e:
            logger.error(f"❌ Demo failed: {e}")
            raise


async def main():
    """Main entry point"""
    demo = RingAttentionEnergyDemo()
    results = await demo.run_complete_demo()

    print("\n" + "="*60)
    print("🎯 Ring Attention Energy Demo Complete!")
    print("="*60)
    print(f"📊 Data processed: {results['metadata']['data_shape']}")
    print(f"🕐 Features: {results['metadata']['feature_names']}")
    print(f"⚡ Ready for energy optimization applications!")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
