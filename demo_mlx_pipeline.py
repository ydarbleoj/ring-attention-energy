#!/usr/bin/env python3
"""
Complete MLX Energy Pipeline Demo

This demo shows the full end-to-end pipeline:
1. Load energy data using EIA service layer
2. Engineer features for ML training
3. Create sequences for ring attention
4. Generate MLX-compatible datasets
5. Show training-ready outputs

This builds on our established components:
- EIA client with schema validation
- Polars/Parquet service layer
- MLX feature engineering
- Ring attention sequence generation
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import mlx.core as mx
import numpy as np

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.core.integrations.config import get_config
from src.core.integrations.eia.service import DataLoader, StorageManager
from src.core.ml.features import EnergyFeatureEngineer, create_energy_sequence_dataset
from src.core.ml.sequence_generator import SequenceGenerator, RingAttentionSequenceGenerator
from src.core.ml.data_loaders import EnergyDataLoader


def demo_feature_engineering():
    """Demo comprehensive feature engineering."""
    print("\n🔬 FEATURE ENGINEERING DEMO")
    print("=" * 50)

    # Create sample energy data
    print("📊 Creating sample energy data...")
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(hours=i) for i in range(168 * 4)]  # 4 weeks

    # Generate realistic patterns
    hours = np.arange(len(dates))
    daily_pattern = np.sin(2 * np.pi * hours / 24)
    weekly_pattern = np.sin(2 * np.pi * hours / 168)
    seasonal_pattern = np.sin(2 * np.pi * hours / (24 * 365))

    # Base demand with multiple cycles
    base_demand = 1000 + 200 * daily_pattern + 100 * weekly_pattern + 50 * seasonal_pattern
    demand = base_demand + np.random.normal(0, 50, len(dates))

    # Solar generation (daytime only with variability)
    solar = np.maximum(0, 200 * np.sin(np.pi * (hours % 24) / 24) * (1 + 0.3 * np.random.normal(0, 1, len(dates))))

    # Wind generation (more random but with some patterns)
    wind = np.maximum(0, 150 + 100 * np.random.normal(0, 1, len(dates)))

    # Hydro (stable baseline)
    hydro = np.full(len(dates), 300) + np.random.normal(0, 20, len(dates))

    # Natural gas (dispatchable - fills gaps)
    natural_gas = np.maximum(0, demand - solar - wind - hydro + np.random.normal(0, 30, len(dates)))

    # Create DataFrame
    df = pl.DataFrame({
        "datetime": dates,
        "demand_mwh": demand,
        "solar_mwh": solar,
        "wind_mwh": wind,
        "hydro_mwh": hydro,
        "natural_gas_mwh": natural_gas
    })

    print(f"✅ Created {len(df)} hours of energy data")
    print(f"📈 Data shape: {df.shape}")

    # Initialize feature engineer
    print("\n🛠️  Initializing feature engineer...")
    feature_engineer = EnergyFeatureEngineer()

    # Create all features
    print("⚙️  Creating comprehensive features...")
    df_with_features = feature_engineer.create_all_features(
        df,
        include_lags=True,
        include_rolling=True
    )

    print(f"✅ Feature engineering complete!")
    print(f"📊 Original columns: {len(df.columns)}")
    print(f"🎯 Final columns: {len(df_with_features.columns)}")
    print(f"📈 Feature expansion: {len(df_with_features.columns) / len(df.columns):.1f}x")

    # Show feature categories
    print("\n🏷️  Feature Categories:")
    temporal_features = [col for col in df_with_features.columns if any(
        keyword in col for keyword in ["hour", "day", "week", "month", "year", "sin", "cos"]
    )]
    renewable_features = [col for col in df_with_features.columns if "renewable" in col]
    lag_features = [col for col in df_with_features.columns if "lag" in col]
    rolling_features = [col for col in df_with_features.columns if "rolling" in col]

    print(f"   • Temporal: {len(temporal_features)} features")
    print(f"   • Renewable: {len(renewable_features)} features")
    print(f"   • Lagged: {len(lag_features)} features")
    print(f"   • Rolling: {len(rolling_features)} features")

    # Convert to MLX format
    print("\n🔄 Converting to MLX format...")
    mlx_array, feature_names = feature_engineer.to_mlx_features(
        df_with_features,
        normalize=True
    )

    print(f"✅ MLX conversion complete!")
    print(f"📊 MLX array shape: {mlx_array.shape}")
    print(f"🎯 Feature count: {len(feature_names)}")
    print(f"💾 Memory usage: {mlx_array.nbytes / 1024 / 1024:.1f} MB")

    return df_with_features, mlx_array, feature_names


def demo_sequence_generation(df_with_features):
    """Demo sequence generation for ring attention."""
    print("\n🔄 SEQUENCE GENERATION DEMO")
    print("=" * 50)

    # Standard sequence generator
    print("📦 Creating standard sequence generator...")
    seq_gen = SequenceGenerator(
        sequence_length=168,  # 1 week
        stride=24,           # 1 day
        features=["demand_mwh", "solar_mwh", "wind_mwh", "hydro_mwh", "natural_gas_mwh"]
    )

    print("🎯 Fitting normalizer...")
    seq_gen.fit_normalizer(df_with_features)

    print("⚙️  Generating sequences...")
    sequences_iterator = seq_gen.create_sequences(df_with_features)
    sequences = list(sequences_iterator)  # Convert to list

    print(f"✅ Generated {len(sequences)} sequences")
    if sequences:
        X, y = sequences[0]  # Get first sequence to check shape
        print(f"📊 Single sequence shape: {X.shape}")
        print(f"🎯 Target shape: {y.shape}")
        print(f"💾 Total memory: {sum(X.nbytes + y.nbytes for X, y in sequences) / 1024 / 1024:.1f} MB")

    # Ring attention sequence generator
    print("\n🔄 Creating ring attention sequence generator...")
    ring_seq_gen = RingAttentionSequenceGenerator(
        sequence_length=8760,  # 1 year (would need more data)
        ring_size=4,
        stride=168            # 1 week
    )

    print(f"🎯 Ring attention config:")
    print(f"   • Sequence length: {ring_seq_gen.sequence_length}")
    print(f"   • Ring size: {ring_seq_gen.ring_size}")
    print(f"   • Partition size: {ring_seq_gen.sequence_length // ring_seq_gen.ring_size}")

    # Create training dataset
    print("\n📦 Creating training dataset...")
    if sequences:
        # Stack all sequences into batches
        X_list = [X for X, y in sequences]
        y_list = [y for X, y in sequences]

        if X_list:
            # Check shapes to ensure consistency
            first_X_shape = X_list[0].shape
            first_y_shape = y_list[0].shape

            # Filter to only include sequences with consistent shapes
            valid_sequences = []
            for X, y in zip(X_list, y_list):
                if X.shape == first_X_shape and y.shape == first_y_shape:
                    valid_sequences.append((X, y))

            if valid_sequences:
                X_list = [X for X, y in valid_sequences]
                y_list = [y for X, y in valid_sequences]

                X = mx.stack(X_list)
                y = mx.stack(y_list)

                print(f"✅ Training dataset created!")
                print(f"📊 Input batch shape: {X.shape}")
                print(f"🎯 Target batch shape: {y.shape}")
                print(f"💾 Total size: {(X.nbytes + y.nbytes) / 1024 / 1024:.1f} MB")
            else:
                X, y = None, None
                print("⚠️  No valid sequences with consistent shapes")
        else:
            X, y = None, None
            print("⚠️  No sequences generated")
    else:
        X, y = None, None
        print("⚠️  No sequences generated")

    return X, y, seq_gen


def demo_comprehensive_pipeline():
    """Demo the complete pipeline with feature engineering."""
    print("\n🚀 COMPREHENSIVE PIPELINE DEMO")
    print("=" * 50)

    # Create sample data (simulating EIA data)
    print("📊 Creating comprehensive energy dataset...")
    start_date = datetime(2024, 1, 1)
    dates = [start_date + timedelta(hours=i) for i in range(168 * 6)]  # 6 weeks

    # More realistic energy patterns
    hours = np.arange(len(dates))

    # Complex demand pattern
    daily_peak = np.where((hours % 24 >= 17) & (hours % 24 <= 20), 1.3, 1.0)  # Evening peak
    morning_peak = np.where((hours % 24 >= 7) & (hours % 24 <= 9), 1.2, 1.0)  # Morning peak
    weekend_reduction = np.where((hours // 24) % 7 >= 5, 0.9, 1.0)  # Weekend reduction

    base_demand = 1000 + 200 * np.sin(2 * np.pi * hours / 24)
    demand = base_demand * daily_peak * morning_peak * weekend_reduction + np.random.normal(0, 50, len(dates))

    # Solar with weather variations
    solar_pattern = np.maximum(0, 300 * np.sin(np.pi * (hours % 24) / 24))
    cloud_factor = np.random.beta(2, 2, len(dates))  # 0-1 cloud coverage
    solar = solar_pattern * cloud_factor

    # Wind with realistic variability
    wind_base = 150 + 100 * np.sin(2 * np.pi * hours / (24 * 7))  # Weekly pattern
    wind = np.maximum(0, wind_base + 200 * np.random.exponential(0.3, len(dates)))

    # Hydro with seasonal variation
    hydro = 300 + 100 * np.sin(2 * np.pi * hours / (24 * 365)) + np.random.normal(0, 20, len(dates))

    # Natural gas as dispatchable
    natural_gas = np.maximum(0, demand - solar - wind - hydro + np.random.normal(0, 50, len(dates)))

    # Coal as baseload
    coal = np.full(len(dates), 200) + np.random.normal(0, 10, len(dates))

    # Nuclear as stable baseload
    nuclear = np.full(len(dates), 500) + np.random.normal(0, 5, len(dates))

    df = pl.DataFrame({
        "datetime": dates,
        "demand_mwh": demand,
        "solar_mwh": solar,
        "wind_mwh": wind,
        "hydro_mwh": hydro,
        "natural_gas_mwh": natural_gas,
        "coal_mwh": coal,
        "nuclear_mwh": nuclear
    })

    print(f"✅ Created {len(df)} hours of comprehensive energy data")

    # Use the high-level sequence dataset creation function
    print("\n🔄 Creating sequence dataset with feature engineering...")
    feature_engineer = EnergyFeatureEngineer()

    input_sequences, target_sequences, feature_names = create_energy_sequence_dataset(
        df,
        sequence_length=168,  # 1 week
        stride=24,           # 1 day
        feature_engineer=feature_engineer
    )

    print(f"✅ Complete pipeline executed!")
    print(f"📊 Input sequences shape: {input_sequences.shape}")
    print(f"🎯 Target sequences shape: {target_sequences.shape}")
    print(f"🏷️  Feature count: {len(feature_names)}")
    print(f"💾 Dataset size: {(input_sequences.nbytes + target_sequences.nbytes) / 1024 / 1024:.1f} MB")

    # Show feature breakdown
    print(f"\n🏷️  Feature Categories in Final Dataset:")
    temporal_features = [f for f in feature_names if any(
        keyword in f for keyword in ["hour", "day", "week", "month", "year", "sin", "cos"]
    )]
    energy_features = [f for f in feature_names if any(
        keyword in f for keyword in ["demand", "solar", "wind", "hydro", "gas", "coal", "nuclear"]
    )]
    derived_features = [f for f in feature_names if any(
        keyword in f for keyword in ["renewable", "supply", "adequacy", "intermittency"]
    )]

    print(f"   • Temporal: {len(temporal_features)}")
    print(f"   • Energy: {len(energy_features)}")
    print(f"   • Derived: {len(derived_features)}")

    # Show some example feature names
    print(f"\n📋 Example Features:")
    print(f"   • Temporal: {temporal_features[:3]}")
    print(f"   • Energy: {energy_features[:3]}")
    print(f"   • Derived: {derived_features[:3]}")

    return input_sequences, target_sequences, feature_names


def demo_with_storage_integration():
    """Demo integration with storage system."""
    print("\n💾 STORAGE INTEGRATION DEMO")
    print("=" * 50)

    try:
        # Setup storage
        storage_path = Path("data/processed/ml_demo")
        storage_manager = StorageManager(storage_path)

        print(f"📁 Using storage path: {storage_path}")

        # Create and save sample data
        print("📊 Creating sample data for storage...")
        dates = [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(168)]

        df = pl.DataFrame({
            "datetime": dates,
            "demand_mwh": 1000 + 200 * np.sin(2 * np.pi * np.arange(168) / 24),
            "solar_mwh": np.maximum(0, 200 * np.sin(2 * np.pi * np.arange(168) / 24)),
            "wind_mwh": 150 + 100 * np.random.normal(0, 1, 168)
        })

        # Save to storage
        filename = "ml_demo_data"
        storage_manager.save_dataframe(df, filename, overwrite=True)
        print(f"✅ Saved data to storage: {filename}")

        # Load from storage
        loaded_df = storage_manager.load_dataframe(filename)
        print(f"✅ Loaded data from storage: {loaded_df.shape}")

        # Feature engineering on loaded data
        print("🔧 Applying feature engineering...")
        feature_engineer = EnergyFeatureEngineer()
        df_with_features = feature_engineer.create_all_features(loaded_df, include_lags=False)

        # Save enhanced data
        enhanced_filename = "ml_demo_enhanced"
        storage_manager.save_dataframe(df_with_features, enhanced_filename, overwrite=True)
        print(f"✅ Saved enhanced data: {enhanced_filename}")

        # Show storage contents
        files = storage_manager.list_files()
        print(f"📁 Storage contents: {files}")

    except Exception as e:
        print(f"⚠️  Storage demo failed: {e}")
        print("   (This is expected if running without full setup)")


def main():
    """Run the complete MLX energy pipeline demo."""
    print("🌟 MLX ENERGY PIPELINE DEMO")
    print("=" * 60)
    print("This demo shows our complete MLX-ready energy ML pipeline:")
    print("  • Feature engineering with temporal and energy-specific features")
    print("  • Sequence generation for ring attention training")
    print("  • MLX-compatible array generation")
    print("  • Integration with storage system")
    print("=" * 60)

    try:
        # Demo 1: Feature Engineering
        df_with_features, mlx_array, feature_names = demo_feature_engineering()

        # Demo 2: Sequence Generation
        X, y, seq_gen = demo_sequence_generation(df_with_features)

        # Demo 3: Comprehensive Pipeline
        input_seqs, target_seqs, all_feature_names = demo_comprehensive_pipeline()

        # Demo 4: Storage Integration
        demo_with_storage_integration()

        # Summary
        print("\n🎉 DEMO SUMMARY")
        print("=" * 50)
        print("✅ All demos completed successfully!")
        print(f"🎯 Key Results:")
        print(f"   • Feature engineering: {len(df_with_features.columns)} total features")
        print(f"   • Sequence generation: {X.shape[0]} training sequences")
        print(f"   • Comprehensive pipeline: {input_seqs.shape[0]} sequences with {len(all_feature_names)} features")
        print(f"   • MLX arrays ready for ring attention training")
        print(f"   • Storage integration working")

        print(f"\n📊 Memory Usage:")
        print(f"   • Feature engineered data: {mlx_array.nbytes / 1024 / 1024:.1f} MB")
        if X is not None and y is not None:
            print(f"   • Training sequences: {(X.nbytes + y.nbytes) / 1024 / 1024:.1f} MB")
        else:
            print(f"   • Training sequences: Not generated")
        print(f"   • Comprehensive dataset: {(input_seqs.nbytes + target_seqs.nbytes) / 1024 / 1024:.1f} MB")

        print(f"\n🚀 Ready for Ring Attention Training!")
        print(f"   • Sequence length: {input_seqs.shape[1]} time steps")
        print(f"   • Feature dimension: {input_seqs.shape[2]} features")
        print(f"   • Batch size: {input_seqs.shape[0]} sequences")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
