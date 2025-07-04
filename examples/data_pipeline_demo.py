#!/usr/bin/env python3
"""
Example: Converting Your VCR Test Data to ML-Ready Format

This demonstrates the complete data pipeline from API responses to MLX arrays
ready for ring attention training.
"""

import json
import yaml
from pathlib import Path
from datetime import datetime
import mlx.core as mx
import numpy as np

# Add the project root to path for imports
import sys
sys.path.append(str(Path(__file__).parent.parent))

from src.core.integrations.energy_data import (
    EIAResponse, EIADemandRecord, EIAGenerationRecord,
    EnergyTimePoint, EnergyDataBatch,
    merge_demand_and_generation, pivot_generation_data
)


def load_cassette_data(cassette_path: Path) -> dict:
    """Load JSON response from VCR cassette file"""
    with open(cassette_path, 'r') as f:
        cassette = yaml.safe_load(f)
    
    # Extract the JSON response string
    response_string = cassette['interactions'][0]['response']['body']['string']
    return json.loads(response_string)


def demonstrate_data_pipeline():
    """Complete example of the data pipeline"""
    
    print("🔄 Energy Data Pipeline Demonstration")
    print("=" * 50)
    
    # Step 1: Load your VCR test data
    cassette_dir = Path("tests/core/integrations/eia/cassettes")
    
    demand_data = load_cassette_data(cassette_dir / "eia_oregon_demand.yaml")
    generation_data = load_cassette_data(cassette_dir / "eia_oregon_renewable.yaml")
    
    print(f"✅ Loaded cassette data:")
    print(f"   • Demand records: {len(demand_data['response']['data'])}")
    print(f"   • Generation records: {len(generation_data['response']['data'])}")
    
    # Step 2: Work with raw API data (skip re-validation for now)
    print(f"\n� Processing raw API responses...")
    
    try:
        # Extract raw records
        demand_records_raw = demand_data['response']['data']
        generation_records_raw = generation_data['response']['data']
        
        print(f"✅ Extracted raw records:")
        print(f"   • Demand records: {len(demand_records_raw)}")
        print(f"   • Generation records: {len(generation_records_raw)}")
        
        # Convert to our internal format manually
        energy_points = []
        
        # Create demand lookup
        demand_by_time = {}
        for record in demand_records_raw:
            timestamp_str = record['period']
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H")
            demand_by_time[timestamp] = float(record['value'])
        
        # Create generation lookup
        generation_by_time = {}
        for record in generation_records_raw:
            timestamp_str = record['period']
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H")
            
            if timestamp not in generation_by_time:
                generation_by_time[timestamp] = {}
            
            # Map fuel codes to names
            fuel_mapping = {
                'NG': 'natural_gas',
                'SUN': 'solar', 
                'WND': 'wind',
                'WAT': 'hydro',
                'OTH': 'other'
            }
            
            fuel_name = fuel_mapping.get(record['fueltype'], record['fueltype'].lower())
            generation_by_time[timestamp][fuel_name] = float(record['value'])
        
        # Merge into energy points
        for timestamp in sorted(demand_by_time.keys()):
            if timestamp in generation_by_time:
                point = EnergyTimePoint(
                    timestamp=timestamp,
                    region="PACW",
                    demand_mwh=demand_by_time[timestamp],
                    generation_by_fuel=generation_by_time[timestamp]
                )
                energy_points.append(point)
        
        print(f"✅ Created {len(energy_points)} unified time points")
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        return
    
    # Show sample data
    if energy_points:
        sample = energy_points[0]
        print(f"\n📊 Sample data point:")
        print(f"   • Timestamp: {sample.timestamp}")
        print(f"   • Region: {sample.region}")
        print(f"   • Demand: {sample.demand_mwh:.1f} MWh")
        print(f"   • Generation: {sample.generation_by_fuel}")
    
    # Step 3: Create ML-ready batch
    print(f"\n🎯 Creating ML-ready data batch...")
    
    batch = EnergyDataBatch(
        region="PACW",
        start_time=energy_points[0].timestamp,
        end_time=energy_points[-1].timestamp,
        data_points=energy_points
    )
    
    # Step 4: Convert to different formats
    print(f"\n📈 Converting to analysis formats...")
    
    # Pandas DataFrame for analysis
    df = batch.to_dataframe()
    print(f"✅ Pandas DataFrame: {df.shape}")
    print(f"   Columns: {list(df.columns)}")
    
    # MLX array for ring attention
    fuel_order = ['solar', 'wind', 'hydro', 'natural_gas', 'other']
    mlx_sequence = batch.to_mlx_sequence(fuel_order)
    print(f"✅ MLX Array: {mlx_sequence.shape}")
    print(f"   Features: {['demand'] + fuel_order}")
    
    # Step 5: Analyze Oregon energy patterns
    print(f"\n🏔️  Oregon Energy Analysis:")
    
    # Demand statistics
    demands = [p.demand_mwh for p in energy_points]
    print(f"   • Demand range: {min(demands):.0f} - {max(demands):.0f} MWh")
    print(f"   • Average demand: {np.mean(demands):.0f} MWh")
    
    # Generation mix analysis
    total_generation = {}
    for point in energy_points:
        for fuel, value in point.generation_by_fuel.items():
            total_generation[fuel] = total_generation.get(fuel, 0) + value
    
    print(f"   • Generation mix (total over {len(energy_points)} hours):")
    for fuel, total in sorted(total_generation.items(), key=lambda x: x[1], reverse=True):
        avg_per_hour = total / len(energy_points)
        print(f"     - {fuel}: {avg_per_hour:.1f} MWh/hour avg")
    
    # Step 6: Data quality checks
    print(f"\n🔍 Data Quality Analysis:")
    
    # Check for missing timestamps
    expected_hours = (energy_points[-1].timestamp - energy_points[0].timestamp).total_seconds() / 3600 + 1
    actual_hours = len(energy_points)
    print(f"   • Time coverage: {actual_hours}/{expected_hours:.0f} hours ({actual_hours/expected_hours*100:.1f}%)")
    
    # Check for zero values
    zero_demand = sum(1 for p in energy_points if p.demand_mwh == 0)
    print(f"   • Zero demand hours: {zero_demand}/{len(energy_points)} ({zero_demand/len(energy_points)*100:.1f}%)")
    
    # Renewable penetration
    renewable_fuels = ['solar', 'wind', 'hydro']
    for point in energy_points:
        renewable_gen = sum(point.generation_by_fuel.get(fuel, 0) for fuel in renewable_fuels)
        total_gen = sum(point.generation_by_fuel.values())
        if total_gen > 0:
            renewable_pct = renewable_gen / total_gen * 100
            break
    
    print(f"   • Renewable penetration: ~{renewable_pct:.1f}% (sample hour)")
    
    # Step 7: Prepare for ring attention
    print(f"\n🧠 Ring Attention Preparation:")
    print(f"   • Sequence length: {mlx_sequence.shape[0]} timesteps")
    print(f"   • Feature dimension: {mlx_sequence.shape[1]} features")
    print(f"   • Memory usage: ~{mlx_sequence.nbytes / 1024 / 1024:.2f} MB")
    
    # Calculate how many 8760-hour sequences this could create
    target_seq_len = 8760  # 1 year
    possible_sequences = max(0, mlx_sequence.shape[0] - target_seq_len + 1)
    print(f"   • Could create {possible_sequences} training sequences of length {target_seq_len}")
    
    if possible_sequences == 0:
        needed_hours = target_seq_len - mlx_sequence.shape[0]
        print(f"   • Need {needed_hours} more hours for full year sequences")
        print(f"   • Recommendation: Fetch ~{needed_hours/24:.0f} more days of data")
    
    print(f"\n✅ Pipeline demonstration complete!")
    print(f"🎯 Your data is ready for ring attention training!")
    
    return batch, mlx_sequence


if __name__ == "__main__":
    demonstrate_data_pipeline()
