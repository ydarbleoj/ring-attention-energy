#!/usr/bin/env python3
"""
Demo: EIA Schema in Action

This script demonstrates how the new EIA schema works with real VCR cassette data.
Shows the complete flow from raw API response to validated Pydantic models to pandas DataFrames.
"""

import json
import yaml
from pathlib import Path
from src.core.integrations.eia.schema import EIAResponse, validate_eia_response

def load_cassette_response(cassette_path: Path) -> dict:
    """Load API response from VCR cassette"""
    with open(cassette_path, 'r') as f:
        cassette_data = yaml.safe_load(f)

    response_string = cassette_data['interactions'][0]['response']['body']['string']
    return json.loads(response_string)

def demo_demand_processing():
    """Demo demand data processing with schema validation"""
    print("🔍 DEMO: Oregon Demand Data Processing")
    print("="*50)

    cassette_path = Path("tests/core/integrations/eia/cassettes/eia_oregon_demand.yaml")
    if not cassette_path.exists():
        print("❌ VCR cassette not found")
        return

    # 1. Load raw API response
    raw_response = load_cassette_response(cassette_path)
    print(f"📥 Raw response total: {raw_response['response']['total']} records")

    # 2. Validate with Pydantic schema
    validated_response = EIAResponse.model_validate(raw_response)
    print(f"✅ Schema validation: SUCCESS")
    print(f"📊 Validated total: {validated_response.response.total_records} records")

    # 3. Convert to pandas DataFrame
    demand_df = validated_response.to_demand_dataframe()
    print(f"🐼 DataFrame shape: {demand_df.shape}")
    print(f"📈 Demand range: {demand_df['demand_mwh'].min():.1f} - {demand_df['demand_mwh'].max():.1f} MWh")

    # 4. Show sample data
    print("\n📋 Sample data:")
    print(demand_df.head(3).to_string(index=False))

    return demand_df

def demo_generation_processing():
    """Demo generation data processing with fuel type pivoting"""
    print("\n\n🌱 DEMO: Oregon Generation Data Processing")
    print("="*50)

    cassette_path = Path("tests/core/integrations/eia/cassettes/eia_oregon_renewable.yaml")
    if not cassette_path.exists():
        print("❌ VCR cassette not found")
        return

    # 1. Load and validate
    raw_response = load_cassette_response(cassette_path)
    validated_response = EIAResponse.model_validate(raw_response)

    print(f"📥 Raw response total: {validated_response.response.total_records} records")

    # 2. Convert to wide DataFrame (pivoted by fuel type)
    generation_df = validated_response.to_generation_dataframe()
    print(f"🐼 DataFrame shape: {generation_df.shape}")

    # 3. Show fuel type breakdown
    fuel_cols = [col for col in generation_df.columns if "_mwh" in col]
    print(f"⚡ Fuel types: {len(fuel_cols)} ({', '.join(fuel_cols)})")

    # 4. Calculate renewable summary
    renewable_cols = [col for col in fuel_cols if any(x in col for x in ["solar", "wind", "hydro"])]
    if renewable_cols:
        generation_df["total_renewable_mwh"] = generation_df[renewable_cols].sum(axis=1)
        avg_renewable = generation_df["total_renewable_mwh"].mean()
        print(f"🔋 Average renewable generation: {avg_renewable:.1f} MWh/hour")

    # 5. Show sample data
    print("\n📋 Sample generation data:")
    sample_cols = ["timestamp"] + fuel_cols[:4]  # Show first few fuel types
    print(generation_df[sample_cols].head(3).to_string(index=False))

    return generation_df

def demo_oregon_energy_insights(demand_df, generation_df):
    """Demo Oregon-specific energy analysis"""
    print("\n\n🏔️  DEMO: Oregon Energy Insights")
    print("="*50)

    if demand_df is None or generation_df is None:
        print("❌ Missing data for analysis")
        return

    # Merge demand and generation data
    merged_df = demand_df.merge(generation_df, on=["timestamp", "region"], how="inner")
    print(f"🔗 Merged dataset: {merged_df.shape}")

    # Energy mix analysis
    renewable_cols = [col for col in merged_df.columns if any(x in col for x in ["solar", "wind", "hydro"])]
    if renewable_cols:
        merged_df["total_renewable_mwh"] = merged_df[renewable_cols].sum(axis=1)
        merged_df["renewable_percentage"] = (merged_df["total_renewable_mwh"] / merged_df["demand_mwh"] * 100).round(1)

        avg_renewable_pct = merged_df["renewable_percentage"].mean()
        print(f"🌿 Average renewable percentage: {avg_renewable_pct:.1f}%")

        # Peak renewable generation
        max_renewable_hour = merged_df.loc[merged_df["total_renewable_mwh"].idxmax()]
        print(f"⚡ Peak renewable hour: {max_renewable_hour['timestamp']}")
        print(f"   • Total renewable: {max_renewable_hour['total_renewable_mwh']:.1f} MWh")
        print(f"   • Total demand: {max_renewable_hour['demand_mwh']:.1f} MWh")
        print(f"   • Renewable coverage: {max_renewable_hour['renewable_percentage']:.1f}%")

        # Fuel type breakdown
        print(f"\n🔋 Oregon fuel mix (average MWh/hour):")
        for col in renewable_cols:
            fuel_name = col.replace("_mwh", "").title()
            avg_gen = merged_df[col].mean()
            print(f"   • {fuel_name}: {avg_gen:.1f} MWh")

def main():
    """Run the complete EIA schema demo"""
    print("🚀 EIA Schema Demo: From Raw API to Insights")
    print("=" * 60)

    # Process demand data
    demand_df = demo_demand_processing()

    # Process generation data
    generation_df = demo_generation_processing()

    # Oregon energy insights
    demo_oregon_energy_insights(demand_df, generation_df)

    print("\n" + "="*60)
    print("✅ Schema Demo Complete!")
    print("🎯 Next steps: Build the full data pipeline with Polars + Parquet")

if __name__ == "__main__":
    main()
