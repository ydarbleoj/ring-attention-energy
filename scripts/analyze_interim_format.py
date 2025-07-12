#!/usr/bin/env python3
"""Analysis of interim data format and Processed Stage strategy options."""

import polars as pl
from pathlib import Path

def analyze_interim_format():
    """Analyze the interim Parquet format and show structure."""

    print("📊 INTERIM DATA FORMAT ANALYSIS")
    print("=" * 50)

    # Load test files
    interim_dir = Path("data/interim")
    pacw_file = interim_dir / "test_transform_pacw_demand_2024_q1.parquet"
    erco_file = interim_dir / "test_transform_erco_demand_2025_problematic.parquet"

    if pacw_file.exists():
        df = pl.read_parquet(pacw_file)

        print(f"✅ PACW 2024 Q1 Data: {len(df):,} records")
        print(f"📅 Date range: {df.select(pl.col('datetime').min()).item()} to {df.select(pl.col('datetime').max()).item()}")
        print(f"🏷️  Columns: {len(df.columns)} total")

        # Show clean sample
        sample = df.sort('datetime').head(5).select([
            'datetime', 'region', 'data_type', 'value', 'value_units'
        ])
        print("\n📋 Sample records:")
        for row in sample.to_dicts():
            print(f"  {row['datetime']} | {row['region']} | {row['data_type']} | {row['value']:.0f} {row['value_units']}")

        # Show value distribution
        stats = df.select([
            pl.col("value").min().alias("min"),
            pl.col("value").max().alias("max"),
            pl.col("value").mean().alias("avg")
        ]).to_dicts()[0]
        print(f"\n📈 Value statistics: {stats['min']:.0f} - {stats['max']:.0f} MWh (avg: {stats['avg']:.0f})")

def show_processed_stage_options():
    """Show different options for the Processed Stage."""

    print("\n\n🎯 PROCESSED STAGE STRATEGY OPTIONS")
    print("=" * 50)

    print("""
📋 Current Interim Format (1-to-1 JSON → Parquet):
✅ Preserves all metadata and traceability
✅ Handles data quality issues consistently
✅ Fast parallel processing (548 files independently)
✅ Easy to reprocess individual files

🎯 Processed Stage Options:

OPTION 1: Time-Series Aggregated Files
├── daily_demand_by_region.parquet (all regions, daily totals)
├── hourly_demand_by_region.parquet (all regions, hourly data)
├── daily_generation_by_region.parquet
└── hourly_generation_by_region.parquet

OPTION 2: Region-Focused Files
├── PACW_complete_timeseries.parquet (demand + generation)
├── ERCO_complete_timeseries.parquet
├── CAL_complete_timeseries.parquet
└── [etc for each region]

OPTION 3: Analysis-Ready Combined Files
├── multi_region_demand_2024.parquet (all regions, demand only)
├── multi_region_generation_2024.parquet (all regions, generation only)
├── complete_energy_dataset_2024.parquet (everything combined)
└── [yearly files for each year 2019-2025]

OPTION 4: ML/Ring-Attention Optimized
├── sequences_by_region/ (time-series sequences for attention models)
├── feature_matrices/ (region x time matrices)
└── attention_ready/ (preprocessed for ring attention)

💭 RECOMMENDATION: Start with Option 3 + Option 1
- Create yearly combined files for easy analysis
- Create time-aggregated views for different use cases
- Keep region-level granularity for ring attention research
""")

def estimate_performance():
    """Estimate performance for full dataset transformation."""

    print("\n⚡ PERFORMANCE ESTIMATION")
    print("=" * 30)

    # Based on single file test: 1,057 records in 0.016s
    records_per_second = 1057 / 0.016
    total_records = 1_200_000  # User mentioned 1.2M+ records
    total_files = 548

    estimated_time = total_records / records_per_second

    print(f"📊 Single file performance: {records_per_second:,.0f} records/second")
    print(f"📁 Total dataset: {total_files} files, ~{total_records:,} records")
    print(f"⏱️  Estimated transform time: {estimated_time:.1f} seconds ({estimated_time/60:.1f} minutes)")
    print(f"🎯 Target: <5 minutes (✅ Well under target!)")

    # File throughput
    files_per_second = 1 / 0.016  # One file in 16ms
    estimated_file_time = total_files / files_per_second
    print(f"📄 File throughput: {files_per_second:.0f} files/second")
    print(f"⏱️  File processing time: {estimated_file_time:.1f} seconds")

if __name__ == "__main__":
    analyze_interim_format()
    show_processed_stage_options()
    estimate_performance()

    print(f"\n🤔 QUESTIONS FOR YOU:")
    print(f"1. Which Processed Stage option appeals most for your ring attention research?")
    print(f"2. Do you want combined demand+generation files or separate?")
    print(f"3. Should we prioritize by year (2024, 2025) or by region (PACW, ERCO)?")
    print(f"4. Any specific time aggregations needed (daily/weekly/monthly)?")
