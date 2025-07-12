"""Interim data structure analysis and recommendations.

This analysis examines the current 548 JSON files and proposes optimal
interim storage strategies for performance, maintainability, and ring attention research.
"""

import json
from pathlib import Path
from collections import defaultdict
import re

def analyze_raw_data_patterns():
    """Analyze the raw data to understand patterns and structure."""

    print("📊 RAW DATA PATTERN ANALYSIS")
    print("=" * 50)

    raw_path = Path("data/raw/eia")
    all_files = list(raw_path.glob("**/*.json"))

    # Pattern analysis
    patterns = {
        'by_year': defaultdict(int),
        'by_region': defaultdict(int),
        'by_data_type': defaultdict(int),
        'by_year_region': defaultdict(int),
        'by_year_type': defaultdict(int),
        'file_sizes': [],
        'record_counts': [],
        'date_ranges': []
    }

    print(f"Total files: {len(all_files)}")

    for file_path in all_files[:20]:  # Sample first 20 for analysis
        try:
            # Parse filename
            filename = file_path.name
            # Format: eia_{data_type}_{region}_{start_date}_to_{end_date}_{timestamp}.json
            parts = filename.replace('.json', '').split('_')

            if len(parts) >= 4:
                data_type = parts[1]
                region = parts[2]
                year = parts[3].split('-')[0]

                patterns['by_year'][year] += 1
                patterns['by_region'][region] += 1
                patterns['by_data_type'][data_type] += 1
                patterns['by_year_region'][f"{year}_{region}"] += 1
                patterns['by_year_type'][f"{year}_{data_type}"] += 1

            # File metadata
            file_size = file_path.stat().st_size
            patterns['file_sizes'].append(file_size)

            # Quick peek at content
            with open(file_path, 'r') as f:
                data = json.load(f)
                metadata = data.get('metadata', {})
                record_count = metadata.get('record_count', 0)
                patterns['record_counts'].append(record_count)

                start_date = metadata.get('start_date')
                end_date = metadata.get('end_date')
                if start_date and end_date:
                    patterns['date_ranges'].append((start_date, end_date))

        except Exception as e:
            print(f"Error analyzing {file_path.name}: {e}")

    # Print analysis
    print(f"\n📅 Years: {sorted(patterns['by_year'].keys())}")
    print(f"🌍 Regions: {sorted(patterns['by_region'].keys())}")
    print(f"📊 Data types: {sorted(patterns['by_data_type'].keys())}")

    if patterns['file_sizes']:
        avg_size = sum(patterns['file_sizes']) / len(patterns['file_sizes'])
        print(f"📁 Average file size: {avg_size/1024:.1f} KB")

    if patterns['record_counts']:
        avg_records = sum(patterns['record_counts']) / len(patterns['record_counts'])
        print(f"📋 Average records per file: {avg_records:.0f}")

    return patterns

def propose_interim_structures():
    """Propose different interim data structure options."""

    print(f"\n\n🏗️ INTERIM DATA STRUCTURE OPTIONS")
    print("=" * 50)

    print("""
OPTION 1: Flat Structure (Current Simple Approach)
data/interim/
├── eia_demand_PACW_2024-01-20_to_2024-03-04.parquet
├── eia_demand_ERCO_2024-01-20_to_2024-03-04.parquet
├── eia_generation_PACW_2024-01-20_to_2024-03-04.parquet
└── ... (548 files)

✅ Pros: Simple, 1-to-1 mapping, easy debugging
❌ Cons: No organization, hard to find specific data

---

OPTION 2: Partitioned by Year/Type/Region (Hive-style)
data/interim/
├── year=2024/
│   ├── data_type=demand/
│   │   ├── region=PACW/
│   │   │   ├── 2024-Q1.parquet
│   │   │   └── 2024-Q2.parquet
│   │   └── region=ERCO/
│   └── data_type=generation/
└── year=2025/

✅ Pros: Excellent for analytics, partition pruning, easy filtering
✅ Pros: Works great with Polars/Spark for time-series analysis
❌ Cons: More complex, might over-partition with small files

---

OPTION 3: Combined by Year (Analytical Focus)
data/interim/
├── energy_data_2024.parquet (all regions, all types for 2024)
├── energy_data_2025.parquet
├── historical_2019_2023.parquet
└── metadata/
    ├── file_lineage.json
    └── data_quality_report.json

✅ Pros: Fast analytical queries, fewer files, great for ML
✅ Pros: Perfect for ring attention (yearly sequences)
❌ Cons: Larger files, less granular access

---

OPTION 4: Hybrid Multi-Level Structure (Recommended)
data/interim/
├── by_file/           # 1-to-1 preserving original structure
│   ├── 2024/
│   │   ├── eia_demand_PACW_2024-Q1.parquet
│   │   └── eia_generation_PACW_2024-Q1.parquet
│   └── 2025/
├── by_year/           # Combined yearly files for analysis
│   ├── energy_2024_complete.parquet
│   └── energy_2025_complete.parquet
├── by_region/         # Region-focused time series
│   ├── PACW_complete_timeseries.parquet
│   └── ERCO_complete_timeseries.parquet
└── metadata/
    ├── lineage.json
    ├── schema_registry.json
    └── quality_reports/

✅ Pros: Best of all worlds, multiple access patterns
✅ Pros: Supports both debugging and high-performance analytics
✅ Pros: Perfect for ring attention research (multiple formats)
❌ Cons: More storage (but Parquet compression helps)

---

RECOMMENDATION: Start with Option 4 Hybrid
""")

def propose_column_schema():
    """Propose optimal column schema and data types."""

    print(f"\n📋 OPTIMAL COLUMN SCHEMA")
    print("=" * 30)

    print("""
🎯 OPTIMIZED SCHEMA for Ring Attention + Analytics:

Core Energy Data:
├── timestamp: TIMESTAMP (UTC, timezone-aware)
├── region: STRING (categorical, enum-like)
├── data_type: STRING ('demand' | 'generation')
├── value_mwh: FLOAT64 (standardized to MWh)
├── data_quality_score: FLOAT32 (0.0-1.0)

Temporal Features (for Ring Attention):
├── year: INT16 (2019-2025)
├── month: INT8 (1-12)
├── day_of_year: INT16 (1-366)
├── hour: INT8 (0-23)
├── day_of_week: INT8 (0-6)
├── is_weekend: BOOLEAN
├── season: STRING ('winter'|'spring'|'summer'|'fall')

Geographic/Grid Features:
├── region_code: STRING (PACW, ERCO, etc.)
├── region_name: STRING (full name)
├── grid_interconnection: STRING (Western, Eastern, ERCOT)
├── timezone: STRING (America/Los_Angeles, etc.)

Source Metadata (for traceability):
├── source_file: STRING
├── extraction_timestamp: TIMESTAMP
├── api_endpoint: STRING
├── batch_id: STRING (for tracking)

ML-Ready Features (computed):
├── value_normalized: FLOAT32 (0-1 normalized within region)
├── rolling_avg_24h: FLOAT32 (24-hour moving average)
├── seasonal_component: FLOAT32 (extracted seasonal pattern)
├── trend_component: FLOAT32 (extracted trend)
├── sequence_position: INT32 (position in time sequence)

🎯 Total: ~20 columns optimized for both analytics and ML
""")

def recommend_partitioning_strategy():
    """Recommend partitioning strategy for performance."""

    print(f"\n⚡ PARTITIONING STRATEGY")
    print("=" * 30)

    print("""
🎯 RECOMMENDED PARTITIONING:

For by_file/ (1-to-1 preservation):
└── partition by year/ (natural from source structure)

For by_year/ (analytical):
└── single file per year (optimal for ring attention sequences)

For by_region/ (region-focused):
└── partition by region, ordered by timestamp

Parquet Configuration:
├── Compression: SNAPPY (good balance of speed/size)
├── Row Groups: 50,000-100,000 rows (optimal for time series)
├── Column Encoding: Dictionary for categoricals
├── Bloom Filters: On region, data_type for fast filtering
├── Statistics: Min/max on timestamp for time pruning

File Sizing Targets:
├── Individual files: 10-50 MB (sweet spot for Polars)
├── Combined yearly: 100-500 MB (manageable for ML)
├── Region time series: 50-200 MB (good for attention models)
""")

if __name__ == "__main__":
    analyze_raw_data_patterns()
    propose_interim_structures()
    propose_column_schema()
    recommend_partitioning_strategy()

    print(f"\n🤔 DISCUSSION QUESTIONS:")
    print(f"1. Which structure option appeals most for your ring attention research?")
    print(f"2. Do you want the hybrid approach or prefer simplicity?")
    print(f"3. Any specific column features needed for your ML models?")
    print(f"4. Should we include computed time series features or keep raw data?")
    print(f"5. What's your preference for file sizes vs. number of files?")
