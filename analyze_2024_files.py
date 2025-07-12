#!/usr/bin/env python3
"""Analyze interim file sizes for 2024 data to inform consolidation strategy."""

import sys
from pathlib import Path
import polars as pl

def analyze_2024_file_sizes():
    """Analyze file sizes and record counts for 2024 interim data."""

    interim_2024_path = Path("data/interim/2024")

    if not interim_2024_path.exists():
        print(f"Directory not found: {interim_2024_path}")
        return

    print("📊 2024 INTERIM DATA ANALYSIS")
    print("=" * 50)

    # Get all parquet files
    parquet_files = list(interim_2024_path.glob("*.parquet"))

    if not parquet_files:
        print("No parquet files found")
        return

    print(f"Total files: {len(parquet_files)}")

    # Analyze each file
    file_stats = []
    total_size_bytes = 0
    total_records = 0

    # Group by data type and region for better analysis
    by_data_type = {"demand": [], "generation": []}
    by_region = {}

    for file_path in parquet_files:
        try:
            # Get file size
            size_bytes = file_path.stat().st_size
            size_mb = size_bytes / (1024 * 1024)

            # Load and get record count
            df = pl.read_parquet(file_path)
            record_count = len(df)

            # Parse filename for categorization
            filename = file_path.name
            if "demand" in filename:
                data_type = "demand"
            elif "generation" in filename:
                data_type = "generation"
            else:
                data_type = "unknown"

            # Extract region
            parts = filename.split("_")
            region = parts[2] if len(parts) > 2 else "unknown"

            file_info = {
                "filename": filename,
                "size_bytes": size_bytes,
                "size_mb": size_mb,
                "records": record_count,
                "data_type": data_type,
                "region": region
            }

            file_stats.append(file_info)
            total_size_bytes += size_bytes
            total_records += record_count

            # Group by data type
            by_data_type[data_type].append(file_info)

            # Group by region
            if region not in by_region:
                by_region[region] = []
            by_region[region].append(file_info)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # Sort by size
    file_stats.sort(key=lambda x: x["size_mb"], reverse=True)

    # Overall statistics
    total_size_mb = total_size_bytes / (1024 * 1024)
    total_size_gb = total_size_mb / 1024
    avg_size_mb = total_size_mb / len(file_stats)

    print(f"\n📈 OVERALL STATISTICS:")
    print(f"  Total files: {len(file_stats)}")
    print(f"  Total size: {total_size_mb:.1f} MB ({total_size_gb:.3f} GB)")
    print(f"  Total records: {total_records:,}")
    print(f"  Average file size: {avg_size_mb:.2f} MB")
    print(f"  Records per file: {total_records // len(file_stats):,}")

    # Size distribution
    small_files = [f for f in file_stats if f["size_mb"] < 1]
    medium_files = [f for f in file_stats if 1 <= f["size_mb"] < 10]
    large_files = [f for f in file_stats if f["size_mb"] >= 10]

    print(f"\n📦 SIZE DISTRIBUTION:")
    print(f"  < 1 MB: {len(small_files)} files")
    print(f"  1-10 MB: {len(medium_files)} files")
    print(f"  >= 10 MB: {len(large_files)} files")

    # By data type
    print(f"\n📊 BY DATA TYPE:")
    for dtype, files in by_data_type.items():
        if files:
            total_mb = sum(f["size_mb"] for f in files)
            total_recs = sum(f["records"] for f in files)
            print(f"  {dtype.upper()}:")
            print(f"    Files: {len(files)}")
            print(f"    Total size: {total_mb:.1f} MB")
            print(f"    Total records: {total_recs:,}")
            print(f"    Avg size: {total_mb/len(files):.2f} MB")

    # By region
    print(f"\n🌍 BY REGION:")
    for region, files in by_region.items():
        total_mb = sum(f["size_mb"] for f in files)
        total_recs = sum(f["records"] for f in files)
        print(f"  {region}:")
        print(f"    Files: {len(files)}")
        print(f"    Total size: {total_mb:.1f} MB")
        print(f"    Total records: {total_recs:,}")

    # Largest and smallest files
    print(f"\n🔍 LARGEST FILES:")
    for i, file_info in enumerate(file_stats[:5]):
        print(f"  {i+1}. {file_info['filename']}: {file_info['size_mb']:.2f} MB ({file_info['records']:,} records)")

    print(f"\n🔍 SMALLEST FILES:")
    for i, file_info in enumerate(file_stats[-5:]):
        print(f"  {i+1}. {file_info['filename']}: {file_info['size_mb']:.3f} MB ({file_info['records']:,} records)")

    # Analysis for consolidation
    print(f"\n🎯 CONSOLIDATION ANALYSIS:")
    print(f"  Current: {len(file_stats)} files averaging {avg_size_mb:.2f} MB each")
    print(f"  Target for 128MB HDFS blocks: Need files ≥ 128 MB")
    print(f"  Target for 1GB optimal: Need files ≥ 1024 MB")
    print(f"  Current largest file: {file_stats[0]['size_mb']:.2f} MB")

    # Consolidation scenarios
    if total_size_mb >= 1024:
        files_for_1gb = max(1, int(total_size_mb / 1024))
        print(f"  To reach 1GB files: Consolidate into ~{files_for_1gb} file(s)")
    else:
        print(f"  Total data ({total_size_mb:.1f} MB) < 1GB - consider single file")

    files_for_128mb = max(1, int(total_size_mb / 128))
    print(f"  To reach 128MB files: Consolidate into ~{files_for_128mb} file(s)")

    return {
        "total_files": len(file_stats),
        "total_size_mb": total_size_mb,
        "total_size_gb": total_size_gb,
        "total_records": total_records,
        "avg_size_mb": avg_size_mb,
        "by_data_type": by_data_type,
        "by_region": by_region
    }

if __name__ == "__main__":
    analyze_2024_file_sizes()
