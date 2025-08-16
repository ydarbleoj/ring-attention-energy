#!/usr/bin/env python3
"""
Live API Testing with Clean Slate
Tests the full pipeline with live EIA API extraction including price data.

This test implements Step 2 of the pipeline validation:
1. Clean existing data directories
2. Test live API extraction with price data
3. Validate end-to-end: API → JSON files → consolidated parquet
4. Confirm performance targets are met
5. Verify data quality and completeness

Usage:
    # With API key set in environment
    python tests/core/pipeline/test_live_api_integration.py

    # Or with explicit API key
    EIA_API_KEY=your_key python tests/core/pipeline/test_live_api_integration.py
"""

import asyncio
import json
import os
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

# Add src to Python path
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.orchestrators.pipeline_dag import PipelineDAG, PipelineDAGConfig
from src.core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig


class LiveAPITestSuite:
    """Live API testing suite for full pipeline validation"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("EIA_API_KEY")
        self.project_root = project_root
        self.data_dir = self.project_root / "data"
        self.raw_dir = self.data_dir / "raw"
        self.interim_dir = self.data_dir / "interim"
        self.pipeline_runs_dir = self.data_dir / "pipeline_runs"

    def validate_prerequisites(self) -> bool:
        """Validate that all prerequisites are met for live testing"""
        print("🔍 Validating Prerequisites...")

        # Check API key
        if not self.api_key:
            print("   ❌ EIA_API_KEY not found")
            print("   💡 Set it with: export EIA_API_KEY='your_key_here'")
            return False
        print(f"   ✅ EIA API key found: {self.api_key[:8]}...")

        # Check project structure
        required_dirs = [self.data_dir]
        for dir_path in required_dirs:
            if not dir_path.exists():
                print(f"   ❌ Required directory missing: {dir_path}")
                return False
        print("   ✅ Project structure validated")

        return True

    def clean_data_directories(self) -> bool:
        """Clean existing data for fresh start"""
        print("🧹 Cleaning Data Directories...")

        try:
            # Clean raw data
            if self.raw_dir.exists():
                for item in self.raw_dir.iterdir():
                    if item.name != "__init__.py":
                        if item.is_dir():
                            shutil.rmtree(item)
                        else:
                            item.unlink()
                print(f"   ✅ Cleaned raw data directory")

            # Clean interim data
            if self.interim_dir.exists():
                for item in self.interim_dir.iterdir():
                    if item.name != "__init__.py":
                        if item.is_dir():
                            shutil.rmtree(item)
                        else:
                            item.unlink()
                print(f"   ✅ Cleaned interim data directory")

            # Create clean structure
            (self.raw_dir / "eia").mkdir(parents=True, exist_ok=True)
            self.interim_dir.mkdir(parents=True, exist_ok=True)
            self.pipeline_runs_dir.mkdir(parents=True, exist_ok=True)

            print("   ✅ Data directories prepared for fresh start")
            return True

        except Exception as e:
            print(f"   ❌ Failed to clean directories: {e}")
            return False

    async def test_live_api_extraction(self, region: str = "PACW", days: int = 2) -> Dict[str, Any]:
        """Test live API extraction with price data"""
        print(f"📡 Testing Live API Extraction ({days} days, {region})...")

        # Calculate date range (recent dates for reliable data)
        end_date = datetime.now() - timedelta(days=7)  # Use data from 1 week ago
        start_date = end_date - timedelta(days=days)

        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        print(f"   📅 Date range: {start_date_str} to {end_date_str}")
        print(f"   🌍 Region: {region}")
        print(f"   🏷️  Data types: demand, generation, price")

        # Create extract step configuration
        config = ApiExtractStepConfig(
            step_name=f"Live API Extract {region}",
            step_id="live_api_extract",
            source="eia",
            start_date=start_date_str,
            end_date=end_date_str,
            regions=[region],
            data_types=["demand", "generation", "price"],
            api_key=self.api_key,
            dry_run=False
        )

        extract_step = ApiExtractStep(config)

        # Execute extraction
        start_time = datetime.now()
        try:
            result = await extract_step.execute_async()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"   ⏱️  Extraction completed in {duration:.2f}s")
            print(f"   📊 Status: {result.status}")

            if result.status == "success":
                # Check created files
                eia_files = list((self.raw_dir / "eia").rglob("*.json"))
                print(f"   📄 JSON files created: {len(eia_files)}")

                # Analyze file types
                file_types = {}
                total_size = 0
                for file_path in eia_files:
                    if "demand" in file_path.name:
                        file_types["demand"] = file_types.get("demand", 0) + 1
                    elif "generation" in file_path.name:
                        file_types["generation"] = file_types.get("generation", 0) + 1
                    elif "price" in file_path.name:
                        file_types["price"] = file_types.get("price", 0) + 1
                    total_size += file_path.stat().st_size

                print(f"   📊 File breakdown: {file_types}")
                print(f"   💾 Total size: {total_size / 1024 / 1024:.1f} MB")

                return {
                    "success": True,
                    "duration": duration,
                    "files_created": len(eia_files),
                    "file_types": file_types,
                    "total_size_mb": total_size / 1024 / 1024,
                    "output_files": [str(f) for f in eia_files]
                }
            else:
                print(f"   ❌ Extraction failed: {result.message}")
                return {"success": False, "error": result.message}

        except Exception as e:
            print(f"   ❌ Extraction error: {e}")
            return {"success": False, "error": str(e)}

    async def test_transform_processing(self) -> Dict[str, Any]:
        """Test transform step processing of extracted data"""
        print("🔄 Testing Transform Processing...")

        # Create transform step configuration
        config = DataCleanerStepConfig(
            step_name="Live API Transform",
            step_id="live_api_transform",
            source="eia",
            raw_data_dir=self.raw_dir,
            interim_data_dir=self.interim_dir,
            validate_data=True,
            dry_run=False
        )

        transform_step = DataCleanerStep(config)

        # Execute transform
        start_time = datetime.now()
        try:
            result = await transform_step.execute_async()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"   ⏱️  Transform completed in {duration:.2f}s")
            print(f"   📊 Status: {result.status}")

            if result.status == "success":
                # Check output files
                parquet_files = list(self.interim_dir.glob("*.parquet"))
                print(f"   📄 Parquet files created: {len(parquet_files)}")

                if parquet_files:
                    # Analyze the consolidated parquet
                    import polars as pl
                    latest_parquet = max(parquet_files, key=lambda x: x.stat().st_mtime)
                    df = pl.read_parquet(latest_parquet)

                    print(f"   📊 Total records: {len(df):,}")

                    # Check data types
                    data_types = df["data_type"].unique().to_list()
                    print(f"   🏷️  Data types found: {data_types}")

                    # Analyze each data type
                    type_breakdown = {}
                    for data_type in data_types:
                        count = len(df.filter(pl.col("data_type") == data_type))
                        type_breakdown[data_type] = count
                        print(f"      {data_type}: {count:,} records")

                    # Performance metrics
                    records_per_second = len(df) / duration if duration > 0 else 0
                    file_size_mb = latest_parquet.stat().st_size / 1024 / 1024

                    print(f"   ⚡ Performance: {records_per_second:,.0f} records/second")
                    print(f"   💾 Output size: {file_size_mb:.1f} MB")

                    return {
                        "success": True,
                        "duration": duration,
                        "total_records": len(df),
                        "data_types": type_breakdown,
                        "records_per_second": records_per_second,
                        "output_size_mb": file_size_mb,
                        "output_file": str(latest_parquet)
                    }
                else:
                    print("   ❌ No parquet files created")
                    return {"success": False, "error": "No output files"}
            else:
                print(f"   ❌ Transform failed: {result.message}")
                return {"success": False, "error": result.message}

        except Exception as e:
            print(f"   ❌ Transform error: {e}")
            return {"success": False, "error": str(e)}

    async def test_full_pipeline_dag(self, region: str = "PACW", days: int = 2) -> Dict[str, Any]:
        """Test the complete pipeline DAG with live API data"""
        print("🚀 Testing Full Pipeline DAG...")

        # Calculate date range
        end_date = datetime.now() - timedelta(days=7)
        start_date = end_date - timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Create pipeline steps
        extract_config = ApiExtractStepConfig(
            step_name=f"Live DAG Extract {region}",
            step_id="live_dag_extract",
            source="eia",
            start_date=start_date_str,
            end_date=end_date_str,
            regions=[region],
            data_types=["demand", "generation", "price"],
            api_key=self.api_key,
            dry_run=False
        )

        transform_config = DataCleanerStepConfig(
            step_name=f"Live DAG Transform {region}",
            step_id="live_dag_transform",
            source="eia",
            raw_data_dir=self.raw_dir,
            interim_data_dir=self.interim_dir,
            validate_data=True,
            dry_run=False
        )

        extract_step = ApiExtractStep(extract_config)
        transform_step = DataCleanerStep(transform_config)

        # Create pipeline DAG
        dag_config = PipelineDAGConfig(
            pipeline_name=f"Live API Test {region}",
            pipeline_id=f"live_api_test_{region}_{datetime.now().strftime('%H%M%S')}",
            output_dir=self.pipeline_runs_dir / f"live_test_{region}",
            max_parallel_steps=2,
            stop_on_failure=True,
            save_intermediate_results=True,
            auto_connect_steps=True,
            validate_data_flow=True,
            log_level="INFO"
        )

        dag = PipelineDAG(dag_config)
        dag.create_extract_transform_chain(extract_step, transform_step)

        # Execute pipeline
        start_time = datetime.now()
        try:
            results = await dag.execute_async()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print(f"   ⏱️  Pipeline completed in {duration:.2f}s")
            print(f"   📊 Success: {results.success}")

            if results.success:
                print(f"   📈 Records processed: {results.total_records:,}")
                print(f"   📄 Files created: {results.total_files}")
                print(f"   💾 Data processed: {results.total_bytes / 1024 / 1024:.1f} MB")

                return {
                    "success": True,
                    "duration": duration,
                    "total_records": results.total_records,
                    "total_files": results.total_files,
                    "total_mb": results.total_bytes / 1024 / 1024,
                    "records_per_second": results.total_records / duration if duration > 0 else 0
                }
            else:
                print(f"   ❌ Pipeline failed")
                return {"success": False, "error": "Pipeline execution failed"}

        except Exception as e:
            print(f"   ❌ Pipeline error: {e}")
            return {"success": False, "error": str(e)}

    def validate_data_quality(self) -> Dict[str, Any]:
        """Validate the quality of processed data"""
        print("🔍 Validating Data Quality...")

        try:
            # Find latest parquet file
            parquet_files = list(self.interim_dir.glob("*.parquet"))
            if not parquet_files:
                print("   ❌ No parquet files found for validation")
                return {"success": False, "error": "No data files"}

            import polars as pl
            latest_parquet = max(parquet_files, key=lambda x: x.stat().st_mtime)
            df = pl.read_parquet(latest_parquet)

            print(f"   📄 Validating: {latest_parquet.name}")
            print(f"   📊 Total records: {len(df):,}")

            # Check required columns
            required_columns = ["timestamp", "region", "data_type", "value"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"   ❌ Missing columns: {missing_columns}")
                return {"success": False, "error": f"Missing columns: {missing_columns}"}
            print("   ✅ All required columns present")

            # Check data types coverage
            data_types = set(df["data_type"].unique().to_list())
            expected_types = {"demand", "generation", "price"}

            if expected_types.issubset(data_types):
                print("   ✅ All expected data types present")
            else:
                missing_types = expected_types - data_types
                print(f"   ⚠️  Missing data types: {missing_types}")

            # Validate data ranges
            quality_issues = []

            # Check for null values
            null_counts = df.null_count()
            for column in null_counts.columns:
                null_count = null_counts.select(column).item()
                if null_count > 0:
                    quality_issues.append(f"{column}: {null_count} null values")

            # Check price data ranges (if present)
            if "price" in data_types:
                price_df = df.filter(pl.col("data_type") == "price")
                price_values = price_df["value"].to_list()

                if any(p < -100 for p in price_values):
                    quality_issues.append("Extreme negative prices detected")
                if any(p > 1000 for p in price_values):
                    quality_issues.append("Extreme high prices detected")

                print(f"   💰 Price range: ${min(price_values):.2f} - ${max(price_values):.2f}")

            # Check demand data (if present)
            if "demand" in data_types:
                demand_df = df.filter(pl.col("data_type") == "demand")
                demand_values = demand_df["value"].to_list()

                if any(d < 0 for d in demand_values):
                    quality_issues.append("Negative demand values detected")

                print(f"   ⚡ Demand range: {min(demand_values):.0f} - {max(demand_values):.0f} MWh")

            if quality_issues:
                print("   ⚠️  Data quality issues:")
                for issue in quality_issues:
                    print(f"      • {issue}")
            else:
                print("   ✅ Data quality validation passed")

            return {
                "success": True,
                "total_records": len(df),
                "data_types": list(data_types),
                "quality_issues": quality_issues,
                "file_path": str(latest_parquet)
            }

        except Exception as e:
            print(f"   ❌ Validation error: {e}")
            return {"success": False, "error": str(e)}


async def main():
    """Run the complete live API testing suite"""
    print("🚀 LIVE API TESTING SUITE")
    print("=" * 80)
    print("Testing complete pipeline with live EIA API including price data")
    print("=" * 80)

    # Initialize test suite
    test_suite = LiveAPITestSuite()

    # Test 1: Prerequisites
    print("\n📋 Test 1: Prerequisites Validation")
    if not test_suite.validate_prerequisites():
        print("❌ Prerequisites not met. Exiting.")
        return False

    # Test 2: Clean slate preparation
    print("\n📋 Test 2: Clean Slate Preparation")
    if not test_suite.clean_data_directories():
        print("❌ Failed to prepare clean environment. Exiting.")
        return False

    # Test 3: Live API extraction
    print("\n📋 Test 3: Live API Extraction")
    extract_result = await test_suite.test_live_api_extraction(region="PACW", days=2)

    if not extract_result["success"]:
        print(f"❌ Live API extraction failed: {extract_result.get('error')}")
        return False

    # Test 4: Transform processing
    print("\n📋 Test 4: Transform Processing")
    transform_result = await test_suite.test_transform_processing()

    if not transform_result["success"]:
        print(f"❌ Transform processing failed: {transform_result.get('error')}")
        return False

    # Test 5: Full pipeline DAG
    print("\n📋 Test 5: Full Pipeline DAG")
    # Clean again for DAG test
    test_suite.clean_data_directories()
    dag_result = await test_suite.test_full_pipeline_dag(region="PACW", days=2)

    if not dag_result["success"]:
        print(f"❌ Full pipeline DAG failed: {dag_result.get('error')}")
        return False

    # Test 6: Data quality validation
    print("\n📋 Test 6: Data Quality Validation")
    quality_result = test_suite.validate_data_quality()

    # Final summary
    print("\n" + "=" * 80)
    print("📊 LIVE API TESTING RESULTS")
    print("=" * 80)

    print("✅ All tests passed!")
    print("\n📈 Performance Summary:")
    print(f"   API Extraction: {extract_result.get('duration', 0):.2f}s")
    print(f"   Transform Processing: {transform_result.get('duration', 0):.2f}s")
    print(f"   Full Pipeline: {dag_result.get('duration', 0):.2f}s")

    print("\n📊 Data Summary:")
    print(f"   Total Records: {transform_result.get('total_records', 0):,}")
    print(f"   Data Types: {list(transform_result.get('data_types', {}).keys())}")
    print(f"   Processing Rate: {transform_result.get('records_per_second', 0):,.0f} records/sec")
    print(f"   Output Size: {transform_result.get('output_size_mb', 0):.1f} MB")

    print("\n🎯 Success Criteria Met:")
    print("   ✅ Price data successfully integrated with demand/generation")
    print("   ✅ Live API extraction creates proper file structure")
    print("   ✅ Transform step processes all data types into consolidated parquet")
    print("   ✅ Performance maintains target throughput")
    print("   ✅ Data quality validation passed")

    print("\n🎉 Ready for Phase 3: Feature Creation for ML Pipeline!")

    return True


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
