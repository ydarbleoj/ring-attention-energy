"""
Test suite for mapping EIA data coverage by region and year.

This module implements Step 1 Action Items 1 & 2:
- Test data availability for major regions (PACW, ERCO, CAL, TEX, MISO)
- Map earliest available hourly data for each region
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
from pathlib import Path
import logging
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.integrations.eia.client import EIAClient

logger = logging.getLogger(__name__)


class DataCoverageMapper:
    """
    Maps EIA data availability by region and year for strategic loading optimization.

    Implements modular, event-driven architecture for data coverage analysis.
    """

    def __init__(self, eia_client: EIAClient):
        """Initialize with EIA client instance."""
        self.eia_client = eia_client
        self.coverage_results = {}

    def test_region_year_availability(
        self,
        region: str,
        year: int,
        month: int = 1,
        sample_days: int = 7
    ) -> Dict[str, any]:
        """
        Test data availability for a specific region and year.

        Args:
            region: EIA region code (PACW, ERCO, CAL, TEX, MISO)
            year: Year to test (e.g., 2007, 2008, 2009)
            month: Month to sample (default: January)
            sample_days: Number of days to sample (default: 7)

        Returns:
            Dict with availability metrics and data sample
        """
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{sample_days:02d}"

        logger.info(f"Testing data availability: {region} for {start_date} to {end_date}")

        try:
            # Attempt to fetch sample data
            data = self.eia_client.get_electricity_demand(
                region=region,
                start_date=start_date,
                end_date=end_date
            )

            # Analyze the response
            result = {
                'region': region,
                'year': year,
                'sample_period': f"{start_date} to {end_date}",
                'data_available': len(data) > 0,
                'record_count': len(data),
                'first_timestamp': data.iloc[0]['timestamp'] if len(data) > 0 else None,
                'last_timestamp': data.iloc[-1]['timestamp'] if len(data) > 0 else None,
                'avg_hourly_records_per_day': len(data) / sample_days if len(data) > 0 else 0,
                'data_quality_score': self._calculate_data_quality_score(data),
                'status': 'SUCCESS'
            }

        except Exception as e:
            logger.warning(f"Data unavailable for {region} {year}: {str(e)}")
            result = {
                'region': region,
                'year': year,
                'sample_period': f"{start_date} to {end_date}",
                'data_available': False,
                'record_count': 0,
                'first_timestamp': None,
                'last_timestamp': None,
                'avg_hourly_records_per_day': 0,
                'data_quality_score': 0.0,
                'status': 'ERROR',
                'error_message': str(e)
            }

        return result

    def _calculate_data_quality_score(self, data: pd.DataFrame) -> float:
        """
        Calculate data quality score based on completeness and consistency.

        Args:
            data: DataFrame with electricity demand data

        Returns:
            Quality score between 0.0 and 1.0
        """
        if len(data) == 0:
            return 0.0

        # Check for null values
        non_null_ratio = 1 - (data.isnull().sum().sum() / (len(data) * len(data.columns)))

        # Check for reasonable value ranges (basic sanity check)
        if 'value' in data.columns:
            value_col = data['value']
            # Electricity demand should be positive and within reasonable bounds
            reasonable_values = ((value_col > 0) & (value_col < 100000)).sum() / len(value_col)
        else:
            reasonable_values = 1.0

        # Overall quality score
        quality_score = (non_null_ratio + reasonable_values) / 2
        return round(quality_score, 3)

    def map_region_earliest_data(
        self,
        region: str,
        start_year: int = 2000,
        end_year: int = 2010
    ) -> Dict[str, any]:
        """
        Find the earliest available data year for a specific region.

        Args:
            region: EIA region code
            start_year: Year to start searching from
            end_year: Year to stop searching at

        Returns:
            Dict with earliest data availability information
        """
        logger.info(f"Mapping earliest data availability for {region} ({start_year}-{end_year})")

        earliest_year = None
        year_results = []

        for year in range(start_year, end_year + 1):
            result = self.test_region_year_availability(region, year)
            year_results.append(result)

            if result['data_available'] and earliest_year is None:
                earliest_year = year
                logger.info(f"Found earliest data for {region}: {year}")

        return {
            'region': region,
            'search_range': f"{start_year}-{end_year}",
            'earliest_available_year': earliest_year,
            'earliest_available': earliest_year is not None,
            'yearly_results': year_results,
            'summary': self._generate_region_summary(region, year_results)
        }

    def _generate_region_summary(self, region: str, year_results: List[Dict]) -> Dict[str, any]:
        """Generate summary statistics for a region's data coverage."""
        available_years = [r['year'] for r in year_results if r['data_available']]
        total_records = sum(r['record_count'] for r in year_results)
        avg_quality = sum(r['data_quality_score'] for r in year_results) / len(year_results)

        return {
            'total_years_tested': len(year_results),
            'years_with_data': len(available_years),
            'data_coverage_percentage': len(available_years) / len(year_results) * 100,
            'total_sample_records': total_records,
            'average_data_quality': round(avg_quality, 3),
            'available_year_range': f"{min(available_years)}-{max(available_years)}" if available_years else "No data"
        }


@pytest.fixture
def eia_client():
    """Fixture providing EIA client for testing."""
    return EIAClient()


@pytest.fixture
def coverage_mapper(eia_client):
    """Fixture providing data coverage mapper."""
    return DataCoverageMapper(eia_client)


class TestDataCoverageMapping:
    """Test suite for EIA data coverage mapping functionality."""

    MAJOR_REGIONS = ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO']

    def test_single_region_year_availability(self, coverage_mapper):
        """Test data availability for a single region/year combination."""
        # Test known problematic region/year from handoff prompt
        result = coverage_mapper.test_region_year_availability('PACW', 2005)

        assert 'region' in result
        assert 'year' in result
        assert 'data_available' in result
        assert 'record_count' in result
        assert 'data_quality_score' in result

        logger.info(f"PACW 2005 availability: {result['data_available']}")
        logger.info(f"Record count: {result['record_count']}")

    def test_known_good_region_year(self, coverage_mapper):
        """Test data availability for a known good region/year."""
        # Test recent year that should have data
        result = coverage_mapper.test_region_year_availability('CAL', 2020)

        assert result['data_available'] is True
        assert result['record_count'] > 0
        assert result['data_quality_score'] > 0

        logger.info(f"CAL 2020 - Records: {result['record_count']}, Quality: {result['data_quality_score']}")

    @pytest.mark.parametrize("region", MAJOR_REGIONS)
    def test_region_earliest_data_mapping(self, coverage_mapper, region):
        """Map earliest available data for each major region."""
        # Test critical range around 2007 (suspected transition point)
        result = coverage_mapper.map_region_earliest_data(
            region=region,
            start_year=2005,
            end_year=2010
        )

        assert 'region' in result
        assert 'earliest_available_year' in result
        assert 'yearly_results' in result
        assert 'summary' in result

        logger.info(f"{region} earliest data year: {result['earliest_available_year']}")
        logger.info(f"{region} coverage: {result['summary']['data_coverage_percentage']:.1f}%")

        # Store result for comprehensive mapping
        coverage_mapper.coverage_results[region] = result

    def test_comprehensive_coverage_mapping(self, coverage_mapper):
        """Generate comprehensive data coverage mapping for all major regions."""
        comprehensive_results = {}

        for region in self.MAJOR_REGIONS:
            logger.info(f"Mapping comprehensive coverage for {region}")
            result = coverage_mapper.map_region_earliest_data(
                region=region,
                start_year=2005,
                end_year=2012  # Extended range for thorough mapping
            )
            comprehensive_results[region] = result

        # Generate cross-region analysis
        cross_region_analysis = self._analyze_cross_region_patterns(comprehensive_results)

        # Save results for strategic planning
        self._save_coverage_mapping_results(comprehensive_results, cross_region_analysis)

        # Validate we have actionable insights
        assert len(comprehensive_results) == len(self.MAJOR_REGIONS)

        for region, result in comprehensive_results.items():
            if result['earliest_available']:
                logger.info(f"✅ {region}: Data available from {result['earliest_available_year']}")
            else:
                logger.warning(f"❌ {region}: No data found in tested range")

    def _analyze_cross_region_patterns(self, results: Dict[str, Dict]) -> Dict[str, any]:
        """Analyze patterns across all regions for strategic insights."""
        earliest_years = []
        coverage_percentages = []

        for region, result in results.items():
            if result['earliest_available']:
                earliest_years.append(result['earliest_available_year'])
                coverage_percentages.append(result['summary']['data_coverage_percentage'])

        return {
            'global_earliest_year': min(earliest_years) if earliest_years else None,
            'average_coverage_percentage': sum(coverage_percentages) / len(coverage_percentages) if coverage_percentages else 0,
            'regions_with_data': len(earliest_years),
            'recommended_start_year': min(earliest_years) if earliest_years else 2010,
            'high_coverage_regions': [
                region for region, result in results.items()
                if result['earliest_available'] and result['summary']['data_coverage_percentage'] > 80
            ]
        }

    def _save_coverage_mapping_results(
        self,
        comprehensive_results: Dict[str, Dict],
        cross_region_analysis: Dict[str, any]
    ):
        """Save coverage mapping results for strategic planning."""
        output_data = {
            'generation_timestamp': datetime.now().isoformat(),
            'purpose': 'EIA Data Coverage Mapping for Strategic Historical Loading',
            'regions_analyzed': list(comprehensive_results.keys()),
            'cross_region_analysis': cross_region_analysis,
            'detailed_results': comprehensive_results,
            'strategic_recommendations': {
                'recommended_start_year': cross_region_analysis['recommended_start_year'],
                'priority_regions': cross_region_analysis['high_coverage_regions'],
                'data_loading_strategy': self._generate_loading_strategy(cross_region_analysis, comprehensive_results)
            }
        }

        # Save to data directory
        output_path = Path('/Users/joelbrady/ring-attention-energy/data/processed/eia_data_coverage_mapping.json')
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)

        logger.info(f"Coverage mapping results saved to: {output_path}")

    def _generate_loading_strategy(
        self,
        cross_region_analysis: Dict[str, any],
        detailed_results: Dict[str, Dict]
    ) -> Dict[str, any]:
        """Generate strategic data loading recommendations."""
        return {
            'phase_1_regions': cross_region_analysis['high_coverage_regions'][:3],  # Top 3 regions
            'phase_1_date_range': f"{cross_region_analysis['recommended_start_year']}-01-01 to 2025-01-01",
            'phase_2_regions': [
                region for region in detailed_results.keys()
                if region not in cross_region_analysis['high_coverage_regions']
            ],
            'expected_data_volume': 'High - based on coverage analysis',
            'batch_configuration': 'Use Ultimate Optimized Configuration (45-day batches, 2 concurrent)',
            'monitoring_focus': 'Maintain 3,000+ RPS, 95%+ success rate'
        }


if __name__ == "__main__":
    """Direct execution for rapid coverage mapping."""
    import sys
    import os

    # Add src to path for imports
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../../'))

    # Initialize and run coverage mapping
    client = EIAClient()
    mapper = DataCoverageMapper(client)

    print("🔍 Starting EIA Data Coverage Mapping...")
    print("=" * 60)

    # Quick test of problematic region from handoff prompt
    print("\n📊 Testing PACW 2005-2007 (known issue range)...")
    for year in [2005, 2006, 2007]:
        result = mapper.test_region_year_availability('PACW', year)
        status = "✅ Available" if result['data_available'] else "❌ Not Available"
        print(f"PACW {year}: {status} ({result['record_count']} records)")

    print("\n🚀 Mapping earliest data for all major regions...")
    comprehensive_results = {}

    for region in ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO']:
        print(f"\nAnalyzing {region}...")
        result = mapper.map_region_earliest_data(region, 2005, 2010)
        comprehensive_results[region] = result

        if result['earliest_available']:
            print(f"  ✅ Earliest data: {result['earliest_available_year']}")
            print(f"  📈 Coverage: {result['summary']['data_coverage_percentage']:.1f}%")
        else:
            print(f"  ❌ No data found in 2005-2010 range")

    print("\n💾 Saving comprehensive results...")
    test_instance = TestDataCoverageMapping()
    cross_analysis = test_instance._analyze_cross_region_patterns(comprehensive_results)
    test_instance._save_coverage_mapping_results(comprehensive_results, cross_analysis)

    print(f"\n🎯 Strategic Recommendations:")
    print(f"  Recommended start year: {cross_analysis['recommended_start_year']}")
    print(f"  High coverage regions: {', '.join(cross_analysis['high_coverage_regions'])}")
    print(f"  Regions with data: {cross_analysis['regions_with_data']}/5")

    print("\n✅ Data coverage mapping complete!")
