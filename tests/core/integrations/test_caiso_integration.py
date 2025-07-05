"""
Tests for CAISO integration with schema validation and multi-source storage
"""
import pytest
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import tempfile
import shutil
from pathlib import Path

from src.core.integrations.caiso.client import CAISOClient
from src.core.integrations.caiso.schema import (
    CAISODemandResponse,
    CAISOGenerationResponse,
    CAISOSystemDemand,
    CAISOGeneration,
    RegionalEnergyMetrics,
    MigrationPredictorFeatures
)
from src.core.integrations.multi_source_storage import (
    MultiSourceStorageManager,
    DataSource,
    DataLayer
)


class TestCAISOSchemas:
    """Test CAISO Pydantic schemas"""

    def test_caiso_system_demand_validation(self):
        """Test system demand schema validation"""
        # Valid data
        valid_demand = CAISOSystemDemand(
            timestamp=datetime.now(),
            demand_mw=25000.0,
            region="CAISO"
        )
        assert valid_demand.demand_mw == 25000.0
        assert valid_demand.region == "CAISO"

        # Invalid data - negative demand
        with pytest.raises(ValueError):
            CAISOSystemDemand(
                timestamp=datetime.now(),
                demand_mw=-1000.0,
                region="CAISO"
            )

    def test_caiso_generation_validation(self):
        """Test generation schema validation"""
        valid_generation = CAISOGeneration(
            timestamp=datetime.now(),
            fuel_type="SOLAR",
            generation_mw=5000.0,
            region="CAISO"
        )
        assert valid_generation.fuel_type == "SOLAR"
        assert valid_generation.generation_mw == 5000.0

    def test_caiso_demand_response_from_dataframe(self):
        """Test creating demand response from DataFrame"""
        # Create test DataFrame with proper timestamp format
        df = pd.DataFrame({
            'OPR_DT': ['2024-01-01', '2024-01-01', '2024-01-01'],
            'OPR_HR': [1, 2, 3],
            'MW': [25000, 26000, 24000]
        })

        response = CAISODemandResponse.from_dataframe(df)
        assert len(response.data) == 3
        assert all(isinstance(d, CAISOSystemDemand) for d in response.data)
        assert response.data[0].demand_mw == 25000

        # Test with pre-constructed timestamps
        df_with_timestamps = pd.DataFrame({
            'timestamp': [
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 2, 0),
                datetime(2024, 1, 1, 3, 0)
            ],
            'MW': [25000, 26000, 24000]
        })

        response2 = CAISODemandResponse.from_dataframe(df_with_timestamps)
        assert len(response2.data) == 3
        assert response2.data[0].demand_mw == 25000

    def test_caiso_generation_response_from_dataframe(self):
        """Test creating generation response from DataFrame"""
        # Create test DataFrame with proper timestamp format
        df = pd.DataFrame({
            'OPR_DT': ['2024-01-01', '2024-01-01'],
            'OPR_HR': [1, 1],
            'FUEL_TYPE': ['SOLAR', 'WIND'],
            'MW': [3000, 2000]
        })

        response = CAISOGenerationResponse.from_dataframe(df)
        assert len(response.data) == 2
        assert response.data[0].fuel_type == 'SOLAR'
        assert response.data[1].fuel_type == 'WIND'

        # Test with pre-constructed timestamps
        df_with_timestamps = pd.DataFrame({
            'timestamp': [
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 1, 0)
            ],
            'FUEL_TYPE': ['SOLAR', 'WIND'],
            'MW': [3000, 2000]
        })

        response2 = CAISOGenerationResponse.from_dataframe(df_with_timestamps)
        assert len(response2.data) == 2
        assert response2.data[0].fuel_type == 'SOLAR'

    def test_regional_energy_metrics_validation(self):
        """Test regional energy metrics validation"""
        metrics = RegionalEnergyMetrics(
            region="California",
            timestamp=datetime.now(),
            total_demand_mw=25000.0,
            total_generation_mw=26000.0,
            renewable_generation_mw=8000.0,
            renewable_percentage=30.8,
            peak_demand_mw=28000.0,
            load_factor=0.89,
            economic_activity_index=25.0,
            grid_stress_index=0.12
        )

        assert metrics.region == "California"
        assert metrics.renewable_percentage == 30.8
        assert 0 <= metrics.load_factor <= 1


class TestCAISOClient:
    """Test CAISO client functionality"""

    def setup_method(self):
        """Setup test client"""
        self.client = CAISOClient()

    @patch('src.core.integrations.caiso.client.requests.Session.get')
    def test_get_real_time_demand_with_validation(self, mock_get):
        """Test real-time demand fetching with validation"""
        # Mock API response
        mock_response = MagicMock()
        mock_response.text = "OPR_DT,OPR_HR,MW\n2024-01-01,1,25000\n2024-01-01,2,26000"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Test with validation
        result = self.client.get_real_time_demand(
            start_date="20240101",
            end_date="20240101",
            validate=True
        )

        assert isinstance(result, CAISODemandResponse)
        assert len(result.data) == 2
        assert all(isinstance(d, CAISOSystemDemand) for d in result.data)

    @patch('src.core.integrations.caiso.client.requests.Session.get')
    def test_get_real_time_demand_without_validation(self, mock_get):
        """Test real-time demand fetching without validation"""
        # Mock API response
        mock_response = MagicMock()
        mock_response.text = "OPR_DT,OPR_HR,MW\n2024-01-01,1,25000\n2024-01-01,2,26000"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Test without validation
        result = self.client.get_real_time_demand(
            start_date="20240101",
            end_date="20240101",
            validate=False
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch('src.core.integrations.caiso.client.requests.Session.get')
    def test_get_generation_mix(self, mock_get):
        """Test generation mix fetching"""
        # Mock API response
        mock_response = MagicMock()
        mock_response.text = "OPR_DT,OPR_HR,FUEL_TYPE,MW\n2024-01-01,1,SOLAR,3000\n2024-01-01,1,WIND,2000"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.client.get_generation_mix(
            start_date="20240101",
            end_date="20240101",
            validate=True
        )

        assert isinstance(result, CAISOGenerationResponse)
        assert len(result.data) == 2
        assert result.data[0].fuel_type == 'SOLAR'
        assert result.data[1].fuel_type == 'WIND'

    @patch('src.core.integrations.caiso.client.CAISOClient.get_real_time_demand')
    @patch('src.core.integrations.caiso.client.CAISOClient.get_generation_mix')
    def test_get_regional_metrics(self, mock_gen, mock_demand):
        """Test regional metrics calculation"""
        # Mock demand response
        demand_data = [
            CAISOSystemDemand(
                timestamp=datetime(2024, 1, 1, 12, 0),
                demand_mw=25000.0
            ),
            CAISOSystemDemand(
                timestamp=datetime(2024, 1, 1, 13, 0),
                demand_mw=26000.0
            )
        ]
        mock_demand.return_value = CAISODemandResponse(data=demand_data)

        # Mock generation response
        gen_data = [
            CAISOGeneration(
                timestamp=datetime(2024, 1, 1, 12, 0),
                fuel_type="SOLAR",
                generation_mw=5000.0
            ),
            CAISOGeneration(
                timestamp=datetime(2024, 1, 1, 12, 0),
                fuel_type="WIND",
                generation_mw=3000.0
            )
        ]
        mock_gen.return_value = CAISOGenerationResponse(data=gen_data)

        metrics = self.client.get_regional_metrics(
            start_date="20240101",
            end_date="20240101"
        )

        assert len(metrics) > 0
        assert all(isinstance(m, RegionalEnergyMetrics) for m in metrics)
        assert all(m.region == "California" for m in metrics)

    def test_error_handling(self):
        """Test error handling in client"""
        with patch('src.core.integrations.caiso.client.requests.Session.get') as mock_get:
            mock_get.side_effect = Exception("Network error")

            result = self.client.get_real_time_demand(validate=True)
            assert isinstance(result, CAISODemandResponse)
            assert len(result.data) == 0


class TestMultiSourceStorageManager:
    """Test multi-source storage manager"""

    def setup_method(self):
        """Setup test storage manager"""
        self.temp_dir = tempfile.mkdtemp()
        self.storage_manager = MultiSourceStorageManager(self.temp_dir)

    def teardown_method(self):
        """Cleanup test directory"""
        shutil.rmtree(self.temp_dir)

    def test_directory_structure_creation(self):
        """Test directory structure creation"""
        base_path = Path(self.temp_dir)

        # Check that all layer directories exist
        for layer in DataLayer:
            layer_path = base_path / layer.value
            assert layer_path.exists()
            assert layer_path.is_dir()

            # Check that all source directories exist
            for source in DataSource:
                source_path = layer_path / source.value
                assert source_path.exists()
                assert source_path.is_dir()

    def test_save_and_load_raw_data(self):
        """Test saving and loading raw data"""
        # Create test data
        test_data = pl.DataFrame({
            'timestamp': [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 13, 0)],
            'demand_mw': [25000.0, 26000.0],
            'region': ['California', 'California']
        })

        # Save data
        file_path = self.storage_manager.save_raw_data(
            data=test_data,
            source=DataSource.CAISO,
            region="California",
            data_type="demand",
            timestamp=datetime(2024, 1, 1)
        )

        assert file_path.exists()
        assert file_path.suffix == '.parquet'

        # Load data
        loaded_data = self.storage_manager.load_raw_data(
            source=DataSource.CAISO,
            region="California",
            data_type="demand"
        )

        assert len(loaded_data) == 2
        assert 'timestamp' in loaded_data.columns
        assert 'demand_mw' in loaded_data.columns

    def test_data_harmonization(self):
        """Test data harmonization across sources"""
        # Create EIA-style data
        eia_data = pl.DataFrame({
            'period': [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 13, 0)],
            'value': [25000.0, 26000.0],
            'units': ['MWh', 'MWh']
        })

        # Create CAISO-style data
        caiso_data = pl.DataFrame({
            'timestamp': [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 1, 13, 0)],
            'demand_mw': [24000.0, 25000.0],
            'region': ['California', 'California']
        })

        # Harmonize data
        source_data = {
            DataSource.EIA: eia_data,
            DataSource.CAISO: caiso_data
        }

        harmonized_data = self.storage_manager.harmonize_data(
            source_data=source_data,
            region="California",
            data_type="demand"
        )

        assert len(harmonized_data) == 4  # 2 from each source
        assert 'timestamp' in harmonized_data.columns
        assert 'data_source' in harmonized_data.columns
        assert 'value' in harmonized_data.columns

        # Check that both sources are represented
        sources = harmonized_data['data_source'].unique().to_list()
        assert 'EIA' in sources
        assert 'CAISO' in sources

    def test_migration_features_creation(self):
        """Test migration features creation"""
        # Create test harmonized data for multiple regions
        test_data = pl.DataFrame({
            'timestamp': [datetime(2024, 1, 1, 12, 0)] * 4,
            'region': ['California', 'California', 'Oregon', 'Oregon'],
            'data_source': ['CAISO', 'CAISO', 'EIA', 'EIA'],
            'value': [25000.0, 26000.0, 15000.0, 16000.0],
            'unit': ['MW', 'MW', 'MW', 'MW'],
            'data_type': ['demand', 'demand', 'demand', 'demand']
        })

        # Save harmonized data
        for region in ['California', 'Oregon']:
            region_data = test_data.filter(pl.col('region') == region)
            self.storage_manager.save_harmonized_data(
                data=region_data,
                region=region,
                data_type="demand",
                timestamp=datetime(2024, 1, 1)
            )

        # Create migration features
        features = self.storage_manager.create_migration_features(
            regions=['California', 'Oregon'],
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2)
        )

        assert len(features) > 0
        assert 'region_from' in features.columns
        assert 'region_to' in features.columns
        assert 'avg_differential' in features.columns

    def test_get_available_datasets(self):
        """Test getting available datasets"""
        # Create and save test data
        test_data = pl.DataFrame({
            'timestamp': [datetime(2024, 1, 1, 12, 0)],
            'value': [25000.0],
            'region': ['California']
        })

        self.storage_manager.save_raw_data(
            data=test_data,
            source=DataSource.CAISO,
            region="California",
            data_type="demand",
            timestamp=datetime(2024, 1, 1)
        )

        # Get available datasets
        datasets = self.storage_manager.get_available_datasets()

        assert len(datasets) >= 1
        assert any(d['source'] == 'caiso' for d in datasets)
        assert any(d['region'] == 'California' for d in datasets)
        assert any(d['data_type'] == 'demand' for d in datasets)


class TestIntegrationScenarios:
    """Test integration scenarios for migration analysis"""

    def setup_method(self):
        """Setup integration test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.storage_manager = MultiSourceStorageManager(self.temp_dir)
        self.client = CAISOClient()

    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.temp_dir)

    @patch('src.core.integrations.caiso.client.requests.Session.get')
    def test_end_to_end_migration_analysis(self, mock_get):
        """Test complete end-to-end migration analysis workflow"""
        # Mock CAISO API responses
        mock_response = MagicMock()
        mock_response.text = "OPR_DT,OPR_HR,MW\n2024-01-01,1,25000\n2024-01-01,2,26000"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Step 1: Fetch CAISO data
        caiso_demand = self.client.get_real_time_demand(
            start_date="20240101",
            end_date="20240101",
            validate=False
        )

        assert isinstance(caiso_demand, pd.DataFrame)
        assert len(caiso_demand) == 2

        # Step 2: Convert to Polars and save
        caiso_pl = pl.from_pandas(caiso_demand)

        # Create proper timestamps based on OPR_DT and OPR_HR
        caiso_pl = caiso_pl.with_columns([
            pl.datetime(
                year=2024,
                month=1,
                day=1,
                hour=pl.col('OPR_HR')
            ).alias('timestamp')
        ])

        # Add required columns for harmonization
        caiso_pl = caiso_pl.with_columns([
            pl.col('MW').alias('demand_mw'),
            pl.lit('California').alias('region')
        ])

        # Step 3: Save raw data
        file_path = self.storage_manager.save_raw_data(
            data=caiso_pl,
            source=DataSource.CAISO,
            region="California",
            data_type="demand",
            timestamp=datetime(2024, 1, 1)
        )

        assert file_path.exists()

        # Step 4: Create synthetic EIA data for comparison
        eia_data = pl.DataFrame({
            'period': [datetime(2024, 1, 1, 1, 0), datetime(2024, 1, 1, 2, 0)],
            'value': [15000.0, 16000.0],
            'units': ['MWh', 'MWh']
        })

        self.storage_manager.save_raw_data(
            data=eia_data,
            source=DataSource.EIA,
            region="Oregon",
            data_type="demand",
            timestamp=datetime(2024, 1, 1)
        )

        # Step 5: Harmonize data
        source_data = {
            DataSource.CAISO: caiso_pl,
            DataSource.EIA: eia_data
        }

        harmonized_data = self.storage_manager.harmonize_data(
            source_data=source_data,
            region="Multi-region",
            data_type="demand"
        )

        assert len(harmonized_data) == 4
        assert 'data_source' in harmonized_data.columns

        # Step 6: Verify available datasets
        datasets = self.storage_manager.get_available_datasets()
        assert len(datasets) >= 2

        sources = [d['source'] for d in datasets]
        assert 'caiso' in sources
        assert 'eia' in sources

    def test_migration_predictor_features_validation(self):
        """Test migration predictor features validation"""
        features = MigrationPredictorFeatures(
            region_from="Oregon",
            region_to="California",
            timestamp=datetime(2024, 1, 1),
            demand_ratio=1.5,
            price_differential=25.0,
            renewable_differential=15.0,
            economic_opportunity_ratio=1.2,
            infrastructure_development_index=0.8,
            climate_adaptability_score=0.9,
            energy_efficiency_ratio=1.1,
            demand_trend_12m=0.05,
            price_trend_12m=0.02
        )

        assert features.region_from == "Oregon"
        assert features.region_to == "California"
        assert features.demand_ratio == 1.5
        assert features.price_differential == 25.0

        # Test JSON serialization with model_dump_json (Pydantic v2)
        features_json = features.model_dump_json()
        assert isinstance(features_json, str)
        assert "Oregon" in features_json
        assert "California" in features_json


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
