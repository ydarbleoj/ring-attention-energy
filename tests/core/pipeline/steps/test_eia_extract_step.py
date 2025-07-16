"""
Test EIAExtractStep implementation.

Tests the EIA data extraction step to ensure it properly follows the BaseStep
interface while maintaining all existing functionality from EIACollector.
"""

import pytest
import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil

from src.core.pipeline.steps.extract import EIAExtractStep, EIAExtractStepConfig
from src.core.pipeline.steps.base import StepOutput


class TestEIAExtractStepConfig:
    """Test EIA extract step configuration validation."""

    def test_valid_config(self):
        """Test creating valid EIA extract step config."""
        config = EIAExtractStepConfig(
            step_name="EIA Extract Test",
            step_id="eia_extract_test",
            api_key="test_api_key",
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["PACW", "ERCO"],
            delay_between_operations=0.2  # Explicitly set
        )

        assert config.step_name == "EIA Extract Test"
        assert config.api_key == "test_api_key"
        assert config.data_type == "demand"
        assert config.regions == ["PACW", "ERCO"]
        assert config.delay_between_operations == 0.2

    def test_default_regions(self):
        """Test default regions are provided."""
        config = EIAExtractStepConfig(
            step_name="EIA Extract Test",
            step_id="eia_extract_test",
            api_key="test_api_key",
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-02"
        )

        # Should have default regions
        assert len(config.regions) > 0
        assert "PACW" in config.regions
        assert "ERCO" in config.regions

    def test_invalid_data_type(self):
        """Test validation fails for invalid data type."""
        with pytest.raises(ValueError, match="data_type must be 'demand' or 'generation'"):
            EIAExtractStepConfig(
                step_name="EIA Extract Test",
                step_id="eia_extract_test",
                api_key="test_api_key",
                data_type="invalid_type",
                start_date="2024-01-01",
                end_date="2024-01-02"
            )

    def test_invalid_date_format(self):
        """Test validation fails for invalid date format."""
        with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
            EIAExtractStepConfig(
                step_name="EIA Extract Test",
                step_id="eia_extract_test",
                api_key="test_api_key",
                data_type="demand",
                start_date="01/01/2024",  # Invalid format
                end_date="2024-01-02"
            )

    def test_empty_regions(self):
        """Test validation fails for empty regions list."""
        with pytest.raises(ValueError, match="regions list cannot be empty"):
            EIAExtractStepConfig(
                step_name="EIA Extract Test",
                step_id="eia_extract_test",
                api_key="test_api_key",
                data_type="demand",
                start_date="2024-01-01",
                end_date="2024-01-02",
                regions=[]
            )

    def test_negative_delay(self):
        """Test validation fails for negative delay."""
        with pytest.raises(ValueError, match="delay_between_operations must be non-negative"):
            EIAExtractStepConfig(
                step_name="EIA Extract Test",
                step_id="eia_extract_test",
                api_key="test_api_key",
                data_type="demand",
                start_date="2024-01-01",
                end_date="2024-01-02",
                delay_between_operations=-1.0
            )


class TestEIAExtractStep:
    """Test EIA extract step execution."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def valid_config(self, temp_dir):
        """Create valid configuration for testing."""
        return EIAExtractStepConfig(
            step_name="Test EIA Extract",
            step_id="test_eia_extract",
            api_key="test_api_key_12345",
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["PACW", "ERCO"],
            output_dir=temp_dir,
            raw_data_path=str(temp_dir / "raw")
        )

    @pytest.fixture
    def mock_api_response(self):
        """Mock EIA API response."""
        return {
            "response": {
                "data": [
                    {
                        "period": "2024-01-01T00:00:00Z",
                        "respondent": "PACW",
                        "respondent-name": "PacifiCorp West",
                        "type-name": "Demand",
                        "value": 1234.5
                    },
                    {
                        "period": "2024-01-01T01:00:00Z",
                        "respondent": "ERCO",
                        "respondent-name": "Electric Reliability Council of Texas",
                        "type-name": "Demand",
                        "value": 2345.6
                    }
                ],
                "total": 2
            }
        }

    def test_step_initialization(self, valid_config):
        """Test step initializes correctly."""
        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService'), \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader'):

            step = EIAExtractStep(valid_config)

            assert step.config == valid_config
            assert step.config.api_key == "test_api_key_12345"
            assert step.config.data_type == "demand"

    def test_validate_input_success(self, valid_config):
        """Test input validation succeeds for valid config."""
        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService'), \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader'):

            step = EIAExtractStep(valid_config)
            # Should not raise any exception
            step.validate_input(valid_config)

    def test_validate_input_empty_api_key(self, temp_dir):
        """Test input validation fails for empty API key."""
        config = EIAExtractStepConfig(
            step_name="Test EIA Extract",
            step_id="test_eia_extract",
            api_key="",  # Empty API key
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-02",
            regions=["PACW", "ERCO"],
            output_dir=temp_dir,
            raw_data_path=str(temp_dir / "raw")
        )

        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService'), \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader'):

            # Expect validation error during initialization
            with pytest.raises(ValueError, match="api_key is required and cannot be empty"):
                EIAExtractStep(config)

    @patch('src.core.pipeline.steps.extract.eia_extract.time.sleep')
    def test_successful_extraction(self, mock_sleep, valid_config, mock_api_response, temp_dir):
        """Test successful data extraction."""
        # Mock file path for saved data
        expected_file_path = temp_dir / "raw" / "test_file.json"

        # Mock metadata for saved file
        mock_saved_metadata = {
            "record_count": 2,
            "response_size_bytes": 1024,
            "timestamp": 1640995200.0,
            "success": True
        }

        # Mock raw package response
        mock_raw_package = {
            "data": mock_api_response,
            "metadata": mock_saved_metadata
        }

        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService') as mock_eia_service, \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader') as mock_raw_loader:

            # Setup mocks
            mock_service_instance = Mock()
            mock_service_instance.get_raw_data.return_value = mock_api_response
            mock_eia_service.return_value = mock_service_instance

            mock_loader_instance = Mock()
            mock_loader_instance.save_raw_data.return_value = expected_file_path
            mock_loader_instance.load_raw_file.return_value = mock_raw_package
            mock_raw_loader.return_value = mock_loader_instance

            # Execute step
            step = EIAExtractStep(valid_config)
            result = step.run()

            # Verify result structure
            assert isinstance(result, StepOutput)
            assert result.success is True
            assert result.step_id == "test_eia_extract"
            assert len(result.output_paths) == 1
            assert result.output_paths[0] == expected_file_path

            # Verify metrics
            assert result.metrics.records_processed == 2
            assert result.metrics.bytes_processed == 1024
            assert result.metrics.api_calls_made == 1
            assert result.metrics.files_created == 1
            assert result.metrics.duration_seconds > 0

            # Verify metadata
            assert result.metadata['success'] is True
            assert result.metadata['data_type'] == "demand"
            assert result.metadata['regions_processed'] == ["PACW", "ERCO"]
            assert result.metadata['total_records'] == 2

            # Verify API service was called correctly
            mock_service_instance.get_raw_data.assert_called_once_with(
                data_type="demand",
                regions=["PACW", "ERCO"],
                start_date="2024-01-01",
                end_date="2024-01-02"
            )

            # Verify raw data was saved
            mock_loader_instance.save_raw_data.assert_called_once()
            mock_loader_instance.load_raw_file.assert_called_once_with(expected_file_path)

            # Verify rate limiting was applied (use actual config value)
            mock_sleep.assert_called_once_with(valid_config.delay_between_operations)

    def test_extraction_with_zero_delay(self, valid_config, mock_api_response, temp_dir):
        """Test extraction with zero delay doesn't call sleep."""
        valid_config.delay_between_operations = 0.0

        expected_file_path = temp_dir / "raw" / "test_file.json"
        mock_saved_metadata = {
            "record_count": 1,
            "response_size_bytes": 512,
            "timestamp": 1640995200.0,
            "success": True
        }
        mock_raw_package = {
            "data": mock_api_response,
            "metadata": mock_saved_metadata
        }

        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService') as mock_eia_service, \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader') as mock_raw_loader, \
             patch('src.core.pipeline.steps.extract.eia_extract.time.sleep') as mock_sleep:

            # Setup mocks
            mock_service_instance = Mock()
            mock_service_instance.get_raw_data.return_value = mock_api_response
            mock_eia_service.return_value = mock_service_instance

            mock_loader_instance = Mock()
            mock_loader_instance.save_raw_data.return_value = expected_file_path
            mock_loader_instance.load_raw_file.return_value = mock_raw_package
            mock_raw_loader.return_value = mock_loader_instance

            # Execute step
            step = EIAExtractStep(valid_config)
            result = step.run()

            # Verify no sleep was called
            mock_sleep.assert_not_called()
            assert result.success is True

    def test_extraction_failure(self, valid_config):
        """Test handling of extraction failure."""
        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService') as mock_eia_service, \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader') as mock_raw_loader:

            # Setup mocks to raise exception
            mock_service_instance = Mock()
            mock_service_instance.get_raw_data.side_effect = Exception("API Error")
            mock_eia_service.return_value = mock_service_instance

            mock_loader_instance = Mock()
            mock_raw_loader.return_value = mock_loader_instance

            # Execute step
            step = EIAExtractStep(valid_config)
            result = step.run()

            # Verify failure handling
            assert isinstance(result, StepOutput)
            assert result.success is False
            assert len(result.errors) == 1
            assert "Failed to extract demand data" in result.errors[0]
            assert "API Error" in result.errors[0]
            assert result.metrics.duration_seconds > 0

    def test_dry_run_mode(self, valid_config):
        """Test dry run mode doesn't execute extraction."""
        valid_config.dry_run = True

        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService') as mock_eia_service, \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader') as mock_raw_loader:

            mock_service_instance = Mock()
            mock_eia_service.return_value = mock_service_instance

            mock_loader_instance = Mock()
            mock_raw_loader.return_value = mock_loader_instance

            # Execute step in dry run mode
            step = EIAExtractStep(valid_config)
            result = step.run()

            # Verify dry run behavior
            assert result.success is True
            assert result.metadata.get("dry_run") is True
            assert result.metrics.duration_seconds == 0.0

            # Verify no actual API calls were made
            mock_service_instance.get_raw_data.assert_not_called()
            mock_loader_instance.save_raw_data.assert_not_called()

    def test_generation_data_type(self, valid_config, mock_api_response, temp_dir):
        """Test extraction works for generation data type."""
        valid_config.data_type = "generation"

        expected_file_path = temp_dir / "raw" / "test_file.json"
        mock_saved_metadata = {
            "record_count": 5,
            "response_size_bytes": 2048,
            "timestamp": 1640995200.0,
            "success": True
        }
        mock_raw_package = {
            "data": mock_api_response,
            "metadata": mock_saved_metadata
        }

        with patch('src.core.pipeline.steps.extract.eia_extract.EIADataService') as mock_eia_service, \
             patch('src.core.pipeline.steps.extract.eia_extract.RawDataLoader') as mock_raw_loader, \
             patch('src.core.pipeline.steps.extract.eia_extract.time.sleep'):

            # Setup mocks
            mock_service_instance = Mock()
            mock_service_instance.get_raw_data.return_value = mock_api_response
            mock_eia_service.return_value = mock_service_instance

            mock_loader_instance = Mock()
            mock_loader_instance.save_raw_data.return_value = expected_file_path
            mock_loader_instance.load_raw_file.return_value = mock_raw_package
            mock_raw_loader.return_value = mock_loader_instance

            # Execute step
            step = EIAExtractStep(valid_config)
            result = step.run()

            # Verify generation-specific behavior
            assert result.success is True
            assert result.metadata['data_type'] == "generation"

            # Verify API was called with generation data type
            mock_service_instance.get_raw_data.assert_called_once_with(
                data_type="generation",
                regions=["PACW", "ERCO"],
                start_date="2024-01-01",
                end_date="2024-01-02"
            )
