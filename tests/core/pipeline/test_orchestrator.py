"""Test the date chunking and batch configuration logic."""

import pytest
from datetime import date
from src.core.pipeline.orchestrator import DataLoadOrchestrator, BatchConfig
from src.core.pipeline.config import EnergyPipelineConfig


class TestDateChunking:
    """Test date chunking logic for different batch sizes."""

    def test_eia_monthly_chunks(self):
        """Test EIA generates 1-month chunks."""
        config = EnergyPipelineConfig()
        orchestrator = DataLoadOrchestrator(config)

        # Test 3 months should generate 3 chunks for EIA
        start_date = date(2023, 1, 1)
        end_date = date(2023, 3, 31)

        chunks = orchestrator.generate_date_chunks(start_date, end_date, "eia")

        assert len(chunks) == 3
        assert chunks[0] == (date(2023, 1, 1), date(2023, 1, 30))  # 30-day chunk
        assert chunks[1] == (date(2023, 1, 31), date(2023, 3, 1))   # 30-day chunk
        assert chunks[2] == (date(2023, 3, 2), date(2023, 3, 31))   # Remaining days

    def test_caiso_quarterly_chunks(self):
        """Test CAISO generates 3-month chunks."""
        config = EnergyPipelineConfig()
        orchestrator = DataLoadOrchestrator(config)

        # Test 6 months should generate 2 chunks for CAISO
        start_date = date(2023, 1, 1)
        end_date = date(2023, 6, 30)

        chunks = orchestrator.generate_date_chunks(start_date, end_date, "caiso")

        assert len(chunks) == 2
        assert chunks[0] == (date(2023, 1, 1), date(2023, 3, 31))  # 90-day chunk
        assert chunks[1] == (date(2023, 4, 1), date(2023, 6, 30))  # 90-day chunk

    def test_custom_batch_config(self):
        """Test custom batch configuration."""
        config = EnergyPipelineConfig()
        orchestrator = DataLoadOrchestrator(config)

        # Add custom collector with 7-day batches
        custom_config = BatchConfig("test_collector", batch_size_days=7)
        orchestrator.batch_configs["test_collector"] = custom_config

        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 21)  # 21 days

        chunks = orchestrator.generate_date_chunks(start_date, end_date, "test_collector")

        assert len(chunks) == 3
        assert chunks[0] == (date(2023, 1, 1), date(2023, 1, 7))   # 7-day chunk
        assert chunks[1] == (date(2023, 1, 8), date(2023, 1, 14))  # 7-day chunk
        assert chunks[2] == (date(2023, 1, 15), date(2023, 1, 21)) # 7-day chunk

    def test_large_date_range(self):
        """Test chunking for large date ranges (2000-2024)."""
        config = EnergyPipelineConfig()
        orchestrator = DataLoadOrchestrator(config)

        # Test the full range you want to load
        start_date = date(2000, 1, 1)
        end_date = date(2024, 12, 31)

        eia_chunks = orchestrator.generate_date_chunks(start_date, end_date, "eia")
        caiso_chunks = orchestrator.generate_date_chunks(start_date, end_date, "caiso")

        # EIA: ~25 years * 12 months = ~300 chunks
        assert len(eia_chunks) > 290
        assert len(eia_chunks) < 310

        # CAISO: ~25 years * 4 quarters = ~100 chunks
        assert len(caiso_chunks) > 95
        assert len(caiso_chunks) < 105

        # Verify no gaps in chunks
        for i in range(len(eia_chunks) - 1):
            current_end = eia_chunks[i][1]
            next_start = eia_chunks[i + 1][0]
            assert (next_start - current_end).days == 1, f"Gap between chunks {i} and {i+1}"


if __name__ == "__main__":
    # Quick test run
    test = TestDateChunking()
    test.test_eia_monthly_chunks()
    test.test_caiso_quarterly_chunks()
    test.test_large_date_range()
    print("✅ All date chunking tests passed!")
