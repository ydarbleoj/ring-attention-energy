"""
Extract steps for data pipeline.

This module contains pipeline steps responsible for extracting data from
external sources like APIs, databases, and file systems. Each extract step
follows the BaseStep interface pattern for consistency and composability.

Available extract steps:
- EIAExtractStep: Extract EIA energy data (demand/generation) - synchronous
- AsyncEIAExtractStep: High-performance async EIA extraction targeting 15,000 RPS
"""

from .eia_extract import EIAExtractStep, EIAExtractStepConfig

try:
    from .eia_extract import AsyncEIAExtractStep, AsyncEIAExtractStepConfig
    __all__ = [
        "EIAExtractStep",
        "EIAExtractStepConfig",
        "AsyncEIAExtractStep",
        "AsyncEIAExtractStepConfig",
    ]
except ImportError:
    # aiohttp not available
    __all__ = [
        "EIAExtractStep",
        "EIAExtractStepConfig",
    ]
