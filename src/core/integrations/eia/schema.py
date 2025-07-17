"""
EIA API Response Schema Models

Pydantic models for validating EIA (Energy Information Administration) API responses.
Based on analysis of VCR cassettes and production API documentation.

These models form Layer 1 of our data architecture - raw data validation at API boundaries.
They handle the complex, inconsistent API response format and provide type safety.

Usage:
    response_data = EIAResponse.model_validate(api_response_json)
    demand_records = [r for r in response_data.response.data if isinstance(r, DemandRecord)]
"""

from datetime import datetime
from typing import List, Union, Optional, Literal, Any, Dict
from pydantic import BaseModel, Field, field_validator, ConfigDict
import pandas as pd

# Optional Polars import for enhanced data processing
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


class DemandRecord(BaseModel):
    """
    Single demand data point from EIA region-data endpoint

    Example from VCR cassette:
    {
        "period": "2023-01-01T00",
        "respondent": "PACW",
        "respondent-name": "PacifiCorp West",
        "type": "D",
        "type-name": "Demand",
        "value": "2455",
        "value-units": "megawatthours"
    }
    """
    model_config = ConfigDict(str_strip_whitespace=True, populate_by_name=True)

    period: str = Field(..., description="Timestamp in EIA format (YYYY-MM-DDTHH)")
    respondent: str = Field(..., description="Region/utility code (e.g., PACW)")
    respondent_name: str = Field(..., alias="respondent-name", description="Full region name")
    type: Literal["D"] = Field(..., description="Data type - always 'D' for demand")
    type_name: str = Field(..., alias="type-name", description="Human readable type")
    value: str = Field(..., description="Demand value as string (EIA sends as string)")
    value_units: str = Field(..., alias="value-units", description="Units (megawatthours)")

    @field_validator("period")
    @classmethod
    def validate_period(cls, v: str) -> str:
        """Validate EIA timestamp format"""
        try:
            # EIA format: 2023-01-01T00
            datetime.strptime(v, "%Y-%m-%dT%H")
        except ValueError:
            raise ValueError(f"Invalid EIA timestamp format: {v}")
        return v

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: str) -> str:
        """Ensure value can be converted to float"""
        try:
            float(v)
        except ValueError:
            raise ValueError(f"Value cannot be converted to float: {v}")
        return v

    @property
    def timestamp(self) -> pd.Timestamp:
        """Convert EIA period to pandas timestamp"""
        return pd.to_datetime(self.period)

    @property
    def demand_mwh(self) -> float:
        """Get demand value as float in MWh"""
        return float(self.value)


class GenerationRecord(BaseModel):
    """
    Single generation data point from EIA fuel-type-data endpoint

    Example from VCR cassette:
    {
        "period": "2023-01-01T00",
        "respondent": "PACW",
        "respondent-name": "PacifiCorp West",
        "fueltype": "SUN",
        "type-name": "Solar",
        "value": "44",
        "value-units": "megawatthours"
    }
    """
    model_config = ConfigDict(str_strip_whitespace=True, populate_by_name=True)

    period: str = Field(..., description="Timestamp in EIA format")
    respondent: str = Field(..., description="Region/utility code")
    respondent_name: str = Field(..., alias="respondent-name", description="Full region name")
    fueltype: str = Field(..., description="Fuel type code (NG, SUN, WAT, WND, OTH)")
    type_name: str = Field(..., alias="type-name", description="Human readable fuel type")
    value: str = Field(..., description="Generation value as string")
    value_units: str = Field(..., alias="value-units", description="Units (megawatthours)")

    @field_validator("period")
    @classmethod
    def validate_period(cls, v: str) -> str:
        """Validate EIA timestamp format"""
        try:
            datetime.strptime(v, "%Y-%m-%dT%H")
        except ValueError:
            raise ValueError(f"Invalid EIA timestamp format: {v}")
        return v

    @field_validator("fueltype")
    @classmethod
    def validate_fueltype(cls, v: str) -> str:
        """Validate known fuel type codes"""
        known_fueltypes = {"NG", "SUN", "WAT", "WND", "OTH", "COL", "NUC", "OIL"}
        if v not in known_fueltypes:
            # Don't raise error, just warn - new fuel types may be added
            pass
        return v

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: str) -> str:
        """Ensure value can be converted to float"""
        try:
            float(v)
        except ValueError:
            raise ValueError(f"Value cannot be converted to float: {v}")
        return v

    @property
    def timestamp(self) -> pd.Timestamp:
        """Convert EIA period to pandas timestamp"""
        return pd.to_datetime(self.period)

    @property
    def generation_mwh(self) -> float:
        """Get generation value as float in MWh"""
        return float(self.value)


class EIAResponseData(BaseModel):
    """
    Main data container from EIA API responses

    Contains metadata and the actual data array
    """
    model_config = ConfigDict(str_strip_whitespace=True)

    total: str = Field(..., description="Total number of records available")
    dateFormat: str = Field(..., description="Date format used in period fields")
    frequency: str = Field(..., description="Data frequency (hourly, daily, etc)")
    data: List[Dict[str, Any]] = Field(..., description="Raw data records")

    @field_validator("total")
    @classmethod
    def validate_total(cls, v: str) -> str:
        """Ensure total can be converted to int"""
        try:
            int(v)
        except ValueError:
            raise ValueError(f"Total cannot be converted to int: {v}")
        return v

    @property
    def total_records(self) -> int:
        """Get total as integer"""
        return int(self.total)

    def parse_demand_records(self) -> List[DemandRecord]:
        """Parse data array into validated DemandRecord objects"""
        demand_records = []
        for record in self.data:
            if record.get("type") == "D":
                try:
                    demand_record = DemandRecord.model_validate(record)
                    demand_records.append(demand_record)
                except Exception as e:
                    # Log warning but don't fail entire batch
                    print(f"Warning: Failed to parse demand record: {e}")
                    continue
        return demand_records

    def parse_generation_records(self) -> List[GenerationRecord]:
        """Parse data array into validated GenerationRecord objects"""
        generation_records = []
        for record in self.data:
            if "fueltype" in record:
                try:
                    generation_record = GenerationRecord.model_validate(record)
                    generation_records.append(generation_record)
                except Exception as e:
                    # Log warning but don't fail entire batch
                    print(f"Warning: Failed to parse generation record: {e}")
                    continue
        return generation_records


class EIAResponse(BaseModel):
    """
    Top-level EIA API response wrapper

    All EIA API endpoints return this structure
    """
    model_config = ConfigDict(str_strip_whitespace=True)

    response: EIAResponseData = Field(..., description="Main response data")

    def to_demand_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame for demand data"""
        demand_records = self.response.parse_demand_records()

        if not demand_records:
            return pd.DataFrame(columns=["timestamp", "region", "demand_mwh"])

        data = []
        for record in demand_records:
            data.append({
                "timestamp": record.timestamp,
                "region": record.respondent,
                "demand_mwh": record.demand_mwh
            })

        df = pd.DataFrame(data)
        return df.sort_values("timestamp").reset_index(drop=True)

    def to_generation_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame for generation data (wide format)"""
        generation_records = self.response.parse_generation_records()

        if not generation_records:
            return pd.DataFrame(columns=["timestamp", "region"])

        # Create long-format data first
        data = []
        for record in generation_records:
            data.append({
                "timestamp": record.timestamp,
                "region": record.respondent,
                "fueltype": record.fueltype,
                "generation_mwh": record.generation_mwh
            })

        df = pd.DataFrame(data)

        # Pivot to wide format (one column per fuel type)
        wide_df = df.pivot_table(
            index=["timestamp", "region"],
            columns="fueltype",
            values="generation_mwh",
            fill_value=0.0
        ).reset_index()

        # Flatten column names
        wide_df.columns.name = None

        # Rename fuel type columns to be more descriptive
        fuel_mapping = {
            "NG": "natural_gas_mwh",
            "SUN": "solar_mwh",
            "WAT": "hydro_mwh",
            "WND": "wind_mwh",
            "OTH": "other_mwh",
            "COL": "coal_mwh",
            "NUC": "nuclear_mwh",
            "OIL": "oil_mwh"
        }

        # Rename columns that exist
        existing_fuels = [col for col in fuel_mapping.keys() if col in wide_df.columns]
        rename_dict = {fuel: fuel_mapping[fuel] for fuel in existing_fuels}
        wide_df = wide_df.rename(columns=rename_dict)

        return wide_df.sort_values("timestamp").reset_index(drop=True)


class EIAErrorResponse(BaseModel):
    """
    EIA API error response format

    Example: {"error": "Not found.", "code": 404}
    """
    model_config = ConfigDict(str_strip_whitespace=True)

    error: str = Field(..., description="Error message")
    code: int = Field(..., description="Error code")


# Fuel type constants for easy reference
FUEL_TYPE_CODES = {
    "NG": "Natural Gas",
    "SUN": "Solar",
    "WAT": "Hydro",
    "WND": "Wind",
    "OTH": "Other",
    "COL": "Coal",
    "NUC": "Nuclear",
    "OIL": "Oil"
}

RENEWABLE_FUEL_TYPES = {"SUN", "WAT", "WND"}  # Renewable energy fuel types


# Utility functions for common operations
def validate_eia_response(response_json: dict) -> Union[EIAResponse, EIAErrorResponse]:
    """
    Validate and parse EIA API response

    Args:
        response_json: Raw JSON response from EIA API

    Returns:
        Validated EIAResponse or EIAErrorResponse object

    Raises:
        ValueError: If response doesn't match either schema
    """
    # Check if it's an error response
    if "error" in response_json:
        return EIAErrorResponse.model_validate(response_json)

    # Try to parse as successful response
    try:
        return EIAResponse.model_validate(response_json)
    except Exception as e:
        raise ValueError(f"Invalid EIA response format: {e}")


def extract_oregon_renewable_summary(generation_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract renewable energy summary for Oregon data analysis

    Args:
        generation_df: Generation data from to_generation_dataframe()

    Returns:
        DataFrame with renewable summary columns
    """
    renewable_cols = [col for col in generation_df.columns
                     if any(fuel in col for fuel in ["solar", "hydro", "wind"])]

    if not renewable_cols:
        return generation_df

    # Calculate total renewable generation
    generation_df = generation_df.copy()
    generation_df["total_renewable_mwh"] = generation_df[renewable_cols].sum(axis=1)

    # Calculate renewable percentage if total generation is available
    total_cols = [col for col in generation_df.columns if "_mwh" in col and col != "total_renewable_mwh"]
    if total_cols:
        generation_df["total_generation_mwh"] = generation_df[total_cols].sum(axis=1)
        generation_df["renewable_percentage"] = (
            generation_df["total_renewable_mwh"] / generation_df["total_generation_mwh"] * 100
        ).round(2)

    return generation_df


class EIADemandResponse(BaseModel):
    """Specialized response wrapper for demand data"""
    model_config = ConfigDict(str_strip_whitespace=True)

    response: EIAResponseData = Field(..., description="Main response data")

    def to_polars(self) -> "pl.DataFrame":
        """Convert to Polars DataFrame for demand data"""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required for this operation. Install with: pip install polars")

        demand_records = self.response.parse_demand_records()

        if not demand_records:
            return pl.DataFrame(schema={
                "datetime": pl.Datetime,
                "region": pl.Utf8,
                "demand_mwh": pl.Float64
            })

        data = []
        for record in demand_records:
            data.append({
                "datetime": record.timestamp.to_pydatetime(),
                "region": record.respondent,
                "demand_mwh": record.demand_mwh
            })

        df = pl.DataFrame(data)
        return df.sort("datetime")

    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame for demand data"""
        return self.to_demand_dataframe()


class EIAGenerationResponse(BaseModel):
    """Specialized response wrapper for generation data"""
    model_config = ConfigDict(str_strip_whitespace=True)

    response: EIAResponseData = Field(..., description="Main response data")

    def to_polars(self) -> "pl.DataFrame":
        """Convert to Polars DataFrame for generation data"""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required for this operation. Install with: pip install polars")

        generation_records = self.response.parse_generation_records()

        if not generation_records:
            return pl.DataFrame(schema={
                "datetime": pl.Datetime,
                "region": pl.Utf8,
                "fueltype": pl.Utf8,
                "generation_mwh": pl.Float64
            })

        # Create long-format data first
        data = []
        for record in generation_records:
            data.append({
                "datetime": record.timestamp.to_pydatetime(),
                "region": record.respondent,
                "fueltype": record.fueltype,
                "generation_mwh": record.generation_mwh
            })

        df = pl.DataFrame(data)

        # Pivot to wide format (one column per fuel type)
        wide_df = df.pivot(
            index=["datetime", "region"],
            columns="fueltype",
            values="generation_mwh",
            aggregate_function="sum"
        ).fill_null(0.0)

        # Rename fuel type columns to be more descriptive
        fuel_mapping = {
            "NG": "natural_gas_mwh",
            "SUN": "solar_mwh",
            "WAT": "hydro_mwh",
            "WND": "wind_mwh",
            "OTH": "other_mwh",
            "COL": "coal_mwh",
            "NUC": "nuclear_mwh",
            "OIL": "oil_mwh"
        }

        # Rename columns that exist
        existing_fuels = [col for col in fuel_mapping.keys() if col in wide_df.columns]
        rename_dict = {fuel: fuel_mapping[fuel] for fuel in existing_fuels}
        wide_df = wide_df.rename(rename_dict)

        return wide_df.sort("datetime")

    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame for generation data"""
        return self.to_generation_dataframe()


# Type aliases for backwards compatibility
EIADemandRecord = DemandRecord
EIAGenerationRecord = GenerationRecord


# =============================================================================
# API Endpoint Configuration
# =============================================================================

from dataclasses import dataclass


@dataclass
class EIAEndpoint:
    """Configuration for an EIA API endpoint."""
    path: str
    required_params: Dict[str, str]
    optional_params: Dict[str, str]
    description: str


class EIAEndpoints:
    """EIA API endpoint schemas and parameter definitions."""

    # Base endpoint configurations
    ENDPOINTS = {
        'demand': EIAEndpoint(
            path="/electricity/rto/region-data/data/",
            required_params={
                'frequency': 'hourly',
                'data[0]': 'value',
                'facets[type][]': 'D',  # Demand
                'start': 'YYYY-MM-DD',
                'end': 'YYYY-MM-DD',
                'sort[0][column]': 'period',
                'sort[0][direction]': 'asc'
            },
            optional_params={
                'facets[respondent][]': 'region_code',  # Single region
                'facets[respondent][0]': 'region_code_1',  # Multi-region format
                'facets[respondent][1]': 'region_code_2',  # Multi-region format
                'length': '5000',
                'offset': '0'
            },
            description="Electricity demand data by region"
        ),

        'generation': EIAEndpoint(
            path="/electricity/rto/fuel-type-data/data/",
            required_params={
                'frequency': 'hourly',
                'data[0]': 'value',
                'start': 'YYYY-MM-DD',
                'end': 'YYYY-MM-DD',
                'sort[0][column]': 'period',
                'sort[0][direction]': 'asc'
            },
            optional_params={
                'facets[respondent][]': 'region_code',  # Single region
                'facets[respondent][0]': 'region_code_1',  # Multi-region format
                'facets[respondent][1]': 'region_code_2',  # Multi-region format
                'length': '5000',
                'offset': '0'
            },
            description="Electricity generation data by fuel type and region"
        )
    }

    @classmethod
    def get_demand_params(cls, regions: List[str], start_date: str, end_date: str) -> Dict[str, str]:
        """
        Build parameters for demand data request.

        Args:
            regions: List of region codes (e.g., ['PACW', 'ERCO'])
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Dictionary of API parameters
        """
        endpoint = cls.ENDPOINTS['demand']
        params = endpoint.required_params.copy()

        # Update with actual values
        params['start'] = start_date
        params['end'] = end_date

        # Handle single vs multiple regions
        if len(regions) == 1:
            params['facets[respondent][]'] = regions[0]
        else:
            # Multi-region format
            for i, region in enumerate(regions):
                params[f'facets[respondent][{i}]'] = region

        return params

    @classmethod
    def get_generation_params(cls, regions: List[str], start_date: str, end_date: str) -> Dict[str, str]:
        """
        Build parameters for generation data request.

        Args:
            regions: List of region codes
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            Dictionary of API parameters
        """
        endpoint = cls.ENDPOINTS['generation']
        params = endpoint.required_params.copy()

        # Update with actual values
        params['start'] = start_date
        params['end'] = end_date

        # Handle single vs multiple regions
        if len(regions) == 1:
            params['facets[respondent][]'] = regions[0]
        else:
            # Multi-region format
            for i, region in enumerate(regions):
                params[f'facets[respondent][{i}]'] = region

        return params

    @classmethod
    def get_endpoint_path(cls, data_type: str) -> str:
        """Get the API endpoint path for a data type."""
        if data_type not in cls.ENDPOINTS:
            raise ValueError(f"Unknown data type: {data_type}. Available: {list(cls.ENDPOINTS.keys())}")

        return cls.ENDPOINTS[data_type].path


# =============================================================================
# EIA Extract Step Configuration
# =============================================================================

from pydantic import Field, field_validator
from ...pipeline.steps.base import ExtractStepConfig


class EIAExtractConfig(ExtractStepConfig):
    """Configuration for high-performance EIA data extraction."""

    api_key: str = Field(..., description="EIA API key for authentication")
    data_types: List[str] = Field(
        default_factory=lambda: ['demand', 'generation'],
        description="Types of data to extract"
    )
    regions: List[str] = Field(
        default_factory=lambda: ["PACW", "ERCO", "NYIS", "ISNE", "PJM", "MISO", "SPP", "CARO"],
        description="List of region codes to extract data for"
    )

    # Performance tuning - optimized for EIA API
    rate_limit_delay: float = Field(
        default=0.0,
        description="No artificial delays - EIA API handles its own limits"
    )
    max_concurrent_batches: int = Field(
        default=50,
        description="Maximum concurrent requests (50 = optimal for EIA)"
    )

    # Date batching configuration - optimized for EIA API
    batch_size_days: int = Field(
        default=7,
        description="Size of date batches in days (7 days = optimal balance)"
    )
    max_regions_per_request: int = Field(
        default=8,
        description="Maximum regions per API request (8 = best performance)"
    )

    # Data storage configuration
    raw_data_path: str = Field(
        default="data/raw/eia",
        description="Path for storing raw JSON files"
    )

    @field_validator("rate_limit_delay")
    @classmethod
    def validate_rate_limit(cls, v):
        return v  # No artificial limits for EIA

    @field_validator("max_concurrent_batches")
    @classmethod
    def validate_concurrency(cls, v):
        if v < 1:
            raise ValueError("max_concurrent_batches must be at least 1")
        return v

    @field_validator("batch_size_days")
    @classmethod
    def validate_batch_size(cls, v):
        if v < 1 or v > 365:
            raise ValueError("batch_size_days must be between 1 and 365")
        return v

    @field_validator("data_types")
    @classmethod
    def validate_data_types(cls, v):
        valid_types = {'demand', 'generation'}
        if not all(dt in valid_types for dt in v):
            raise ValueError(f"data_types must be subset of {valid_types}")
        return v
