"""
Pydantic schemas for CAISO (California Independent System Operator) API responses.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any, Self
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
import pandas as pd


class CAISOTimestamp(BaseModel):
    """Base timestamp model for CAISO data"""
    timestamp: datetime = Field(..., description="Timestamp in ISO format")

    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v):
        """Parse CAISO timestamp formats"""
        if isinstance(v, str):
            # CAISO uses various timestamp formats
            formats = [
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S-0000',
                '%Y-%m-%dT%H:%M:%S',
                '%m/%d/%Y %H:%M:%S',
                '%Y%m%d %H:%M'
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse timestamp: {v}")
        return v


class CAISOSystemDemand(CAISOTimestamp):
    """System Load Demand (SLD) data"""
    demand_mw: float = Field(..., ge=0, description="System demand in MW")
    region: str = Field(default="CAISO", description="Grid region")

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )


class CAISOGeneration(CAISOTimestamp):
    """Generation data by fuel type"""
    fuel_type: str = Field(..., description="Fuel type (e.g., 'SOLAR', 'WIND', 'NATURAL GAS')")
    generation_mw: float = Field(..., ge=0, description="Generation in MW")
    region: str = Field(default="CAISO", description="Grid region")


class CAISOPrice(CAISOTimestamp):
    """Locational Marginal Price (LMP) data"""
    location: str = Field(..., description="Price location/node")
    lmp_price: float = Field(..., description="Locational Marginal Price ($/MWh)")
    energy_price: float = Field(..., description="Energy component of LMP")
    congestion_price: float = Field(..., description="Congestion component of LMP")
    loss_price: float = Field(..., description="Loss component of LMP")


class CAISOLoadForecast(CAISOTimestamp):
    """Load forecast data"""
    forecast_mw: float = Field(..., ge=0, description="Forecasted load in MW")
    forecast_type: str = Field(..., description="Forecast type (e.g., 'DAM', 'RTM')")
    region: str = Field(default="CAISO", description="Grid region")


class CAISOInterfaceFlow(CAISOTimestamp):
    """Interface flow data"""
    interface_name: str = Field(..., description="Interface name")
    flow_mw: float = Field(..., description="Flow in MW (positive = import)")
    direction: str = Field(..., description="Flow direction")


class CAISODemandResponse(BaseModel):
    """Container for system demand response"""
    data: List[CAISOSystemDemand] = Field(..., description="List of demand data points")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Response metadata")

    @model_validator(mode='after')
    def validate_data_consistency(self) -> Self:
        """Validate data consistency"""
        data = self.data
        if data:
            # Check for duplicate timestamps
            timestamps = [d.timestamp for d in data]
            if len(timestamps) != len(set(timestamps)):
                raise ValueError("Duplicate timestamps found in data")

            # Check for reasonable demand values
            demands = [d.demand_mw for d in data]
            if any(d < 0 or d > 100000 for d in demands):  # Reasonable bounds for CA
                raise ValueError("Demand values outside reasonable range")

        return self

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> 'CAISODemandResponse':
        """Create from pandas DataFrame"""
        if df.empty:
            return cls(data=[])

        # Map common CAISO column names
        column_mapping = {
            'OPR_DT': 'date',  # Don't rename to timestamp yet
            'OPR_HR': 'hour',
            'OPR_INTERVAL': 'interval',
            'MW': 'demand_mw',
            'LOAD': 'demand_mw',
            'DEMAND': 'demand_mw'
        }

        # Rename columns
        df_mapped = df.rename(columns=column_mapping)

        # Handle timestamp construction
        if 'timestamp' not in df_mapped.columns:
            if 'date' in df_mapped.columns and 'hour' in df_mapped.columns:
                # Combine date and hour - handle both string and numeric hours
                def combine_date_hour(date_str, hour_val):
                    try:
                        # Ensure hour is an integer
                        hour = int(hour_val)
                        # Handle 24-hour format (CAISO sometimes uses 1-24 instead of 0-23)
                        if hour == 24:
                            hour = 0
                        elif hour > 24:
                            hour = hour % 24

                        # Parse date and add hour
                        date_part = pd.to_datetime(date_str).strftime('%Y-%m-%d')
                        timestamp_str = f"{date_part} {hour:02d}:00:00"
                        result = pd.to_datetime(timestamp_str)
                        print(f"  Combined {date_str} + {hour_val} -> {result} (type: {type(result)})")
                        return result
                    except Exception as e:
                        print(f"Error parsing date/hour {date_str}/{hour_val}: {e}")
                        # Better fallback - try to parse the date_str and default to hour 0
                        try:
                            return pd.to_datetime(date_str)
                        except:
                            return pd.to_datetime(f"{date_str} 00:00:00")

                df_mapped['timestamp'] = df_mapped.apply(
                    lambda row: combine_date_hour(row['date'], row['hour']), axis=1
                )
            else:
                raise ValueError("Unable to determine timestamp from DataFrame columns")

        # Convert to demand objects
        data = []
        for _, row in df_mapped.iterrows():
            try:
                demand = CAISOSystemDemand(
                    timestamp=row['timestamp'],
                    demand_mw=row['demand_mw']
                )
                data.append(demand)
            except Exception as e:
                # Skip invalid rows but log them
                print(f"Skipping invalid row: {e}")
                continue

        return cls(data=data)


class CAISOGenerationResponse(BaseModel):
    """Container for generation response"""
    data: List[CAISOGeneration] = Field(..., description="List of generation data points")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Response metadata")

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> 'CAISOGenerationResponse':
        """Create from pandas DataFrame"""
        if df.empty:
            return cls(data=[])

        # Map common CAISO generation column names
        column_mapping = {
            'OPR_DT': 'date',  # Don't rename to timestamp yet
            'OPR_HR': 'hour',
            'FUEL_TYPE': 'fuel_type',
            'MW': 'generation_mw',
            'GENERATION': 'generation_mw'
        }

        df_mapped = df.rename(columns=column_mapping)

        # Handle timestamp construction
        if 'timestamp' not in df_mapped.columns:
            if 'date' in df_mapped.columns and 'hour' in df_mapped.columns:
                # Combine date and hour - handle both string and numeric hours
                def combine_date_hour(date_str, hour_val):
                    try:
                        # Ensure hour is an integer
                        hour = int(hour_val)
                        # Handle 24-hour format (CAISO sometimes uses 1-24 instead of 0-23)
                        if hour == 24:
                            hour = 0
                        elif hour > 24:
                            hour = hour % 24

                        # Parse date and add hour
                        date_part = pd.to_datetime(date_str).strftime('%Y-%m-%d')
                        timestamp_str = f"{date_part} {hour:02d}:00:00"
                        result = pd.to_datetime(timestamp_str)
                        print(f"  Combined {date_str} + {hour_val} -> {result} (type: {type(result)})")
                        return result
                    except Exception as e:
                        print(f"Error parsing date/hour {date_str}/{hour_val}: {e}")
                        # Better fallback - try to parse the date_str and default to hour 0
                        try:
                            return pd.to_datetime(date_str)
                        except:
                            return pd.to_datetime(f"{date_str} 00:00:00")

                df_mapped['timestamp'] = df_mapped.apply(
                    lambda row: combine_date_hour(row['date'], row['hour']), axis=1
                )
            else:
                raise ValueError("Unable to determine timestamp from DataFrame columns")

        # Convert to generation objects
        data = []
        for _, row in df_mapped.iterrows():
            try:
                gen = CAISOGeneration(
                    timestamp=row['timestamp'],
                    fuel_type=row['fuel_type'],
                    generation_mw=row['generation_mw']
                )
                data.append(gen)
            except Exception as e:
                print(f"Skipping invalid generation row: {e}")
                continue

        return cls(data=data)


class CAISOAPIError(BaseModel):
    """Error response from CAISO API"""
    error_code: str = Field(..., description="Error code")
    error_message: str = Field(..., description="Error message")
    timestamp: datetime = Field(default_factory=datetime.now, description="Error timestamp")

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )


# Migration-specific schemas for cross-source analysis
class RegionalEnergyMetrics(BaseModel):
    """Regional energy metrics for migration analysis"""
    region: str = Field(..., description="Region identifier")
    timestamp: datetime = Field(..., description="Timestamp")

    # Demand metrics
    total_demand_mw: float = Field(..., ge=0, description="Total demand in MW")
    residential_demand_mw: Optional[float] = Field(None, ge=0, description="Residential demand in MW")
    commercial_demand_mw: Optional[float] = Field(None, ge=0, description="Commercial demand in MW")
    industrial_demand_mw: Optional[float] = Field(None, ge=0, description="Industrial demand in MW")

    # Generation metrics
    total_generation_mw: float = Field(..., ge=0, description="Total generation in MW")
    renewable_generation_mw: Optional[float] = Field(None, ge=0, description="Renewable generation in MW")
    renewable_percentage: Optional[float] = Field(None, ge=0, le=100, description="Renewable percentage")

    # Economic indicators
    average_price_mwh: Optional[float] = Field(None, description="Average price $/MWh")
    peak_demand_mw: Optional[float] = Field(None, ge=0, description="Peak demand in MW")
    load_factor: Optional[float] = Field(None, ge=0, le=1, description="Load factor (avg/peak)")

    # Migration-relevant derived metrics
    demand_per_capita: Optional[float] = Field(None, ge=0, description="Demand per capita (MW/person)")
    economic_activity_index: Optional[float] = Field(None, ge=0, description="Economic activity index")
    grid_stress_index: Optional[float] = Field(None, ge=0, description="Grid stress index")

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )


class MigrationPredictorFeatures(BaseModel):
    """Features for migration prediction"""
    region_from: str = Field(..., description="Origin region")
    region_to: str = Field(..., description="Destination region")
    timestamp: datetime = Field(..., description="Timestamp")

    # Energy differential features
    demand_ratio: float = Field(..., description="Demand ratio (to/from)")
    price_differential: float = Field(..., description="Price differential (to - from)")
    renewable_differential: float = Field(..., description="Renewable % differential")

    # Economic indicators
    economic_opportunity_ratio: float = Field(..., description="Economic opportunity ratio")
    infrastructure_development_index: float = Field(..., description="Infrastructure development index")

    # Climate/comfort indicators
    climate_adaptability_score: float = Field(..., description="Climate adaptability score")
    energy_efficiency_ratio: float = Field(..., description="Energy efficiency ratio")

    # Trend indicators
    demand_trend_12m: float = Field(..., description="12-month demand trend")
    price_trend_12m: float = Field(..., description="12-month price trend")

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )
