# VCR Cassettes for CAISO Data

This directory contains VCR (Video Cassette Recorder) cassettes that record and replay HTTP interactions with the CAISO API for testing.

## How VCR Works

1. **First Run (Recording)**: When tests run with a real CAISO API connection, VCR records all HTTP requests/responses to YAML files in this directory.

2. **Subsequent Runs (Playback)**: VCR replays the recorded responses without making actual HTTP requests, ensuring consistent and fast tests.

## CAISO-Specific Cassettes

### `caiso_california_demand.yaml`

Records California electricity demand data from CAISO OASIS API. Captures:

- Hourly demand patterns typical of California (~25-45 GW range)
- Seasonal variations (summer cooling peaks, winter heating)
- Daily patterns showing California's high demand profile
- Real-time market data (RTM)

### `caiso_california_generation.yaml`

Records California generation mix data, focusing on:

- **Natural Gas**: California's primary dispatchable generation
- **Solar**: Massive solar resources (utility-scale + distributed)
- **Wind**: Wind resources from mountain passes and coastal areas
- **Hydro**: Significant hydro resources from Sierra Nevada
- **Nuclear**: Diablo Canyon (until closure)
- **Imports**: Significant imports from Pacific Northwest and Desert Southwest

### `caiso_california_renewable.yaml`

Records renewable generation data specific to California:

- **Solar**: Leading solar state with >10 GW capacity
- **Wind**: Significant wind resources from multiple regions
- **Hydro**: Large hydro resources (though not counted as renewable in some contexts)
- **Geothermal**: Unique geothermal resources in Northern California

### `caiso_california_comprehensive.yaml`

Records combined dataset including:

- All demand, generation, and renewable data
- Price information (LMP - Locational Marginal Prices)
- Interface flows (imports/exports)
- Integrated dataset suitable for ML model training

## California Energy Characteristics

California's unique energy profile includes:

- **High Solar Penetration**: Leading solar generation in US
- **Duck Curve**: Characteristic demand pattern with solar
- **High Imports**: ~25% of electricity from out-of-state
- **Aggressive Renewables**: 100% clean electricity by 2045
- **Peak Demand**: Summer cooling drives highest demand
- **Grid Complexity**: Large, interconnected system with multiple regions

## CAISO API Notes

- **No API Key Required**: CAISO OASIS API is publicly accessible
- **Real-time Data**: Data available with minimal delay
- **Multiple Markets**: Day-ahead market (DAM) and real-time market (RTM)
- **Regional Granularity**: Data available by pricing zones and regions
- **Data Formats**: CSV format via ZIP files from OASIS API

## Test Data Periods

Tests focus on representative periods:

- **Summer Peak**: July-August data for cooling load patterns
- **Winter Shoulder**: January-February for heating patterns
- **Spring Solar**: March-April for high solar generation
- **Fall Transition**: October-November for moderate conditions

This provides comprehensive coverage of California's diverse energy patterns.
