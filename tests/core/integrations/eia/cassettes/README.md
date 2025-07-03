# VCR Cassettes for Oregon EIA Data

This directory contains VCR (Video Cassette Recorder) cassettes that record and replay HTTP interactions with the EIA API for Oregon energy data testing.

## How VCR Works

1. **First Run (Recording)**: When tests run with a real EIA API key, VCR records all HTTP requests/responses to YAML files in this directory.

2. **Subsequent Runs (Playback)**: VCR replays the recorded responses without making actual HTTP requests, ensuring consistent and fast tests.

## Oregon-Specific Cassettes

### `eia_oregon_demand_test.yaml`

Records Oregon electricity demand data from the Pacific West (PACW) region. Captures:

- Hourly demand patterns typical of Oregon (~8-15 GW range)
- Seasonal variations (winter heating peaks, summer cooling loads)
- Daily patterns showing Oregon's moderate demand profile

### `eia_oregon_generation_test.yaml`

Records Oregon generation mix data, focusing on:

- **Hydro**: Oregon's dominant generation source (~60-80%)
- **Natural Gas**: Backup and peaking generation
- **Wind**: Significant resource from Columbia River Gorge
- **Coal**: Declining but historically present

### `eia_oregon_renewable_test.yaml`

Records renewable generation data specific to Oregon:

- **Wind**: Columbia River Gorge wind farms
- **Solar**: Growing rooftop and utility-scale solar
- **Hydro**: While not technically "renewable" in EIA categorization, Oregon's massive hydro resources

### `eia_oregon_comprehensive_test.yaml`

Records combined dataset including:

- All demand, generation, and renewable data
- Price information (if available for Pacific Northwest)
- Integrated dataset suitable for ML model training

## Oregon Energy Characteristics

Oregon's unique energy profile includes:

- **High Hydro Dependence**: ~60% of generation from hydro
- **Seasonal Patterns**: Spring snowmelt creates high hydro output
- **Wind Resources**: Columbia River Gorge is major wind corridor
- **Moderate Demand**: ~8-15 GW peak demand (smaller than CA/TX)
- **Low Carbon**: Clean electricity grid due to hydro dominance

## Recording Fresh Data

To record new cassettes with current Oregon data:

```bash
# Set your real EIA API key
export EIA_API_KEY=your_actual_eia_api_key

# Delete existing cassettes (optional)
rm -f tests/core/integrations/eia/cassettes/*.yaml

# Run tests to record new cassettes
pytest tests/core/integrations/eia/test_client.py::TestEIAClientAPIIntegration -v
```

## Cassette File Format

Cassettes are stored as YAML files with:

- Request details (URL, headers, body)
- Response data (status, headers, body)
- Sensitive data automatically filtered (API keys)

Example structure:

```yaml
interactions:
  - request:
      uri: https://api.eia.gov/v2/electricity/rto/region-data/data/
      method: GET
      headers: {}
    response:
      status: { code: 200, message: OK }
      headers: {}
      body: { string: '{"response": {"data": [...]}}' }
```

## Benefits for Oregon Energy Research

1. **Reproducible Tests**: Same Oregon data every time
2. **Fast Development**: No API delays during development
3. **Offline Testing**: Can test without internet connection
4. **Rate Limit Friendly**: No repeated API calls during development
5. **CI/CD Ready**: Tests run in CI without API keys

## Data Privacy & Security

- API keys are automatically filtered from cassettes
- No sensitive data is stored in version control
- Only public EIA energy data is recorded
- Cassettes can be committed safely to git

## Oregon-Specific Research Applications

This Oregon data is ideal for:

- **Ring Attention**: Testing long sequences of Oregon's seasonal patterns
- **Renewable Forecasting**: Modeling Oregon's wind/hydro variability
- **Grid Optimization**: Understanding Oregon's unique hydro-dominated grid
- **Climate Analysis**: Studying hydroelectric sensitivity to precipitation
- **Energy Justice**: Analyzing Oregon's relatively clean grid access

---

_Last updated: 2025-07-02_
_Data source: EIA (Energy Information Administration) API_
_Region focus: PACW (Pacific West) - includes Oregon_
