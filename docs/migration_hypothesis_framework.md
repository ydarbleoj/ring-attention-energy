# Energy-Migration Correlation Framework

## Core Hypothesis

Energy flow patterns contain predictive signals for human migration patterns due to their reflection of economic activity, infrastructure development, and quality of life indicators.

## Research Questions

### Primary Questions

1. **Economic Migration**: Do energy consumption patterns predict economic opportunity and thus migration flows?
2. **Seasonal Migration**: Are temporary migration patterns (snowbirds, seasonal workers) correlated with energy demand cycles?
3. **Infrastructure-Led Migration**: Does new energy infrastructure development precede population growth?
4. **Climate-Driven Migration**: How do energy patterns reflect climate adaptability and migration decisions?

### Specific Hypotheses

#### H1: Economic Activity Correlation

- **Hypothesis**: Regions with increasing industrial energy consumption will show net positive migration
- **Indicators**:
  - Industrial electricity demand trends
  - Commercial sector growth patterns
  - Peak demand changes (indicating economic activity)
- **Prediction**: 6-12 month lead time for energy patterns to predict migration

#### H2: Seasonal Migration Patterns

- **Hypothesis**: Seasonal energy patterns correlate with temporary migration flows
- **Indicators**:
  - Residential cooling/heating demand cycles
  - Tourism-related energy spikes
  - Agricultural energy patterns
- **Prediction**: Annual cycles with predictable seasonal migration flows

#### H3: Infrastructure Development

- **Hypothesis**: New energy infrastructure investment precedes population growth
- **Indicators**:
  - Grid capacity expansions
  - New generation facility construction
  - Transmission line development
- **Prediction**: 1-3 year lead time for infrastructure to predict migration

#### H4: Climate Adaptability

- **Hypothesis**: Energy efficiency trends predict climate-driven migration decisions
- **Indicators**:
  - Extreme weather energy spikes
  - Grid resilience events
  - Renewable energy adoption rates
- **Prediction**: Climate stress events correlate with out-migration

## Data Requirements

### Energy Data Sources

1. **EIA (Federal)**: Multi-state comparison, long-term trends
2. **CAISO (California)**: High-resolution grid data, economic hub
3. **Weather Data**: Climate correlation, extreme events
4. **Additional**: Texas (ERCOT), New York (NYISO) for comparison

### Migration Data Sources (for validation)

1. **US Census Bureau**: American Community Survey, migration flows
2. **IRS**: Tax return migration data
3. **BLS**: Employment-based migration indicators
4. **Real Estate**: Housing market trends, construction permits

### Geographic Focus

- **Primary**: Oregon ↔ California corridor
- **Secondary**: Pacific Northwest ↔ Southwest
- **Tertiary**: National patterns for validation

## Analytical Approach

### Time Series Analysis

- **Granularity**: Hourly energy data → monthly migration patterns
- **Sequence Length**: 2-5 years for pattern recognition
- **Lag Analysis**: 1-36 month lag between energy and migration signals

### Feature Engineering

- **Energy Features**:
  - Consumption trends (residential, commercial, industrial)
  - Peak demand patterns
  - Renewable energy mix
  - Grid stability metrics
  - Price signals
- **Derived Features**:
  - Energy intensity (per capita, per GDP)
  - Seasonal deviation patterns
  - Infrastructure investment proxies
  - Climate stress indicators

### Machine Learning Approach

- **Model Type**: Ring attention for long sequences across regions
- **Input**: Multi-region energy time series
- **Output**: Migration flow predictions
- **Validation**: Hold-out periods, cross-regional validation

## Success Metrics

### Quantitative

- **Prediction Accuracy**: R² > 0.6 for migration flow prediction
- **Lead Time**: 3-12 months advance warning
- **Regional Specificity**: State-level migration prediction

### Qualitative

- **Pattern Discovery**: Identification of novel energy-migration correlations
- **Policy Insights**: Actionable findings for regional planning
- **Reproducibility**: Framework applicable to other regions

## Implementation Phases

### Phase 1: Data Foundation (Current)

- [x] EIA Oregon data pipeline
- [ ] CAISO California data integration
- [ ] Weather data integration
- [ ] Multi-source data fusion

### Phase 2: Pattern Analysis

- [ ] Historical correlation analysis
- [ ] Seasonal pattern identification
- [ ] Economic cycle correlation
- [ ] Climate event correlation

### Phase 3: Predictive Modeling

- [ ] Ring attention model training
- [ ] Multi-region sequence modeling
- [ ] Migration flow prediction
- [ ] Model validation and tuning

### Phase 4: Validation and Insights

- [ ] Historical validation against known migration patterns
- [ ] Cross-regional model validation
- [ ] Policy and planning insights
- [ ] Framework generalization

## Technical Considerations

### Data Volume

- **Energy Data**: ~1M records per region per year
- **Multi-region**: 3-5 regions → 15-25M records
- **Time Horizon**: 5-10 years → 75-250M records
- **Memory**: 128GB RAM sufficient for in-memory processing

### Processing Pipeline

- **Ingestion**: Multi-source APIs with different schemas
- **Alignment**: Temporal and geographic alignment
- **Feature Engineering**: Cross-region feature computation
- **Storage**: Parquet files for efficient access
- **Modeling**: MLX for high-performance training

### Validation Framework

- **Time Series Split**: Chronological validation
- **Cross-Region**: Train on some regions, validate on others
- **Synthetic Data**: Generate energy patterns, predict known migration
- **Ground Truth**: Validate against census and IRS data

## Next Steps

1. **Literature Review**: Research existing energy-migration correlation studies
2. **Data Acquisition**: Expand to CAISO and weather data
3. **Exploratory Analysis**: Initial correlation analysis
4. **Stakeholder Engagement**: Connect with migration researchers
5. **Pilot Study**: Focus on specific migration event (e.g., 2020 pandemic migration)
