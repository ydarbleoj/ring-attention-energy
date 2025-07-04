# Data Architecture Plan for Ring Attention Energy Pipeline

## ✅ COMPLETED: MLX Integration Phase

### MLX Feature Engineering Module (`src/core/ml/features.py`)

**EnergyFeatureEngineer Class:**

- ✅ Temporal features (cyclical encodings, binary flags)
- ✅ Energy-specific features (renewable metrics, supply-demand balance)
- ✅ Time series features (lags, rolling statistics)
- ✅ MLX array conversion with normalization
- ✅ 25x+ feature expansion capability

**Key Features:**

- Automatic temporal feature extraction (hour/day/month cycles)
- Renewable energy analysis (penetration, intermittency)
- Supply-demand balance indicators (grid stress, reserve margins)
- MLX-compatible array generation for training

### MLX Sequence Generation (`src/core/ml/sequence_generator.py`)

**SequenceGenerator Class:**

- ✅ Configurable sequence length and stride
- ✅ Feature normalization and scaling
- ✅ Iterator-based sequence generation
- ✅ Memory-efficient processing

**RingAttentionSequenceGenerator Class:**

- ✅ Ring-specific sequence partitioning
- ✅ Distributed training ready
- ✅ Long sequence handling (8760+ hours)

### MLX Data Integration (`src/core/ml/data_loaders.py`)

**EnergyDataLoader Class:**

- ✅ Bridges EIA service layer with MLX
- ✅ Automatic caching and storage integration
- ✅ Configurable sequence generation

### Demo Results (Real Performance Metrics)

```
🎯 Feature Engineering Performance:
   • Original features: 6 columns
   • Engineered features: 153 columns (25.5x expansion)
   • Memory usage: 0.4 MB for 672 hours
   • Categories: 18 temporal, 10 renewable, 40 lagged, 72 rolling

🚀 Sequence Generation Results:
   • Input sequences: (35, 168, 149) - 35 batches, 1 week, 149 features
   • Target sequences: (35, 149)
   • Dataset size: 3.4 MB
   • Ready for ring attention training

💾 Storage Integration:
   • Polars/Parquet backend working
   • Feature-engineered data stored and reloaded
   • Validation: 100% data integrity verified
```

---

## Current State Analysis

- **VCR Test Data**: 7 days, 1,014 total records (169 demand + 845 generation)
- **Production Needs**: 8760+ hours (1+ years) for ring attention training
- **Scale Factor**: ~520x larger datasets needed for production

## Recommended Architecture: Layered Data Pipeline

### Layer 1: Raw Data Ingestion (Pydantic Validated)

```python
# Use Pydantic for API boundary validation
class EIAResponse(BaseModel):
    response: EIAResponseData

class EIAResponseData(BaseModel):
    total: str
    dateFormat: str
    frequency: str
    data: List[Union[DemandRecord, GenerationRecord]]

class DemandRecord(BaseModel):
    period: datetime
    respondent: str
    respondent_name: str = Field(alias="respondent-name")
    type: str
    type_name: str = Field(alias="type-name")
    value: float
    value_units: str = Field(alias="value-units")

class GenerationRecord(BaseModel):
    period: datetime
    respondent: str
    respondent_name: str = Field(alias="respondent-name")
    fueltype: str
    type_name: str = Field(alias="type-name")
    value: float
    value_units: str = Field(alias="value-units")
```

**Purpose**: Data validation at API boundaries, error catching, type safety

### Layer 2: Normalized Storage (Your Own Models)

```python
# Simplified internal models optimized for ML
@dataclass
class EnergyTimeSeries:
    timestamp: pd.Timestamp
    region: str
    demand_mwh: float
    generation_by_fuel: Dict[str, float]  # {solar: 44, wind: 282, ...}

class EnergyDataStore:
    """Time-series optimized storage"""
    def __init__(self, storage_backend: str = "parquet"):
        self.backend = storage_backend

    def save_batch(self, data: List[EnergyTimeSeries]) -> None:
        """Save to optimized format (Parquet/HDF5)"""

    def load_sequence(self,
                     start: datetime,
                     end: datetime,
                     features: List[str]) -> np.ndarray:
        """Load ML-ready sequences for ring attention"""
```

**Purpose**: Fast ML data loading, efficient storage, simplified schema

### Layer 3: Feature Store (MLX-Ready)

```python
class EnergyFeatureStore:
    """Optimized for ring attention training"""

    def get_training_sequences(self,
                              seq_len: int = 8760,  # 1 year hourly
                              stride: int = 24,      # Daily stride
                              features: List[str] = None) -> Iterator[mx.array]:
        """Yield MLX arrays ready for ring attention"""

    def get_real_time_context(self,
                             lookback_hours: int = 168) -> mx.array:
        """Get recent data for inference"""
```

**Purpose**: ML-optimized data loading, memory efficiency, MLX integration

## Storage Strategy

### 1. **Raw Data Cache** (Short-term)

- **Format**: JSON files or lightweight SQLite
- **Retention**: 30 days
- **Purpose**: API response caching, debugging, replay tests

### 2. **Processed Data Store** (Long-term)

- **Format**: Parquet files (columnar, compressed)
- **Structure**:
  ```
  data/
  ├── processed/
  │   ├── demand/
  │   │   ├── 2023/
  │   │   │   ├── 2023-01.parquet
  │   │   │   └── 2023-02.parquet
  │   └── generation/
  │       ├── 2023/
  │           ├── 2023-01.parquet
  ```
- **Advantages**: Fast columnar access, compression, pandas/polars compatible

### 3. **Feature Store** (ML-Optimized)

- **Format**: HDF5 or NPZ (for MLX arrays)
- **Structure**: Pre-computed sequences ready for training
- **Update**: Incremental, event-driven

## Implementation Plan

### Phase 1: Foundation (Week 3-4 from llm-guide.yaml)

```python
# 1. Create Pydantic models for validation
# 2. Build basic data pipeline
# 3. Implement Parquet storage
# 4. Create initial feature extraction
```

### Phase 2: Scale & Optimize (Week 5-6)

```python
# 1. Implement incremental data loading
# 2. Add multiple data sources (CAISO, weather)
# 3. Create ring attention data loaders
# 4. Performance optimization
```

## ✅ Implementation Checklist

### Phase 1: Foundation (Current Week)

- [x] **EIA Schema Implementation**

  - [x] Create Pydantic models for EIA API responses (`src/core/integrations/eia/schema.py`)
  - [x] Write comprehensive tests (`tests/core/integrations/eia/test_schema.py`)
  - [x] Validate against VCR cassette data
  - [x] Handle edge cases (missing fields, data type conversions)

- [x] **Basic Data Pipeline**
  - [x] Implement raw data ingestion with validation
  - [x] Create simple file-based storage for processed data (Polars/Parquet)
  - [x] Add data transformation utilities (pivot, align timestamps)
  - [x] Test with existing 7-day Oregon dataset

### Phase 2: Storage & Processing (Current Week)

- [x] **Polars Integration**

  - [x] Install and configure Polars (`pip install polars`)
  - [x] Create Parquet file writers for time series data (`StorageManager`)
  - [x] Implement incremental data loading (`DataLoader`)
  - [x] Add compression and performance optimization

- [x] **Service Layer Foundation**
  - [x] Create `service/` folder in EIA integration (`src/core/integrations/eia/service/`)
  - [x] Implement `StorageManager` for Parquet operations
  - [x] Create `DataLoader` for high-level data operations
  - [x] Add comprehensive tests for service layer
  - [x] Demonstrate Polars DataFrame operations

### Phase 3: MLX Integration (✅ COMPLETED)

- [x] **Feature Engineering Module**

  - [x] Create `EnergyFeatureEngineer` class (`src/core/ml/features.py`)
  - [x] Implement temporal feature extraction (cyclical, binary)
  - [x] Add energy-specific features (renewable metrics, supply-demand)
  - [x] Support lagged and rolling window features
  - [x] MLX array conversion with normalization

- [x] **Sequence Generation**

  - [x] Implement `SequenceGenerator` for training data
  - [x] Create `RingAttentionSequenceGenerator` for distributed training
  - [x] Support configurable sequence lengths and strides
  - [x] Memory-efficient iterator-based generation

- [x] **MLX Data Loaders**

  - [x] Bridge EIA service layer with MLX training
  - [x] Automatic caching and storage integration
  - [x] High-level dataset creation functions

- [x] **Testing & Validation**
  - [x] Comprehensive test suite for ML modules
  - [x] Demo script showing full pipeline
  - [x] Performance validation with real data

**Results Achieved:**

- ✅ 25x+ feature expansion capability
- ✅ Ring attention ready sequences (8760+ hour support)
- ✅ 3.4MB datasets from 1008 hours of data
- ✅ Full integration with existing storage layer
- ✅ Production-ready MLX pipeline

### Phase 4: Scale & Production (Next Priority)

- [ ] **Multi-Source Integration**

  - [ ] Extend schema for CAISO and other APIs
  - [ ] Implement data fusion and alignment
  - [ ] Add error handling and retry logic
  - [ ] Create monitoring and alerting

- [ ] **ML Pipeline Integration**
  - [ ] Connect to ring attention training pipeline
  - [ ] Implement real-time data serving
  - [ ] Add experiment tracking (W&B integration)
  - [ ] Performance optimization and profiling

### Quick Wins (Today/Tomorrow)

- [x] ✅ Fix test configuration and environment variables
- [x] ✅ **Complete**: EIA schema + tests (this session)
- [x] 📊 Analyze VCR data with new schema validation
- [x] 🧪 **COMPLETE**: Create comprehensive data processing service layer (StorageManager + DataLoader)
- [x] 📁 **COMPLETE**: Set up and validate directory structure for processed data (raw/, interim/, processed/, external/)
- [x] 🚀 **BONUS**: Full Polars/Parquet integration with 24 passing tests
- [x] 🎯 **VALIDATED**: Real data test with Oregon energy (169 records, perfect data integrity)

### Ready for Phase 3 (Choose Your Adventure!)

**Option A: MLX Integration** 🤖

- [ ] Create `src/core/ml/` module for ring attention
- [ ] Implement sequence generators (8760-hour windows)
- [ ] Add MLX-compatible data loaders
- [ ] Feature engineering for renewable patterns

**Option B: Multi-Source Data** 🔌

- [ ] Add CAISO API integration (California data)
- [ ] Weather API for solar/wind correlation
- [ ] Data fusion and alignment utilities
- [ ] Multi-region analysis capabilities

**Option C: Production Pipeline** ⚙️

- [ ] Incremental data updates (daily/hourly)
- [ ] Data quality monitoring and alerts
- [ ] Pipeline orchestration (Prefect/Airflow)
- [ ] Cloud storage integration (S3/GCP)

### Technical Debt & Future Enhancements

- [ ] Add async API clients for better performance
- [ ] Implement data lineage tracking
- [ ] Add automated data quality monitoring
- [ ] Create data catalog and documentation
- [ ] Implement caching layers for frequently accessed data

## Recommended Tech Stack

### Data Processing

- **Polars**: Faster than pandas for large datasets
- **Parquet**: Columnar storage format
- **DuckDB**: Fast analytical queries on Parquet

### ML Pipeline

- **MLX**: Apple Silicon optimized arrays
- **Weights & Biases**: Experiment tracking
- **Prefect/Airflow**: Pipeline orchestration

### Storage

- **Local**: Parquet files + SQLite for metadata
- **Production**: Cloud storage (S3) + data lake pattern

## Code Example: Complete Pipeline

```python
from pathlib import Path
import polars as pl
import mlx.core as mx

class EnergyDataPipeline:
    def __init__(self, data_dir: Path):
        self.raw_cache = data_dir / "raw"
        self.processed = data_dir / "processed"
        self.features = data_dir / "features"

    async def ingest_api_data(self,
                             start_date: str,
                             end_date: str) -> None:
        """Fetch and validate API data"""
        # Use your EIAClient with Pydantic validation

    def process_raw_to_features(self,
                               date_range: Tuple[str, str]) -> None:
        """Convert raw API responses to ML features"""
        # Pivot generation data, align timestamps, create features

    def create_training_sequences(self,
                                 seq_len: int = 8760) -> None:
        """Pre-compute sequences for ring attention"""
        # Create overlapping sequences, save as MLX-compatible format

# Usage:
pipeline = EnergyDataPipeline(Path("data"))
await pipeline.ingest_api_data("2020-01-01", "2024-01-01")  # 4 years
pipeline.process_raw_to_features(("2020-01-01", "2024-01-01"))
pipeline.create_training_sequences(seq_len=8760)
```

## Why This Architecture?

### ✅ **Advantages**

1. **Separation of Concerns**: API validation ≠ ML data processing
2. **Performance**: Parquet + Polars for fast queries
3. **Scalability**: Can handle years of hourly data
4. **ML-Optimized**: Pre-computed sequences for ring attention
5. **Debugging**: Raw data preserved for analysis
6. **Incremental**: Add new data without reprocessing everything

### 🎯 **Aligns with Goals**

- **Ring Attention**: Handles 8760+ hour sequences efficiently
- **Production-Ready**: Fault-tolerant, monitorable pipeline
- **Apple Silicon**: MLX integration throughout
- **Research Quality**: Reproducible, version-controlled data

## Next Steps

1. **Start Simple**: Implement Layer 1 (Pydantic validation) first
2. **Validate Architecture**: Test with your 7-day dataset
3. **Scale Gradually**: Add one month of data, then expand
4. **Optimize**: Profile performance, add caching where needed
