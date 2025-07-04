"""
EIA API Response Structure Analysis
=====================================

Based on the VCR cassettes captured from the Oregon EIA tests, here's what each endpoint provides:
"""

# 1. DEMAND DATA (eia_oregon_demand.yaml)
demand_response_shape = {
    "response": {
        "total": "169",                    # Total records available
        "dateFormat": "YYYY-MM-DD\"T\"HH24",
        "frequency": "hourly",
        "data": [
            {
                "period": "2023-01-01T00",
                "respondent": "PACW",
                "respondent-name": "PacifiCorp West",
                "type": "D",               # D = Demand
                "type-name": "Demand",
                "value": "2455",           # Megawatthours
                "value-units": "megawatthours"
            }
            # ... 168 more hourly records (7 days × 24 hours + 1)
        ]
    }
}

# 2. RENEWABLE/FUEL MIX DATA (eia_oregon_renewable.yaml)
renewable_response_shape = {
    "response": {
        "total": "845",                    # Much larger! Multiple fuel types per hour
        "dateFormat": "YYYY-MM-DD\"T\"HH24",
        "frequency": "hourly",
        "data": [
            {
                "period": "2023-01-01T00",
                "respondent": "PACW",
                "respondent-name": "PacifiCorp West",
                "fueltype": "NG",          # Natural Gas
                "type-name": "Natural Gas",
                "value": "0",
                "value-units": "megawatthours"
            },
            {
                "period": "2023-01-01T00",  # Same hour, different fuel
                "respondent": "PACW",
                "respondent-name": "PacifiCorp West",
                "fueltype": "SUN",         # Solar
                "type-name": "Solar",
                "value": "44",
                "value-units": "megawatthours"
            },
            {
                "period": "2023-01-01T00",  # Same hour, different fuel
                "respondent": "PACW",
                "respondent-name": "PacifiCorp West",
                "fueltype": "WAT",         # Hydro
                "type-name": "Hydro",
                "value": "476",
                "value-units": "megawatthours"
            },
            {
                "period": "2023-01-01T00",  # Same hour, different fuel
                "respondent": "PACW",
                "respondent-name": "PacifiCorp West",
                "fueltype": "WND",         # Wind
                "type-name": "Wind",
                "value": "282",
                "value-units": "megawatthours"
            }
            # ... continues for all hours × all fuel types
        ]
    }
}

# 3. COMPREHENSIVE DATA (eia_oregon_comprehensive.yaml)
# NOTE: This appears to be identical to the demand data!
comprehensive_response_shape = demand_response_shape  # Same structure

"""
KEY DIFFERENCES:
===============

1. **DEMAND Data** (169 records):
   - One record per hour for 7 days
   - Single "type": "D" (Demand)
   - Simple time series: timestamp → demand value

2. **RENEWABLE/FUEL MIX Data** (845 records):
   - Multiple records per hour (one per fuel type)
   - Contains "fueltype" field instead of "type"
   - Fuel types found: NG, OTH, SUN, WAT, WND
   - 845 ÷ 169 hours ≈ 5 fuel types per hour
   - Requires pivoting to get time series per fuel type

3. **COMPREHENSIVE Data** (169 records):
   - Currently identical to demand data
   - The client method likely calls multiple endpoints and merges them

FUEL TYPE CODES:
===============
- NG  = Natural Gas
- OTH = Other
- SUN = Solar
- WAT = Hydro (Water)
- WND = Wind

OREGON ENERGY PATTERNS (from the data):
=====================================
- Demand ranges: ~2,200-3,300 MWh per hour
- High hydro generation: ~480-490 MWh consistently
- Variable wind: 56-334 MWh depending on conditions
- Minimal solar in January (winter): 0-44 MWh
- Zero natural gas generation in this timeframe
- Small "other" category: ~44-50 MWh

DATA STRUCTURE FOR ML:
====================
For ring attention and time series analysis, you'll want to:

1. Pivot renewable data by fuel type to get parallel time series
2. Align timestamps between demand and generation data
3. Create feature matrix: [timestamp, demand, solar, wind, hydro, other, ng]
4. Handle missing values and ensure consistent sampling frequency
"""

if __name__ == "__main__":
    print(__doc__)
