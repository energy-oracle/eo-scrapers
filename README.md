# eo-scrapers

Data ingestion layer for EnergyOracle - API clients for UK energy market data with Supabase storage.

## Primary Focus: PPA Settlement

The #1 priority is providing **audit-grade reference prices for PPA (Power Purchase Agreement) settlement**. Every feature is evaluated against: "Does this help PPA settlement?"

## Features

- **4 data sources** - System prices, Day-ahead, Carbon intensity, Fuel mix
- **Supabase storage** - PostgreSQL with automatic upsert and audit logging
- **CLI tool** - Fetch, backfill, and monitor data
- **Scheduler** - Continuous data fetching with APScheduler
- **63,000+ records** - Historical data from January 2025

## Data Sources

### UK (Implemented)

| Source | Data | API | Status |
|--------|------|-----|--------|
| **Elexon BMRS** | System Price (SSP/SBP) | `/balancing/settlement/system-prices` | ✅ |
| **Elexon BMRS** | Day-Ahead (APXMIDP) | `/balancing/settlement/market-index` | ✅ |
| **National Grid** | Carbon Intensity | `carbonintensity.org.uk` | ✅ |
| **National Grid** | Fuel Mix | `carbonintensity.org.uk` | ✅ |

### Database Tables

| Table | Records | Description |
|-------|---------|-------------|
| `system_prices` | 16,400+ | SSP/SBP imbalance prices |
| `day_ahead_prices` | 14,100+ | APXMIDP market index |
| `carbon_intensity` | 16,200+ | gCO2/kWh readings |
| `fuel_mix` | 16,300+ | Generation breakdown % |
| `fetch_logs` | - | Audit trail |

## Installation

```bash
cd eo-scrapers
pip install -e ".[dev]"
```

## Environment Setup

Create `.env` file in project root:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key
```

## CLI Usage

### Fetch Recent Data

```bash
# Fetch all sources (last 2 days)
eo-scrapers fetch

# Fetch specific source
eo-scrapers fetch --source system
eo-scrapers fetch --source dayahead
eo-scrapers fetch --source carbon
eo-scrapers fetch --source fuelmix

# Fetch more days
eo-scrapers fetch --days 7
```

### Backfill Historical Data

```bash
# Backfill all sources
eo-scrapers backfill --from 2025-01-01 --to 2025-12-31

# Backfill specific source
eo-scrapers backfill --from 2025-01-01 --to 2025-06-30 --source dayahead
```

### Check Status

```bash
eo-scrapers status
```

Output:
```
EnergyOracle Database Status
==================================================

System Prices:
  Date range: 2025-01-01 to 2025-12-09

Last system_prices fetch:
  Completed: 2025-12-09T14:30:00+00:00
  Records: 96
  Status: success
```

### Calculate Monthly Average

```bash
eo-scrapers monthly-avg 2025 11
```

Output:
```
Monthly System Price Summary - 2025-11
==================================================
  Average:  £72.50/MWh
  Minimum:  £-15.00/MWh
  Maximum:  £285.00/MWh
  Periods:  1440

PPA Settlement Example (£5/MWh discount):
  Settlement Price = £72.50 - £5.00 = £67.50/MWh
```

### Run Scheduler

```bash
# Run continuous fetcher (every 30 min)
eo-scrapers scheduler
```

## Python API

### Elexon Client

```python
import asyncio
from datetime import date
from eo_scrapers import ElexonClient

async def main():
    async with ElexonClient() as client:
        # Get system prices for a date
        prices = await client.get_system_prices(date(2025, 11, 1))
        print(f"Fetched {len(prices)} settlement periods")

        # Get date range
        prices = await client.get_system_prices_range(
            from_date=date(2025, 11, 1),
            to_date=date(2025, 11, 30)
        )

        # Get monthly average for PPA settlement
        avg = await client.monthly_average(2025, 11)
        print(f"November 2025: £{avg.average_price}/MWh")

        # Get day-ahead prices
        dayahead = await client.get_market_index_prices(
            from_date=date(2025, 11, 1),
            to_date=date(2025, 11, 7)
        )

asyncio.run(main())
```

### Carbon Intensity Client

```python
from datetime import date
from eo_scrapers import CarbonIntensityClient

async def main():
    async with CarbonIntensityClient() as client:
        # Get current intensity
        current = await client.get_current()
        print(f"Current: {current.intensity} gCO2/kWh ({current.intensity_index})")

        # Get by date
        readings = await client.get_by_date(date(2025, 11, 1))
        print(f"Fetched {len(readings)} half-hourly readings")

        # Get fuel mix
        fuel_mix = await client.get_fuel_mix_by_date(date(2025, 11, 1))
        for fm in fuel_mix[:1]:
            print(f"Wind: {fm.wind}%, Gas: {fm.gas}%, Nuclear: {fm.nuclear}%")

asyncio.run(main())
```

### Data Fetcher (with Storage)

```python
from eo_scrapers.scheduler import DataFetcher

async def main():
    fetcher = DataFetcher()

    # Fetch all sources
    results = await fetcher.fetch_all(days_back=2)
    print(results)
    # {'system_prices': {'fetched': 96, 'inserted': 96},
    #  'day_ahead_prices': {'fetched': 96, 'inserted': 96},
    #  'carbon_intensity': {'fetched': 96, 'inserted': 96},
    #  'fuel_mix': {'fetched': 48, 'inserted': 48}}

    # Backfill historical data
    results = await fetcher.backfill(
        from_date=date(2025, 1, 1),
        to_date=date(2025, 6, 30),
        data_types=["system_prices", "carbon_intensity"]
    )

asyncio.run(main())
```

## Project Structure

```
eo-scrapers/
├── pyproject.toml
├── src/eo_scrapers/
│   ├── __init__.py
│   ├── cli.py                 # CLI commands
│   ├── scheduler.py           # DataFetcher + Scheduler
│   ├── clients/
│   │   ├── base.py            # Base client with retry
│   │   ├── elexon.py          # Elexon BMRS API
│   │   └── carbon_intensity.py # National Grid Carbon API
│   ├── models/
│   │   ├── price.py           # SystemPrice, DayAheadPrice
│   │   └── carbon.py          # CarbonIntensity, FuelMix
│   └── storage/
│       └── supabase.py        # SupabaseWriter
├── sql/
│   └── 001_initial_schema.sql # Database schema
├── tests/
│   └── test_elexon.py
└── examples/
    └── fetch_system_price.py
```

## Database Schema

```sql
-- System Prices (Elexon SSP/SBP)
CREATE TABLE system_prices (
    id BIGSERIAL PRIMARY KEY,
    settlement_date DATE NOT NULL,
    settlement_period INT NOT NULL,
    system_sell_price DECIMAL(12,4),
    system_buy_price DECIMAL(12,4),
    price DECIMAL(12,4),
    data_source TEXT DEFAULT 'elexon_bmrs',
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(settlement_date, settlement_period)
);

-- Day Ahead Prices (APXMIDP)
CREATE TABLE day_ahead_prices (
    id BIGSERIAL PRIMARY KEY,
    settlement_date DATE NOT NULL,
    settlement_period INT NOT NULL,
    price DECIMAL(12,4),
    data_provider TEXT DEFAULT 'APXMIDP',
    data_source TEXT DEFAULT 'elexon_bmrs',
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(settlement_date, settlement_period, data_provider)
);

-- Carbon Intensity
CREATE TABLE carbon_intensity (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMPTZ NOT NULL UNIQUE,
    intensity INT NOT NULL,
    intensity_index TEXT,
    data_source TEXT DEFAULT 'national_grid',
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);

-- Fuel Mix
CREATE TABLE fuel_mix (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMPTZ NOT NULL UNIQUE,
    biomass DECIMAL(5,2),
    coal DECIMAL(5,2),
    gas DECIMAL(5,2),
    hydro DECIMAL(5,2),
    imports DECIMAL(5,2),
    nuclear DECIMAL(5,2),
    other DECIMAL(5,2),
    solar DECIMAL(5,2),
    wind DECIMAL(5,2),
    data_source TEXT DEFAULT 'national_grid',
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);

-- Audit log
CREATE TABLE fetch_logs (
    id BIGSERIAL PRIMARY KEY,
    fetch_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    records_fetched INT,
    records_inserted INT,
    records_updated INT,
    status TEXT,
    error_message TEXT,
    metadata JSONB
);
```

## PPA Settlement Context

UK PPAs commonly reference these indices:

| Index | Source | Use Case |
|-------|--------|----------|
| **System Price** | Elexon SSP/SBP | Primary settlement index |
| **Day-Ahead** | APXMIDP | Forward pricing, discounts |
| **Carbon Intensity** | National Grid | Green premiums, ESG |
| **Fuel Mix** | National Grid | Renewable % verification |

### Typical PPA Formula

```
Settlement Price = Monthly Average System Price - £X/MWh discount
```

Example with £5/MWh discount:
```
November 2025 = £72.50 - £5.00 = £67.50/MWh
```

### SQL Query for PPA Settlement

```sql
SELECT
    DATE_TRUNC('month', settlement_date) as month,
    ROUND(AVG(price)::numeric, 2) as avg_system_price,
    COUNT(*) as settlement_periods
FROM system_prices
WHERE settlement_date BETWEEN '2025-11-01' AND '2025-11-30'
GROUP BY DATE_TRUNC('month', settlement_date);
```

## Tech Stack

- **Python 3.11+**
- **httpx** - Async HTTP client
- **Pydantic** - Data validation
- **tenacity** - Retry logic
- **click** - CLI framework
- **supabase** - Database client
- **apscheduler** - Task scheduling

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=eo_scrapers
```

## Next Steps

- [ ] FastAPI REST server
- [ ] Authentication (API keys)
- [ ] Rate limiting
- [ ] SDK packages (Python, JavaScript)
- [ ] Chainlink oracle integration

## License

MIT
