# eo-scrapers

Data ingestion layer for EnergyOracle - API clients for UK and European energy market data.

## Primary Focus: PPA Settlement

The #1 priority is providing **audit-grade reference prices for PPA (Power Purchase Agreement) settlement**. Every feature is evaluated against: "Does this help PPA settlement?"

## Data Sources

### UK (Primary - Implemented)

| Source | Data | Status |
|--------|------|--------|
| **Elexon BMRS** | System Price (SSP/SBP), Day-Ahead (MID) | ✅ Implemented |
| National Grid ESO | Demand, generation, forecasts | Planned |
| Carbon Intensity API | gCO2/kWh | Planned |

### EU (Planned)

| Source | Coverage | Status |
|--------|----------|--------|
| ENTSO-E | 35+ EU countries | Planned |
| EPEX | Germany, France | Planned |

## Installation

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
import asyncio
from eo_scrapers import ElexonClient

async def main():
    async with ElexonClient() as client:
        # Get monthly average System Price for PPA settlement
        avg = await client.monthly_average(2024, 11)
        print(f"November 2024 average: £{avg.average_price}/MWh")

        # Get daily prices
        daily = await client.daily_average(date(2024, 11, 1))
        print(f"Nov 1 average: £{daily.average_price}/MWh")

asyncio.run(main())
```

## Elexon Client API

### System Prices (Primary PPA Index)

```python
# Get all 48 settlement periods for a day
prices = await client.get_system_prices(date(2024, 11, 1))

# Get specific settlement period
prices = await client.get_system_prices(date(2024, 11, 1), settlement_period=23)

# Get date range
prices = await client.get_system_prices_range(
    from_date=date(2024, 11, 1),
    to_date=date(2024, 11, 30)
)
```

### Day-Ahead Prices (Market Index)

```python
# Get day-ahead prices from APXMIDP
prices = await client.get_market_index_prices(
    from_date=date(2024, 11, 1),
    to_date=date(2024, 11, 30)
)
```

### PPA Aggregations

```python
# Daily average (any day)
daily = await client.daily_average(date(2024, 11, 1), price_type="system_price")

# Monthly average (primary PPA settlement period)
monthly = await client.monthly_average(2024, 11, price_type="system_price")

# Access aggregate data
print(f"Average: £{monthly.average_price}/MWh")
print(f"Min: £{monthly.min_price}/MWh")
print(f"Max: £{monthly.max_price}/MWh")
print(f"Periods: {monthly.num_periods}")
```

## Example: November 2024 System Price

```bash
python examples/fetch_system_price.py
```

Output:
```
NOVEMBER 2024 SYSTEM PRICE SUMMARY
============================================================
  Average:     £93.72/MWh
  Minimum:     £-26.63/MWh
  Maximum:     £327.38/MWh
  Periods:     1,440
```

## Project Structure

```
eo-scrapers/
├── pyproject.toml
├── src/eo_scrapers/
│   ├── __init__.py
│   ├── clients/
│   │   ├── base.py          # Base client with retry logic
│   │   └── elexon.py        # Elexon BMRS API client
│   ├── models/
│   │   └── price.py         # Pydantic models (SystemPrice, DayAheadPrice)
│   └── utils/
│       └── time.py          # UK timezone utilities
├── tests/
│   └── test_elexon.py       # 13 tests with real API calls
└── examples/
    └── fetch_system_price.py
```

## Tech Stack

- **Python 3.11+**
- **httpx** - Async HTTP client
- **Pydantic** - Data validation
- **tenacity** - Retry logic
- **pytest-asyncio** - Async testing

## Running Tests

```bash
# Run all tests (makes real API calls)
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=eo_scrapers
```

## PPA Settlement Context

UK PPAs commonly reference these indices:

| Index | Source | Use Case |
|-------|--------|----------|
| **System Price** | Elexon SSP/SBP | Primary settlement index |
| **Day-Ahead** | APXMIDP | Forward pricing, discounts |
| **Carbon Intensity** | National Grid | Green premiums |

Typical PPA formula:
```
Settlement Price = System Price - £X/MWh discount
```

Example with £5/MWh discount:
```
November 2024 = £93.72 - £5.00 = £88.72/MWh
```

## License

MIT
