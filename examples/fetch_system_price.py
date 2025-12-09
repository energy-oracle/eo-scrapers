#!/usr/bin/env python3
"""Example: Fetch UK System Price data from Elexon.

This script demonstrates how to use the EnergyOracle Elexon client
to fetch PPA settlement reference prices.

Usage:
    python examples/fetch_system_price.py
"""

import asyncio
from datetime import date

from eo_scrapers.clients.elexon import ElexonClient


async def main():
    """Fetch and display System Price data for PPA settlement."""

    print("=" * 60)
    print("EnergyOracle - UK System Price for PPA Settlement")
    print("=" * 60)
    print()

    async with ElexonClient() as client:
        # 1. Get monthly average for November 2024
        print("Fetching November 2024 System Price data...")
        print("(This may take a moment as we fetch 30 days of data)")
        print()

        monthly = await client.monthly_average(2024, 11, price_type="system_price")

        print("=" * 60)
        print("NOVEMBER 2024 SYSTEM PRICE SUMMARY")
        print("=" * 60)
        print(f"  Average:     £{monthly.average_price:.2f}/MWh")
        print(f"  Minimum:     £{monthly.min_price:.2f}/MWh")
        print(f"  Maximum:     £{monthly.max_price:.2f}/MWh")
        print(f"  Periods:     {monthly.num_periods}")
        print(f"  Date range:  {monthly.start_date} to {monthly.end_date}")
        print()

        # 2. Get a single day's prices
        print("=" * 60)
        print("SAMPLE DAY: November 1, 2024")
        print("=" * 60)

        daily = await client.daily_average(date(2024, 11, 1), price_type="system_price")
        print(f"  Daily average: £{daily.average_price:.2f}/MWh")
        print(f"  Min: £{daily.min_price:.2f}/MWh  Max: £{daily.max_price:.2f}/MWh")
        print()

        # 3. Show some individual settlement periods
        print("First 6 settlement periods:")
        print("-" * 50)
        print(f"{'Period':<8} {'Time':<12} {'SSP':<12} {'SBP':<12} {'Net':<12}")
        print("-" * 50)

        prices = await client.get_system_prices(date(2024, 11, 1))
        for p in prices[:6]:
            start_hour = (p.settlement_period - 1) // 2
            start_min = ((p.settlement_period - 1) % 2) * 30
            time_str = f"{start_hour:02d}:{start_min:02d}"
            print(
                f"SP{p.settlement_period:<5} {time_str:<12} "
                f"£{p.system_sell_price:<10.2f} "
                f"£{p.system_buy_price:<10.2f} "
                f"£{p.price:<10.2f}"
            )

        print()
        print("=" * 60)
        print("For PPA Settlement:")
        print("=" * 60)
        print()
        print("  Typical PPA formula: Price = System Price - £X/MWh discount")
        print()
        print(f"  Example with £5/MWh discount:")
        print(f"  November 2024 settlement = £{monthly.average_price:.2f} - £5.00")
        print(f"                           = £{monthly.average_price - 5:.2f}/MWh")
        print()

        # 4. Also show Day-Ahead prices for comparison
        print("=" * 60)
        print("DAY-AHEAD COMPARISON (APXMIDP)")
        print("=" * 60)

        try:
            da_daily = await client.daily_average(date(2024, 11, 1), price_type="day_ahead")
            print(f"  Nov 1, 2024 Day-Ahead average: £{da_daily.average_price:.2f}/MWh")
            print(f"  Nov 1, 2024 System Price avg:  £{daily.average_price:.2f}/MWh")
            spread = daily.average_price - da_daily.average_price
            print(f"  Spread (System - DA):          £{spread:.2f}/MWh")
        except Exception as e:
            print(f"  Day-ahead data not available: {e}")

        print()
        print("=" * 60)
        print("Data source: Elexon BMRS API (https://bmrs.elexon.co.uk)")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
