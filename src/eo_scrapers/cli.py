"""CLI for EnergyOracle data scrapers.

Commands:
    fetch      - Fetch data from all sources
    backfill   - Backfill historical data
    scheduler  - Run continuous data fetcher
    status     - Show database status

Usage:
    eo-scrapers fetch
    eo-scrapers backfill --from 2024-11-01 --to 2024-11-30
    eo-scrapers scheduler
    eo-scrapers status
"""

import asyncio
import logging
from datetime import date, datetime

import click
from dotenv import load_dotenv

# Load environment variables before imports that need them
load_dotenv()

from eo_scrapers.scheduler import DataFetcher, Scheduler
from eo_scrapers.storage.supabase import SupabaseWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def main(verbose: bool) -> None:
    """EnergyOracle Data Scrapers - UK Energy Market Data for PPA Settlement."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


@main.command()
@click.option(
    "--source",
    "-s",
    type=click.Choice(["all", "system", "dayahead", "carbon", "fuelmix"]),
    default="all",
    help="Data source to fetch",
)
@click.option("--days", "-d", default=2, help="Number of days to fetch")
def fetch(source: str, days: int) -> None:
    """Fetch recent data from energy APIs and store in database."""
    click.echo(f"Fetching {source} data for last {days} days...")

    fetcher = DataFetcher()

    async def run_fetch() -> dict:
        if source == "all":
            return await fetcher.fetch_all(days_back=days)
        elif source == "system":
            return {"system_prices": await fetcher.fetch_system_prices(days_back=days)}
        elif source == "dayahead":
            return {"day_ahead_prices": await fetcher.fetch_day_ahead_prices(days_back=days)}
        elif source == "carbon":
            return {"carbon_intensity": await fetcher.fetch_carbon_intensity()}
        elif source == "fuelmix":
            return {"fuel_mix": await fetcher.fetch_fuel_mix()}
        return {}

    results = asyncio.run(run_fetch())

    click.echo("\nResults:")
    for source_name, stats in results.items():
        if "error" in stats:
            click.echo(f"  {source_name}: ERROR - {stats['error']}")
        else:
            click.echo(f"  {source_name}: {stats.get('fetched', 0)} fetched, {stats.get('inserted', 0)} saved")


@main.command()
@click.option("--from", "from_date", required=True, help="Start date (YYYY-MM-DD)")
@click.option("--to", "to_date", required=True, help="End date (YYYY-MM-DD)")
@click.option(
    "--source",
    "-s",
    type=click.Choice(["all", "system", "dayahead", "carbon", "fuelmix"]),
    default="all",
    help="Data source to backfill",
)
def backfill(from_date: str, to_date: str, source: str) -> None:
    """Backfill historical data for a date range."""
    start = parse_date(from_date)
    end = parse_date(to_date)

    if start > end:
        click.echo("Error: from_date must be before to_date")
        return

    days = (end - start).days + 1
    click.echo(f"Backfilling {source} data from {start} to {end} ({days} days)...")

    # Map source names
    source_map = {
        "all": None,  # None means all
        "system": ["system_prices"],
        "dayahead": ["day_ahead_prices"],
        "carbon": ["carbon_intensity"],
        "fuelmix": ["fuel_mix"],
    }

    fetcher = DataFetcher()

    async def run_backfill() -> dict:
        return await fetcher.backfill(start, end, data_types=source_map[source])

    results = asyncio.run(run_backfill())

    click.echo("\nBackfill Results:")
    for source_name, stats in results.items():
        if "error" in stats:
            click.echo(f"  {source_name}: ERROR - {stats['error']}")
        else:
            click.echo(f"  {source_name}: {stats.get('fetched', 0)} fetched, {stats.get('inserted', 0)} saved")


@main.command()
def scheduler() -> None:
    """Run the continuous data fetcher.

    Fetches data every 30 minutes and stores in database.
    Press Ctrl+C to stop.
    """
    click.echo("Starting EnergyOracle scheduler...")
    click.echo("Press Ctrl+C to stop")
    click.echo()

    sched = Scheduler()
    sched.start()


@main.command()
def status() -> None:
    """Show database status and data coverage."""
    click.echo("EnergyOracle Database Status")
    click.echo("=" * 50)

    try:
        writer = SupabaseWriter()

        # Get date range for system prices
        min_date, max_date = writer.get_system_price_date_range()

        if min_date and max_date:
            click.echo(f"\nSystem Prices:")
            click.echo(f"  Date range: {min_date} to {max_date}")
        else:
            click.echo(f"\nSystem Prices: No data")

        # Get recent fetch logs
        for fetch_type in ["system_prices", "day_ahead_prices", "carbon_intensity", "fuel_mix"]:
            latest = writer.get_latest_fetch(fetch_type)
            if latest:
                click.echo(f"\nLast {fetch_type} fetch:")
                click.echo(f"  Completed: {latest['completed_at']}")
                click.echo(f"  Records: {latest['records_fetched']}")
                click.echo(f"  Status: {latest['status']}")

    except Exception as e:
        click.echo(f"\nError connecting to database: {e}")
        click.echo("Make sure SUPABASE_URL and SUPABASE_SERVICE_KEY are set in .env")


@main.command()
@click.argument("year", type=int)
@click.argument("month", type=int)
def monthly_avg(year: int, month: int) -> None:
    """Calculate monthly average prices for PPA settlement.

    Example: eo-scrapers monthly-avg 2024 11
    """
    from eo_scrapers.clients.elexon import ElexonClient

    click.echo(f"Calculating monthly average for {year}-{month:02d}...")

    async def get_avg():
        async with ElexonClient() as client:
            return await client.monthly_average(year, month)

    try:
        result = asyncio.run(get_avg())

        click.echo()
        click.echo(f"Monthly System Price Summary - {year}-{month:02d}")
        click.echo("=" * 50)
        click.echo(f"  Average:  £{result.average_price:.2f}/MWh")
        click.echo(f"  Minimum:  £{result.min_price:.2f}/MWh")
        click.echo(f"  Maximum:  £{result.max_price:.2f}/MWh")
        click.echo(f"  Periods:  {result.num_periods}")
        click.echo()
        click.echo("PPA Settlement Example (£5/MWh discount):")
        click.echo(f"  Settlement Price = £{result.average_price:.2f} - £5.00 = £{result.average_price - 5:.2f}/MWh")

    except Exception as e:
        click.echo(f"Error: {e}")


if __name__ == "__main__":
    main()
