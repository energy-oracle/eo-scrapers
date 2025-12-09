"""Scheduler for automatic data fetching.

Uses APScheduler to run periodic fetch jobs for all data sources.
Supports both continuous operation and one-shot fetches.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from eo_scrapers.clients.carbon_intensity import CarbonIntensityClient
from eo_scrapers.clients.elexon import ElexonClient
from eo_scrapers.storage.supabase import SupabaseWriter

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetches energy data and stores it in the database.

    Handles all data sources:
    - System prices from Elexon
    - Day-ahead prices from Elexon
    - Carbon intensity from National Grid
    - Fuel mix from National Grid

    Example:
        fetcher = DataFetcher()
        await fetcher.fetch_all()
    """

    def __init__(self, writer: SupabaseWriter | None = None):
        """Initialize the data fetcher.

        Args:
            writer: Supabase writer instance. Creates new one if not provided.
        """
        self.writer = writer or SupabaseWriter()

    async def fetch_system_prices(
        self,
        target_date: date | None = None,
        days_back: int = 2,
    ) -> dict[str, int]:
        """Fetch system prices for a date or recent days.

        Args:
            target_date: Specific date to fetch, or None for recent data
            days_back: Number of days to fetch if target_date is None

        Returns:
            Stats dict with fetch counts
        """
        async with ElexonClient() as client:
            if target_date:
                prices = await client.get_system_prices(target_date)
                logger.info(f"Fetched {len(prices)} system prices for {target_date}")
            else:
                # Fetch last N days
                from_date = date.today() - timedelta(days=days_back)
                to_date = date.today()
                prices = await client.get_system_prices_range(from_date, to_date)
                logger.info(f"Fetched {len(prices)} system prices for {from_date} to {to_date}")

            return self.writer.save_system_prices(prices)

    async def fetch_day_ahead_prices(
        self,
        target_date: date | None = None,
        days_back: int = 2,
    ) -> dict[str, int]:
        """Fetch day-ahead prices for a date or recent days.

        Args:
            target_date: Specific date to fetch, or None for recent data
            days_back: Number of days to fetch if target_date is None

        Returns:
            Stats dict with fetch counts
        """
        async with ElexonClient() as client:
            if target_date:
                from_date = to_date = target_date
            else:
                from_date = date.today() - timedelta(days=days_back)
                to_date = date.today()

            prices = await client.get_market_index_prices(from_date, to_date)
            logger.info(f"Fetched {len(prices)} day-ahead prices for {from_date} to {to_date}")

            return self.writer.save_day_ahead_prices(prices)

    async def fetch_carbon_intensity(
        self,
        target_date: date | None = None,
    ) -> dict[str, int]:
        """Fetch carbon intensity data.

        Args:
            target_date: Specific date to fetch, or None for today/yesterday

        Returns:
            Stats dict with fetch counts
        """
        async with CarbonIntensityClient() as client:
            if target_date:
                readings = await client.get_by_date(target_date)
            else:
                # Get yesterday (complete data) and today
                readings = await client.get_yesterday()
                readings.extend(await client.get_today())

            logger.info(f"Fetched {len(readings)} carbon intensity readings")

            # Convert to dict format for storage
            records = [
                {
                    "datetime": r.datetime_from.isoformat(),
                    "intensity": r.intensity,
                    "intensity_index": r.intensity_index,
                    "data_source": r.data_source,
                }
                for r in readings
            ]

            return self.writer.save_carbon_intensity(records)

    async def fetch_fuel_mix(
        self,
        target_date: date | None = None,
    ) -> dict[str, int]:
        """Fetch fuel mix data.

        Args:
            target_date: Specific date to fetch, or None for today/yesterday

        Returns:
            Stats dict with fetch counts
        """
        async with CarbonIntensityClient() as client:
            if target_date:
                fuel_mixes = await client.get_fuel_mix_by_date(target_date)
            else:
                # Get yesterday (complete data)
                yesterday = date.today() - timedelta(days=1)
                fuel_mixes = await client.get_fuel_mix_by_date(yesterday)

            logger.info(f"Fetched {len(fuel_mixes)} fuel mix readings")
            return self.writer.save_fuel_mix(fuel_mixes)

    async def fetch_all(self, days_back: int = 2) -> dict[str, dict[str, int]]:
        """Fetch all data sources.

        Args:
            days_back: Number of days of historical data to fetch

        Returns:
            Dict mapping source name to stats
        """
        results = {}

        try:
            results["system_prices"] = await self.fetch_system_prices(days_back=days_back)
        except Exception as e:
            logger.error(f"Failed to fetch system prices: {e}")
            results["system_prices"] = {"error": str(e)}

        try:
            results["day_ahead_prices"] = await self.fetch_day_ahead_prices(days_back=days_back)
        except Exception as e:
            logger.error(f"Failed to fetch day-ahead prices: {e}")
            results["day_ahead_prices"] = {"error": str(e)}

        try:
            results["carbon_intensity"] = await self.fetch_carbon_intensity()
        except Exception as e:
            logger.error(f"Failed to fetch carbon intensity: {e}")
            results["carbon_intensity"] = {"error": str(e)}

        try:
            results["fuel_mix"] = await self.fetch_fuel_mix()
        except Exception as e:
            logger.error(f"Failed to fetch fuel mix: {e}")
            results["fuel_mix"] = {"error": str(e)}

        return results

    async def backfill(
        self,
        from_date: date,
        to_date: date,
        data_types: list[str] | None = None,
    ) -> dict[str, dict[str, int]]:
        """Backfill historical data.

        Args:
            from_date: Start date (inclusive)
            to_date: End date (inclusive)
            data_types: List of types to backfill. Defaults to all.
                       Options: system_prices, day_ahead_prices, carbon_intensity, fuel_mix

        Returns:
            Dict mapping source name to stats
        """
        if data_types is None:
            data_types = ["system_prices", "day_ahead_prices", "carbon_intensity", "fuel_mix"]

        results = {}

        if "system_prices" in data_types:
            logger.info(f"Backfilling system prices from {from_date} to {to_date}")
            async with ElexonClient() as client:
                prices = await client.get_system_prices_range(from_date, to_date)
                results["system_prices"] = self.writer.save_system_prices(prices)

        if "day_ahead_prices" in data_types:
            logger.info(f"Backfilling day-ahead prices from {from_date} to {to_date}")
            # Market Index API has a max range limit, so we batch by week
            async with ElexonClient() as client:
                all_prices = []
                current = from_date
                while current <= to_date:
                    batch_end = min(current + timedelta(days=6), to_date)
                    try:
                        prices = await client.get_market_index_prices(current, batch_end)
                        all_prices.extend(prices)
                        logger.info(f"Fetched {len(prices)} day-ahead prices for {current} to {batch_end}")
                    except Exception as e:
                        logger.warning(f"Failed to fetch day-ahead for {current} to {batch_end}: {e}")
                    current = batch_end + timedelta(days=1)
                results["day_ahead_prices"] = self.writer.save_day_ahead_prices(all_prices)

        if "carbon_intensity" in data_types:
            logger.info(f"Backfilling carbon intensity from {from_date} to {to_date}")
            async with CarbonIntensityClient() as client:
                all_readings = []
                current = from_date
                while current <= to_date:
                    try:
                        readings = await client.get_by_date(current)
                        all_readings.extend(readings)
                    except Exception as e:
                        logger.warning(f"Failed to fetch carbon for {current}: {e}")
                    current += timedelta(days=1)

                records = [
                    {
                        "datetime": r.datetime_from.isoformat(),
                        "intensity": r.intensity,
                        "intensity_index": r.intensity_index,
                        "data_source": r.data_source,
                    }
                    for r in all_readings
                ]
                results["carbon_intensity"] = self.writer.save_carbon_intensity(records)

        if "fuel_mix" in data_types:
            logger.info(f"Backfilling fuel mix from {from_date} to {to_date}")
            async with CarbonIntensityClient() as client:
                all_fuel_mixes = []
                current = from_date
                while current <= to_date:
                    try:
                        fuel_mixes = await client.get_fuel_mix_by_date(current)
                        all_fuel_mixes.extend(fuel_mixes)
                    except Exception as e:
                        logger.warning(f"Failed to fetch fuel mix for {current}: {e}")
                    current += timedelta(days=1)

                results["fuel_mix"] = self.writer.save_fuel_mix(all_fuel_mixes)

        return results


class Scheduler:
    """Scheduler for continuous data fetching.

    Runs jobs at specified intervals to keep the database up to date.

    Example:
        scheduler = Scheduler()
        scheduler.start()
        # Runs until interrupted
    """

    def __init__(self):
        """Initialize the scheduler."""
        self.scheduler = AsyncIOScheduler()
        self.fetcher = DataFetcher()

    def _setup_jobs(self) -> None:
        """Configure scheduled jobs."""
        # Fetch system prices every 30 minutes
        # Settlement data is published ~30 min after the settlement period
        self.scheduler.add_job(
            self._fetch_system_prices_job,
            IntervalTrigger(minutes=30),
            id="system_prices",
            name="Fetch System Prices",
            replace_existing=True,
        )

        # Fetch day-ahead prices every hour
        # Day-ahead prices are published daily but we check hourly
        self.scheduler.add_job(
            self._fetch_day_ahead_job,
            IntervalTrigger(hours=1),
            id="day_ahead_prices",
            name="Fetch Day-Ahead Prices",
            replace_existing=True,
        )

        # Fetch carbon intensity every 30 minutes
        # Matches the half-hourly settlement periods
        self.scheduler.add_job(
            self._fetch_carbon_job,
            IntervalTrigger(minutes=30),
            id="carbon_intensity",
            name="Fetch Carbon Intensity",
            replace_existing=True,
        )

        # Daily cleanup/integrity check at 3am
        self.scheduler.add_job(
            self._daily_maintenance_job,
            CronTrigger(hour=3, minute=0),
            id="daily_maintenance",
            name="Daily Maintenance",
            replace_existing=True,
        )

    async def _fetch_system_prices_job(self) -> None:
        """Job: Fetch recent system prices."""
        logger.info("Running system prices job")
        try:
            stats = await self.fetcher.fetch_system_prices(days_back=1)
            logger.info(f"System prices job completed: {stats}")
        except Exception as e:
            logger.error(f"System prices job failed: {e}")

    async def _fetch_day_ahead_job(self) -> None:
        """Job: Fetch recent day-ahead prices."""
        logger.info("Running day-ahead prices job")
        try:
            stats = await self.fetcher.fetch_day_ahead_prices(days_back=1)
            logger.info(f"Day-ahead prices job completed: {stats}")
        except Exception as e:
            logger.error(f"Day-ahead prices job failed: {e}")

    async def _fetch_carbon_job(self) -> None:
        """Job: Fetch recent carbon intensity."""
        logger.info("Running carbon intensity job")
        try:
            stats = await self.fetcher.fetch_carbon_intensity()
            logger.info(f"Carbon intensity job completed: {stats}")
        except Exception as e:
            logger.error(f"Carbon intensity job failed: {e}")

    async def _daily_maintenance_job(self) -> None:
        """Job: Daily maintenance - backfill any gaps."""
        logger.info("Running daily maintenance")
        try:
            # Ensure we have complete data for the last 7 days
            from_date = date.today() - timedelta(days=7)
            to_date = date.today() - timedelta(days=1)
            await self.fetcher.backfill(from_date, to_date)
            logger.info("Daily maintenance completed")
        except Exception as e:
            logger.error(f"Daily maintenance failed: {e}")

    def start(self) -> None:
        """Start the scheduler.

        This is a blocking call that runs until interrupted.
        """
        self._setup_jobs()

        logger.info("Starting scheduler...")
        logger.info("Jobs scheduled:")
        for job in self.scheduler.get_jobs():
            logger.info(f"  - {job.name}: {job.trigger}")

        self.scheduler.start()

        try:
            # Run forever
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down scheduler...")
            self.scheduler.shutdown()

    async def run_once(self) -> dict[str, dict[str, int]]:
        """Run all fetch jobs once (useful for testing).

        Returns:
            Results from all fetchers
        """
        return await self.fetcher.fetch_all()
