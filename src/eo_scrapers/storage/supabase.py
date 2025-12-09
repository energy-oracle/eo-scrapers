"""Supabase storage backend for EnergyOracle data.

Provides upsert operations for all price data types with
proper conflict handling and audit logging.
"""

import logging
import os
from datetime import UTC, datetime
from typing import Any

from dotenv import load_dotenv
from supabase import Client, create_client

from eo_scrapers.models.carbon import FuelMix
from eo_scrapers.models.price import DayAheadPrice, SystemPrice

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class SupabaseWriter:
    """Writer for storing energy price data in Supabase.

    Handles upsert operations with conflict resolution on unique constraints.
    All operations are logged for audit trail.

    Example:
        writer = SupabaseWriter()
        await writer.save_system_prices(prices)
    """

    def __init__(
        self,
        url: str | None = None,
        key: str | None = None,
    ):
        """Initialize Supabase client.

        Args:
            url: Supabase project URL. Defaults to SUPABASE_URL env var.
            key: Supabase service key. Defaults to SUPABASE_SERVICE_KEY env var.
        """
        self.url = url or os.getenv("SUPABASE_URL")
        self.key = key or os.getenv("SUPABASE_SERVICE_KEY")

        if not self.url or not self.key:
            raise ValueError(
                "Supabase credentials required. Set SUPABASE_URL and SUPABASE_SERVICE_KEY "
                "environment variables or pass them directly."
            )

        self._client: Client = create_client(self.url, self.key)

    def _start_fetch_log(self, fetch_type: str, metadata: dict[str, Any] | None = None) -> int:
        """Start a fetch log entry and return its ID."""
        result = (
            self._client.table("fetch_logs")
            .insert(
                {
                    "fetch_type": fetch_type,
                    "started_at": datetime.now(UTC).isoformat(),
                    "status": "running",
                    "metadata": metadata,
                }
            )
            .execute()
        )
        return result.data[0]["id"]

    def _complete_fetch_log(
        self,
        log_id: int,
        records_fetched: int,
        records_inserted: int,
        records_updated: int,
        status: str = "success",
        error_message: str | None = None,
    ) -> None:
        """Complete a fetch log entry."""
        self._client.table("fetch_logs").update(
            {
                "completed_at": datetime.now(UTC).isoformat(),
                "records_fetched": records_fetched,
                "records_inserted": records_inserted,
                "records_updated": records_updated,
                "status": status,
                "error_message": error_message,
            }
        ).eq("id", log_id).execute()

    def save_system_prices(
        self,
        prices: list[SystemPrice],
        log_fetch: bool = True,
    ) -> dict[str, int]:
        """Save system prices to Supabase.

        Uses upsert to handle duplicates gracefully.

        Args:
            prices: List of SystemPrice objects to save
            log_fetch: Whether to log this fetch operation

        Returns:
            Dict with counts: {'fetched', 'inserted', 'updated'}
        """
        if not prices:
            return {"fetched": 0, "inserted": 0, "updated": 0}

        log_id = None
        if log_fetch:
            metadata = {
                "date_range": {
                    "from": str(min(p.settlement_date for p in prices)),
                    "to": str(max(p.settlement_date for p in prices)),
                }
            }
            log_id = self._start_fetch_log("system_prices", metadata)

        try:
            # Convert to dicts for Supabase
            records = [
                {
                    "settlement_date": str(p.settlement_date),
                    "settlement_period": p.settlement_period,
                    "system_sell_price": float(p.system_sell_price),
                    "system_buy_price": float(p.system_buy_price),
                    "price": float(p.price),
                    "data_source": p.data_source,
                    "fetched_at": datetime.now(UTC).isoformat(),
                }
                for p in prices
            ]

            # Upsert with conflict on unique constraint
            result = (
                self._client.table("system_prices")
                .upsert(records, on_conflict="settlement_date,settlement_period")
                .execute()
            )

            # Count results (Supabase doesn't distinguish insert vs update)
            count = len(result.data)
            stats = {"fetched": len(prices), "inserted": count, "updated": 0}

            if log_id:
                self._complete_fetch_log(
                    log_id,
                    stats["fetched"],
                    stats["inserted"],
                    stats["updated"],
                )

            logger.info(f"Saved {count} system prices")
            return stats

        except Exception as e:
            if log_id:
                self._complete_fetch_log(
                    log_id, len(prices), 0, 0, status="error", error_message=str(e)
                )
            logger.error(f"Failed to save system prices: {e}")
            raise

    def save_day_ahead_prices(
        self,
        prices: list[DayAheadPrice],
        log_fetch: bool = True,
    ) -> dict[str, int]:
        """Save day-ahead prices to Supabase.

        Args:
            prices: List of DayAheadPrice objects to save
            log_fetch: Whether to log this fetch operation

        Returns:
            Dict with counts: {'fetched', 'inserted', 'updated'}
        """
        if not prices:
            return {"fetched": 0, "inserted": 0, "updated": 0}

        log_id = None
        if log_fetch:
            metadata = {
                "date_range": {
                    "from": str(min(p.settlement_date for p in prices)),
                    "to": str(max(p.settlement_date for p in prices)),
                }
            }
            log_id = self._start_fetch_log("day_ahead_prices", metadata)

        try:
            records = [
                {
                    "settlement_date": str(p.settlement_date),
                    "settlement_period": p.settlement_period,
                    "price": float(p.price),
                    "data_provider": p.data_provider,
                    "data_source": p.data_source,
                    "fetched_at": datetime.now(UTC).isoformat(),
                }
                for p in prices
            ]

            result = (
                self._client.table("day_ahead_prices")
                .upsert(records, on_conflict="settlement_date,settlement_period,data_provider")
                .execute()
            )

            count = len(result.data)
            stats = {"fetched": len(prices), "inserted": count, "updated": 0}

            if log_id:
                self._complete_fetch_log(
                    log_id,
                    stats["fetched"],
                    stats["inserted"],
                    stats["updated"],
                )

            logger.info(f"Saved {count} day-ahead prices")
            return stats

        except Exception as e:
            if log_id:
                self._complete_fetch_log(
                    log_id, len(prices), 0, 0, status="error", error_message=str(e)
                )
            logger.error(f"Failed to save day-ahead prices: {e}")
            raise

    def save_carbon_intensity(
        self,
        records: list[dict[str, Any]],
        log_fetch: bool = True,
    ) -> dict[str, int]:
        """Save carbon intensity data to Supabase.

        Args:
            records: List of carbon intensity records
            log_fetch: Whether to log this fetch operation

        Returns:
            Dict with counts: {'fetched', 'inserted', 'updated'}
        """
        if not records:
            return {"fetched": 0, "inserted": 0, "updated": 0}

        log_id = None
        if log_fetch:
            log_id = self._start_fetch_log("carbon_intensity")

        try:
            # Prepare records with fetched_at timestamp
            db_records = [
                {
                    "datetime": r["datetime"],
                    "intensity": r["intensity"],
                    "intensity_index": r.get("intensity_index"),
                    "data_source": r.get("data_source", "national_grid"),
                    "fetched_at": datetime.now(UTC).isoformat(),
                }
                for r in records
            ]

            result = (
                self._client.table("carbon_intensity")
                .upsert(db_records, on_conflict="datetime")
                .execute()
            )

            count = len(result.data)
            stats = {"fetched": len(records), "inserted": count, "updated": 0}

            if log_id:
                self._complete_fetch_log(
                    log_id,
                    stats["fetched"],
                    stats["inserted"],
                    stats["updated"],
                )

            logger.info(f"Saved {count} carbon intensity records")
            return stats

        except Exception as e:
            if log_id:
                self._complete_fetch_log(
                    log_id, len(records), 0, 0, status="error", error_message=str(e)
                )
            logger.error(f"Failed to save carbon intensity: {e}")
            raise

    def save_fuel_mix(
        self,
        fuel_mixes: list[FuelMix],
        log_fetch: bool = True,
    ) -> dict[str, int]:
        """Save fuel mix data to Supabase.

        Args:
            fuel_mixes: List of FuelMix objects to save
            log_fetch: Whether to log this fetch operation

        Returns:
            Dict with counts: {'fetched', 'inserted', 'updated'}
        """
        if not fuel_mixes:
            return {"fetched": 0, "inserted": 0, "updated": 0}

        log_id = None
        if log_fetch:
            log_id = self._start_fetch_log("fuel_mix")

        try:
            records = [
                {
                    "datetime": fm.datetime_from.isoformat(),
                    "biomass": float(fm.biomass),
                    "coal": float(fm.coal),
                    "gas": float(fm.gas),
                    "hydro": float(fm.hydro),
                    "imports": float(fm.imports),
                    "nuclear": float(fm.nuclear),
                    "other": float(fm.other),
                    "solar": float(fm.solar),
                    "wind": float(fm.wind),
                    "data_source": fm.data_source,
                    "fetched_at": datetime.now(UTC).isoformat(),
                }
                for fm in fuel_mixes
            ]

            result = (
                self._client.table("fuel_mix")
                .upsert(records, on_conflict="datetime")
                .execute()
            )

            count = len(result.data)
            stats = {"fetched": len(fuel_mixes), "inserted": count, "updated": 0}

            if log_id:
                self._complete_fetch_log(
                    log_id,
                    stats["fetched"],
                    stats["inserted"],
                    stats["updated"],
                )

            logger.info(f"Saved {count} fuel mix records")
            return stats

        except Exception as e:
            if log_id:
                self._complete_fetch_log(
                    log_id, len(fuel_mixes), 0, 0, status="error", error_message=str(e)
                )
            logger.error(f"Failed to save fuel mix: {e}")
            raise

    def get_latest_fetch(self, fetch_type: str) -> dict[str, Any] | None:
        """Get the most recent successful fetch for a type.

        Args:
            fetch_type: Type of fetch (system_prices, day_ahead_prices, carbon_intensity)

        Returns:
            Fetch log record or None if no successful fetches
        """
        result = (
            self._client.table("fetch_logs")
            .select("*")
            .eq("fetch_type", fetch_type)
            .eq("status", "success")
            .order("completed_at", desc=True)
            .limit(1)
            .execute()
        )

        return result.data[0] if result.data else None

    def get_system_price_date_range(self) -> tuple[str | None, str | None]:
        """Get the date range of stored system prices.

        Returns:
            Tuple of (min_date, max_date) as ISO strings, or (None, None) if empty
        """
        # Get min date
        min_result = (
            self._client.table("system_prices")
            .select("settlement_date")
            .order("settlement_date")
            .limit(1)
            .execute()
        )

        # Get max date
        max_result = (
            self._client.table("system_prices")
            .select("settlement_date")
            .order("settlement_date", desc=True)
            .limit(1)
            .execute()
        )

        min_date = min_result.data[0]["settlement_date"] if min_result.data else None
        max_date = max_result.data[0]["settlement_date"] if max_result.data else None

        return min_date, max_date
