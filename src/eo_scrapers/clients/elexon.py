"""Elexon BMRS API client for UK electricity market data.

This client provides access to the Elexon Balancing Mechanism Reporting Service (BMRS)
API, which is the primary source of UK electricity settlement data.

API Documentation: https://bmrs.elexon.co.uk/api-documentation
API Base URL: https://data.elexon.co.uk/bmrs/api/v1

No authentication required - all endpoints are public.
Rate limit: 100 requests/minute
"""

import calendar
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from eo_scrapers.clients.base import BaseClient
from eo_scrapers.models.price import DayAheadPrice, PriceAggregate, SystemPrice

logger = logging.getLogger(__name__)

ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"


class ElexonClient(BaseClient):
    """Client for Elexon BMRS API.

    Provides access to:
    - System Prices (SSP/SBP) for PPA settlement
    - Day-Ahead Market Index prices
    - Price aggregations (daily, monthly)

    Example:
        async with ElexonClient() as client:
            # Get system prices for a day
            prices = await client.get_system_prices(date(2024, 11, 1))

            # Get monthly average
            avg = await client.monthly_average(2024, 11)
            print(f"November 2024 average: £{avg.average_price}/MWh")
    """

    def __init__(self, timeout: float = 30.0, max_retries: int = 3):
        """Initialize Elexon client.

        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        super().__init__(ELEXON_BASE_URL, timeout, max_retries)

    async def health_check(self) -> bool:
        """Check if Elexon API is accessible.

        Returns:
            True if API is accessible, False otherwise
        """
        try:
            # Try to get system prices for yesterday
            yesterday = date.today() - timedelta(days=1)
            response = await self.get(
                f"/balancing/settlement/system-prices/{yesterday.isoformat()}"
            )
            return "data" in response
        except Exception as e:
            logger.warning(f"Elexon health check failed: {e}")
            return False

    async def get_system_prices(
        self,
        settlement_date: date,
        settlement_period: int | None = None,
    ) -> list[SystemPrice]:
        """Get System Prices (SSP/SBP) for a settlement date.

        System Prices are the primary settlement index for UK PPAs.

        Args:
            settlement_date: The trading date to fetch
            settlement_period: Optional specific period (1-48)

        Returns:
            List of SystemPrice objects for each settlement period
        """
        endpoint = f"/balancing/settlement/system-prices/{settlement_date.isoformat()}"

        if settlement_period is not None:
            endpoint += f"/{settlement_period}"

        response = await self.get(endpoint)

        prices = []
        for item in response.get("data", []):
            try:
                prices.append(SystemPrice.from_elexon_response(item))
            except Exception as e:
                logger.warning(f"Failed to parse system price: {e}, data: {item}")

        return sorted(prices, key=lambda p: (p.settlement_date, p.settlement_period))

    async def get_system_prices_range(
        self,
        from_date: date,
        to_date: date,
    ) -> list[SystemPrice]:
        """Get System Prices for a date range.

        Args:
            from_date: Start date (inclusive)
            to_date: End date (inclusive)

        Returns:
            List of SystemPrice objects for all settlement periods in range
        """
        all_prices: list[SystemPrice] = []
        current_date = from_date

        while current_date <= to_date:
            try:
                day_prices = await self.get_system_prices(current_date)
                all_prices.extend(day_prices)
            except Exception as e:
                logger.error(f"Failed to fetch system prices for {current_date}: {e}")

            current_date += timedelta(days=1)

        return sorted(all_prices, key=lambda p: (p.settlement_date, p.settlement_period))

    async def get_market_index_prices(
        self,
        from_date: date,
        to_date: date | None = None,
        data_provider: str = "APXMIDP",
    ) -> list[DayAheadPrice]:
        """Get Day-Ahead Market Index prices.

        Market Index Data (MID) provides day-ahead reference prices
        from power exchanges.

        Args:
            from_date: Start date
            to_date: End date (defaults to from_date)
            data_provider: Price source - APXMIDP (default) or N2EXMIDP

        Returns:
            List of DayAheadPrice objects
        """
        if to_date is None:
            to_date = from_date

        params: dict[str, Any] = {
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
        }

        if data_provider:
            params["dataProviders"] = data_provider

        response = await self.get("/balancing/pricing/market-index", params=params)

        prices = []
        for item in response.get("data", []):
            try:
                prices.append(DayAheadPrice.from_elexon_response(item))
            except Exception as e:
                logger.warning(f"Failed to parse market index price: {e}, data: {item}")

        return sorted(prices, key=lambda p: (p.settlement_date, p.settlement_period))

    async def daily_average(
        self,
        settlement_date: date,
        price_type: str = "system_price",
    ) -> PriceAggregate:
        """Calculate daily average price.

        Args:
            settlement_date: Date to calculate average for
            price_type: "system_price" or "day_ahead"

        Returns:
            PriceAggregate with daily statistics
        """
        if price_type == "system_price":
            prices = await self.get_system_prices(settlement_date)
            price_values = [p.price for p in prices]
        elif price_type == "day_ahead":
            prices = await self.get_market_index_prices(settlement_date)
            # Filter out zero prices (no trading in that period)
            price_values = [p.price for p in prices if p.price > 0]
        else:
            raise ValueError(f"Invalid price_type: {price_type}")

        if not price_values:
            raise ValueError(f"No prices found for {settlement_date}")

        avg_price = sum(price_values) / len(price_values)

        return PriceAggregate(
            start_date=settlement_date,
            end_date=settlement_date,
            average_price=Decimal(str(avg_price)).quantize(Decimal("0.01")),
            min_price=min(price_values),
            max_price=max(price_values),
            num_periods=len(price_values),
            price_type=price_type,
        )

    async def monthly_average(
        self,
        year: int,
        month: int,
        price_type: str = "system_price",
    ) -> PriceAggregate:
        """Calculate monthly average price.

        This is the most common settlement period for PPAs.

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            price_type: "system_price" or "day_ahead"

        Returns:
            PriceAggregate with monthly statistics
        """
        _, last_day = calendar.monthrange(year, month)
        from_date = date(year, month, 1)
        to_date = date(year, month, last_day)

        if price_type == "system_price":
            prices = await self.get_system_prices_range(from_date, to_date)
            price_values = [p.price for p in prices]
        elif price_type == "day_ahead":
            prices = await self.get_market_index_prices(from_date, to_date)
            # Filter out zero prices
            price_values = [p.price for p in prices if p.price > 0]
        else:
            raise ValueError(f"Invalid price_type: {price_type}")

        if not price_values:
            raise ValueError(f"No prices found for {year}-{month:02d}")

        avg_price = sum(price_values) / len(price_values)

        return PriceAggregate(
            start_date=from_date,
            end_date=to_date,
            average_price=Decimal(str(avg_price)).quantize(Decimal("0.01")),
            min_price=min(price_values),
            max_price=max(price_values),
            num_periods=len(price_values),
            price_type=price_type,
        )

    async def get_latest_system_price(self) -> SystemPrice | None:
        """Get the most recent available system price.

        Returns:
            Most recent SystemPrice or None if not available
        """
        # Try today first, then yesterday
        for days_back in range(0, 3):
            try:
                check_date = date.today() - timedelta(days=days_back)
                prices = await self.get_system_prices(check_date)
                if prices:
                    return prices[-1]  # Return most recent period
            except Exception:
                continue

        return None
