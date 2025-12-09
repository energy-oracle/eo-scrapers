"""Tests for Elexon BMRS API client.

These tests make real API calls to the Elexon API.
No authentication is required - the API is public.
"""

from datetime import date, timedelta
from decimal import Decimal

import pytest

from eo_scrapers.clients.elexon import ElexonClient
from eo_scrapers.models.price import DayAheadPrice, PriceAggregate, SystemPrice


class TestElexonClient:
    """Test Elexon API client with real API calls."""

    @pytest.fixture
    async def client(self):
        """Create an Elexon client for testing."""
        async with ElexonClient() as client:
            yield client

    @pytest.mark.asyncio
    async def test_health_check(self, client: ElexonClient):
        """Test API health check."""
        is_healthy = await client.health_check()
        assert is_healthy is True

    @pytest.mark.asyncio
    async def test_get_system_prices_single_day(self, client: ElexonClient):
        """Test fetching system prices for a single day."""
        # Use a date we know has data (November 2024)
        test_date = date(2024, 11, 1)

        prices = await client.get_system_prices(test_date)

        # Should have 48 settlement periods (or 46/50 on clock change days)
        assert len(prices) >= 46
        assert len(prices) <= 50

        # Check first price
        first_price = prices[0]
        assert isinstance(first_price, SystemPrice)
        assert first_price.settlement_date == test_date
        assert first_price.settlement_period == 1
        assert isinstance(first_price.system_sell_price, Decimal)
        assert isinstance(first_price.system_buy_price, Decimal)
        assert isinstance(first_price.price, Decimal)
        assert first_price.price > 0

    @pytest.mark.asyncio
    async def test_get_system_prices_single_period(self, client: ElexonClient):
        """Test fetching system price for a single settlement period."""
        test_date = date(2024, 11, 1)
        test_period = 23  # 11:00-11:30

        prices = await client.get_system_prices(test_date, settlement_period=test_period)

        assert len(prices) == 1
        assert prices[0].settlement_period == test_period

    @pytest.mark.asyncio
    async def test_get_market_index_prices(self, client: ElexonClient):
        """Test fetching day-ahead market index prices."""
        test_date = date(2024, 11, 1)

        prices = await client.get_market_index_prices(test_date)

        # Should have prices for most settlement periods
        assert len(prices) > 0

        first_price = prices[0]
        assert isinstance(first_price, DayAheadPrice)
        assert first_price.settlement_date == test_date
        assert isinstance(first_price.price, Decimal)
        assert first_price.data_provider in ("APXMIDP", "N2EXMIDP")

    @pytest.mark.asyncio
    async def test_daily_average_system_price(self, client: ElexonClient):
        """Test daily average system price calculation."""
        test_date = date(2024, 11, 1)

        aggregate = await client.daily_average(test_date, price_type="system_price")

        assert isinstance(aggregate, PriceAggregate)
        assert aggregate.start_date == test_date
        assert aggregate.end_date == test_date
        assert aggregate.price_type == "system_price"
        assert isinstance(aggregate.average_price, Decimal)
        assert aggregate.average_price > 0
        assert aggregate.min_price <= aggregate.average_price <= aggregate.max_price
        assert aggregate.num_periods >= 46  # At least 46 periods

    @pytest.mark.asyncio
    async def test_daily_average_day_ahead(self, client: ElexonClient):
        """Test daily average day-ahead price calculation."""
        test_date = date(2024, 11, 1)

        aggregate = await client.daily_average(test_date, price_type="day_ahead")

        assert isinstance(aggregate, PriceAggregate)
        assert aggregate.price_type == "day_ahead"
        assert aggregate.average_price > 0

    @pytest.mark.asyncio
    async def test_monthly_average_system_price(self, client: ElexonClient):
        """Test monthly average system price calculation.

        This is the primary PPA settlement index.
        """
        # Use November 2024 - a complete month with data
        aggregate = await client.monthly_average(2024, 11, price_type="system_price")

        assert isinstance(aggregate, PriceAggregate)
        assert aggregate.start_date == date(2024, 11, 1)
        assert aggregate.end_date == date(2024, 11, 30)
        assert aggregate.price_type == "system_price"
        assert isinstance(aggregate.average_price, Decimal)
        assert aggregate.average_price > 0
        # November has 30 days * ~48 periods = ~1440 periods
        # Allow tolerance for missing data in some periods
        assert aggregate.num_periods >= 1200

        print(f"\n=== November 2024 System Price Summary ===")
        print(f"Average: £{aggregate.average_price}/MWh")
        print(f"Min: £{aggregate.min_price}/MWh")
        print(f"Max: £{aggregate.max_price}/MWh")
        print(f"Periods: {aggregate.num_periods}")

    @pytest.mark.asyncio
    async def test_get_system_prices_range(self, client: ElexonClient):
        """Test fetching system prices for a date range."""
        from_date = date(2024, 11, 1)
        to_date = date(2024, 11, 3)  # 3 days

        prices = await client.get_system_prices_range(from_date, to_date)

        # Should have ~144 periods (3 days * 48)
        # Allow tolerance for missing data in some periods
        assert len(prices) >= 130
        assert len(prices) <= 150

        # Check dates are in range
        dates = set(p.settlement_date for p in prices)
        assert from_date in dates
        assert to_date in dates

    @pytest.mark.asyncio
    async def test_get_latest_system_price(self, client: ElexonClient):
        """Test fetching the latest available system price."""
        latest = await client.get_latest_system_price()

        # Should find something within the last 3 days
        assert latest is not None
        assert isinstance(latest, SystemPrice)
        assert latest.settlement_date >= date.today() - timedelta(days=3)


class TestSystemPriceModel:
    """Test SystemPrice Pydantic model."""

    def test_from_elexon_response(self):
        """Test creating SystemPrice from API response."""
        api_data = {
            "settlementDate": "2024-11-01",
            "settlementPeriod": 23,
            "systemSellPrice": 109.0,
            "systemBuyPrice": 109.0,
        }

        price = SystemPrice.from_elexon_response(api_data)

        assert price.settlement_date == date(2024, 11, 1)
        assert price.settlement_period == 23
        assert price.system_sell_price == Decimal("109.0")
        assert price.system_buy_price == Decimal("109.0")
        assert price.price == Decimal("109.00")  # Average of SSP/SBP

    def test_from_elexon_response_different_prices(self):
        """Test SystemPrice when SSP != SBP."""
        api_data = {
            "settlementDate": "2024-11-01",
            "settlementPeriod": 10,
            "systemSellPrice": 100.0,
            "systemBuyPrice": 110.0,
        }

        price = SystemPrice.from_elexon_response(api_data)

        assert price.system_sell_price == Decimal("100.0")
        assert price.system_buy_price == Decimal("110.0")
        assert price.price == Decimal("105.00")  # Average

    def test_settlement_period_validation(self):
        """Test settlement period validation."""
        with pytest.raises(ValueError):
            SystemPrice(
                settlement_date=date(2024, 11, 1),
                settlement_period=0,  # Invalid
                system_sell_price=Decimal("100"),
                system_buy_price=Decimal("100"),
                price=Decimal("100"),
            )

        with pytest.raises(ValueError):
            SystemPrice(
                settlement_date=date(2024, 11, 1),
                settlement_period=51,  # Invalid
                system_sell_price=Decimal("100"),
                system_buy_price=Decimal("100"),
                price=Decimal("100"),
            )


class TestDayAheadPriceModel:
    """Test DayAheadPrice Pydantic model."""

    def test_from_elexon_response(self):
        """Test creating DayAheadPrice from API response."""
        api_data = {
            "settlementDate": "2024-11-01",
            "settlementPeriod": 1,
            "price": 91.24,
            "dataProvider": "APXMIDP",
        }

        price = DayAheadPrice.from_elexon_response(api_data)

        assert price.settlement_date == date(2024, 11, 1)
        assert price.settlement_period == 1
        assert price.price == Decimal("91.24")
        assert price.data_provider == "APXMIDP"
