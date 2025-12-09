"""Price data models for PPA settlement.

UK electricity settles in half-hourly periods (48 per day):
- Period 1 = 00:00-00:30
- Period 48 = 23:30-00:00

These models capture the essential data for PPA settlement indices.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Self

from pydantic import BaseModel, Field, field_validator


def _utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(UTC)


class SystemPrice(BaseModel):
    """System Sell Price (SSP) / System Buy Price (SBP) from Elexon.

    The System Price is the primary settlement index for UK PPAs.
    Most contracts reference the "System Price" which typically means
    the volume-weighted average of SSP and SBP.

    Attributes:
        settlement_date: The trading day for this price
        settlement_period: Half-hourly period (1-48)
        system_sell_price: SSP in £/MWh - price paid to generators
        system_buy_price: SBP in £/MWh - price paid by suppliers
        price: The net system price in £/MWh (typically average of SSP/SBP)
        created_at: When this record was fetched (audit trail)
        data_source: Origin of the data for audit purposes
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)  # 50 for clock change days
    system_sell_price: Decimal  # SSP in £/MWh
    system_buy_price: Decimal  # SBP in £/MWh
    price: Decimal  # Net/average price in £/MWh
    created_at: datetime = Field(default_factory=_utc_now)
    data_source: str = "elexon_bmrs"

    @field_validator("settlement_period")
    @classmethod
    def validate_settlement_period(cls, v: int) -> int:
        """Validate settlement period is within valid range.

        Normal days have 48 periods, but clock change days can have
        46 (spring forward) or 50 (fall back) periods.
        """
        if not 1 <= v <= 50:
            raise ValueError(f"Settlement period must be 1-50, got {v}")
        return v

    @classmethod
    def from_elexon_response(cls, data: dict) -> Self:
        """Create SystemPrice from Elexon API response.

        The Elexon API returns:
        - systemSellPrice: SSP in £/MWh
        - systemBuyPrice: SBP in £/MWh

        For PPA settlement, we typically use the average of SSP and SBP
        as the "net" system price.
        """
        ssp = Decimal(str(data["systemSellPrice"]))
        sbp = Decimal(str(data["systemBuyPrice"]))
        # Use average of SSP and SBP as the net price
        net_price = (ssp + sbp) / 2

        return cls(
            settlement_date=date.fromisoformat(data["settlementDate"]),
            settlement_period=data["settlementPeriod"],
            system_sell_price=ssp,
            system_buy_price=sbp,
            price=net_price.quantize(Decimal("0.01")),
        )


class DayAheadPrice(BaseModel):
    """Day-Ahead Market Price from Elexon Market Index.

    Day-ahead prices are used for forward pricing in PPAs and
    calculating discounts/premiums to market.

    Attributes:
        settlement_date: The trading day for this price
        settlement_period: Half-hourly period (1-48)
        price: Day-ahead price in £/MWh
        data_provider: Source of the price data (e.g., N2EX, EPEX)
        created_at: When this record was fetched (audit trail)
        data_source: Origin of the data for audit purposes
    """

    settlement_date: date
    settlement_period: int = Field(ge=1, le=50)
    price: Decimal  # Day-ahead price in £/MWh
    data_provider: str = "APXMIDP"  # Default to APX (main UK exchange)
    created_at: datetime = Field(default_factory=_utc_now)
    data_source: str = "elexon_bmrs"

    @field_validator("settlement_period")
    @classmethod
    def validate_settlement_period(cls, v: int) -> int:
        """Validate settlement period is within valid range."""
        if not 1 <= v <= 50:
            raise ValueError(f"Settlement period must be 1-50, got {v}")
        return v

    @classmethod
    def from_elexon_response(cls, data: dict) -> Self:
        """Create DayAheadPrice from Elexon Market Index API response.

        The Elexon API returns:
        - dataProvider: APXMIDP (APX Market Index Data Provider) or N2EXMIDP
        - price: Market price in £/MWh
        - volume: Trading volume

        APXMIDP is the primary UK day-ahead market index.
        """
        return cls(
            settlement_date=date.fromisoformat(data["settlementDate"]),
            settlement_period=data["settlementPeriod"],
            price=Decimal(str(data["price"])),
            data_provider=data.get("dataProvider", "APXMIDP"),
        )


class PriceAggregate(BaseModel):
    """Aggregated price data for PPA settlement periods.

    PPAs typically settle on monthly averages, though some use
    daily or even half-hourly granularity.

    Attributes:
        start_date: Start of the aggregation period
        end_date: End of the aggregation period
        average_price: Simple average price in £/MWh
        min_price: Minimum price in the period
        max_price: Maximum price in the period
        num_periods: Number of settlement periods included
        price_type: Type of price (system_price, day_ahead)
        created_at: When this aggregate was calculated
    """

    start_date: date
    end_date: date
    average_price: Decimal  # Average price in £/MWh
    min_price: Decimal  # Minimum price in £/MWh
    max_price: Decimal  # Maximum price in £/MWh
    num_periods: int = Field(ge=1)
    price_type: str
    created_at: datetime = Field(default_factory=_utc_now)
