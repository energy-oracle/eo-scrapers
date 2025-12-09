"""Carbon intensity data models.

UK Carbon Intensity API provides half-hourly carbon intensity
forecasts and actuals for the GB electricity grid.

API Documentation: https://carbonintensity.org.uk/
"""

from datetime import UTC, datetime
from typing import Self

from pydantic import BaseModel, Field


def _utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(UTC)


class CarbonIntensity(BaseModel):
    """UK Grid Carbon Intensity from National Grid.

    Carbon intensity is measured in gCO2/kWh and represents
    the carbon dioxide emissions per unit of electricity generated.

    Attributes:
        datetime_from: Start of the half-hour period
        datetime_to: End of the half-hour period
        intensity_forecast: Forecasted intensity in gCO2/kWh
        intensity_actual: Actual intensity in gCO2/kWh (may be None for future)
        intensity_index: Qualitative rating (very low, low, moderate, high, very high)
        created_at: When this record was fetched
        data_source: Origin of the data
    """

    datetime_from: datetime
    datetime_to: datetime
    intensity_forecast: int
    intensity_actual: int | None = None
    intensity_index: str  # very low, low, moderate, high, very high
    created_at: datetime = Field(default_factory=_utc_now)
    data_source: str = "national_grid"

    @property
    def intensity(self) -> int:
        """Return actual intensity if available, otherwise forecast."""
        return self.intensity_actual if self.intensity_actual is not None else self.intensity_forecast

    @classmethod
    def from_api_response(cls, data: dict) -> Self:
        """Create CarbonIntensity from API response.

        The API returns:
        {
            "from": "2024-11-01T00:00Z",
            "to": "2024-11-01T00:30Z",
            "intensity": {
                "forecast": 150,
                "actual": 145,
                "index": "moderate"
            }
        }
        """
        intensity_data = data.get("intensity", {})

        return cls(
            datetime_from=datetime.fromisoformat(data["from"].replace("Z", "+00:00")),
            datetime_to=datetime.fromisoformat(data["to"].replace("Z", "+00:00")),
            intensity_forecast=intensity_data.get("forecast", 0),
            intensity_actual=intensity_data.get("actual"),
            intensity_index=intensity_data.get("index", "unknown"),
        )


class FuelMix(BaseModel):
    """UK Generation Mix by fuel type.

    Attributes:
        datetime_from: Start of the period
        datetime_to: End of the period
        fuel_percentages: Dict of fuel type to percentage
        created_at: When this record was fetched
        data_source: Origin of the data
    """

    datetime_from: datetime
    datetime_to: datetime
    biomass: float = 0.0
    coal: float = 0.0
    gas: float = 0.0
    hydro: float = 0.0
    imports: float = 0.0
    nuclear: float = 0.0
    other: float = 0.0
    solar: float = 0.0
    wind: float = 0.0
    created_at: datetime = Field(default_factory=_utc_now)
    data_source: str = "national_grid"

    @classmethod
    def from_api_response(cls, data: dict) -> Self:
        """Create FuelMix from API response.

        The API returns generationmix as a list:
        [
            {"fuel": "gas", "perc": 35.5},
            {"fuel": "wind", "perc": 25.0},
            ...
        ]
        """
        fuel_map = {item["fuel"]: item["perc"] for item in data.get("generationmix", [])}

        return cls(
            datetime_from=datetime.fromisoformat(data["from"].replace("Z", "+00:00")),
            datetime_to=datetime.fromisoformat(data["to"].replace("Z", "+00:00")),
            biomass=fuel_map.get("biomass", 0.0),
            coal=fuel_map.get("coal", 0.0),
            gas=fuel_map.get("gas", 0.0),
            hydro=fuel_map.get("hydro", 0.0),
            imports=fuel_map.get("imports", 0.0),
            nuclear=fuel_map.get("nuclear", 0.0),
            other=fuel_map.get("other", 0.0),
            solar=fuel_map.get("solar", 0.0),
            wind=fuel_map.get("wind", 0.0),
        )

    @property
    def renewable_percentage(self) -> float:
        """Calculate total renewable percentage (wind, solar, hydro, biomass)."""
        return self.wind + self.solar + self.hydro + self.biomass

    @property
    def low_carbon_percentage(self) -> float:
        """Calculate total low-carbon percentage (renewables + nuclear)."""
        return self.renewable_percentage + self.nuclear
