"""National Grid Carbon Intensity API client.

Provides access to UK grid carbon intensity data for PPA
green premium calculations.

API Documentation: https://carbonintensity.org.uk/
API Base URL: https://api.carbonintensity.org.uk
No authentication required.
"""

import logging
from datetime import date, datetime, timedelta

from eo_scrapers.clients.base import BaseClient
from eo_scrapers.models.carbon import CarbonIntensity, FuelMix

logger = logging.getLogger(__name__)

CARBON_INTENSITY_BASE_URL = "https://api.carbonintensity.org.uk"


class CarbonIntensityClient(BaseClient):
    """Client for National Grid Carbon Intensity API.

    Provides access to:
    - Current carbon intensity
    - Historical carbon intensity by date
    - Generation fuel mix

    Example:
        async with CarbonIntensityClient() as client:
            current = await client.get_current()
            print(f"Current intensity: {current.intensity} gCO2/kWh")

            # Get a day's data
            data = await client.get_by_date(date(2024, 11, 1))
            for ci in data:
                print(f"{ci.datetime_from}: {ci.intensity} gCO2/kWh")
    """

    def __init__(self, timeout: float = 30.0, max_retries: int = 3):
        """Initialize Carbon Intensity client."""
        super().__init__(CARBON_INTENSITY_BASE_URL, timeout, max_retries)

    async def health_check(self) -> bool:
        """Check if Carbon Intensity API is accessible."""
        try:
            response = await self.get("/intensity")
            return "data" in response
        except Exception as e:
            logger.warning(f"Carbon Intensity health check failed: {e}")
            return False

    async def get_current(self) -> CarbonIntensity:
        """Get current carbon intensity.

        Returns:
            Current CarbonIntensity for the current half-hour period
        """
        response = await self.get("/intensity")

        data = response.get("data", [])
        if not data:
            raise ValueError("No current carbon intensity data available")

        return CarbonIntensity.from_api_response(data[0])

    async def get_by_date(self, target_date: date) -> list[CarbonIntensity]:
        """Get all carbon intensity readings for a specific date.

        Args:
            target_date: Date to fetch (48 half-hourly readings)

        Returns:
            List of CarbonIntensity objects for each half-hour
        """
        # API expects format: /intensity/date/{date}
        response = await self.get(f"/intensity/date/{target_date.isoformat()}")

        readings = []
        for item in response.get("data", []):
            try:
                readings.append(CarbonIntensity.from_api_response(item))
            except Exception as e:
                logger.warning(f"Failed to parse carbon intensity: {e}")

        return sorted(readings, key=lambda x: x.datetime_from)

    async def get_range(
        self,
        from_datetime: datetime,
        to_datetime: datetime,
    ) -> list[CarbonIntensity]:
        """Get carbon intensity for a datetime range.

        Args:
            from_datetime: Start datetime (inclusive)
            to_datetime: End datetime (inclusive)

        Returns:
            List of CarbonIntensity objects
        """
        # API format: /intensity/{from}/{to}
        from_str = from_datetime.strftime("%Y-%m-%dT%H:%MZ")
        to_str = to_datetime.strftime("%Y-%m-%dT%H:%MZ")

        response = await self.get(f"/intensity/{from_str}/{to_str}")

        readings = []
        for item in response.get("data", []):
            try:
                readings.append(CarbonIntensity.from_api_response(item))
            except Exception as e:
                logger.warning(f"Failed to parse carbon intensity: {e}")

        return sorted(readings, key=lambda x: x.datetime_from)

    async def get_today(self) -> list[CarbonIntensity]:
        """Get all carbon intensity readings for today.

        Returns:
            List of CarbonIntensity objects (forecast and actual)
        """
        return await self.get_by_date(date.today())

    async def get_yesterday(self) -> list[CarbonIntensity]:
        """Get all carbon intensity readings for yesterday.

        Returns:
            List of CarbonIntensity objects (all actual)
        """
        return await self.get_by_date(date.today() - timedelta(days=1))

    async def get_fuel_mix_current(self) -> FuelMix:
        """Get current generation fuel mix.

        Returns:
            Current FuelMix showing percentage by fuel type
        """
        response = await self.get("/generation")

        data = response.get("data", {})
        if not data:
            raise ValueError("No fuel mix data available")

        return FuelMix.from_api_response(data)

    async def get_fuel_mix_by_date(self, target_date: date) -> list[FuelMix]:
        """Get generation fuel mix for a specific date.

        Args:
            target_date: Date to fetch

        Returns:
            List of FuelMix objects for each half-hour
        """
        # Use the intensity endpoint with fuel mix
        from_dt = datetime.combine(target_date, datetime.min.time())
        to_dt = datetime.combine(target_date, datetime.max.time())

        from_str = from_dt.strftime("%Y-%m-%dT%H:%MZ")
        to_str = to_dt.strftime("%Y-%m-%dT%H:%MZ")

        response = await self.get(f"/generation/{from_str}/{to_str}")

        fuel_mixes = []
        for item in response.get("data", []):
            try:
                fuel_mixes.append(FuelMix.from_api_response(item))
            except Exception as e:
                logger.warning(f"Failed to parse fuel mix: {e}")

        return sorted(fuel_mixes, key=lambda x: x.datetime_from)

    async def get_average_intensity(self, target_date: date) -> dict[str, float]:
        """Calculate average carbon intensity for a date.

        Args:
            target_date: Date to calculate average for

        Returns:
            Dict with 'average', 'min', 'max' intensity values
        """
        readings = await self.get_by_date(target_date)

        if not readings:
            raise ValueError(f"No data for {target_date}")

        intensities = [r.intensity for r in readings]

        return {
            "average": round(sum(intensities) / len(intensities), 1),
            "min": min(intensities),
            "max": max(intensities),
            "num_periods": len(intensities),
        }
