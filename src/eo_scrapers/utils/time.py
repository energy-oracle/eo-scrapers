"""UK timezone utilities for settlement period handling.

UK electricity settles in half-hourly periods (48 per day):
- Period 1 = 00:00-00:30 UK time
- Period 48 = 23:30-00:00 UK time

Clock change days have different period counts:
- Spring forward (March): 46 periods (lose an hour)
- Fall back (October): 50 periods (gain an hour)
"""

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

UK_TZ = ZoneInfo("Europe/London")


def get_uk_now() -> datetime:
    """Get current time in UK timezone."""
    return datetime.now(UK_TZ)


def get_uk_today() -> date:
    """Get today's date in UK timezone."""
    return get_uk_now().date()


def settlement_period_to_time(period: int) -> time:
    """Convert settlement period (1-48) to start time.

    Args:
        period: Settlement period number (1-48 for normal days)

    Returns:
        Start time of the settlement period

    Example:
        >>> settlement_period_to_time(1)
        datetime.time(0, 0)
        >>> settlement_period_to_time(48)
        datetime.time(23, 30)
    """
    if not 1 <= period <= 50:
        raise ValueError(f"Settlement period must be 1-50, got {period}")

    minutes = (period - 1) * 30
    hours = minutes // 60
    mins = minutes % 60
    return time(hour=hours, minute=mins)


def time_to_settlement_period(t: time) -> int:
    """Convert time to settlement period number.

    Args:
        t: Time to convert

    Returns:
        Settlement period number (1-48)

    Example:
        >>> time_to_settlement_period(time(0, 0))
        1
        >>> time_to_settlement_period(time(23, 30))
        48
    """
    total_minutes = t.hour * 60 + t.minute
    return (total_minutes // 30) + 1


def settlement_period_to_datetime(settlement_date: date, period: int) -> datetime:
    """Convert settlement date and period to UK datetime.

    Args:
        settlement_date: The trading date
        period: Settlement period number

    Returns:
        UK-localized datetime for the start of the period
    """
    t = settlement_period_to_time(period)
    return datetime.combine(settlement_date, t, tzinfo=UK_TZ)


def get_periods_in_day(d: date) -> int:
    """Get the number of settlement periods in a given day.

    Normal days have 48 periods. Clock change days differ:
    - Spring forward: 46 periods
    - Fall back: 50 periods

    Args:
        d: Date to check

    Returns:
        Number of settlement periods (46, 48, or 50)
    """
    # Check if this is a clock change day by comparing offsets
    start = datetime.combine(d, time(0, 0), tzinfo=UK_TZ)
    end = datetime.combine(d + timedelta(days=1), time(0, 0), tzinfo=UK_TZ)

    # Duration in hours
    duration_hours = (end - start).total_seconds() / 3600

    if duration_hours == 24:
        return 48
    elif duration_hours == 23:
        return 46  # Spring forward
    else:
        return 50  # Fall back


def format_settlement_period(settlement_date: date, period: int) -> str:
    """Format settlement period for display.

    Args:
        settlement_date: The trading date
        period: Settlement period number

    Returns:
        Formatted string like "2024-01-15 SP23 (11:00-11:30)"
    """
    start_time = settlement_period_to_time(period)
    end_minutes = (period * 30) % (24 * 60)
    end_time = time(hour=end_minutes // 60, minute=end_minutes % 60)

    return f"{settlement_date} SP{period} ({start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')})"
