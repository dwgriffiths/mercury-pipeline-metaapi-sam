from datetime import datetime, timedelta

from src.config import *

def get_datetime_boundaries(
    datetimeutc_from: datetime,
    datetimeutc_to: datetime,
    window_size: int = 3600, # in seconds
    reverse: bool = True
):
    """
    Get the time windows between two datetimes.
    """
    n_steps = int((datetimeutc_to - datetimeutc_from).total_seconds() / window_size)
    datetimesutc_from = [datetimeutc_from + timedelta(seconds=i*window_size) for i in range(n_steps)]
    datetimesutc_to = [x + timedelta(seconds=window_size) for x in datetimesutc_from]
    if reverse:
        return [x for x in reversed(datetimesutc_from)], [y for y in reversed(datetimesutc_to)]
    return datetimesutc_from, datetimesutc_to

def get_datetime_boundaries_in_month(
    month: str,
    window_size: int = 3600, # in seconds
    reverse: bool = True
):
    """
    Get the time windows between two datetimes within a month.
    """
    datetimeutc_from = datetime.strptime(month, "%Y%m")
    datetimeutc_to = min(
        (datetime.strptime(month, "%Y%m") + timedelta(days=40)).replace(day=1),
        DATETIMEUTC_TO
    )
    return get_datetime_boundaries(
        datetimeutc_from,
        datetimeutc_to,
        window_size,
        reverse
    )

def get_months(
    datetimestr_from: str = DATETIMESTR_FROM,
    datetimestr_to: str = DATETIMESTR_TO,
    reverse: bool = True
):
    """
    Get the time windows between two datetimes.
    """
    datetimeutc_from = datetime.strptime(
        datetimestr_from,
        DATETIME_FORMAT
    )
    datetimeutc_to = datetime.strptime(
        datetimestr_to,
        DATETIME_FORMAT
    )
    datetimesutc_from, datetimesutc_to = get_datetime_boundaries(
        datetimeutc_from,
        datetimeutc_to,
        reverse=False
    )
    months = [dt.strftime("%Y%m") for dt in datetimesutc_from]
    months.extend([dt.strftime("%Y%m") for dt in datetimesutc_to])
    months = sorted(list(set(months)))
    if reverse:
        return list(reversed(months))
    return list(months)