import pandas as pd
from datetime import datetime, time
from typing import Tuple
from dateutil.relativedelta import relativedelta


def str_to_offset(offset_str: str) -> pd.DateOffset:
    """Converting Strings to Offset Parameters"""
    if offset_str.endswith('d'):
        return pd.DateOffset(days=int(offset_str[:-1]))
    if offset_str.endswith('w'):
        return pd.DateOffset(weeks=int(offset_str[:-1]))
    elif offset_str.endswith('m'):
        return pd.DateOffset(months=int(offset_str[:-1]))
    elif offset_str.endswith('y'):
        return pd.DateOffset(years=int(offset_str[:-1]))
    else:
        raise ValueError("Invalid offset string")


def datetime_offset(dt: datetime, offset_str: str) -> datetime:
    """ offsetting datetime """
    return dt + str_to_offset(offset_str)


def datetime_range(dt: datetime, range_type: str) -> Tuple[datetime, datetime]:
    """ get datetime range according to range type """
    if range_type == "week":
        monday = dt - relativedelta(days=dt.weekday())
        monday_start = datetime.combine(monday.date(), time.min)
        return monday_start, datetime_offset(monday_start, "1w")
    elif range_type == "month":
        month_start = datetime(dt.year, dt.month, 1)
        return month_start, datetime_offset(month_start, "1m")
    elif range_type == "seasonal":
        seasonal_start = datetime(dt.year, ((dt.month - 1) // 3) * 3 + 1, 1)
        return seasonal_start, datetime_offset(seasonal_start, "3m")
    elif range_type == "year":
        year_start = datetime(dt.year, 1, 1)
        return year_start, datetime_offset(year_start, "1y")
    else:
        raise ValueError("Invalid range type")
