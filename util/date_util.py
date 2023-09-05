from datetime import datetime
import pytz, logging

logger = logging.Logger(__name__)

TZ_CEST =pytz.timezone("Europe/Berlin")
STRFTIME_FORMAT = '%a %d %b %Y %H:%M:%S %Z'

def convert_date(long_date):
    if long_date is None:
        return None
    utc_convert = datetime.fromtimestamp(long_date, tz=TZ_CEST)
    rettime = utc_convert.strftime(STRFTIME_FORMAT)
    return rettime

def convert_into_date(year, month, day, hour=0, minute=0, second=0):
    if year is None or month is None or day is None:
        return None
    new_dt = datetime(year, month, day, hour, minute, second)
    return int(new_dt.timestamp())