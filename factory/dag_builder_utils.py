import re
from datetime import datetime, timedelta

import pendulum


def get_start_date(start_date, tzinfo) -> datetime:
    """Get default start date"""
    if start_date:
        splits = start_date.split(("-"))
        year = int(splits[0])
        month = int(splits[1])
        day = int(splits[2])
    else:
        year = 2023
        month = 5
        day = 1

    timezone = pendulum.timezone(tzinfo)

    return datetime(year, month, day, tzinfo=timezone)


def format_input_date(date_string: str):
    import logging

    logging.info(f"The date string passed in is: {date_string}")
    if not date_string:
        logging.info("Returning false as no comparison required.")
        return False

    string_format = "%Y-%m-%dT%H:%M:%S+00:00"

    if isinstance(date_string, datetime):
        return date_string.strftime(string_format)

    if not isinstance(date_string, str):
        date_string = str(date_string)

    # Regex Match seconds & milliseconds & timezone
    match = re.match(
        r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}", date_string
    )
    if match:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z").strftime(
            string_format
        )

    # Regex Match seconds & timezone but NO milliseconds
    match = re.match(
        r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}", date_string
    )
    if match:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z").strftime(
            string_format
        )

    # Regex Match seconds & milliseconds but NO timezone
    match = re.match(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\.\d+", date_string)
    if match:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f").strftime(
            string_format
        )

    # Regex Match seconds but NOT milliseconds or timezone NO timezone
    match = re.match(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}", date_string)
    if match:
        return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S").strftime(
            string_format
        )

    return False


def ds_timedelta_formatted(
    datetime_base: str,
    timedelta_format: int = 0,
    format: str = "%Y-%m-%d 00:00:00",
    local_timezone: str = "Europe/London",
):
    local_tz = pendulum.timezone(local_timezone)
    base = datetime.strptime(datetime_base, "%Y-%m-%d").astimezone(local_tz)
    timedelta_output = base + timedelta(days=timedelta_format)
    return timedelta_output.replace(hour=0, minute=0, second=0).strftime(format)


DF_USER_DEFINED_MACROS = {
    "format_input_date": format_input_date,
    "ds_timedelta_formatted": ds_timedelta_formatted,
}

DF_USER_DEFINED_FILTERS = {}
