from datetime import datetime, timezone, timedelta
import pytz

# Define the function that converts various timestamp formats to UTC and a specified local timezone
def convert_timestamps(timestamp_str: str, to_timezone_str: str = 'America/New_York') -> dict:
    """
    Function that takes multiple timestamp formats, converts them to UTC, and then to a specified timezone.
    """
    formats_to_try = [
        "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M", "%d/%m/%y %H:%M:%S", "%m/%d/%y %H:%M:%S"
    ]

    to_timezone = pytz.timezone(to_timezone_str)
    results = {}

    for format_str in formats_to_try:
        try:
            # Try to create a datetime object using the current format string
            datetime_obj = datetime.strptime(timestamp_str, format_str)
            
            # If the datetime object doesn't have tzinfo, we assume it's in UTC time
            if 'Z' in timestamp_str or '.000Z' in timestamp_str:
                datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)

            # Convert the datetime object to UTC and the specified local timezone
            datetime_obj_utc = datetime_obj.astimezone(timezone.utc)
            datetime_obj_local = datetime_obj_utc.astimezone(to_timezone)

            # Format the datetime objects to the specified ISO 8601 format
            utc_iso8601_str = datetime_obj_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            local_iso8601_str = datetime_obj_local.strftime("%Y-%m-%dT%H:%M:%S")

            results['utc_time'] = utc_iso8601_str
            results['utc_datetime'] = datetime_obj_utc
            results['local_time'] = local_iso8601_str
            results['local_datetime'] = datetime_obj_local
            return results
        except ValueError:
            continue
    
    raise ValueError("Unsupported timestamp format")

# Define a list of timestamps to convert
timestamps = [
    "2023-11-02T00:14",
    "2023-10-09T18:33:18.000Z",
    "2023-11-07T01:25:30",
    "2023-11-07T08:00:30",
    "2023-11-04T07:04:33Z",
    "2023-10-18T00:00:00Z",
    "2023-11-05T08:06:00",
    "2023-10-10T00:49:52.000Z",
    "2023-11-04T02:55:00",
    "11/01/23 00:00:00",
    "2023-11-07T01:25:30.000",
    "10/19/23 00:00:00"
]

# Convert the timestamps and store the results
converted_timestamps_info = []
for ts in timestamps:
    try:
        converted_timestamps_info.append(convert_timestamps(ts))
    except ValueError as e:
        # If a ValueError is raised, store the error message instead
        converted_timestamps_info.append(str(e))

for struct in converted_timestamps_info:
    print(struct['utc_time'])
    print(struct['utc_datetime'])
    print(struct['local_time'])
    print(struct['local_datetime'])
    print()