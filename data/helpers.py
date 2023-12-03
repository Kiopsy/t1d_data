import os
from datetime import datetime, timezone
import pytz

export_folder = "export_data" 
unused_folder = "unused_data"
cleaned_folder = "cleaned_data"
tidepool_folder = "tidepool"
fitbit_folder = "fitbit"
bytesnap_folder = "bitesnap"

local_timezone_str = "America/New_York"

local_time_col = "local_Time"
utc_time_col = "utc_Time"

formats_to_try =  [
    "%Y-%m-%dT%H:%M:%S.%fZ", 
    "%Y-%m-%dT%H:%M:%S.%f", 
    "%Y-%m-%dT%H:%M:%SZ", 
    "%Y-%m-%dT%H:%M:%S", 
    "%Y-%m-%dT%H:%M", 
    "%m/%d/%y %H:%M:%S", 
    "%m/%d/%y"
]

def create_folders():
    """
    Creates the "cleaned" and "export" folders with subdirectories for "tidepool,"
    "fitbit," and "bytesnap" if they are missing.
    """
    for folder in [cleaned_folder, export_folder]:
        os.makedirs(folder, exist_ok=True)

    for subfolder in [tidepool_folder, fitbit_folder, bytesnap_folder]:
        subfolder_export_path = os.path.join(export_folder, subfolder)
        os.makedirs(subfolder_export_path, exist_ok=True)

        subfolder_cleaned_path = os.path.join(cleaned_folder, subfolder)
        os.makedirs(subfolder_cleaned_path, exist_ok=True)

def move_folder_contents(export_folder_path, destination_folder_path: str):
    # Walk through the export folder and move each item to the destination folder
    for root, dirs, files in os.walk(export_folder_path):
        for directory in dirs:
            source_dir = os.path.join(root, directory)
            destination_dir = os.path.join(destination_folder_path, os.path.relpath(source_dir, export_folder_path))
            shutil.move(source_dir, destination_dir)

        for file in files:
            source_file = os.path.join(root, file)
            destination_file = os.path.join(destination_folder_path, os.path.relpath(source_file, export_folder_path))
            shutil.move(source_file, destination_file)

def get_filepaths(folder_path: str) -> list:
    """
    Returns a list of file paths in the folder path, going through all subfolders recursively.
    """
    filepaths = []
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isdir(file_path):
            filepaths.extend(get_filepaths(file_path))
        else:
            filepaths.append(file_path)
    return filepaths

def get_foldernames(folder_path: str) -> list:
    """
    Returns a list of all folder paths in a parent folder
    """
    if not os.path.exists(folder_path):
        raise ValueError("Folder does not exist: " + folder_path)
    items = os.listdir(folder_path)
    folder_names = [item for item in items if os.path.isdir(os.path.join(folder_path, item))]
    return folder_names

def convert_timestamp(timestamp_str: str) -> dict:
    """
    Function that takes multiple timestamp formats, converts them to UTC, and then to a specified timezone.
    """
    local_timezone = pytz.timezone(local_timezone_str)
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
            datetime_obj_local = datetime_obj_utc.astimezone(local_timezone)

            # Offset in minutes from local to UTC
            offset_minutes = datetime_obj_local.utcoffset().total_seconds() // 60

            # Format the datetime objects to the specified ISO 8601 format
            utc_iso8601_str = datetime_obj_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            local_iso8601_str = datetime_obj_local.strftime("%Y-%m-%dT%H:%M:%S")

            results['utc_time'] = utc_iso8601_str
            results['utc_datetime'] = datetime_obj_utc
            results['local_time'] = local_iso8601_str
            results['local_datetime'] = datetime_obj_local
            results['offset'] = offset_minutes
            return results
        except ValueError:
            continue
    
    raise ValueError("Unsupported timestamp format")


def binary_search_by_time(data, target_entry):
    """
    Perform a binary search to determine if an entry exists in the data based on its timestamp.

    This function not only looks for the exact timestamp match but also checks nearby entries 
    for possible duplicates with the same timestamp. 
    """
    target_dateStr, target_dateTime = convert_timestamp(target_entry["dateTime"])
    duplicates = []
    start, end = 0, len(data) - 1

    while start <= end:
        mid = (start + end) // 2
        current_time = convert_timestamp(data[mid]["dateTime"])[1]

        if current_time == target_dateTime:
            duplicates.append(data[mid])
            
            # Check for duplicates in the right half
            right_index = mid + 1
            while right_index < len(data) and data[right_index]["dateTime"] == target_dateStr:
                duplicates.append(data[right_index])
                right_index += 1

            # Check for duplicates in the left half
            left_index = mid - 1
            while left_index >= 0 and data[left_index]["dateTime"] == target_dateStr:
                duplicates.append(data[left_index])
                left_index -= 1

            return target_entry in duplicates
            
        elif current_time < target_dateTime:
            start = mid + 1
        else:
            end = mid - 1

    return False