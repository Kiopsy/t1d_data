"""
A Python script designed to process data from the 'export_data' folder from
Tidepool, Fitbit, and Bytesnap sources. The script generates separate files to
create individual timelines for various metrics like  steps, calories, distance.
"""

import os
import json
import pandas as pd
from datetime import datetime, timezone
import shutil
from collections import defaultdict
import time
import re
import csv
from config import *
import pytz
import sys

all_events = [] # List of all events from all data sources (CSV & JSON)

# Define all the folder names
export_folder = "export_data" 
used_folder = "used_data"
cleaned_folder = "cleaned_data"
tidepool_folder = "tidepool"
fitbit_folder = "fitbit"
bitesnap_folder = "bitesnap"

local_time_col = "local_Time"
utc_time_col = "utc_Time"

# metric -> json file path
metrics = dict()

local_timezone_str = "America/New_York"

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
    folders = [cleaned_folder, export_folder, used_folder]
    subfolders = [tidepool_folder, fitbit_folder, bitesnap_folder]

    for folder in folders:
        os.makedirs(folder, exist_ok=True)

        for subfolder in subfolders:
            subfolder_path = os.path.join(folder, subfolder)
            os.makedirs(subfolder_path, exist_ok=True)

def move_folder_contents(export_folder_path, destination_folder_path: str):
    for root, dirs, files in os.walk(export_folder_path):
        for directory in dirs:
            source_dir = os.path.join(root, directory)
            destination_dir = os.path.join(destination_folder_path, os.path.relpath(source_dir, export_folder_path))
            if os.path.exists(destination_dir):
                timestamp = datetime.now().strftime("%Y%m%d")
                destination_dir += f"_{timestamp}"
            shutil.move(source_dir, destination_dir)

        for file in files:
            source_file = os.path.join(root, file)
            destination_file = os.path.join(destination_folder_path, os.path.relpath(source_file, export_folder_path))
            if os.path.exists(destination_file):
                timestamp = datetime.now().strftime("%Y%m%d")
                base_name, file_extension = os.path.splitext(destination_file)
                destination_file = f"{base_name}_{timestamp}{file_extension}"

            shutil.move(source_file, destination_file)

def convert_timestamp_old(timestamp_str: str) -> (str, datetime):
    """
    Function that takes multiple timestamp formats and returns a tuple containing 
    date string in '%Y-%m-%dT%H:%M:%SZ' format and a datetime object.

    Should handle both Tidepool and Fitbit JSON & CSV data sources as of Oct 10, 2023.
    """
    formats_to_try = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%d/%m/%y %H:%M:%S"]
    
    for format_str in formats_to_try:
        try:
            datetime_obj = datetime.strptime(timestamp_str, format_str)
            # Convert the datetime object to ISO 8601 format with 'Z'
            iso8601_str = datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
            return iso8601_str, datetime_obj
        except ValueError:
            pass
    
    # If none of the formats match, raise an error
    raise ValueError("Unsupported timestamp format")

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

def matches_timestamp_format(timestamp_str: str):
    """
    Checks if a timestamp is within accepted formats.
    """
    for format_str in formats_to_try:
        try:
            datetime.strptime(timestamp_str, format_str)
            return True
        except ValueError:
            continue
    return False

def binary_search_by_time(data, target_entry):
    """
    Perform a binary search to determine if an entry exists in the data based on its timestamp.

    This function not only looks for the exact timestamp match but also checks nearby entries 
    for possible duplicates with the same timestamp. 
    """

    date_key = next((k for k, v in target_entry.items() if 'utc' in k))
    target_dateStr = target_entry[date_key]
    target_dateTime = convert_timestamp(target_dateStr)['utc_datetime']
    duplicates = []
    start, end = 0, len(data) - 1

    while start <= end:
        mid = (start + end) // 2
        current_time = convert_timestamp(data[mid][date_key])['utc_datetime']

        if current_time == target_dateTime:
            duplicates.append(data[mid])
            
            # Check for duplicates in the right half
            right_index = mid + 1
            while right_index < len(data) and data[right_index][date_key] == target_dateStr:
                duplicates.append(data[right_index])
                right_index += 1

            # Check for duplicates in the left half
            left_index = mid - 1
            while left_index >= 0 and data[left_index][date_key] == target_dateStr:
                duplicates.append(data[left_index])
                left_index -= 1

            return target_entry in duplicates
            
        elif current_time < target_dateTime:
            start = mid + 1
        else:
            end = mid - 1

    return False
    
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

def parse_batch(data_batch: defaultdict(list), dateobj: datetime, source: str):
    """
    Function that takes a list of events and generates a separate JSON file for each metric.
    If the file already exists, it will be appended to without duplicates.
    """
    # Iterate through each metric in the input data batch
    for metric in data_batch:
        # Construct the directory path for the metric and create it if it doesn't exist
        metric_folder = os.path.join(cleaned_folder, source, metric)
        os.makedirs(metric_folder, exist_ok=True)

        # Construct the file path for the JSON file
        filename = f"{metric}-{dateobj.year}-{dateobj.month}.json"
        json_file_path = os.path.join(metric_folder, filename)

        if os.path.exists(json_file_path):
            try:
                with open(json_file_path, 'r') as json_file:
                    loaded_data = json.load(json_file)
                    # Merge the entries from the data batch into the existing JSON file, avoiding duplicates
                    for entry in data_batch[metric]:
                        # if entry not in loaded_data:
                        #     loaded_data.append(entry)
                        # print('here')
                        # TODO: this is not actually faster for some reason which is strange

                        
                        # TODO: Fix duplicates
                        if entry not in loaded_data:
                            print('not missing')

                            print(loaded_data)

                            print()

                            print(entry)
                            time.sleep(1000)

                        if not binary_search_by_time(loaded_data, entry):
                            print('not missing binary')


                        if not binary_search_by_time(loaded_data, entry):
                            loaded_data.append(entry)

                    loaded_data.sort(key=lambda entry: convert_timestamp(next((v for k, v in entry.items() if 'utc' in k)))['utc_datetime'])
                    with open(json_file_path, 'w') as json_file:
                        json.dump(loaded_data, json_file)

            except json.JSONDecodeError as e:
                print(f"Error loading JSON from {json_file_path}: {e}")
        else:
            with open(json_file_path, "w") as json_file:
                json.dump(data_batch[metric], json_file)

            """
            csv code:

            csv_filename = f"{metric}-{dateobj.year}-{dateobj.month}.csv"
            csv_file_path = os.path.join(metric_folder, csv_filename)
            fieldnames = data_batch[metric][0].keys()
            with open(csv_file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                for row in data_batch[metric]:
                    writer.writerow(row)
            """

def parse_tidepool_data(filepaths: list):
    """
    Function that takes a list of filepaths to Tidepool data files and generates a separate json file for each metric.
    """
    set_of_metrics = set()

    for filepath in filepaths:
        if filepath.endswith(".json"):
            print("current file:", filepath)
            with open(filepath, 'r') as json_file:
                # TODO: Address this bottleneck.. will load all of the data at once. 
                data = json.load(json_file) 
                sorted_data = sorted(data, key=lambda entry: convert_timestamp(entry['time'])['utc_datetime'])
                # TODO: again bottleneck if you run into issues

                data_batch = defaultdict(list)
                current_month = None
                current_dateobj = None
                for entry in sorted_data:
                    # Clean the entry's time data
                    try:
                        if "time" in entry:
                            converted_time = convert_timestamp(entry["time"])
                            del entry["time"]

                            if "deviceTime" in entry:
                                del entry["deviceTime"]
                            if "localTime" in entry:
                                del entry["localTime"]
                            if "timezoneOffset" in entry:
                                del entry["timezoneOffset"]
                            
                            entry[utc_time_col] = converted_time['utc_time']
                            entry[local_time_col] = converted_time['local_time']
                            entry['timezoneOffset'] = converted_time['offset']
                            entry['datetime'] = converted_time['utc_datetime']
                    except:
                        print("error:", entry)

                    metric_type = entry["type"]
                    set_of_metrics.add(metric_type)
                    date_obj = entry['datetime']
                    del entry['datetime']

                    if current_month is None:
                        current_month = date_obj.month
                        current_dateobj = date_obj

                    if date_obj.month == current_month:
                        data_batch[metric_type].append(entry)
                    else:
                        # Process the data batch for the previous month
                        parse_batch(data_batch, current_dateobj, source="tidepool")

                        # Start a new data batch for the current month
                        data_batch = defaultdict(list)
                        data_batch[metric_type].append(entry)
                        current_month = date_obj.month
                        current_dateobj = date_obj

                # Process the last data batch after exiting the loop
                if data_batch:
                    parse_batch(data_batch, date_obj, source="tidepool")

    print("Total metric lst:", set_of_metrics)

def parse_fitbit_data(filepaths: list):

    filepaths = [path for path in filepaths if path.endswith((".csv", ".json"))]

    # Regex pattern to match filenames and capture the metric name, dates (including those in parentheses), and extension.
    pattern = r'^(.*?)(?: - )?(?:(\d{4}-\d{2}(?:-\d{2})?(?:-\d{4}-\d{2}-\d{2})?(?:-\(\d+\))?)?)(\.csv|\.json)$'

    missing_matches = []

    # metric -> list of struct {filename, dates, extension}
    metrics = defaultdict(list) 

    for filename in filepaths:
        base_name = filename.split('/')[-1]
        matches = re.match(pattern, base_name)
        if matches:
            metric = matches.group(1).rstrip(' -')
            dates = matches.group(2).strip() if matches.group(2) else ""
            extension = matches.group(3).strip()

            # Additional checks for date range and parentheses within the metric
            date_range_match = re.search(r'(\d{4}-\d{2}-\d{2}-\d{4}-\d{2}-\d{2})$', metric)
            date_parentheses_match = re.search(r'(\d{4}-\d{2}-\(\d+\))$', metric)
            partial_date_check = re.search(r'(\d{4}-\d{2})$', metric)

            if date_range_match:
                dates = date_range_match.group(1)
                metric = metric[:metric.rfind(dates)].rstrip(' -')
            elif date_parentheses_match:
                dates = date_parentheses_match.group(1)
                metric = metric[:metric.rfind(dates)].rstrip(' -')
            elif partial_date_check:
                dates = partial_date_check.group(1)
                metric = metric[:metric.rfind(dates)].rstrip(' -')

            print(f'Matched Filename: {filename}')
            print(f'Metric: {metric}')
            print(f'Dates: {dates}')
            print(f'Extension: {extension}\n')

            metrics[metric].append({
                'filename': filename,
                'date': dates, 
                'extension': extension
            })
        else:
            missing_matches.append(filename)

    if missing_matches:
        print("REGEX MATCHES MISSING")
        print(missing_matches)

    print("Metric keys:", metrics.keys())

    for metric, file_struct_list in metrics.items():

        print("Current metric:", metric)

        if metric in FITBIT_SKIPPED_METRICS:
            continue

        total_metric_data = []
    
        # Get all the data for a specific metric
        for file_struct in file_struct_list:

            filename = file_struct['filename']
            date = file_struct['date']
            extension = file_struct['extension']

            print("Current file:", filename)

            if extension == '.csv':
                with open(filename, 'r') as csvfile:
                    csvreader = csv.DictReader(csvfile)
                    for row in csvreader:
                        # some entries might have more than one timestamp: say start/end time
                        found_timestamps = []
                        for key, value in list(row.items()):
                            if not isinstance(value, dict) and isinstance(value, str) and matches_timestamp_format(value):
                                found_timestamps.append((key, value))

                        if len(found_timestamps) > 1:
                            for key, value in found_timestamps:
                                converted_time = convert_timestamp(value)
                                del row[key]
                                row['utc_' + key] = converted_time['utc_time']
                                row['local_' + key] = converted_time['local_time']
                                row['timezoneOffset'] = converted_time['offset']
                                row['datetime'] = converted_time['utc_datetime']

                        elif len(found_timestamps) == 1:
                            key, value = found_timestamps.pop()
                            converted_time = convert_timestamp(value)
                            del row[key]
                            row[utc_time_col] = converted_time['utc_time']
                            row[local_time_col] = converted_time['local_time']
                            row['timezoneOffset'] = converted_time['offset']
                            row['datetime'] = converted_time['utc_datetime']
                        
                        total_metric_data.append(row)
            elif extension == '.json':
                with open(filename, 'r') as jsonfile:
                    data = json.load(jsonfile)  
                    for item in data:
                        # some entries might have more than one timestamp: say start/end time
                        found_timestamps = []
                        for key, value in list(item.items()):
                            if not isinstance(value, dict) and isinstance(value, str) and matches_timestamp_format(value):
                                found_timestamps.append((key, value))

                        if len(found_timestamps) > 1:
                            for key, value in found_timestamps:
                                converted_time = convert_timestamp(value)
                                del item[key]
                                item['utc_' + key] = converted_time['utc_time']
                                item['local_' + key] = converted_time['local_time']
                                item['timezoneOffset'] = converted_time['offset']
                                item['datetime'] = converted_time['utc_datetime']

                        elif len(found_timestamps) == 1:
                            key, value = found_timestamps.pop()
                            converted_time = convert_timestamp(value)
                            del item[key]
                            item[utc_time_col] = converted_time['utc_time']
                            item[local_time_col] = converted_time['local_time']
                            item['timezoneOffset'] = converted_time['offset']
                            item['datetime'] = converted_time['utc_datetime']
                        
                        total_metric_data.append(item)
            else:
                print("Trying to read an incorrect filepath: ", file_struct)

        print("In memory: ", sys.getsizeof(total_metric_data))
        for item in total_metric_data:
            print(item)
            print()

        total_metric_data = sorted(total_metric_data, key=lambda x: x['datetime'])

        data_batch = defaultdict(list)
        current_month = None
        current_dateobj = None
        for entry in total_metric_data:

            date_obj = entry['datetime']
            del entry['datetime']

            if current_month is None:
                current_month = date_obj.month
                current_dateobj = date_obj

            if date_obj.month == current_month:
                data_batch[metric].append(entry)
            else:
                # Process the data batch for the previous month
                parse_batch(data_batch, current_dateobj, source="fitbit")

                # Start a new data batch for the current month
                data_batch = defaultdict(list)
                data_batch[metric].append(entry)
                current_month = date_obj.month
                current_dateobj = date_obj

        # Process the last data batch after exiting the loop
        if data_batch:
            parse_batch(data_batch, date_obj, source="fitbit")


def parse_bitesnap_data(filepaths: list):

    """
    "eatenAtUTC": "2023-11-12T18:10:23.000Z",
    "eatenAtLocalTime": 20231112131023,
    "lastModifiedUTC": 1699813049780,
    """

    for filepath in filepaths:
        if filepath.endswith(".json"):
            print("current file:", filepath)
            with open(filepath, 'r') as json_file:
                # TODO: Address this bottleneck.. will load all of the data at once. 
                data = json.load(json_file)['entries'] 
                sorted_data = sorted(data, key=lambda entry: convert_timestamp(entry['eatenAtUTC'])['utc_datetime'])
                # TODO: again bottleneck if you run into issues

                data_batch = defaultdict(list)
                current_month = None
                current_dateobj = None
                for entry in sorted_data:
                    # Clean the entry's time data
                    if "eatenAtUTC" in entry:
                        converted_time = convert_timestamp(entry["eatenAtUTC"])
                        del entry["eatenAtUTC"]

                        if "eatenAtLocalTime" in entry:
                            del entry["eatenAtLocalTime"]
                        if "lastModifiedUTC" in entry:
                            del entry["lastModifiedUTC"]
                        
                        entry[utc_time_col] = converted_time['utc_time']
                        entry[local_time_col] = converted_time['local_time']
                        entry['timezoneOffset'] = converted_time['offset']
                        entry['datetime'] = converted_time['utc_datetime']

                    metric_type = 'food'
                    date_obj = entry['datetime']
                    del entry['datetime']

                    if current_month is None:
                        current_month = date_obj.month
                        current_dateobj = date_obj

                    if date_obj.month == current_month:
                        data_batch[metric_type].append(entry)
                    else:
                        # Process the data batch for the previous month
                        parse_batch(data_batch, current_dateobj, source="bitesnap")

                        # Start a new data batch for the current month
                        data_batch = defaultdict(list)
                        data_batch[metric_type].append(entry)
                        current_month = date_obj.month
                        current_dateobj = date_obj

                # Process the last data batch after exiting the loop
                if data_batch:
                    parse_batch(data_batch, date_obj, source="bitesnap")


def process_data(data_folder, parser_function):
    data_export_path = os.path.join(export_folder, data_folder)
    data_used_path = os.path.join(used_folder, data_folder)
    data_files = get_filepaths(data_export_path)
    parser_function(data_files)
    move_folder_contents(data_export_path, data_used_path)

if __name__ == "__main__":
    tic = time.time()
    create_folders()

    # For Fitbit data
    process_data(fitbit_folder, parse_fitbit_data)

    # For Tidepool data
    process_data(tidepool_folder, parse_tidepool_data)

    # For Bitesnap data
    process_data(bitesnap_folder, parse_bitesnap_data)

    toc = time.time()
    print("time elapsed:", toc - tic)
