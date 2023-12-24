import re
import os
import csv
import json
import sys
from datetime import datetime, timezone
from collections import defaultdict
from logger import *

# Fitbit Filename Regexes
# FITBIT_MAIN_FILENAME_PATTERN = re.compile(r'^(.*?)(?: - )?(?:(\d{4}-\d{2}(?:-\d{2})?(?:-\d{4}-\d{2}-\d{2})?(?:-\(\d+\))?)?)(\.csv|\.json)$')
# FITBIT_DATE_RANGE_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2}-\d{4}-\d{2}-\d{2})$')
# FITBIT_DATE_PARENTHESES_PATTERN = re.compile(r'(\d{4}-\d{2}-\(\d+\))$')
# FITBIT_PARTIAL_DATE_PATTERN = re.compile(r'(\d{4}-\d{2})$')

FITBIT_FILENAME_PATTERN = re.compile(r'^(.*?)(?: - .*?)*(\.csv|\.json)$')

logger = configure_logging()

def extract_fitbit_filepath(filepath):
    """
    Breaks down a Fibit filepath (from export) into a struct comprised of metric and extension.
    
    Example filenames:
    - 'Daily Heart Rate Variability Summary - 2023-10-(14).csv'
    - 'Active Zone Minutes - 2023-11-01.csv'
    - 'User_Retired_Password.csv'
    - 'heart_rate-2023-10-25.json'
    """

    base_name = os.path.basename(filepath)
    matches = FITBIT_FILENAME_PATTERN.match(base_name)
    
    if not matches:
        logger.error(f'Fitbit file {base_name} not accounted for.')
    
    metric = matches.group(1).rstrip(' -')
    extension = matches.group(2).strip()
    file_struct = {
        'filename': filepath,
        'extension': extension
    }
    return metric, file_struct

def find_timestamps(data_entry):
    """
    Finds all keys associated with a timestamp value in an entry dict. Returns a list of these key/value pairs.
    """
    found_timestamps = []
    for key, value in data_entry.items():
        if isinstance(value, dict) or isinstance(value, str):
            continue
        if matches_timestamp_format(value):
            found_timestamps.append((key, value))
    return found_timestamps

def clean_entry(data_entry):
    """
    Cleans all timestamps in a data entry from any source.
    """
    found_timestamps = find_timestamps(data_entry)
    for key, value in found_timestamps:
        converted_time = convert_timestamp(value)
        del data_entry[key]
        data_entry[utc_time_col if len(found_timestamps) == 1 else 'utc_' + key] = converted_time['utc_time']
        data_entry[local_time_col if len(found_timestamps) == 1 else 'local_' + key] = converted_time['local_time']
        data_entry['timezoneOffset'] = converted_time['offset']
        data_entry['datetime'] = converted_time['utc_datetime']
    return data_entry

def process_file(file_struct):
    """
    Processes a file (via {filename, extension} dict), returns list of clean entries.
    """
    filename = file_struct['filename']
    extension = file_struct['extension']

    if extension != '.csv' or extension != '.json':
        logger.error('Trying to read an incorrect filepath:', file_struct['filename'])
        return []

    with open(filename, 'r') as file:
        entries = []
        data = csv.DictReader(file) if extension == '.csv' else json.load(file)
        for entry in data:
            cleaned_entry = clean_entry(entry)
            entries.append(cleaned_entry)
        return entries
    
def get_utc_datetime(entry):
    """
    Extracts a UTC datetime from an entry. Used for sorting purposes.
    """
    utc_key = next(k for k, v in entry.items() if 'utc' in k)
    return convert_timestamp(entry[utc_key])['utc_datetime']

def merge_data(loaded_data, new_data):
    """
    Merges new data into loaded data, avoiding duplicates.
    """
    # TODO 
    for entry in new_data:
        if not binary_search_by_time(loaded_data, entry):
            loaded_data.append(entry)

    # TODO is sorting needed here? is it wasting time?
    loaded_data.sort(key=get_utc_datetime)
    return loaded_data

def write_json_file(file_path, data):
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)

def parse_batch(data_batch: defaultdict(list), dateobj: datetime, source: str):
    """
    Function that takes a data batch (metric -> list of entries) for a particular
    month. Generates a separate JSON file for each metric. If the file already 
    exists, it will be appended to without duplicates.
    """
    for metric in data_batch:
        metric_folder = os.path.join(CLEANED_FOLDER, source, metric)
        os.makedirs(metric_folder, exist_ok=True)

        json_file_path = os.path.join(metric_folder, f"{metric}-{dateobj.year}-{dateobj.month}.json")

        if os.path.exists(json_file_path):
            try:
                with open(json_file_path, 'r') as json_file:
                    loaded_data = json.load(json_file)

                merged_data = merge_data(loaded_data, data_batch[metric])
                write_json_file(json_file_path, merged_data)

            except json.JSONDecodeError as e:
                logger.error(f"Error loading JSON from {json_file_path}: {e}")
        else:
            write_json_file(json_file_path, data_batch[metric])
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
    
def parse_fitbit_data(filepaths: list):
    logger.debug('Parsing Fitbit data...')

    filepaths = [path for path in filepaths if path.endswith((".csv", ".json"))]
    metrics = defaultdict(list) # metric -> struct list {filename, extension}

    for filepath in filepaths:
        metric, file_struct = extract_fitbit_filename(filepath)
        metrics[metric].append(file_struct)

    logger.debug('Metric keys:', metrics.keys())

    for metric, file_struct_list in metrics.items():
        logger.debug('Current metric', metric)
        if metric in FITBIT_SKIPPED_METRICS:
            logger.debug('Skipping...')
            continue

        # Loads all the data for one metric and parses/cleans the batch into storage
        total_metric_data = []
        for file_struct in file_struct_list:
            logger.debug('Current file', file_struct['filename'])
            total_metric_data.extend(process_file(file_struct))

        logger.debug('In memory:', sys.getsizeof(total_metric_data))

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

