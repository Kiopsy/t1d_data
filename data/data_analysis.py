import json
import os
import re
import time
from collections import defaultdict

import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tabulate import tabulate

from data.helpers_old import *

"""
notes: 

- what is the difference between distance and steps?
- 
"""
# GLOBALS:
total_metrics = set()
total_days = set()

metric_days = defaultdict(set)
metric_entry_count = dict()

# Plotting:
plot_timeline = True
plot_entries = True
plot_metrics = True


def get_month_day(entry):
    # TODO: maybe do it by local time?
    time_obj = convert_timestamp(next((v for k, v in entry.items() if 'local' in k)))
    local_datetime = time_obj['local_datetime']
    month_day_str = local_datetime.strftime("%m-%d")
    return month_day_str

def combine_dict(dict1, dict2):
    results = dict1.copy()

    for key, value in dict2.items():
        if key in results:
            results[key].update(value)
        else:
            results[key] = value

    return results

def analyze_cbg(data):
    results = {
        'cbg_total': defaultdict(list),
        'cbg_avg': defaultdict(float)
    }

    for entry in data:
        month_day_str = get_month_day(entry)
        cbg_value = entry['value']
        results['cbg_total'][month_day_str].append(cbg_value)

    for key, value in results['cbg_total'].items():
        results['cbg_avg'][key] = sum(value) / len(value)

    return results

def analyze_heartrate(data):
    results = {
        'heart_rate_total': defaultdict(list),
        'heart_rate_avg': defaultdict(float)
    }

    for entry in data:
        month_day_str = get_month_day(entry)
        heart_rate_value = entry['value']['bpm']
        results['heart_rate_total'][month_day_str].append(heart_rate_value)

    for key, value in results['heart_rate_total'].items():
        results['heart_rate_avg'][key] = sum(value) / len(value)

    return results

def analyze_food(data):
    results = {
        'calories_total': defaultdict(list),
        'protein_total': defaultdict(list),
        'fat_total': defaultdict(list),
        'carbs_total': defaultdict(list),
        'sugars_total': defaultdict(list),
        'calories_sum': defaultdict(float),
        'protein_sum': defaultdict(float),
        'fat_sum': defaultdict(float),
        'carbs_sum': defaultdict(float),
        'sugars_sum': defaultdict(float),
    }

    for entry in data:
        nutrients = entry.get('nutrients', [])
        calories = 0
        fat = 0
        protein = 0
        carbs = 0
        sugars = 0
        for nutrient in nutrients:
            name = nutrient['name']
            amount = nutrient['amount']
            if name == 'calories':
                calories += amount 
            elif name == 'totalFat':
                fat += amount
            elif name == 'totalCarb':
                carbs += amount
            elif name == 'sugars':
                sugars += amount
            elif name == 'protein':
                protein += amount

        month_day_str = get_month_day(entry)
        results['calories_total'][month_day_str].append(calories)
        results['protein_total'][month_day_str].append(protein)
        results['fat_total'][month_day_str].append(fat)
        results['carbs_total'][month_day_str].append(carbs)
        results['sugars_total'][month_day_str].append(sugars)

        for key, value in results['calories_total'].items():
            results['calories_sum'][key] = sum(value)

        for key, value in results['protein_total'].items():
            results['protein_sum'][key] = sum(value)

        for key, value in results['fat_total'].items():
            results['fat_sum'][key] = sum(value)

        for key, value in results['carbs_total'].items():
            results['carbs_sum'][key] = sum(value)

        for key, value in results['sugars_total'].items():
            results['sugars_sum'][key] = sum(value)

    return results

def analyze_sleep(data):
    keys = ["minutesAsleep", "minutesAwake", "minutesAfterWakeup", "timeInBed", "efficiency"]
    results = {key: defaultdict(float) for key in keys}

    for entry in data:
        month_day_str = get_month_day(entry)
        for key in keys:
            results[key][month_day_str] += entry[key]

    return results

def analyze_steps(data):
    results = {
        'steps_total': defaultdict(list),
        'steps_sum': defaultdict(float)
    }

    for entry in data:
        month_day_str = get_month_day(entry)
        steps_value = int(entry['value'])
        results['steps_total'][month_day_str].append(steps_value)

    for key, value in results['steps_total'].items():
        results['steps_sum'][key] = sum(value)

    return results

def analyze_very_active(data):
    results = {
        'very_active_minutes': defaultdict(float)
    }

    for entry in data:
        month_day_str = get_month_day(entry)
        active_mins_value = int(entry['value'])
        results['very_active_minutes'][month_day_str] += active_mins_value

    return results

def analyze_moderately_active(data):
    results = {
        'moderately_active_minutes': defaultdict(float)
    }

    for entry in data:
        month_day_str = get_month_day(entry)
        moderate_mins_value = int(entry['value'])
        results['moderately_active_minutes'][month_day_str] += moderate_mins_value

    return results

def analyze_sedentary(data):
    results = {
        'sedentary_minutes': defaultdict(float)
    }

    for entry in data:
        month_day_str = get_month_day(entry)
        sedentary_mins_value = int(entry['value'])
        results['sedentary_minutes'][month_day_str] += sedentary_mins_value

    return results

def analyze_basal(data):
    """
    {
        'clockDriftOffset': -403000,
        'conversionOffset': 0,
        'deliveryType': 'suspend',
        'deviceId': 'tandemCIQ1002717694869',
        'duration': 51.916666666666664,
        'id': 'iur27h8n4iq43n5525q9ihs7epsau9ph',
        'payload': '{"algorithm_rate":null,"commanded_rate":0,"logIndices":[303439],"profile_basal_rate":1.3,"temp_rate":null}',
        'type': 'basal',
        'uploadId': 'upid_153e7ac9598a',
        'utc_Time': '2023-09-10T21:13:25Z',
        'local_Time': '2023-09-10T17:13:25',
        'timezoneOffset': -240.0
    }
    """

    def units_delivered(x):
        rate_u_per_hr, duration_minutes = x
        duration_hours = duration_minutes / 60
        units_delivered = rate_u_per_hr * duration_hours
        return units_delivered

    results = {
        'basal_total': defaultdict(list),
        'insulin_sum_basal': defaultdict(float),
    }

    for entry in data:
        if 'rate' in entry.keys():
            month_day_str = get_month_day(entry)
            rate = entry['rate']
            duration = entry['duration']
            results['basal_total'][month_day_str].append((rate, duration))

    for key, value in results['basal_total'].items():
        results['insulin_sum_basal'][key] = sum(map(units_delivered, value))

    return results

def analyze_bolus(data):
    results = {
        'bolus_total': defaultdict(list),
        'insulin_sum_bolus': defaultdict(float),
    }

    for entry in data:
        month_day_str = get_month_day(entry)
        dose = entry['normal']
        results['bolus_total'][month_day_str].append(dose)

    for key, value in results['bolus_total'].items():
        results['insulin_sum_bolus'][key] = sum(value)

    return results

METRICS_TO_ANALYZE = {
    'cbg': analyze_cbg,
    'heart_rate': analyze_heartrate,
    'sleep': analyze_sleep,
    'steps': analyze_steps,
    'very_active_minutes': analyze_very_active,
    'sedentary_minutes': analyze_sedentary,
    'moderately_active_minutes': analyze_moderately_active,
    'bolus': analyze_bolus,
    'basal': analyze_basal,
    'food': analyze_food,
    # 'Daily Heart Rate Variability Summary'
}

def analyze_file(filepath):
    pass

def analyze_metric(metric, folderpath):
    print("Current metric: ", metric)
    metric_files = get_filepaths(folderpath)
    total_entries = 0
    total_days = 0
    year_month = ""
    results = {}

    for filepath in metric_files:
        pattern = r'-(\d{4}-\d{2})\.json'
        match = re.search(pattern, filepath)
        daily_stats = defaultdict(int) # 'MM-DD' -> num entries for that day

        if match:
            year_month = match.group(1)
            print("Current date: ", year_month)

        try:
            with open(filepath, 'r') as json_file:
                loaded_data = json.load(json_file)
                curr_entries = len(loaded_data)
                total_entries += curr_entries
                if year_month:
                    print(f"Number of entries in {year_month}: {curr_entries}")
                else:
                    print(f"Number of entries: {curr_entries}")

                if metric in METRICS_TO_ANALYZE:
                    tmp = METRICS_TO_ANALYZE[metric](loaded_data)
                    results = combine_dict(results, tmp)

                for entry in loaded_data:
                    time_obj = convert_timestamp(next((v for k, v in entry.items() if 'utc' in k)))
                    utc_datetime = time_obj['utc_datetime']
                    month_day_str = utc_datetime.strftime("%m-%d")
                    month_day_yr_str = utc_datetime.strftime("%m-%d-%Y")

                    # track the days w/ data avaiable
                    metric_days[metric].add(month_day_yr_str)

                    # track the number of entries for all of these days
                    if metric not in metric_entry_count:
                        metric_entry_count[metric] = defaultdict(int)
                    metric_entry_count[metric][month_day_yr_str] += 1

                    daily_stats[month_day_str] += 1
                    # TODO: put any additional stuff that you want to track about the day here
                    total_entries += 1

                # print monthly statistics
                daily_stats_table = [["Date", "Entries"]]
                for date, entries in daily_stats.items():
                    daily_stats_table.append([date, entries])
                    total_days += 1

                print(tabulate(daily_stats_table, headers="firstrow"))

        except json.JSONDecodeError as e:
            print(f"Error loading JSON from {filepath}: {e}")

    print("Total number of entries: ", total_entries)
    print("Total number of days: ", total_days)

    return results

def generate_statistics():
    results = {}

    for source in [tidepool_folder, fitbit_folder, bytesnap_folder]:
        print("Currently on: ", source)
        cleaned_folder_path = os.path.join(cleaned_folder, source)
        metric_names = get_foldernames(cleaned_folder_path)

        print("Metrics: ", metric_names)
        print("Number of metrics: ", len(metric_names))
        total_metrics.update(metric_names)

        for metric in metric_names:
            metric_folder_path = os.path.join(cleaned_folder_path, metric)
            metric_results = analyze_metric(metric, metric_folder_path)
            results = combine_dict(results, metric_results) # TODO: retrive extracted results

    return results

def compare_metrics(metric1, metric2, data):
    
    # plot all of your graphs comparing metrics against each other
    # maybe put things inside a table to compare as well?
    matching_dates = set(data[metric1].keys()) & set(data[metric2].keys())
    x_values = [data[metric1][date] for date in matching_dates]
    y_values = [data[metric2][date] for date in matching_dates]

    # Create a scatterplot
    plt.scatter(x_values, y_values, marker='o', color='blue')
    plt.xlabel(metric1)
    plt.ylabel(metric2)
    plt.grid(True)
    plt.legend()
    plt.show()

def plot_timeline(data):
    disregarded_metrics = [
        "swim_lengths_data",
        "Computed Temperature",
        "height",
        "weight",
        "upload",
        "cgmSettings",
        "pumpSettings.basalSchedules",
        "smbg",
        "wizard",
        "pumpSettings.bgTargets",
        "pumpSettings.carbRatios",
        "pumpSettings.insulinSensitivities",
    ]

    # I stopped collecting data after 12/3 
    cutoff_date = pd.to_datetime("12-03-2023", format='%m-%d-%Y')
    for metric, dates in data.items():
        dates_to_remove = {date for date in dates if pd.to_datetime(date, format='%m-%d-%Y') > cutoff_date}
        data[metric] -= dates_to_remove

    for metric in disregarded_metrics:
        del data[metric]

    data["exercise"] = data["exercise-0"]
    del data["exercise-0"]

    bar_height = 2
    plt.figure(figsize=(12, 8))
    colormap = plt.cm.get_cmap('viridis', len(data))

    for i, (metric, dates) in enumerate(data.items(), start=1):
        dates = pd.to_datetime(list(dates), format='%m-%d-%Y')
        color = colormap(i - 1)
        for date in dates:
            plt.barh(i, 1, left=date, height=bar_height, color=color)

    plt.yticks(range(1, len(data)+1), data.keys())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=10))
    plt.gcf().autofmt_xdate()
    plt.title("9/2023-12/2023 Timeline for Diabetes Metrics")
    plt.xlabel("Date")
    plt.ylabel("Metrics")
    plt.grid(True, axis='x')
    plt.show()

def plot_entries(data, metrics_to_plot=[]):
    plt.figure(figsize=(12, 6))

    if not metrics_to_plot:
        metrics_to_plot = list(data.keys())

    colormap = plt.cm.get_cmap('viridis', len(metrics_to_plot))

    for i, metric in enumerate(metrics_to_plot):
        if metric in data:
            metric_dates = [pd.to_datetime(date) for date in data[metric].keys()]
            start_date, end_date = min(metric_dates), max(metric_dates)
            all_dates = pd.date_range(start=start_date, end=end_date)

            metric_series = pd.Series(0, index=all_dates)
            for date, count in data[metric].items():
                metric_series[pd.to_datetime(date)] = count

            plt.plot(metric_series.index, metric_series, label=metric, color=colormap(i), marker='o')

    plt.xlabel("Date")
    plt.ylabel("Number of Entries")
    plt.title("Metric Entries Over Time")
    plt.legend()
    plt.grid(True)
    plt.show()

def analyze_blood_sugars():

    file_paths = [
        '/Users/victorgoncalves/Desktop/t1d_data/data/cleaned_data/tidepool/cbg/cbg-2023-9.json',
        '/Users/victorgoncalves/Desktop/t1d_data/data/cleaned_data/tidepool/cbg/cbg-2023-10.json',
        '/Users/victorgoncalves/Desktop/t1d_data/data/cleaned_data/tidepool/cbg/cbg-2023-11.json',
        '/Users/victorgoncalves/Desktop/t1d_data/data/cleaned_data/tidepool/cbg/cbg-2023-12.json',
    ]

    combined_df = pd.DataFrame()
    for file_path in file_paths:
        with open(file_path, 'r') as file:
            data = json.load(file)
            temp_df = pd.DataFrame(data)
            combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

    combined_df['utc_Time'] = pd.to_datetime(combined_df['utc_Time'])
    combined_df['local_Time'] = pd.to_datetime(combined_df['local_Time'])
    combined_df['date'] = combined_df['local_Time'].dt.date
    combined_df['hour'] = combined_df['local_Time'].dt.hour
    combined_df['day_of_week'] = combined_df['local_Time'].dt.dayofweek
    combined_df['weekday_name'] = combined_df['local_Time'].dt.day_name()
    combined_df['month'] = combined_df['local_Time'].dt.month

    hourly_avg_by_month = combined_df.groupby(['month', 'hour'])['value'].mean().unstack(level=0)
    weekday_avg_by_month = combined_df.groupby(['month', 'day_of_week'])['value'].mean().unstack(level=0)
    weekday_avg_by_month.index = combined_df['weekday_name'].unique()

    fig, ax = plt.subplots(2, 1, figsize=(15, 10))

    # Plot hourly averages by month
    hourly_avg_by_month.plot(ax=ax[0], marker='o', linestyle='-')
    ax[0].set_title('Average Blood Sugar Levels by Hour of Day (Monthly)')
    ax[0].set_xlabel('Hour of Day')
    ax[0].set_ylabel('Average Level (mg/dL)')
    ax[0].set_xticks(range(24))  # Set x-ticks for every hour
    ax[0].legend(title='Month')

    # Plot weekday averages by month
    weekday_avg_by_month.plot(ax=ax[1], kind='bar')
    ax[1].set_title('Average Blood Sugar Levels by Day of the Week (Monthly)')
    ax[1].set_xlabel('Day of the Week')
    ax[1].set_ylabel('Average Level (mg/dL)')
    ax[1].legend(title='Month')

    plt.tight_layout()
    plt.show()

    # Initialize an empty DataFrame to store all data
    all_data = pd.DataFrame()

    # Process each file
    for file_path in file_paths:
        # Load JSON data into a DataFrame
        with open(file_path, 'r') as file:
            blood_sugar_data = pd.read_json(file)
        
        # Convert time fields to datetime objects
        blood_sugar_data['utc_Time'] = pd.to_datetime(blood_sugar_data['utc_Time'])
        blood_sugar_data['local_Time'] = pd.to_datetime(blood_sugar_data['local_Time'])

        # Add a date column for daily analysis
        blood_sugar_data['date'] = blood_sugar_data['local_Time'].dt.date

        # Append this DataFrame to the main DataFrame
        all_data = all_data._append(blood_sugar_data, ignore_index=True)

    # Calculate daily averages
    daily_avg = all_data.groupby('date')['value'].mean()

    # Define thresholds for high and low blood sugar levels
    low_threshold = 80
    high_threshold = 180

    # Count the number of high and low episodes per day
    daily_highs = all_data[all_data['value'] > high_threshold].groupby('date')['value'].count()
    daily_lows = all_data[all_data['value'] < low_threshold].groupby('date')['value'].count()

    # Plotting
    fig, ax = plt.subplots(2, 1, figsize=(12, 8))

    # Plot daily averages
    daily_avg.plot(ax=ax[0], color='blue', marker='o', linestyle='-')
    ax[0].set_title('Daily Average Blood Sugar Levels')
    ax[0].set_ylabel('Average Level (mg/dL)')
    ax[0].axhline(y=low_threshold, color='green', linestyle='--', label='Low Threshold')
    ax[0].axhline(y=high_threshold, color='red', linestyle='--', label='High Threshold')
    ax[0].legend()

    # Plot daily high and low episodes
    daily_highs.plot(ax=ax[1], kind='bar', color='red', position=0, label='High Episodes', width=0.4)
    daily_lows.plot(ax=ax[1], kind='bar', color='green', position=1, label='Low Episodes', width=0.4)
    ax[1].set_title('Daily High and Low Blood Sugar Episodes')
    ax[1].set_ylabel('Number of Episodes')
    ax[1].legend()

    plt.tight_layout()
    plt.show()


def calculate_total_entries(data_dict):
    total_entries = {}
    for key, value in data_dict.items():
        total_entries[key] = sum(value.values())
    return total_entries
            
if __name__ == '__main__':
    analyze_blood_sugars()
    results = generate_statistics()

    # Combine basal and bolus results to the dictionary
    print(results['insulin_sum_bolus'])
    results['insulin_sum'] = defaultdict(float)
    for key, value in results['insulin_sum_bolus'].items():
        if key in results['insulin_sum_basal']:
            results['insulin_sum'][key] = value + results['insulin_sum_basal'][key]
    print(results['insulin_sum'])
    print(results.keys())

    if plot_timeline:
        print("Total metrics", total_metrics)
        plot_timeline(metric_days)

    print(calculate_total_entries(metric_entry_count))

    if plot_entries:
        metric_entry_count["exercise"] = metric_entry_count["exercise-0"]
        del metric_entry_count["exercise-0"]

        plot_entries(metric_entry_count, ['bolus', 'basal'])
        plot_entries(metric_entry_count, ['cbg'])

        # Plot heart and respiratory metrics
        heart_respiratory_metrics = ['resting_heart_rate', 'Respiratory Rate Summary', 
                                    'Daily Respiratory Rate Summary', 'Heart Rate Variability Details', 
                                    'Heart Rate Variability Histogram', 'Daily Heart Rate Variability Summary']
        plot_entries(metric_entry_count, heart_respiratory_metrics)

        plot_entries(metric_entry_count, ['heart_rate'])
        plot_entries(metric_entry_count, ['exercise'])
        plot_entries(metric_entry_count, ['sleep'])
        plot_entries(metric_entry_count, ['steps', 'calories'])
        plot_entries(metric_entry_count, ['food'])


    if plot_metrics:
        # use extracted results to print
        # Extract matching date keys and their corresponding values
        compare_metrics('insulin_sum', 'cbg_avg', results)
        compare_metrics('insulin_sum', 'minutesAsleep', results)
        compare_metrics('insulin_sum', 'steps_sum', results)
        compare_metrics('insulin_sum', 'very_active_minutes', results)
        compare_metrics('cbg_avg', 'very_active_minutes', results)
        compare_metrics('cbg_avg', 'minutesAsleep', results)
        compare_metrics('cbg_avg', 'steps_sum', results)
        compare_metrics('cbg_avg', 'carbs_sum', results)
        compare_metrics('cbg_avg', 'calories_sum', results)
        compare_metrics('insulin_sum', 'carbs_sum', results)
        compare_metrics('insulin_sum', 'calories_sum', results)
    
    