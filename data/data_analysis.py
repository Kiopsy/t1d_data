from helpers import *
import os
import json
import re
from collections import defaultdict
from tabulate import tabulate
import time
import matplotlib.pyplot as plt

"""
notes: 

- what is the difference between distance and steps?
- 
"""

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
    plt.scatter(x_values, y_values, marker='o', color='blue', label='Data Points')
    plt.xlabel(metric1)
    plt.ylabel(metric2)
    plt.title('Scatterplot of Data with Matching Dates')
    plt.grid(True)
    plt.legend()
    plt.show()
    
            
if __name__ == '__main__':
    results = generate_statistics()

    # Combine basal and bolus results to the dictionary
    print(results['insulin_sum_bolus'])
    results['insulin_sum'] = defaultdict(float)
    for key, value in results['insulin_sum_bolus'].items():
        if key in results['insulin_sum_basal']:
            results['insulin_sum'][key] = value + results['insulin_sum_basal'][key]
    print(results['insulin_sum'])
    print(results.keys())

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
    