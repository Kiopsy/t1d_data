# NOTES
## Example filenames:
example_filenames = [
    'Daily SpO2 - 2023-10-09-2023-11-07.csv',
    'Minute SpO2 - 2023-10-31.csv',
    'sleep_score.csv',
    'Computed Temperature - 2023-10-09.csv',
    'Daily Heart Rate Variability Summary - 2023-10-(15).csv',
    'estimated_oxygen_variation-2023-10-14.csv',
    'Activity Goals.csv',
    'time_in_heart_rate_zones-2023-10-26.json'
]

## Example dates
2023-11-02T00:14
2023-10-09T18:33:18.000Z
2023-11-07T01:25:30
2023-11-07T08:00:30
2023-11-04T07:04:33Z
2023-10-18T00:00:00Z
2023-11-05T08:06:00
2023-10-10T00:49:52.000Z
2023-11-04T02:55:00
11/01/23 00:00:00
2023-11-07T01:25:30.000
10/19/23 00:00:00

## TODO:
- november month of heartrate not getting pushed

## Optimizations
- Binary search
- How many times are we sorting (is it needed??)
- heartrate file bottleneck.
    - is indent messing it up? 
    - is it csv?
- maybe worth into looking at different file types for different metrics
- does the file size matter?
- ex: sleep has a complicated data structure

## Cleaning the code
- 

# Running Notebook
## 11/12/2023
Currently in the process of getting the fitbit code to wokr properly. Binary search to check
for duplicates seems much faster than the O(n^2) solution. However, this did not bring much
improvement when dealing with smaller filesizes. 

Also, might be interesting to keep in mind loading into memory issues. 

### File Sizes
(one month of heartrate data)
- JSON (w/ indent)
    - 46.1 mb
- JSON (no indent)
    - 29 mb
- CSV
    - 17.6 mb