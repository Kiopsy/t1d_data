import os

export_folder = "export_data" 
unused_folder = "unused_data"
cleaned_folder = "cleaned_data"
tidepool_folder = "tidepool"
fitbit_folder = "fitbit"
bytesnap_folder = "bytesnap"

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