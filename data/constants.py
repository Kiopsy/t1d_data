# Folder names
EXPORT_FOLDER = "export_data" 
USED_FOLDER = "used_data"
CLEANED_FOLDER = "cleaned_data"
TIDEPOOL_FOLDER = "tidepool"
FITBIT_FOLDER = "fitbit"
BITESNAP_FOLDER = "bitesnap"

# 
local_time_col = "local_Time"
utc_time_col = "utc_Time"

TIMESTAMP_FORMATS =  [
    "%Y-%m-%dT%H:%M:%S.%fZ", 
    "%Y-%m-%dT%H:%M:%S.%f", 
    "%Y-%m-%dT%H:%M:%SZ", 
    "%Y-%m-%dT%H:%M:%S", 
    "%Y-%m-%dT%H:%M", 
    "%m/%d/%y %H:%M:%S", 
    "%m/%d/%y"
]

FITBIT_SKIPPED_METRICS = [
    'Daily Readiness User Properties',
    'menstrual_health_cycles',
    'menstrual_health_symptoms',
    'menstrual_health_birth_control',
    'menstrual_health_settings',
    'Glucose Target Ranges',
    'Account_Management_Events_1',
    'Account_Access_Events_1',
    'iOS App Notification Settings',
    'Devices',
    'Scales',
    'Tracker Optional Configuration',
    'Trackers',
    'mindfulness_goals',
    'mindfulness_sessions',
    'mindfulness_eda_data_sessions',
    'Activity Goals',
    'badge',
    'Heart Rate Notifications Profile',
    'Heart Rate Notifications Alerts',
    'User_Email_Audit_Entry',
    'User_Retired_Password',
    'Profile',
    'Stress Score'
]

