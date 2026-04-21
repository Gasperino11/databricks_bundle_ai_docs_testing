# Databricks notebook source
from datetime import datetime
import pytz
import time

# Get current UTC time
utc_now = datetime.now(pytz.UTC)
epoch_time = time.time()

print("=" * 60)
print(f"Timezone Report - Generated at {utc_now.isoformat()}")
print("=" * 60)
print()

# US Timezones
us_timezones = [
    ("UTC", "UTC"),
    ("Eastern Time (ET)", "America/New_York"),
    ("Central Time (CT)", "America/Chicago"),
    ("Mountain Time (MT)", "America/Denver"),
    ("Pacific Time (PT)", "America/Los_Angeles"),
    ("Alaska Time (AKT)", "America/Anchorage"),
    ("Hawaii-Aleutian Time (HST)", "Pacific/Honolulu"),
]

# Print time in each timezone
for tz_name, tz_id in us_timezones:
    tz = pytz.timezone(tz_id)
    local_time = utc_now.astimezone(tz)
    print(f"{tz_name:30} | {local_time.strftime('%Y-%m-%d %H:%M:%S %Z (UTC%z)')}")

print()
print("=" * 60)
print(f"Epoch Time (Unix Timestamp): {epoch_time}")
print("=" * 60)
