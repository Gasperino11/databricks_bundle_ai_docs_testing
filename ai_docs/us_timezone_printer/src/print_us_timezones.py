# Databricks notebook source

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

us_timezones = {
    "Eastern":  "US/Eastern",
    "Central":  "US/Central",
    "Mountain": "US/Mountain",
    "Pacific":  "US/Pacific",
    "Alaska":   "US/Alaska",
    "Hawaii":   "US/Hawaii",
}

print("Current time in US timezones:")
print("-" * 40)
for label, tz in us_timezones.items():
    now = datetime.now(ZoneInfo(tz))
    print(f"{label:10s}  {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")
