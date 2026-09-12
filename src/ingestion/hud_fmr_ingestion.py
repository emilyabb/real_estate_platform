# Databricks notebook source
# DBTITLE 1,Setup
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # HUD Fair Market Rent (FMR) Ingestion
# MAGIC
# MAGIC **Purpose:** Recurring ingestion of Fair Market Rent data for all counties
# MAGIC
# MAGIC **Run Frequency:** Daily, weekly, or monthly (based on HUD data update schedule)
# MAGIC
# MAGIC **Input Files:**
# MAGIC * `/Volumes/bronze_dev/hud/hud_raw/reference_data/counties_reference_*.json` (reads latest file for county list)
# MAGIC
# MAGIC **Output Files:**
# MAGIC * `/Volumes/bronze_dev/hud/hud_raw/fair_market_rents_county/fmr_data_YYYYMMDD_HHMMSS.json`
# MAGIC
# MAGIC **Note:** This directory is monitored by DLT for streaming ingestion into bronze/silver layers

# COMMAND ----------

# DBTITLE 1,Imports
from src.utils.helpers import fetch_all
from src.ingestion import hud_common
import json
import glob
import os
from typing import List, Dict, Any

# COMMAND ----------

# DBTITLE 1,Get API Token
token = hud_common.get_hud_token(dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Counties from Reference JSON

# COMMAND ----------

# DBTITLE 1,Load counties reference data
# Find the latest counties reference file
reference_path = "/Volumes/bronze_dev/hud/hud_raw/reference_data"
files = glob.glob(f"{reference_path}/counties_reference_*.json")

if not files:
    raise FileNotFoundError(f"No counties reference files found in {reference_path}")

latest_file = max(files, key=os.path.getmtime)
print(f"Reading from: {latest_file}")

# Load and parse the JSON
with open(latest_file, 'r') as f:
    counties_data = json.load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build FMR URLs for All Counties

# COMMAND ----------

# DBTITLE 1,Build FMR URLs
# Extract county FIPS codes from the nested JSON structure
county_codes = []

for batch in counties_data:
    if 'data' in batch:
        for county in batch['data']:
            if 'fips_code' in county:
                county_codes.append(county['fips_code'])

print(f"Extracted {len(county_codes)} county FIPS codes")

# Build URLs for individual county FMR data
county_detail_urls = [hud_common.build_fmr_data_url(code) for code in county_codes]
print(f"Total counties to fetch: {len(county_detail_urls)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Fair Market Rent Data

# COMMAND ----------

# DBTITLE 1,Fetch FMR data from HUD API
# Retrieve detailed FMR data for each county with rate limiting
county_details = fetch_all(
    urls=county_detail_urls,
    token=token,
    request_fn=hud_common.retrieve_hud_json
)

print(f"Total FMR records fetched: {len(county_details)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Raw JSON to Streaming Directory

# COMMAND ----------

# DBTITLE 1,Save to streaming volume
# Save FMR fact data to streaming directory for DLT
filename = hud_common.generate_timestamped_filename("fmr_data")
volume_path = hud_common.save_to_volume(
    county_details, 
    filename, 
    volume_path="/Volumes/bronze_dev/hud/hud_raw/fair_market_rents_county"
)
print(f"Saved FMR data to: {volume_path}")
