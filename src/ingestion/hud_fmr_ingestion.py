# Databricks notebook source
# DBTITLE 1,Setup
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# DBTITLE 1,Imports
from src.utils.helpers import fetch_all
from src.ingestion import hud_common
import json
from typing import List, Dict, Any

# COMMAND ----------

# DBTITLE 1,Get API Token
token = hud_common.get_hud_token(dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Counties from Bronze Table

# COMMAND ----------

# DBTITLE 1,Load counties reference data
# Read counties from the reference bronze table
counties_df = spark.table("bronze_dev.hud.counties_reference")
print(f"Loaded {counties_df.count()} counties from bronze table")
display(counties_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build FMR URLs for All Counties

# COMMAND ----------

# DBTITLE 1,Build FMR URLs
# Extract county codes from the counties dataframe
county_codes = [row.fips_code for row in counties_df.collect()]

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
