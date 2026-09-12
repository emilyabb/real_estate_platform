# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # HUD Reference Data Ingestion
# MAGIC
# MAGIC **Purpose:** One-time/infrequent ingestion of reference data (states and counties)
# MAGIC
# MAGIC **Run Frequency:** Quarterly or annually (reference data changes rarely)
# MAGIC
# MAGIC **Output Files:**
# MAGIC * `/Volumes/bronze_dev/hud/hud_raw/reference_data/states_reference_YYYYMMDD_HHMMSS.json`
# MAGIC * `/Volumes/bronze_dev/hud/hud_raw/reference_data/counties_reference_YYYYMMDD_HHMMSS.json`
# MAGIC
# MAGIC **Note:** These raw JSON files are used by FMR ingestion and will be processed by the DLT pipeline

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
# MAGIC ## Fetch States

# COMMAND ----------

# DBTITLE 1,Fetch all states from HUD API
url = hud_common.build_list_states_url()
print(url)
states = hud_common.retrieve_hud_json(url, token)
states_df = spark.createDataFrame(states["data"])
display(states_df)

# COMMAND ----------

# DBTITLE 1,Save states to volume
# Save states reference data to reference_data directory
filename = hud_common.generate_timestamped_filename("states_reference")
volume_path = hud_common.save_to_volume(
    states, 
    filename, 
    volume_path="/Volumes/bronze_dev/hud/hud_raw/reference_data"
)
print(f"Saved states reference data to: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Counties for All States

# COMMAND ----------

# DBTITLE 1,Build state URLs
# Build URLs for each state's counties
list_of_state_urls = []

for row in states_df.collect():
    state_code = row["state_code"]
    url = hud_common.build_list_counties_url(state_code)
    list_of_state_urls.append(url)

print(f"Total states to fetch: {len(list_of_state_urls)}")

# COMMAND ----------

# DBTITLE 1,Fetch all counties from HUD API
# Retrieve data with rate limiting
counties = fetch_all(
    urls=list_of_state_urls,
    token=token,
    request_fn=hud_common.retrieve_hud_json
)

print(f"Total batches fetched: {len(counties)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Raw JSON to Volume
# MAGIC
# MAGIC Save both states and counties as timestamped JSON files for pipeline ingestion.

# COMMAND ----------

# DBTITLE 1,Save counties to volume
# Save reference data (counties) to reference_data directory
filename = hud_common.generate_timestamped_filename("counties_reference")
volume_path = hud_common.save_to_volume(
    counties, 
    filename, 
    volume_path="/Volumes/bronze_dev/hud/hud_raw/reference_data"
)
print(f"Saved reference data to: {volume_path}")
