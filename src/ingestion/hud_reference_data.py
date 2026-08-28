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
# MAGIC ## Fetch States

# COMMAND ----------

# DBTITLE 1,Fetch all states from HUD API
url = hud_common.build_list_states_url()
print(url)
states = hud_common.retrieve_hud_json(url, token)
states_df = spark.createDataFrame(states["data"])
display(states_df)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DataFrame for Bronze Table

# COMMAND ----------

# DBTITLE 1,Create counties DataFrame
# Flatten counties data into list of dictionaries
list_of_dicts = []

for batch_dict in counties:
    for data_point in batch_dict["data"]:
        list_of_dicts.append(data_point)

counties_df = spark.createDataFrame(list_of_dicts)
print(f"Total counties: {counties_df.count()}")
display(counties_df)

# COMMAND ----------

# DBTITLE 1,Write to bronze table
# Write to bronze table for reference by FMR pipeline
counties_df.write.mode("overwrite").saveAsTable("bronze_dev.hud.counties_reference")
print("Saved to bronze_dev.hud.counties_reference")

# COMMAND ----------


