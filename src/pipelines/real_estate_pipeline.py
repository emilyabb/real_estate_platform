import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.utils.sources_ref import REDFIN_TABLES, REDFIN_VOLUME_BASE, OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG, CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG


SCHEMA_LOCATION_BASE = "/Volumes/bronze_dev/real_estate/_schemas"


# ============================================================================
# BRONZE LAYER - Auto Loader Streaming Tables from Volumes
# ============================================================================

# TEMPORARY NOTES
# Where to add bronze processing (clean column names)? Or maybe it's no longer necessary.
# #todo Add LAST_UPDATED to redfin keys for bronze. However with silver use LAST_UPDATED as a tiebreaker
# Deduping happens in silver layer -- however bronze layer should be idempotent

# ==========================
# REDFIN
# ==========================
def create_bronze_redfin_table(table_name, bronze_key):
    
    @dlt.table(
        name=f"bronze_dev.redfin.{table_name}",
        # schema="redfin",
        comment=f"Bronze layer: Raw {table_name} data from Redfin ingested via Auto Loader",
        table_properties={
            "quality": "bronze",
            "pipelines.autoOptimize.zOrderCols": ",".join(bronze_key), # Optimize table and join by natural key
            "delta.columnMapping.mode": "name"
        }
    )
    def load_table():
        # Return a Spark DataFrame
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/{table_name}")
            .load(f"{REDFIN_VOLUME_BASE}/{table_name}/") # All files in directory for this table
            .withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

for redfin_table in REDFIN_TABLES:
    create_bronze_redfin_table(redfin_table["table_name"], redfin_table["bronze_key"])


# ==========================
# OPPORTUNITY INSIGHTS
# ==========================

opp_insights_sc_bronze_key = ",".join(OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG["bronze_key"])
opp_insights_sc_table_name = OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG["bronze_table_name"]
@dlt.table(
    name=f"bronze_dev.opportunity_insights.{opp_insights_sc_table_name}",
    # schema="opportunity_insights",
    comment="Bronze layer: Raw social capital by zipcode data from Opportunity Insights",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": opp_insights_sc_bronze_key
    }
)
def bronze_opportunity_insights_social_capital_zip():

    opp_insights_sc_directory = OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG["csv_directory"]
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation",  f"{SCHEMA_LOCATION_BASE}/{opp_insights_sc_table_name}")
        .load(opp_insights_sc_directory) # All files in directory
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ==========================
# CENSUS AMERICAN COMMUNITY SURVEY
# ==========================
cb_acs_bronze_key = ",".join(CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG["bronze_key"])
cb_acs_table_name = CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG["bronze_table_name"]
@dlt.table(
    name=f"bronze_dev.census_bureau.{cb_acs_table_name}",
    # schema="census_bureau",
    comment="Bronze layer: Raw American Community Survey ZCTA-level data from Census Bureau",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": cb_acs_bronze_key
    }
)
def bronze_census_bureau_american_community_survey():

    cb_acs_directory = CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG["csv_directory"]
    table_name = CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG["bronze_table_name"]
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation",  f"{SCHEMA_LOCATION_BASE}/{table_name}")
        .load(cb_acs_directory) # All files in directory
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )





