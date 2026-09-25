import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType

from src.utils.sources_ref import REDFIN_TABLES, REDFIN_VOLUME_BASE, OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG, CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG


SCHEMA_LOCATION_BASE = "/Volumes/bronze_dev/real_estate/_schemas"


# ============================================================================
# BRONZE LAYER - Auto Loader Streaming Tables from Volumes
# ============================================================================

# TEMPORARY NOTES
# Where to add bronze processing (clean column names)? Or maybe it's no longer necessary.
# #todo Add LAST_UPDATED to redfin keys for bronze. However with silver use LAST_UPDATED as a tiebreaker
# Deduping happens in silver layer -- however bronze layer should be idempotent

# ==========================================================
# REDFIN
# ==========================================================
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


# ==========================================================
# OPPORTUNITY INSIGHTS
# ==========================================================

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


# ==========================================================
# HUD REFERENCE DATA (States & Counties)
# ==========================================================

@dlt.table(
    name="bronze_dev.hud.states",
    comment="Bronze layer: HUD state reference data",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "state_code"
    }
)
def bronze_hud_states():
    """Load HUD state reference data from volume."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/hud_states")
        .load("/Volumes/bronze_dev/hud/hud_raw/reference_data/states_*.json")
        .select(F.explode("data").alias("state_data"))
        .select(
            F.col("state_data.state_code").alias("state_code"),
            F.col("state_data.state_name").alias("state_name"),
            F.current_timestamp().alias("_ingest_timestamp")
        )
    )


@dlt.table(
    name="bronze_dev.hud.counties",
    comment="Bronze layer: HUD county reference data",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "fips_code,state_code"
    }
)
def bronze_hud_counties():
    """Load HUD county reference data from volume."""
    return (
        spark.readStream 
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/hud_counties")
        .load("/Volumes/bronze_dev/hud/hud_raw/reference_data/counties_*.json")
        .select(F.explode("data").alias("county"))
        .select(
            F.col("county.fips_code").alias("fips_code"),
            F.col("county.state_code").alias("state_code"),
            F.col("county.county_name").alias("county_name"),
            F.col("county.cntyname").alias("cntyname"),
            F.col("county.category").alias("category"),
            F.col("county.town_name").alias("town_name"),
            F.current_timestamp().alias("_ingest_timestamp")
        )
    )


@dlt.table(
    name="bronze_dev.hud.fair_market_rents_county",
    comment="Bronze layer: HUD Fair Market Rents by county and zip code",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "county_name,zip_code,year"
    }
)
def bronze_hud_fair_market_rents_county():
    """Load HUD Fair Market Rent data from volume."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/hud_fmr")
        .load("/Volumes/bronze_dev/hud/hud_raw/fair_market_rents_county/fmr_data_*.json")
        # Extract nested data structure
        .select(
            F.col("data.data").alias("fmr_data"),
            F.col("url").alias("source_url")
        )
        # Parse the basicdata JSON string to array
        .withColumn("basicdata_array", F.from_json(
            "fmr_data.basicdata",
            ArrayType(StructType([
                StructField("zip_code", StringType()),
                StructField("Efficiency", IntegerType()),
                StructField("One-Bedroom", IntegerType()),
                StructField("Two-Bedroom", IntegerType()),
                StructField("Three-Bedroom", IntegerType()),
                StructField("Four-Bedroom", IntegerType())
            ]))
        ))
        # Explode to get one row per zip code
        .withColumn("zip_data", F.explode("basicdata_array"))
        .select(
            F.col("fmr_data.county_name").alias("county_name"),
            F.col("fmr_data.metro_name").alias("metro_name"),
            F.col("fmr_data.area_name").alias("area_name"),
            F.col("fmr_data.metro_status").alias("metro_status"),
            F.col("fmr_data.smallarea_status").alias("smallarea_status"),
            F.col("fmr_data.year").alias("year"),
            F.col("zip_data.zip_code").alias("zip_code"),
            F.col("zip_data.Efficiency").alias("efficiency"),
            F.col("zip_data.One-Bedroom").alias("one_bedroom"),
            F.col("zip_data.Two-Bedroom").alias("two_bedroom"),
            F.col("zip_data.Three-Bedroom").alias("three_bedroom"),
            F.col("zip_data.Four-Bedroom").alias("four_bedroom"),
            F.col("source_url").alias("source_url"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.col("_metadata.file_path").alias("_source_file")
        )
    )


# ==========================================================
# CENSUS AMERICAN COMMUNITY SURVEY
# ==========================================================
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
        .option("cloudFiles.format", "text")
        .option("wholeText", "true")
        .option("cloudFiles.schemaLocation",  f"{SCHEMA_LOCATION_BASE}/{table_name}")
        .load(cb_acs_directory) # All files in directory
        # Parse the list-of-lists JSON: [[headers...], [row1...], [row2...], ...]
        .withColumn("parsed", F.from_json(
            "value",
            ArrayType(ArrayType(StringType()))
        ))
        .withColumn("row", F.explode("parsed"))
        # Skip the header row (first element of each file is column names)
        .filter(F.element_at("row", 1) != "NAME")
        .select(
            F.element_at("row", 1).alias("NAME"),
            F.element_at("row", 2).alias("B01003_001E"),
            F.element_at("row", 3).alias("B01002_001E"),
            F.element_at("row", 4).alias("B19013_001E"),
            F.element_at("row", 5).alias("B17001_002E"),
            F.element_at("row", 6).alias("B02001_002E"),
            F.element_at("row", 7).alias("B02001_003E"),
            F.element_at("row", 8).alias("B02001_005E"),
            F.element_at("row", 9).alias("B03003_003E"),
            F.element_at("row", 10).alias("zip"),
            F.current_timestamp().alias("_ingest_timestamp"),
            F.col("_metadata.file_path").alias("_source_file"),
        )
    )





