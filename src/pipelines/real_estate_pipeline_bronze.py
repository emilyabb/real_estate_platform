import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType

from src.utils.sources_ref import REDFIN_TABLES, REDFIN_VOLUME_BASE, OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG, CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG, USPS_ZIP_CODES_CONFIG


SCHEMA_LOCATION_BASE = "/Volumes/bronze_dev/real_estate/_schemas"


# ============================================================================
# BRONZE LAYER - Auto Loader Streaming Tables from Volumes
# ============================================================================

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
# HUD REFERENCE DATA (States & Counties) - Batch reads (static reference data)
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
    """Load HUD state reference data from volume (batch read — static reference data)."""
    return (
        spark.read
        .format("json")
        .option("multiLine", "true")
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
    """Load HUD county reference data from volume (batch read — static reference data)."""
    return (
        spark.read
        .format("json")
        .option("multiLine", "true")
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
    name="bronze_dev.hud.fair_market_rents",
    comment="Bronze layer: HUD Fair Market Rents (county, MSA, and ZIP levels)",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "county_name,zip_code,year"
    }
)
def bronze_hud_fair_market_rents():
    """Load HUD Fair Market Rent data from volume."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/hud_fmr")
        .load("/Volumes/bronze_dev/hud/hud_raw/fair_market_rents/fmr_data_*.json")
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
# Raw files for all three ACS grains land in the same directory; grains are
# distinguishable by filename pattern. Unlike HUD FMR (one geography-neutral
# bronze table), each ACS grain gets its own bronze table because the raw
# JSON column layout differs per grain (the geography columns differ).

# Filename patterns per grain (matched against the full source file path).
CB_ACS_GRAIN_FILE_PATTERNS = {
    "county": r"american_community_survey_county_[0-9]{8}_[0-9]{6}\.json$",
    "state": r"american_community_survey_state_[0-9]{8}_[0-9]{6}\.json$",
    # ZCTA also includes legacy ZCTA-content files: american_community_survey_{ts}.json
    # (no grain segment) and misnamed social_capital_zip_{ts}.json -- keep ingesting
    # them or a future full refresh loses their data.
    "zcta": r"(american_community_survey_(zcta_)?[0-9]{8}_[0-9]{6}|social_capital_zip_[0-9]{8}_[0-9]{6})\.json$",
}

# Geography columns at the end of the raw rows, after the 9 shared ACS
# variables (raw JSON is a list-of-lists, first row the header):
# NAME, B01003_001E, B01002_001E, B19013_001E, B17001_002E,
# B02001_002E, B02001_003E, B02001_005E, B03003_003E, then grain geography.
CB_ACS_GRAIN_GEOGRAPHY_COLUMNS = {
    "zcta": [F.element_at("row", 10).alias("zip")],
    "county": [
        F.element_at("row", 10).alias("state"),
        F.element_at("row", 11).alias("county"),
    ],
    "state": [F.element_at("row", 10).alias("state")],
}


def create_bronze_census_acs_table(grain):
    """Create a bronze layer streaming table for one ACS geographic grain.

    Reads only this grain's files from the shared raw directory (filtered by
    filename pattern), then parses the list-of-lists JSON into one row per
    record.
    """
    grain_config = CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG["grains"][grain]
    table_name = grain_config["bronze_table_name"]
    bronze_key = ",".join(grain_config["bronze_key"])

    @dlt.table(
        name=f"bronze_dev.census_bureau.{table_name}",
        # schema="census_bureau",
        comment=f"Bronze layer: Raw American Community Survey {grain}-level data from Census Bureau",
        table_properties={
            "quality": "bronze",
            "pipelines.autoOptimize.zOrderCols": bronze_key
        }
    )
    def bronze_census_bureau_acs():

        cb_acs_directory = CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG["csv_directory"]
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholeText", "true")
            .option("cloudFiles.schemaLocation",  f"{SCHEMA_LOCATION_BASE}/{table_name}")
            .load(cb_acs_directory) # All files in directory; filtered to this grain below
            .withColumn("_source_file", F.col("_metadata.file_path"))
            # Keep only this grain's files -- the directory is shared by all three
            # grains, whose raw column layouts differ.
            .filter(F.col("_source_file").rlike(CB_ACS_GRAIN_FILE_PATTERNS[grain]))
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
                # Grain-specific geography columns (zip / state / state+county)
                *CB_ACS_GRAIN_GEOGRAPHY_COLUMNS[grain],
                F.current_timestamp().alias("_ingest_timestamp"),
                F.col("_metadata.file_path").alias("_source_file"),
            )
        )

    return bronze_census_bureau_acs


# Create bronze tables for all three ACS grains
for cb_acs_grain in CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG["grains"]:
    create_bronze_census_acs_table(cb_acs_grain)


# ==========================================================
# USPS ZIP CODES (Reference data) - Batch read (static reference data)
# ==========================================================

usps_zip_bronze_key = ",".join(USPS_ZIP_CODES_CONFIG["bronze_key"])
usps_zip_table_name = USPS_ZIP_CODES_CONFIG["bronze_table_name"]
@dlt.table(
    name=f"bronze_dev.geo_re.{usps_zip_table_name}",
    comment="Bronze layer: ZIP code reference data from zip_code_database.csv",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": usps_zip_bronze_key
    }
)
def bronze_geo_re_usps_zip_codes():
    """Load ZIP code reference data from volume (batch read — static reference data)."""
    directory = USPS_ZIP_CODES_CONFIG["csv_directory"]
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{directory}/zip_code_database_*.csv")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
