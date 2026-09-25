from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, DateType
from pyspark.sql import DataFrame

from src.utils.sources_ref import REDFIN_TABLES
from src.utils.helpers import clean_columns_silver

# ============================================================================
# SILVER LAYER - Cleaned, Deduplicated, and Standardized Tables
# ============================================================================

# PURPOSE:
# - Read from bronze streaming tables
# - Apply deduplication logic
# - Enforce data quality with expectations
# - Standardize column names and types
# - Add business logic and derived fields
# - Enrich with reference data (counties, states)

# PATTERN:
# Silver tables should be Streaming Tables when reading from bronze streaming sources.
# Use batch reads (spark.read.table) only for reference/dimension lookups.


# ==========================================================
# REDFIN SILVER TABLES
# ==========================================================

# Silver transformations for Redfin tables:
# - Standardize column names (spaces to underscores)
# - Cast numeric columns from strings to proper types
# - Replace "NA" strings with null
# - Deduplicate using natural_key with LAST_UPDATE as tiebreaker
# - Apply data quality expectations

def cast_redfin_columns(df: DataFrame) -> DataFrame:
    """Cast Redfin columns to appropriate data types.
    
    Redfin-specific casting rules:
    - Date columns: last_updated, period_begin, period_end → DateType
    - Percentage columns (_perc, _ppts): divide by 100 → DoubleType
    - String columns: region_id, region_name, region_type, metro, frequency → unchanged
    - Metadata columns: _ingest_timestamp, _rescued_data, _source_file → unchanged
    - All other columns: → DoubleType
    
    Args:
        df: DataFrame with cleaned column names (lowercase, snake_case)
        
    Returns:
        DataFrame with properly typed columns
    """
    # Define columns that should remain as strings
    keep_as_string = ["region_id", "region_name", "region_type", "metro", "frequency"]
    # Metadata columns to keep untouched
    metadata_cols = ["_ingest_timestamp", "_rescued_data", "_source_file"]
    
    # Cast date columns to DateType
    date_cols = ["last_updated", "period_begin", "period_end"]
    for col_name in date_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DateType()))
    
    # Cast percentage columns: divide by 100 and cast to double
    # Note: After bronze cleaning, "(%)" becomes "_PERC" (then lowercased to "_perc")
    # and "(ppts)" becomes "_ppts"
    pct_cols = [c for c in df.columns 
                if (c.endswith("_perc") or "_ppts" in c) 
                and c not in metadata_cols]
    for col_name in pct_cols:
        df = df.withColumn(col_name, (F.col(col_name).cast(DoubleType()) / 100.0))
    
    # Cast all other non-metadata, non-string-keep columns to DoubleType
    # (Columns that should be numeric but aren't dates or percentages)
    for col_name in df.columns:
        if (col_name.lower() not in keep_as_string 
            and col_name not in metadata_cols 
            and col_name not in date_cols
            and col_name not in pct_cols):
            # Only cast if it's currently a string type (from bronze)
            field_type = [f.dataType for f in df.schema.fields if f.name == col_name][0]
            if str(field_type) == "StringType()":
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
    
    return df

def create_silver_redfin_table(t):
    """Create silver layer table for a given Redfin bronze table.
    
    Args:
        t: Dictionary with 'table_name' and 'natural_key' fields
    """
    table_name = t["table_name"]
    natural_key = t["natural_key"]
    

    #TODO -- determine natural/primary key for each individual redfin table
    @dp.table(
        name=f"silver_dev.redfin.{table_name}",
        replace_using=natural_key,
        sequence_by="last_updated",
        comment=f"Silver layer: Cleaned and deduplicated {table_name} data from Redfin",
        table_properties={
            "quality": "silver",
            "delta.columnMapping.mode": "name",
            "pipelines.autoOptimize.zOrderCols": ",".join([col.replace(" ", "_").lower() for col in natural_key])
        }
    )
    @dp.expect_or_drop("valid_period", "period_end >= period_begin")
    @dp.expect_or_drop("has_last_updated", "last_updated IS NOT NULL")
    def load():

        df = spark.readStream.table(f"bronze_dev.redfin.{table_name}")        

        # Clean column names first (lowercase, snake_case)
        df = clean_columns_silver(df)
        
        # Replace "NA" strings with null across all columns
        string_cols = [field.name for field in df.schema.fields if str(field.dataType) == "StringType()"]
        for col_name in string_cols:
            df = df.withColumn(col_name, F.when(F.col(col_name) == "NA", None).otherwise(F.col(col_name)))
        
        # Cast columns to appropriate types (Redfin-specific logic)
        df = cast_redfin_columns(df)
        
        # Deduplicate: Use natural_key + LAST_UPDATED
        # Keep the most recent record per natural key
        natural_key_cols = [col.replace(" ", "_").lower() for col in natural_key]
        # window_spec = Window.partitionBy(*natural_key_cols).orderBy(F.col("last_updated").desc())
        # df = df.withColumn("_row_num", F.row_number().over(window_spec))
        # df = df.filter(F.col("_row_num") == 1).drop("_row_num")

        df = df.withColumn("primary_key", F.concat(*natural_key_cols))
        # df = df.dropDuplicates(F.col("primary_key"), F.col("last_updated"))  


        # Add processing timestamp
        df = df.withColumn("_processed_timestamp", F.current_timestamp())
        
        return df
    
    return load

# Create silver tables for all Redfin tables
for t in REDFIN_TABLES:
    create_silver_redfin_table(t)


# ==========================================================
# OPPORTUNITY INSIGHTS SILVER
# ==========================================================

@dp.table(
    name="silver_dev.opportunity_insights.social_capital_zip",
    replace_using=["zip_code"],
    sequence_by="_ingest_timestamp",
    comment="Silver layer: Cleaned and standardized social capital by zipcode data from Opportunity Insights",
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name",
        "pipelines.autoOptimize.zOrderCols": "zip_code"
    }
)
def social_capital_zip():
    """Silver transformation for Opportunity Insights social capital data.
    
    Transformations:
    - Rename columns to descriptive names
    - Cast zip and county to string (FIPS codes should be strings to preserve leading zeros)
    - Keep numeric columns as-is (doubles and ints)
    - Deduplicate by zip_code
    - Add processing timestamp
    """
    df = spark.readStream.table("bronze_dev.opportunity_insights.social_capital_zip")
    
    # Rename and cast columns in the order specified
    df = df.select(
        F.col("zip").cast("string").alias("zip_code"),
        F.col("county").cast("string").alias("county_fips_code"),
        F.col("num_below_p50").alias("children_below_median_parental_income_count"),
        F.col("pop2018").alias("population_2018"),
        F.col("ec_zip").alias("economic_connectedness_low_ses"),
        F.col("ec_se_zip").alias("economic_connectedness_low_ses_standard_error"),
        F.col("nbhd_ec_zip").alias("neighborhood_economic_connectedness_low_ses"),
        F.col("ec_grp_mem_zip").alias("group_economic_connectedness_low_ses"),
        F.col("ec_high_zip").alias("economic_connectedness_high_ses"),
        F.col("ec_high_se_zip").alias("economic_connectedness_high_ses_standard_error"),
        F.col("nbhd_ec_high_zip").alias("neighborhood_economic_connectedness_high_ses"),
        F.col("ec_grp_mem_high_zip").alias("group_economic_connectedness_high_ses"),
        F.col("exposure_grp_mem_zip").alias("group_exposure_to_high_ses_low_ses"),
        F.col("exposure_grp_mem_high_zip").alias("group_exposure_to_high_ses_high_ses"),
        F.col("nbhd_exposure_zip").alias("neighborhood_exposure_to_high_ses"),
        F.col("bias_grp_mem_zip").alias("group_friending_bias_low_ses"),
        F.col("bias_grp_mem_high_zip").alias("group_friending_bias_high_ses"),
        F.col("nbhd_bias_zip").alias("neighborhood_friending_bias_low_ses"),
        F.col("nbhd_bias_high_zip").alias("neighborhood_friending_bias_high_ses"),
        F.col("clustering_zip").alias("friend_network_clustering"),
        F.col("support_ratio_zip").alias("within_zcta_friendship_support_ratio"),
        F.col("volunteering_rate_zip").alias("volunteering_group_membership_rate"),
        F.col("civic_organizations_zip").alias("civic_organizations_per_1000_users"),
        # Metadata columns
        F.col("_rescued_data"),
        F.col("_ingest_timestamp"),
        F.col("_source_file")
    )
    
    # Deduplicate by zip_code, keeping the most recent record by _ingest_timestamp
    # Use _source_file as a stable tie-breaker for deterministic ordering
    # window_spec = Window.partitionBy("zip_code").orderBy(F.col("_ingest_timestamp").desc(), F.col("_source_file"))
    # df = df.withColumn("_row_num", F.row_number().over(window_spec))
    # df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    df = df.dropDuplicates(F.col("zip_code"), F.col("_ingest_timestamp"))
    # Add processing timestamp
    df = df.withColumn("_processed_timestamp", F.current_timestamp())
    
    return df


# ==========================================================
# CENSUS BUREAU SILVER
# ==========================================================

@dp.table(
    name="silver_dev.census_bureau.american_community_survey_zcta",
    comment="Silver layer: Cleaned and standardized American Community Survey ZCTA-level data from Census Bureau",
    table_properties={
        "quality": "silver",
        "delta.columnMapping.mode": "name",
        "pipelines.autoOptimize.zOrderCols": "zcta_code"
    }
)
def american_community_survey_zcta():
    """Silver transformation for Census Bureau ACS ZCTA data.
    
    Transformations:
    - Rename Census variable codes to descriptive names
    - Cast zip and NAME to string (ZCTA codes should be strings to preserve leading zeros)
    - Cast demographic variables to int or double as specified
    - Deduplicate by zcta_code
    - Add processing timestamp
    """
    df = spark.readStream.table("bronze_dev.census_bureau.american_community_survey_zcta")
    
    # Rename and cast columns in the order specified
    df = df.select(
        F.col("NAME").cast("string").alias("zcta_name"),
        F.col("zip").cast("string").alias("zcta_code"),
        F.col("B01003_001E").cast("int").alias("total_population"),
        F.col("B01002_001E").cast("double").alias("median_age"),
        F.col("B19013_001E").cast("int").alias("median_household_income"),
        F.col("B17001_002E").cast("int").alias("population_below_poverty_level"),
        F.col("B02001_002E").cast("int").alias("white_alone_population"),
        F.col("B02001_003E").cast("int").alias("black_or_african_american_alone_population"),
        F.col("B02001_005E").cast("int").alias("asian_alone_population"),
        F.col("B03003_003E").cast("int").alias("hispanic_or_latino_population"),
        # Metadata columns
        F.col("_ingest_timestamp"),
        F.col("_source_file")
    )
    
    # Deduplicate by zcta_code, keeping the most recent record by _ingest_timestamp
    # Use _source_file as a stable tie-breaker for deterministic ordering
    # window_spec = Window.partitionBy("zcta_code").orderBy(F.col("_ingest_timestamp").desc(), F.col("_source_file"))
    # df = df.withColumn("_row_num", F.row_number().over(window_spec))
    # df = df.filter(F.col("_row_num") == 1).drop("_row_num")
    
    # Add processing timestamp
    df = df.withColumn("_processed_timestamp", F.current_timestamp())
    
    return df


# ==========================================================
# HUD REFERENCE - SILVER (if needed)
# ==========================================================

# NOTE: States and counties may not need silver versions
# They're already clean reference data from the API
# Consider silver only if you need:
# - Additional enrichment
# - Derived fields
# - Quality checks beyond bronze
