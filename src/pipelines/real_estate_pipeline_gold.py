from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ============================================================================
# GOLD LAYER — Analytics-Ready Fact Tables
# ============================================================================
#
# PURPOSE:
# - Read from silver tables (batch reads → materialized views)
# - Join multiple silver sources at the same grain
# - Drop pipeline metadata columns (rescued_data, ingest_timestamp, etc.)
# - Add gold_refresh_timestamp for freshness tracking
# - Expose only business-relevant columns
#
# PATTERN:
# Gold fact tables are materialized views (batch reads from silver).
# This avoids the complexity of joining multiple streaming sources while
# providing a consistent snapshot for analytics and dashboards.
#
# See docs/GOLD_PLAN.md for the full gold layer design.

# Metadata columns to drop from silver tables (not needed in gold)
METADATA_COLS = ["rescued_data", "ingest_timestamp", "source_file", "_processed_timestamp"]

# Common columns shared across all Redfin silver tables at the same grain.
# These come from the base (housing_market) table; joined tables skip them
# to avoid column conflicts.
COMMON_COLS = ["last_updated", "frequency", "region_type", "metro"]

# Join keys for Redfin tables at the same grain
REDFIN_JOIN_KEYS = ["region_name", "period_begin", "period_end"]


def _drop_metadata(df: DataFrame) -> DataFrame:
    """Drop pipeline metadata columns that should not appear in gold."""
    for col_name in METADATA_COLS:
        if col_name in df.columns:
            df = df.drop(col_name)
    return df


def _select_unique_cols(df: DataFrame) -> DataFrame:
    """Select join keys + columns unique to this table (skip common + metadata).

    Used for the enrichment tables (price_drops, delistings_relistings) to
    avoid column conflicts when joining onto the base housing_market table.
    """
    drop_cols = set(METADATA_COLS + COMMON_COLS)
    keep_cols = [c for c in df.columns if c not in drop_cols]
    return df.select(*keep_cols)


def _create_redfin_gold_table(grain: str, gold_table_name: str, comment: str):
    """Create a gold fact table joining 3 Redfin silver tables at the given grain.

    Joins housing_market, price_drops, and delistings_relistings silver tables
    on region_name + period_begin + period_end.

    Args:
        grain: 'county' or 'neighborhood'
        gold_table_name: Full 3-part table name (catalog.schema.table)
        comment: Table comment
    """

    @dp.table(
        name=gold_table_name,
        comment=comment,
        table_properties={
            "quality": "gold",
            "delta.columnMapping.mode": "name",
            "pipelines.autoOptimize.zOrderCols": "region_name,period_begin",
        },
    )
    def load():
        # Base: housing market (has region_id + all market metrics)
        hm = _drop_metadata(spark.read.table(f"silver_dev.redfin.housing_market_{grain}"))

        # Price drops — keep only unique columns + join keys
        pd = _drop_metadata(spark.read.table(f"silver_dev.redfin.price_drops_{grain}"))
        pd = _select_unique_cols(pd)

        # Delistings/relistings — keep only unique columns + join keys
        dr = _drop_metadata(spark.read.table(f"silver_dev.redfin.delistings_relistings_{grain}"))
        dr = _select_unique_cols(dr)

        # Left joins — base table is housing_market; enrich with price drops and delistings
        df = (
            hm.join(pd, on=REDFIN_JOIN_KEYS, how="left")
              .join(dr, on=REDFIN_JOIN_KEYS, how="left")
        )

        # Add gold refresh timestamp
        df = df.withColumn("gold_refresh_timestamp", F.current_timestamp())

        return df

    return load


# ==========================================================
# COUNTY HOUSING PROFILE
# ==========================================================
# Grain: one row per county per time period.
#
# Joins:
#   1. silver_dev.redfin.housing_market_county  (base — sales, prices, inventory)
#   2. silver_dev.redfin.price_drops_county    (left join — price drop activity)
#   3. silver_dev.redfin.delistings_relistings_county (left join — delisting/relisting)
#
# Future enrichment (not yet included — see GOLD_PLAN.md §2):
#   - gold_dev.geo_re.dim_county  — state info via county name → FIPS join
#   - HUD FMR — fair market rents aggregated to county × year
#   - ACS — demographics aggregated from ZCTA to county

_create_redfin_gold_table(
    grain="county",
    gold_table_name="gold_dev.default.county_housing_profile",
    comment=(
        "Gold layer: County-level housing market profile joining Redfin "
        "housing market, price drops, and delistings/relistings. "
        "Grain: one row per county per time period."
    ),
)


# ==========================================================
# NEIGHBORHOOD MARKET ACTIVITY
# ==========================================================
# Grain: one row per neighborhood per time period.
#
# Joins:
#   1. silver_dev.redfin.housing_market_neighborhood  (base)
#   2. silver_dev.redfin.price_drops_neighborhood      (left join)
#   3. silver_dev.redfin.delistings_relistings_neighborhood (left join)
#
# No external enrichment data at neighborhood grain yet.

_create_redfin_gold_table(
    grain="neighborhood",
    gold_table_name="gold_dev.default.neighborhood_market_activity",
    comment=(
        "Gold layer: Neighborhood-level market activity joining Redfin "
        "housing market, price drops, and delistings/relistings. "
        "Grain: one row per neighborhood per time period."
    ),
)


# ==========================================================
# ZIP MARKET HEALTH
# ==========================================================
# Grain: one row per ZIP code per time period (time-series).
#
# Joins:
#   1. silver_dev.redfin.housing_market_zipcode  (base)
#   2. silver_dev.redfin.price_drops_zipcode      (left join)
#   3. silver_dev.redfin.delistings_relistings_zipcode (left join)
#
# Built as time-series to preserve trend analysis. For dashboards that need
# a single-row-per-ZIP snapshot, filter to the latest period:
#   WHERE period_begin = (SELECT MAX(period_begin) FROM gold_dev.default.zip_market_health)
#
# Future enrichment (not yet included — see GOLD_PLAN.md §3):
#   - silver_dev.opportunity_insights.social_capital_zip (static, join on zip_code)
#   - silver_dev.census_bureau.american_community_survey_zcta (static, ZCTA ≈ ZIP)
#   - silver_dev.hud.fair_market_rents_county (filter to latest year, join on zip_code)

_create_redfin_gold_table(
    grain="zipcode",
    gold_table_name="gold_dev.default.zip_market_health",
    comment=(
        "Gold layer: ZIP-level market health joining Redfin "
        "housing market, price drops, and delistings/relistings. "
        "Grain: one row per ZIP code per time period (time-series)."
    ),
)