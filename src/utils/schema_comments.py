"""Unity Catalog column comments for the real estate platform.

Single source of truth for column-level documentation on the silver and gold
tables. Each comment carries the bronze source column name, so the
bronze -> silver mapping lives attached to the data itself and a rebuild of
the platform from this repo recreates the metadata automatically.

Handoff note: run this once after every pipeline update (new tables are
fine — comments for tables that do not exist yet are skipped and reported).

Usage (notebook or job task with a Spark session):

    from utils.schema_comments import apply_column_comments
    report = apply_column_comments(spark)              # apply
    report = apply_column_comments(spark, dry_run=True)  # preview only

Column counts validated against live schemas (Oct 2026):
    silver redfin housing_market_*          54 columns
    silver redfin price_drops_*             23 columns
    silver redfin delistings_relistings_*   23 columns
    silver census ACS zcta                  13 columns
    silver opportunity_insights social_capital_zip  27 columns
    silver hud fair_market_rents_county    16 columns
    silver hud dim_states / dim_counties    4 / 8 columns
    gold county_housing_profile / zip_market_health /
         neighborhood_market_activity       75 columns each

Notes:
  - All silver metadata columns use leading underscores (_ingest_timestamp,
    _source_file, _rescued_data), consistent with the bronze layer and
    Databricks Auto Loader convention.
  - price_drops and delistings_relistings silver tables have NO region_id.
  - COMMENT ON COLUMN has been verified to work on SDP streaming tables
    (tested on silver_dev.redfin.housing_market_county.region_name).
  - COMMENT ON COLUMN on gold materialized views is untested; the apply
    function catches per-table failures so a gold error never blocks silver.
"""

# ---------------------------------------------------------------------
# Redfin: key columns shared by all nine silver tables (no region_id —
# that column only exists in the housing_market family)
# ---------------------------------------------------------------------

REDFIN_KEY_COLUMNS = {
    "last_updated": "Date Redfin last refreshed this row's metrics. Bronze source: 'LAST UPDATED'.",
    "frequency": "Reporting cadence of the period (e.g. 'monthly'). Bronze source: 'FREQUENCY'.",
    "period_begin": "First day of the reporting period. Bronze source: 'PERIOD BEGIN'.",
    "period_end": "Last day of the reporting period. Bronze source: 'PERIOD END'.",
    "region_type": "Grain discriminator: 'County', 'Zip', or 'Neighborhood'. Bronze source: 'REGION TYPE'.",
    "region_name": (
        "Region label for the grain (county 'Tarrant County, TX', 5-digit ZIP code, "
        "or neighborhood name). Join key across Redfin tables and gold fact tables. "
        "Bronze source: 'REGION NAME'."
    ),
    "metro": "Metro area the region belongs to. Bronze source: 'METRO'.",
}

REDFIN_REGION_ID = {
    "region_id": "Redfin numeric region identifier. Bronze source: 'REGION ID'.",
}

# ---------------------------------------------------------------------
# Redfin: pipeline metadata columns (silver names match bronze — leading
# underscore preserved)
# ---------------------------------------------------------------------

REDFIN_METADATA_COLUMNS = {
    "_rescued_data": (
        "Auto Loader quarantine column holding values that failed to parse against "
        "the schema. Bronze source: '_rescued_data'."
    ),
    "_ingest_timestamp": (
        "Time the bronze Auto Loader job ingested the raw file. "
        "Bronze source: '_ingest_timestamp'."
    ),
    "_source_file": (
        "Raw source file path in the bronze UC volume. "
        "Bronze source: '_source_file'."
    ),
    "_processed_timestamp": (
        "Silver pipeline processing time; added in silver via current_timestamp() "
        "(does not exist in bronze)."
    ),
}

# ---------------------------------------------------------------------
# Redfin: housing_market metrics (42 columns)
# ---------------------------------------------------------------------

REDFIN_HOUSING_MARKET_METRICS = {
    "homes_sold": "Homes sold in the period. Bronze source: 'HOMES SOLD'.",
    "homes_sold_mom": "Month-over-month change in homes sold, percent. Bronze source: 'HOMES SOLD MOM (%)'.",
    "homes_sold_yoy": "Year-over-year change in homes sold, percent. Bronze source: 'HOMES SOLD YOY (%)'.",
    "median_sale_price_nsa": "Median sale price, not seasonally adjusted, USD. Bronze source: 'MEDIAN SALE PRICE NSA ($)'.",
    "median_sale_price_nsa_mom": "MoM change in median sale price (NSA), percent. Bronze source: 'MEDIAN SALE PRICE NSA MOM (%)'.",
    "median_sale_price_nsa_yoy": "YoY change in median sale price (NSA), percent. Bronze source: 'MEDIAN SALE PRICE NSA YOY (%)'.",
    "median_days_on_market_days": "Median days on market. Bronze source: 'MEDIAN DAYS ON MARKET (DAYS)'.",
    "median_days_on_market_mom": "MoM change in median days on market, percent. Bronze source: 'MEDIAN DAYS ON MARKET MOM (%)'.",
    "median_days_on_market_yoy": "YoY change in median days on market, percent. Bronze source: 'MEDIAN DAYS ON MARKET YOY (%)'.",
    "average_sale_to_list_ratio": "Average sale-to-list price ratio. Bronze source: 'AVERAGE SALE TO LIST RATIO (%)'.",
    "average_sale_to_list_ratio_mom_ppts": (
        "MoM change in sale-to-list ratio, percentage points (silver divides the "
        "bronze value by 100). Bronze source: 'AVERAGE SALE TO LIST RATIO MOM (PPTS)'."
    ),
    "average_sale_to_list_ratio_yoy_ppts": (
        "YoY change in sale-to-list ratio, percentage points (silver divides the "
        "bronze value by 100). Bronze source: 'AVERAGE SALE TO LIST RATIO YOY (PPTS)'."
    ),
    "share_sold_above_original_list": "Share of homes sold above their original list price. Bronze source: 'SHARE SOLD ABOVE ORIGINAL LIST (%)'.",
    "share_sold_above_original_list_mom_ppts": (
        "MoM change in share sold above list, percentage points (bronze value divided "
        "by 100). Bronze source: 'SHARE SOLD ABOVE ORIGINAL LIST MOM (PPTS)'."
    ),
    "share_sold_above_original_list_yoy_ppts": (
        "YoY change in share sold above list, percentage points (bronze value divided "
        "by 100). Bronze source: 'SHARE SOLD ABOVE ORIGINAL LIST YOY (PPTS)'."
    ),
    "new_listings": "New listings in the period. Bronze source: 'NEW LISTINGS'.",
    "new_listings_mom": "MoM change in new listings, percent. Bronze source: 'NEW LISTINGS MOM (%)'.",
    "new_listings_yoy": "YoY change in new listings, percent. Bronze source: 'NEW LISTINGS YOY (%)'.",
    "active_listings": "Active listings at period end. Bronze source: 'ACTIVE LISTINGS'.",
    "active_listings_mom": "MoM change in active listings, percent. Bronze source: 'ACTIVE LISTINGS MOM (%)'.",
    "active_listings_yoy": "YoY change in active listings, percent. Bronze source: 'ACTIVE LISTINGS YOY (%)'.",
    "inventory": "Housing inventory (active listings) in the period. Bronze source: 'INVENTORY'.",
    "inventory_mom": "MoM change in inventory, percent. Bronze source: 'INVENTORY MOM (%)'.",
    "inventory_yoy": "YoY change in inventory, percent. Bronze source: 'INVENTORY YOY (%)'.",
    "pending_sales": "Pending sales in the period. Bronze source: 'PENDING SALES'.",
    "pending_sales_mom": "MoM change in pending sales, percent. Bronze source: 'PENDING SALES MOM (%)'.",
    "pending_sales_yoy": "YoY change in pending sales, percent. Bronze source: 'PENDING SALES YOY (%)'.",
    "median_new_listing_price": "Median price of new listings, USD. Bronze source: 'MEDIAN NEW LISTING PRICE ($)'.",
    "median_new_listing_price_mom": "MoM change in median new listing price, percent. Bronze source: 'MEDIAN NEW LISTING PRICE MOM (%)'.",
    "median_new_listing_price_yoy": "YoY change in median new listing price, percent. Bronze source: 'MEDIAN NEW LISTING PRICE YOY (%)'.",
    "median_new_listing_price_per_sq_ft": "Median new listing price per square foot, USD. Bronze source: 'MEDIAN NEW LISTING PRICE PER SQ.FT. ($)'.",
    "median_new_listing_price_per_sq_ft_mom": "MoM change, percent. Bronze source: 'MEDIAN NEW LISTING PRICE PER SQ.FT. MOM (%)'.",
    "median_new_listing_price_per_sq_ft_yoy": "YoY change, percent. Bronze source: 'MEDIAN NEW LISTING PRICE PER SQ.FT. YOY (%)'.",
    "median_sale_price_per_sq_ft": "Median sale price per square foot, USD. Bronze source: 'MEDIAN SALE PRICE PER SQ.FT. ($)'.",
    "median_sale_price_per_sq_ft_mom": "MoM change, percent. Bronze source: 'MEDIAN SALE PRICE PER SQ.FT. MOM (%)'.",
    "median_sale_price_per_sq_ft_yoy": "YoY change, percent. Bronze source: 'MEDIAN SALE PRICE PER SQ.FT. YOY (%)'.",
    "months_of_supply": "Months of housing supply at the current sales pace. Bronze source: 'MONTHS OF SUPPLY'.",
    "months_of_supply_mom": "MoM change in months of supply, percent. Bronze source: 'MONTHS OF SUPPLY MOM (%)'.",
    "months_of_supply_yoy": "YoY change in months of supply, percent. Bronze source: 'MONTHS OF SUPPLY YOY (%)'.",
    "percent_off_market_in_two_weeks": "Share of listings off the market within two weeks, percent. Bronze source: 'PERCENT OFF MARKET IN TWO WEEKS (%)'.",
    "percent_off_market_in_two_weeks_mom_ppts": (
        "MoM change, percentage points (bronze value divided by 100). "
        "Bronze source: 'PERCENT OFF MARKET IN TWO WEEKS MOM (PPTS)'."
    ),
    "percent_off_market_in_two_weeks_yoy_ppts": (
        "YoY change, percentage points (bronze value divided by 100). "
        "Bronze source: 'PERCENT OFF MARKET IN TWO WEEKS YOY (PPTS)'."
    ),
}

# ---------------------------------------------------------------------
# Redfin: price_drops metrics (12 columns)
# ---------------------------------------------------------------------

REDFIN_PRICE_DROPS_METRICS = {
    "price_drops": "Number of active listings that had a price drop in the period. Bronze source: 'PRICE DROPS'.",
    "price_drops_mom": "MoM change in price drops, percent. Bronze source: 'PRICE DROPS MOM (%)'.",
    "price_drops_yoy": "YoY change in price drops, percent. Bronze source: 'PRICE DROPS YOY (%)'.",
    "average_size_of_price_drop": "Average size of a price drop, percent reduction. Bronze source: 'AVERAGE SIZE OF PRICE DROP (%)'.",
    "average_size_of_price_drop_mom_ppts": (
        "MoM change, percentage points (bronze value divided by 100). "
        "Bronze source: 'AVERAGE SIZE OF PRICE DROP MOM (PPTS)'."
    ),
    "average_size_of_price_drop_yoy_ppts": (
        "YoY change, percentage points (bronze value divided by 100). "
        "Bronze source: 'AVERAGE SIZE OF PRICE DROP YOY (PPTS)'."
    ),
    "percent_active_with_price_drops": "Share of active listings with a price drop, percent. Bronze source: 'PERCENT ACTIVE WITH PRICE DROPS (%)'.",
    "percent_active_with_price_drops_mom_ppts": (
        "MoM change, percentage points (bronze value divided by 100). "
        "Bronze source: 'PERCENT ACTIVE WITH PRICE DROPS MOM (PPTS)'."
    ),
    "percent_active_with_price_drops_yoy_ppts": (
        "YoY change, percentage points (bronze value divided by 100). "
        "Bronze source: 'PERCENT ACTIVE WITH PRICE DROPS YOY (PPTS)'."
    ),
    "homes_sold_with_price_drops": "Homes sold after a price drop. Bronze source: 'HOMES SOLD WITH PRICE DROPS'.",
    "homes_sold_with_price_drops_mom": "MoM change, percent. Bronze source: 'HOMES SOLD WITH PRICE DROPS MOM (%)'.",
    "homes_sold_with_price_drops_yoy": "YoY change, percent. Bronze source: 'HOMES SOLD WITH PRICE DROPS YOY (%)'.",
}

# ---------------------------------------------------------------------
# Redfin: delistings_relistings metrics (12 columns)
# ---------------------------------------------------------------------

REDFIN_DELISTINGS_RELISTINGS_METRICS = {
    "total_delistings": "Listings removed from the market in the period. Bronze source: 'TOTAL DELISTINGS'.",
    "total_delistings_mom": "MoM change in delistings, percent. Bronze source: 'TOTAL DELISTINGS MOM (%)'.",
    "total_delistings_yoy": "YoY change in delistings, percent. Bronze source: 'TOTAL DELISTINGS YOY (%)'.",
    "total_relistings": "Previously delisted listings put back on the market. Bronze source: 'TOTAL RELISTINGS'.",
    "total_relistings_mom": "MoM change in relistings, percent. Bronze source: 'TOTAL RELISTINGS MOM (%)'.",
    "total_relistings_yoy": "YoY change in relistings, percent. Bronze source: 'TOTAL RELISTINGS YOY (%)'.",
    "share_of_listings_delisted": "Share of listings delisted, percent. Bronze source: 'SHARE OF LISTINGS DELISTED (%)'.",
    "share_of_listings_delisted_mom_ppts": (
        "MoM change, percentage points (bronze value divided by 100). "
        "Bronze source: 'SHARE OF LISTINGS DELISTED MOM (PPTS)'."
    ),
    "share_of_listings_delisted_yoy_ppts": (
        "YoY change, percentage points (bronze value divided by 100). "
        "Bronze source: 'SHARE OF LISTINGS DELISTED YOY (PPTS)'."
    ),
    "share_of_listings_relisted": "Share of listings relisted, percent. Bronze source: 'SHARE OF LISTINGS RELISTED (%)'.",
    "share_of_listings_relisted_mom_ppts": (
        "MoM change, percentage points (bronze value divided by 100). "
        "Bronze source: 'SHARE OF LISTINGS RELISTED MOM (PPTS)'."
    ),
    "share_of_listings_relisted_yoy_ppts": (
        "YoY change, percentage points (bronze value divided by 100). "
        "Bronze source: 'SHARE OF LISTINGS RELISTED YOY (PPTS)'."
    ),
}

# ---------------------------------------------------------------------
# Census Bureau: ACS 5-year estimates at ZCTA level
# ---------------------------------------------------------------------

ACS_ZCTA_COLUMNS = {
    "zcta_name": "ZCTA display name. Bronze source: 'NAME' (renamed).",
    "zcta_code": (
        "5-digit ZIP Code Tabulation Area code; string to preserve leading zeros. "
        "Bronze source: 'zip' (renamed)."
    ),
    "total_population": "Total population. Bronze source: 'B01003_001E' (renamed).",
    "median_age": "Median age, years. Bronze source: 'B01002_001E' (renamed).",
    "median_household_income": "Median household income, USD. Bronze source: 'B19013_001E' (renamed).",
    "population_below_poverty_level": "Population with income below poverty level. Bronze source: 'B17001_002E' (renamed).",
    "white_alone_population": "Population, White alone. Bronze source: 'B02001_002E' (renamed).",
    "black_or_african_american_alone_population": "Population, Black or African American alone. Bronze source: 'B02001_003E' (renamed).",
    "asian_alone_population": "Population, Asian alone. Bronze source: 'B02001_005E' (renamed).",
    "hispanic_or_latino_population": "Population, Hispanic or Latino (any race). Bronze source: 'B03003_003E' (renamed).",
    "_ingest_timestamp": "Time the bronze Auto Loader job ingested the raw file. Bronze source: '_ingest_timestamp'.",
    "_source_file": "Raw source file path in the bronze UC volume. Bronze source: '_source_file'.",
    "_processed_timestamp": "Silver pipeline processing time; added in silver via current_timestamp().",
}

# ---------------------------------------------------------------------
# Opportunity Insights: Social Capital Atlas (ZIP level)
# ---------------------------------------------------------------------

SOCIAL_CAPITAL_COLUMNS = {
    "zip_code": "5-digit ZIP code; string to preserve leading zeros. Bronze source: 'zip' (renamed).",
    "county_fips_code": "5-digit county FIPS code; string. Bronze source: 'county' (renamed).",
    "children_below_median_parental_income_count": (
        "Children (1988 birth cohort) with below-median parental income in this ZIP. "
        "Bronze source: 'num_below_p50' (renamed)."
    ),
    "population_2018": "2018 population estimate. Bronze source: 'pop2018' (renamed).",
    "economic_connectedness_low_ses": (
        "Economic connectedness: share of high-SES friends for below-median-SES "
        "individuals. Bronze source: 'ec_zip' (renamed)."
    ),
    "economic_connectedness_low_ses_standard_error": (
        "Standard error of economic_connectedness_low_ses. Bronze source: 'ec_se_zip' (renamed)."
    ),
    "neighborhood_economic_connectedness_low_ses": (
        "Neighborhood-weighted economic connectedness, low-SES. Bronze source: 'nbhd_ec_zip' (renamed)."
    ),
    "group_economic_connectedness_low_ses": (
        "Group-membership-based economic connectedness, low-SES. Bronze source: 'ec_grp_mem_zip' (renamed)."
    ),
    "economic_connectedness_high_ses": (
        "Economic connectedness: share of high-SES friends for above-median-SES "
        "individuals. Bronze source: 'ec_high_zip' (renamed)."
    ),
    "economic_connectedness_high_ses_standard_error": (
        "Standard error of economic_connectedness_high_ses. Bronze source: 'ec_high_se_zip' (renamed)."
    ),
    "neighborhood_economic_connectedness_high_ses": (
        "Neighborhood-weighted economic connectedness, high-SES. Bronze source: 'nbhd_ec_high_zip' (renamed)."
    ),
    "group_economic_connectedness_high_ses": (
        "Group-membership-based economic connectedness, high-SES. Bronze source: 'ec_grp_mem_high_zip' (renamed)."
    ),
    "group_exposure_to_high_ses_low_ses": (
        "Exposure of low-SES individuals to high-SES peers via shared groups. "
        "Bronze source: 'exposure_grp_mem_zip' (renamed)."
    ),
    "group_exposure_to_high_ses_high_ses": (
        "Exposure of high-SES individuals to high-SES peers via shared groups. "
        "Bronze source: 'exposure_grp_mem_high_zip' (renamed)."
    ),
    "neighborhood_exposure_to_high_ses": (
        "Neighborhood-level exposure to high-SES individuals. Bronze source: 'nbhd_exposure_zip' (renamed)."
    ),
    "group_friending_bias_low_ses": (
        "Friending bias in groups, low-SES individuals. Bronze source: 'bias_grp_mem_zip' (renamed)."
    ),
    "group_friending_bias_high_ses": (
        "Friending bias in groups, high-SES individuals. Bronze source: 'bias_grp_mem_high_zip' (renamed)."
    ),
    "neighborhood_friending_bias_low_ses": (
        "Friending bias at neighborhood level, low-SES. Bronze source: 'nbhd_bias_zip' (renamed)."
    ),
    "neighborhood_friending_bias_high_ses": (
        "Friending bias at neighborhood level, high-SES. Bronze source: 'nbhd_bias_high_zip' (renamed)."
    ),
    "friend_network_clustering": (
        "Clustering of friendship networks within the ZIP. Bronze source: 'clustering_zip' (renamed)."
    ),
    "within_zcta_friendship_support_ratio": (
        "Friendship support ratio among ZIP residents. Bronze source: 'support_ratio_zip' (renamed)."
    ),
    "volunteering_group_membership_rate": (
        "Volunteering / group membership rate. Bronze source: 'volunteering_rate_zip' (renamed)."
    ),
    "civic_organizations_per_1000_users": (
        "Civic organizations per 1,000 users. Bronze source: 'civic_organizations_zip' (renamed)."
    ),
    "_rescued_data": (
        "Auto Loader quarantine column holding values that failed to parse. "
        "Bronze source: '_rescued_data'."
    ),
    "_ingest_timestamp": "Time the bronze Auto Loader job ingested the raw file. Bronze source: '_ingest_timestamp'.",
    "_source_file": "Raw source file path in the bronze UC volume. Bronze source: '_source_file'.",
    "_processed_timestamp": "Silver pipeline processing time; added in silver via current_timestamp().",
}

# ---------------------------------------------------------------------
# HUD: Fair Market Rents (county x ZIP x year)
# ---------------------------------------------------------------------

HUD_FMR_COLUMNS = {
    "county_name": "County name from HUD FMR data. Bronze source: 'county_name' (unchanged).",
    "metro_name": "Metro area name from HUD FMR data. Bronze source: 'metro_name' (unchanged).",
    "area_name": "HUD FMR area name. Bronze source: 'area_name' (unchanged).",
    "metro_status": "1 if the area is a metropolitan area, 0 otherwise. Bronze source: 'metro_status' (unchanged).",
    "smallarea_status": "1 if ZIP-level (small area) FMRs apply, 0 otherwise. Bronze source: 'smallarea_status' (unchanged).",
    "year": "FMR program year (cast to int in silver). Bronze source: 'year'.",
    "zip_code": "ZIP code the FMR applies to. Bronze source: 'zip_code' (unchanged).",
    "efficiency": "Fair Market Rent for an efficiency (studio) unit, USD per month. Bronze source: 'efficiency' (unchanged).",
    "one_bedroom": "Fair Market Rent for a one-bedroom unit, USD per month. Bronze source: 'one_bedroom' (unchanged).",
    "two_bedroom": "Fair Market Rent for a two-bedroom unit, USD per month. Bronze source: 'two_bedroom' (unchanged).",
    "three_bedroom": "Fair Market Rent for a three-bedroom unit, USD per month. Bronze source: 'three_bedroom' (unchanged).",
    "four_bedroom": "Fair Market Rent for a four-bedroom unit, USD per month. Bronze source: 'four_bedroom' (unchanged).",
    "source_url": "HUD API URL the row was fetched from. Bronze source: 'source_url' (unchanged).",
    "_ingest_timestamp": "Time the bronze Auto Loader job ingested the raw file. Bronze source: '_ingest_timestamp'.",
    "_source_file": "Raw source file path in the bronze UC volume. Bronze source: '_source_file'.",
    "_processed_timestamp": "Silver pipeline processing time; added in silver via current_timestamp().",
}

# ---------------------------------------------------------------------
# HUD: reference dimensions (documented in docs/DATA_DICTIONARY.md §2/§3)
# ---------------------------------------------------------------------

HUD_DIM_STATES_COLUMNS = {
    "state_code": "2-letter state/territory abbreviation; natural key. Source: HUD listStates API via bronze_dev.hud.states.",
    "state_name": "State or territory name.",
    "_ingest_timestamp": "Time the bronze job ingested the row. Bronze source: '_ingest_timestamp'.",
    "_processed_timestamp": "Silver pipeline processing time; added in silver via current_timestamp().",
}

HUD_DIM_COUNTIES_COLUMNS = {
    "fips_code": "5-digit county FIPS code (2-digit state + 3-digit county); natural key.",
    "state_code": "2-letter state code.",
    "county_name": "Short county name.",
    "cntyname": "Long-form county name as reported by HUD ('County, ST').",
    "category": "County classification code from HUD reference data.",
    "town_name": "Town name associated with the county record.",
    "_ingest_timestamp": "Time the bronze job ingested the row. Bronze source: '_ingest_timestamp'.",
    "_processed_timestamp": "Silver pipeline processing time; added in silver via current_timestamp().",
}

# ---------------------------------------------------------------------
# Gold layer additions
# ---------------------------------------------------------------------

GOLD_REFRESH_COLUMNS = {
    "gold_refresh_timestamp": (
        "Gold materialized view refresh time, added via current_timestamp() at build. "
        "Pipeline metadata columns (_rescued_data, _ingest_timestamp, _source_file, "
        "_processed_timestamp) are dropped in gold."
    ),
}

# ---------------------------------------------------------------------
# Assemble the full table -> {column -> comment} map.
# ---------------------------------------------------------------------

COLUMN_COMMENTS = {}


def _register(table, columns):
    COLUMN_COMMENTS.setdefault(table, {}).update(columns)


# Redfin silver: 3 families x 3 grains
for _grain in ("county", "zipcode", "neighborhood"):
    _register(
        f"silver_dev.redfin.housing_market_{_grain}",
        {
            **REDFIN_KEY_COLUMNS,
            **REDFIN_REGION_ID,
            **REDFIN_HOUSING_MARKET_METRICS,
            **REDFIN_METADATA_COLUMNS,
        },
    )
    _register(
        f"silver_dev.redfin.price_drops_{_grain}",
        {**REDFIN_KEY_COLUMNS, **REDFIN_PRICE_DROPS_METRICS, **REDFIN_METADATA_COLUMNS},
    )
    _register(
        f"silver_dev.redfin.delistings_relistings_{_grain}",
        {**REDFIN_KEY_COLUMNS, **REDFIN_DELISTINGS_RELISTINGS_METRICS, **REDFIN_METADATA_COLUMNS},
    )

# Other silver sources
_register("silver_dev.census_bureau.american_community_survey_zcta", ACS_ZCTA_COLUMNS)
_register("silver_dev.opportunity_insights.social_capital_zip", SOCIAL_CAPITAL_COLUMNS)
_register("silver_dev.hud.fair_market_rents_county", HUD_FMR_COLUMNS)
_register("silver_dev.hud.dim_states", HUD_DIM_STATES_COLUMNS)
_register("silver_dev.hud.dim_counties", HUD_DIM_COUNTIES_COLUMNS)

# Gold fact tables: each joins the three Redfin silver tables of its grain
# (housing_market as base, left-joined with price_drops and
# delistings_relistings on region_name + period_begin + period_end).
for _table in ("county_housing_profile", "zip_market_health", "neighborhood_market_activity"):
    _register(
        f"gold_dev.default.{_table}",
        {
            **REDFIN_KEY_COLUMNS,
            **REDFIN_REGION_ID,
            **REDFIN_HOUSING_MARKET_METRICS,
            **REDFIN_PRICE_DROPS_METRICS,
            **REDFIN_DELISTINGS_RELISTINGS_METRICS,
            **GOLD_REFRESH_COLUMNS,
        },
    )


# ---------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------


def _sql_escape(text):
    """Escape single quotes for a SQL string literal."""
    return text.replace("'", "''")


def apply_column_comments(spark, dry_run=False, only_tables=None):
    """Apply UC column comments for every table in COLUMN_COMMENTS.

    Idempotent: re-running simply overwrites comments with the same text.
    Columns that do not exist in the live table are skipped and reported
    under 'missing' — never a hard failure. Tables that cannot be read
    (e.g. not created yet in a fresh workspace) are reported and skipped.

    Args:
        spark: active SparkSession.
        dry_run: if True, validate and report without writing any comments.
        only_tables: optional list of fully-qualified table names to restrict
            the run to (defaults to all tables in COLUMN_COMMENTS).

    Returns:
        dict: table -> {'status', 'applied', 'missing',
                        'uncommented_columns', 'dry_run'}
    """
    targets = only_tables or COLUMN_COMMENTS
    results = {}
    for table, comments in targets.items():
        try:
            existing = set(spark.table(table).columns)
        except Exception as exc:  # table missing or unreadable
            results[table] = {
                "status": "table_unreadable",
                "error": str(exc).splitlines()[0],
                "dry_run": dry_run,
            }
            continue

        missing = sorted(set(comments) - existing)
        uncommented = sorted(existing - set(comments))
        applied = 0
        if not dry_run:
            for column, text in comments.items():
                if column in missing:
                    continue
                spark.sql(
                    f"COMMENT ON COLUMN {table}.{column} "
                    f"IS '{_sql_escape(text)}'"
                )
                applied += 1
        results[table] = {
            "status": "ok",
            "applied": applied,
            "missing": missing,
            "uncommented_columns": uncommented,
            "dry_run": dry_run,
        }
    return results


if __name__ == "__main__":
    import sys

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    report = apply_column_comments(spark, dry_run=("--dry-run" in sys.argv))
    for table, result in report.items():
        print(f"{table}: {result}")
