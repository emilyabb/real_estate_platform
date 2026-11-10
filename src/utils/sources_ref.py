

# -------------------------------------------------------------------
# REDFIN
# -------------------------------------------------------------------

DIRECTORIES = (
    "housing_market",
    "price_drops",
    "delistings_relistings",
)

GRAINS = {
    "counties": "county",
    "zips": "zipcode",
    "neighborhoods": "neighborhood",
}

DEFAULT_KEY = (
    "PERIOD BEGIN",
    "PERIOD END",
    "REGION NAME",
)


REDFIN_VOLUME_BASE = "/Volumes/bronze_dev/redfin/redfin_raw"

REDFIN_TABLES = [
    {
        'table_name': f"{d}_{s}",
        "natural_key": DEFAULT_KEY,
        "bronze_key": [*DEFAULT_KEY, "LAST UPDATED"],
        "natural_key_silver": [k.lower().replace(" ","_") for k in DEFAULT_KEY],
        "redfin_dir": d,
        "redfin_grain": g
    }
    for d in DIRECTORIES
    for g, s in GRAINS.items()
]


# -------------------------------------------------------------------
# OPPORTUNITY INSIGHTS
# -------------------------------------------------------------------
OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG = {
    "bronze_table_name": "social_capital_zip",
    # "schema_name":"opportunity_insights",
    "natural_key": ["zip"],
    "bronze_key": ["zip", "_ingest_timestamp"],
    "silver_key": ["zip_code"],
    "csv_directory":"/Volumes/bronze_dev/opportunity_insights/opportunity_insights_raw/social_capital_zip"
}



# -------------------------------------------------------------------
# CENSUS BUREAU (ACS)
# -------------------------------------------------------------------
# Raw files for all three grains land in the same directory as
# american_community_survey_{grain}_{timestamp}.json. Unlike HUD FMR (whose
# bronze table is geography-neutral), each ACS grain gets its own bronze and
# silver table because the raw JSON column layout differs per grain.
CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG = {
    "csv_directory": "/Volumes/bronze_dev/census_bureau/census_bureau_raw/american_community_survey",
    "grains": {
        "zcta": {
            "bronze_table_name": "american_community_survey_zcta",
            "natural_key": ["zip"],
            "bronze_key": ["zip", "_ingest_timestamp"],
            "silver_key": ["zcta_code"],  # silver renames zip -> zcta_code; key must match the output column
        },
        "county": {
            "bronze_table_name": "american_community_survey_county",
            "natural_key": ["state", "county"],
            "bronze_key": ["state", "county", "_ingest_timestamp"],
            "silver_key": ["state_fips", "county_fips"],  # silver renames state/county -> state_fips/county_fips
        },
        "state": {
            "bronze_table_name": "american_community_survey_state",
            "natural_key": ["state"],
            "bronze_key": ["state", "_ingest_timestamp"],
            "silver_key": ["state_fips"],  # silver renames state -> state_fips
        },
    },
}


# -------------------------------------------------------------------
# USPS ZIP CODES
# -------------------------------------------------------------------
USPS_ZIP_CODES_CONFIG = {
    "bronze_table_name": "usps_zip_codes",
    "natural_key": ["zip_code"],
    "bronze_key": ["zip_code", "_ingest_timestamp"],
    "silver_key": ["zip_code"],
    "csv_directory": "/Volumes/bronze_dev/geo_re/geo_re_raw/usps_zip_codes"
}


# -------------------------------------------------------------------
# HUD FAIR MARKET RENTS
# -------------------------------------------------------------------
HUD_FAIR_MARKET_RENTS_CONFIG = {
    "bronze_table_name": "fair_market_rents",  # geography-neutral: raw file contains county-, MSA-, and zip-level rows
    "natural_key": ["county_name", "zip_code", "year"],
    "silver_keys": {
        # Per-level replace_using keys for the silver FMR tables
        "county": ["county_name", "year"],
        "msa": ["metro_name", "year"],
        "zipcode": ["zip_code", "year"],
    },
}


if __name__ == "__main__":
        
    print('Redfin tables -------------------------------------------------------')
    for t in REDFIN_TABLES:
        print(t["table_name"])
        print(t["natural_key_silver"])

