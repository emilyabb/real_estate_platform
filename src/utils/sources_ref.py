

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
# For pipeline testing, remove large tables
REDFIN_TABLES = [
    t for t in REDFIN_TABLES
    if t["table_name"] not in [
        "housing_market_zipcode",
        "housing_market_neighborhood",
    ]
]


# -------------------------------------------------------------------
# OPPORTUNITY INSIGHTS
# -------------------------------------------------------------------
OPPORTUNITY_INSIGHTS_SOCIAL_CAPITAL_CONFIG = {
    "bronze_table_name": "social_capital_zip",
    # "schema_name":"opportunity_insights",
    "natural_key": ["zip"],
    "bronze_key": ["zip", "_ingest_timestamp"],
    "silver_key": ["zip_code", "_ingest_timestamp"],
    "csv_directory":"/Volumes/bronze_dev/opportunity_insights/opportunity_insights_raw/social_capital_zip"
}



# -------------------------------------------------------------------
# OPPORTUNITY INSIGHTS
# -------------------------------------------------------------------
CENSUS_BUREAU_AMERICAN_COMMUNITY_SURVEY_CONFIG = {
    "bronze_table_name": "american_community_survey_zcta",
    # "schema_name": "census_bureau",
    "natural_key": ["zip"],
    "bronze_key": ["zip", "_ingest_timestamp"],
    "silver_key": ["zip_code"],
    "csv_directory":"/Volumes/bronze_dev/census_bureau/census_bureau_raw/american_community_survey"
}


# -------------------------------------------------------------------
# HUD FAIR MARKET RENTS
# -------------------------------------------------------------------
HUD_FAIR_MARKET_RENTS_CONFIG = {
    "bronze_table_name": "fair_market_rents_county",
    "natural_key": ["county_name", "zip_code", "year"],
    "silver_key": ["county_name", "zip_code", "year"],
}


if __name__ == "__main__":
        
    print('Redfin tables -------------------------------------------------------')
    for t in REDFIN_TABLES:
        print(t["table_name"])
        print(t["natural_key_silver"])

