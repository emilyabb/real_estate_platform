

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
    "PERIOD_BEGIN",
    "PERIOD_END",
    "REGION_NAME",
)


REDFIN_VOLUME_BASE = "/Volumes/bronze_dev/redfin/redfin_raw"

REDFIN_TABLES = [
    {
        'table_name': f"{d}_{s}",
        # "local_path": f"{volume_path}/{d}_{g}.csv",
        # "remote_path": (
        #     f"s3://redfin-public-data/redfin_data_center/"
        #     f"{d}/monthly/all_{g}.csv"
        # ),
        # "schema_name": "redfin",
        "natural_key": DEFAULT_KEY,
        "bronze_key": [*DEFAULT_KEY, "LAST_UPDATE"],
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
    "csv_directory":"/Volumes/bronze_dev/census_bureau/census_bureau_raw/american_community_survey"
}


if __name__ == "__main__":
        
    print('Redfin tables -------------------------------------------------------')
    for t in REDFIN_TABLES:
        print(t["table_name"])

