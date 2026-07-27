import sys
sys.path.append("/Workspace/Repos/dev/real_estate_platform")
from src.config.table_config import TableConfig

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

TABLES = {
    f"{d}_{s}": TableConfig(
        table_name=f"{d}_{s}",
        source_path=(
            f"s3://redfin-public-data/redfin_data_center/"
            f"{d}/monthly/all_{g}.csv"
        ),
        natural_key=DEFAULT_KEY,
    )
    for d in DIRECTORIES
    for g, s in GRAINS.items()
}