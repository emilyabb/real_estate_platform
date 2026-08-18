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


volume_path = "/Volumes/bronze_dev/redfin/redfin_raw"

TABLES = {
    f"{d}_{s}": TableConfig(
        table_name=f"{d}_{s}",
        source_path=f"{volume_path}/{d}_{s}.csv",
        remote_path=(
            f"s3://redfin-public-data/redfin_data_center/"
            f"{d}/monthly/all_{g}.csv"
        ),
        natural_key=DEFAULT_KEY,
        source_type="file",
    )
    for d in DIRECTORIES
    for g, s in GRAINS.items()
}

for k,v in TABLES.items():
    print(f"{k=}")
    print(f"{v=}")