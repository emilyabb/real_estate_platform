from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
import requests
from src.ingestion.api import add_api_metadata

def fetch_zcta_population() -> DataFrame:
    """
    Download the ZCTA-level population data from the CEnsus
    """
    API_KEY = dbutils.secrets.get(scope="api-secrets", key="census-api-key" ).lstrip("\x00")

    url = "https://api.census.gov/data/2023/acs/acs5"

    column_list = [
            "NAME",

            # Total population
            "B01003_001E",

            # Median age
            "B01002_001E",

            # Median household income
            "B19013_001E",

            # Poverty count
            "B17001_002E",

            # Race counts
            "B02001_002E",  # White alone
            "B02001_003E",  # Black alone
            "B02001_005E",  # Asian alone

            # Hispanic population
            "B03003_003E"
        ]

    params = {
        "get": ",".join(column_list),
        "for": "zip code tabulation area:*",
        "key": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Extract header and rows
    columns = data[0]
    rows = data[1:]

    # Create Spark DataFrame directly
    df = spark.createDataFrame(rows, schema=columns)
    df = add_api_metadata(
        df=df,
        source="census_api",
        endpoint=url,
        params={k: v for k, v in params.items() if k != "key"}
    )
        
    return df