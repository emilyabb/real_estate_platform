from pyspark import pipelines as dp

from pyspark.sql import functions as F
import src.bronze.prep


@dp.table(
    name="bronze_dev.opportunity_insights.social_capital_zip"
)
def social_capital_zip():

    url = "https://data.humdata.org/dataset/85ee8e10-0c66-4635-b997-79b6fad44c71/resource/ab878625-279b-4bef-a2b3-c132168d536e/download/social_capital_zip.csv"

    # Read directly if possible
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(url)
    )

    df = src.bronze.prep.prep_bronze_file_df(df)

    return df