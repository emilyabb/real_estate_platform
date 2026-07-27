# from pyspark import pipelines as dp

from src.config.sources__redfin import TABLES as REDFIN_TABLES
from src.bronze.ingestion import read_table
from src.bronze.prep import read_bronze_dataset
from src.bronze.prep import prep_bronze_file_df
import src.bronze.opportunity_insights as opportunity_insights


# Handle generic tables
# for cfg in REDFIN_TABLES.values():
#     continue


# @dp.table
def read_opportunity_insights():
    
    df = opportunity_insights.social_capital_zip()

    return prep_bronze_file_df(df)