# from pyspark import pipelines as dp

from src.config.sources.redfin import TABLES as REDFIN_TABLES
from src.ingestion import read_table
from src.bronze.prep import read_bronze_dataset
from src.bronze.prep import prep_bronze_df
import src.bronze.opportunity_insights as opportunity_insights


# Handle generic tables
# for cfg in REDFIN_TABLES.values():
#     continue

def make_bronze_table_from_config(config):

    # @dp.table(
    #     name=config.destination_table,
    #     comment=config.description
    # )
    def table():

        df = load_table(config)

        return prep_bronze_df(df, config)
# @dp.table
def read_opportunity_insights():
    
    df = opportunity_insights.social_capital_zip()

    return prep_bronze_file_df(df)