import re
from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime
from delta.tables import DeltaTable
from typing import List
from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass, field
from  src.config.table_config import TableConfig

"""
Read data from a variety of sources:

Local-ish landing area:
    /Volumes/bronze_dev/opportunity_insights/raw/file.csv

DBFS:
    dbfs:/mnt/data/file.csv

AWS:
    s3://my-bucket/raw/file.csv

Azure:
    abfss://container@account.dfs.core.windows.net/raw/file.csv

Delta:
    bronze_dev.schema.table
"""
def read_bronze_dataset(
    file_path:str,
    file_format:str,
    spark:SparkSession,
    read_options:dict = {}
) -> DataFrame:
    
    reader = (
        spark.read
        .format(file_format)
        .option("header", "true")
        .option("inferSchema", "false")
    )

    # Add additional options, if any
    for key, value in read_options.items():
        reader = reader.option(key, value)

    df = reader.load(file_path)
    return df


def read_table_from_config(tc:TableConfig) -> DataFrame:

    return read_bronze_dataset(
        tc.source_path,
        tc.file_format,
        spark,
        tc.read_options
    )



# def create_bronze_table(pull):

#     @dp.table(name=pull.table_name)
#     def table():
#         return src.utils.helpers.read_bronze_dataset(
#             pull.file_path,
#             pull.file_format,
#             pull.table_name,
#             spark,
#             pull.options
#         )

#     return table