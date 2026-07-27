import re
from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime
from delta.tables import DeltaTable
from typing import List
from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass, field
import src.bronze.transforms



def bronze_read_prep_upsert(
    file_path:str,
    file_format:str,
    table_name:str,
    natural_key:list,
    spark:SparkSession,
    options: dict[str, str] = field(default_factory=lambda: {
        "header": "true",
        "inferSchema": "false",
    })
) -> None:
    """Read, prep, and upsert a bronze table from a file source."""
    
    df = read_bronze_dataset(
        file_path,
        file_format,
        table_name,
        natural_key,
        spark,
        options
    )

    # Prep and upsert data
    df_prepped = src.bronze.transforms.prep_bronze_file_df(df)

    record_count = df_prepped.count()
    print(f"Loading {record_count:,} records into {table_name}")

    upsert_table(table_name, df_prepped, natural_key, spark)



def upsert_table(
    table_name: str,
    df: DataFrame,             # Spark DataFrame
    natural_key: List[str],    # List of column names
    spark: SparkSession        # Spark session
) -> None:
    
    for k in natural_key:
        if k not in df.columns:
            raise ValueError(f"Column {k} not found in dataframe")
    
    # Define merge key
    merge_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in natural_key]
    )

    if spark.catalog.tableExists(table_name):

        # Upsert
        delta_table = DeltaTable.forName(spark, table_name)

        delta_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        # First-time write
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)



import time
from typing import List, Callable, Any, Dict

def fetch_all(
    urls: List[str],
    token: str,
    request_fn: Callable[[str], Any],
    requests_per_minute = 60) -> List[Any]:

    """
    Takes a list of URLs and returns a list of JSON responses.
    """
    sleep_time = 1 / (requests_per_minute / 60)

    results = []

    for url in urls:
        try:
            res = request_fn(url=url, token=token)
            results.append(res)
        except Exception as e:
            print(f"Failed for {url}: {e}")
            results.append(None)

        # sleep for the time it took to run the function, plus the specified interval
        time.sleep(sleep_time)

    return results