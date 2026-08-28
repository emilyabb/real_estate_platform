import re
from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime
from delta.tables import DeltaTable
from typing import List
from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass, field


import time
from typing import List, Callable, Any, Dict
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime

# -------------------------------------------------------------------
# Bronze column cleaning
# -------------------------------------------------------------------
def clean_column_names_bronze(df: DataFrame):
    """
    Minimal cleaning for bronze layer column names:
    - Trim whitespace
    - Replace spaces with underscores
    - Remove characters not supported in Databricks column names
    - Preserve original casing as much as possible
    """
    cleaned_cols = []
    
    column_names = df.columns # Compute column names once before the loop
    for c in column_names:
        new_col = c.strip()

        # Replace percentage suffix
        new_col = re.sub(r"\s*\(%\)$", "_PERC", new_col)

        new_col = new_col.replace(" ", "_")
        
        # Remove problematic characters (keep letters, numbers, underscore)
        new_col = re.sub(r"[^\w]", "", new_col)
        
        # Replace any double underscores
        new_col = new_col.replace("__", "_")

        cleaned_cols.append(new_col)
    
    return df.toDF(*cleaned_cols)

# -------------------------------------------------------------------
# Bronze dataframe cleaning
# -------------------------------------------------------------------
def cast_void_columns(df: DataFrame) -> DataFrame:
    """
    Find columns of NullType and cast to string type.
    """

    # Find all void columns
    void_columns = [field.name for field in df.schema.fields if str(field.dataType) == 'NullType()']
    if len(void_columns) > 0:
        print(f"Found {len(void_columns)} void type columns: {void_columns}")

    # Cast void type columns to string type
    for col_name in void_columns:
        df = df.withColumn(col_name, col(col_name).cast("string"))

    return df

def rename_duplicate_columns(df: DataFrame) -> DataFrame:
    """
    Rename duplicate columns by appending _2, _3, etc.

    Example:
        ["id", "name", "name", "value", "name"]
        -> ["id", "name", "name_2", "value", "name_3"]
    """
    column_names = [c.strip() for c in df.columns]

    # Find duplicate names
    dups = [k for k, v in Counter(column_names).items() if v > 1]
    if dups:
        print("Warning: Duplicate columns:", dups)

    seen = Counter()
    new_column_names = []

    for col in column_names:
        seen[col] += 1
        if seen[col] == 1:
            new_column_names.append(col)
        else:
            new_column_names.append(f"{col}_{seen[col]}")

    return df.toDF(*new_column_names)

import uuid


# -------------------------------------------------------------------
# Bronze dataframe add metadata
# -------------------------------------------------------------------
def add_bronze_metadata(df: DataFrame, source_type:str):
    """ 
    Add bronze metadata to dataframe
    """
    if source_type == 'file':
        df = df.select(
            "*", 
            "_metadata.file_name", 
            "_metadata.file_path", 
            "_metadata.file_modification_time"
        )
    elif source_type == 'api':
        column_names = df.columns # Compute schema once before the loop
        # for api_col_name in ["source","api_endpoint","api_params"]:
        #     if api_col_name not in column_names:
        #         raise ValueError(f"Column {api_col_name} not found in API-sourced dataframe")

        if "ingestion_id" not in column_names:
            df = df.withColumn("ingestion_id", lit(str(uuid.uuid4()))) # Add ingestion ID
    else:
        raise ValueError(f"Invalid source type: {source_type}")
    
    # Add ingesttime
    df = df.withColumn("ingest_time", lit(datetime.now()))
    return df

# -------------------------------------------------------------------
# Bronze dataframe main function
# -------------------------------------------------------------------
def prep_bronze_df(df: DataFrame, source_type:str) -> DataFrame:
    """ 
    Main function for bronze layer dataframe preparation
    """

    # Clean column names, cast void types to string, and check for duplicate column names
    df = clean_column_names_bronze(df) # Clean column names
    df = cast_void_columns(df)          # Cast void type columns (causes exception) to string
    df = rename_duplicate_columns(df)   # Find and alert for duplicate column names; rename dupes

    # Add metadata
    df = add_bronze_metadata(df, source_type)
    return df




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