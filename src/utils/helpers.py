import re
from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime
from delta.tables import DeltaTable
from typing import List
from pyspark.sql import DataFrame, SparkSession


def _clean_column_names_bronze(df: DataFrame):
    """
    Minimal cleaning for bronze layer:
    - Trim whitespace
    - Replace spaces with underscores
    - Remove characters not supported in Databricks column names
    - Preserve original casing as much as possible
    """
    cleaned_cols = []
    
    for c in df.columns:
        new_col = c.strip()

        # Replace percentage suffix
        new_col = re.sub(r"\s*\(%\)$", "_PERC", new_col)

        new_col = new_col.replace(" ", "_")
        
        # Remove problematic characters (keep letters, numbers, underscore)
        new_col = re.sub(r"[^\w]", "", new_col)
        
        # Replace any double underscores
        new_col = new_col.replace("__", "_")

        cleaned_cols.append(new_col)
    
    #print(cleaned_cols)
    return df.toDF(*cleaned_cols)


def _clean_column_names_silver(df):
    """
    Standardized cleaning for silver layer:
    - Lowercase
    - Convert to snake_case
    - Remove punctuation
    - Collapse multiple underscores
    """
    cleaned_cols = []
    
    def standardize_column_name_silver(col_name: str) -> str:
        # Step 1: insert underscore before capital letters (camelCase / PascalCase)
        col_name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', col_name)
        
        # Step 2: split acronym boundaries (e.g., FIPSCode → FIPS_Code)
        col_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col_name)
        
        # Step 3: split letters and numbers (FIPS23 → FIPS_23)
        col_name = re.sub(r'([a-zA-Z])(\d)', r'\1_\2', col_name)
        
        # Step 4: normalize
        col_name = col_name.lower()
        col_name = re.sub(r"[^\w]", "_", col_name)   # replace non-alphanumeric with _
        col_name = re.sub(r"_+", "_", col_name)      # collapse multiple _
        col_name = col_name.strip("_")              # trim
        
        return col_name

    for c in df.columns:
        new_col = standardize_column_name_silver(c)
        cleaned_cols.append(new_col)
    
    return df.toDF(*cleaned_cols)


def _clean_bronze_df(df: DataFrame) -> DataFrame:

    # Clean column names
    df = _clean_column_names_bronze(df)

    # Find all void columns
    void_columns = [field.name for field in df.schema.fields if str(field.dataType) == 'NullType()']
    if len(void_columns) > 0:
        print(f"Found {len(void_columns)} void type columns: {void_columns}")

    # Cast void type columns to string type
    for col_name in void_columns:
        df = df.withColumn(col_name, col(col_name).cast("string"))

    # Find and alert for duplicate column names
    columns = [c.strip() for c in df.columns]
    dups = [k for k, v in Counter(columns).items() if v > 1]
    if len(dups) > 1:
        print("Warning: Duplicate Columns: ", dups)

    return df

def prep_bronze_file_df(df: DataFrame) -> DataFrame:

    # Clean column names, cast void types to string, and check for duplicate column names
    df_cleaned = _clean_bronze_df(df)

    # Add metadata
    df_with_metadata = df_cleaned.select(
        "*", 
        "_metadata.file_name", 
        "_metadata.file_path", 
        "_metadata.file_modification_time"
    )

    # Add ingesttime
    df_with_metadata = df_with_metadata.withColumn("ingest_time", lit(datetime.now()))

    return df_with_metadata

def prep_bronze_api_df(df: DataFrame):

    # Check that the appropriate columns are in the dataframe
    #for col_name in ["source", "api_endpoint", "api_params"]:
    for col_name in ["url"]:
        if col_name not in df.columns:
            raise ValueError(f"Column {col_name} not found in API-sourced dataframe")
    
    df = _clean_bronze_df(df)
    # Add ingesttime
    df = df.withColumn("ingest_time", lit(datetime.now()))
    return df

def prep_silver_df(df):

    return _clean_column_names_silver(df)



def bronze_read_prep_upsert(
    file_path:str,
    file_format:str,
    table_name:str,
    natural_key:list,
    spark:SparkSession,
    options:dict = {}
) -> None:
    """Read, prep, and upsert a bronze table from a file source."""

    reader = (
        spark.read
        .format(file_format)
        .option("header", "true")
        .option("inferSchema", "false")
    )

    # Add additional options, if any
    for key, value in options.items():
        reader = reader.option(key, value)

    df = reader.load(file_path)

    # Prep and upsert data
    df_prepped = prep_bronze_file_df(df)

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