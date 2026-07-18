
from pyspark.sql import DataFrame
import re

from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime

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

def clean_bronze_df(df: DataFrame) -> DataFrame:

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
    df_cleaned = clean_bronze_df(df)

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
    
    df = clean_bronze_df(df)
    # Add ingesttime
    df = df.withColumn("ingest_time", lit(datetime.now()))
    return df
