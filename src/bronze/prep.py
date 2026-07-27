
from pyspark.sql import DataFrame
import re

from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime

# -------------------------------------------------------------------
# Bronze column cleaning
# -------------------------------------------------------------------
def _clean_column_names_bronze(df: DataFrame):
    """
    Minimal cleaning for bronze layer:
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


# def clean_bronze_df(df: DataFrame) -> DataFrame:

#     df = _clean_column_names_bronze(df) # Clean column names
#     df = cast_void_columns(df)          # Cast void type columns (causes exception) to string
#     df = rename_duplicate_columns(df)   # Find and alert for duplicate column names

#     return df

# -------------------------------------------------------------------
# Bronze dataframe metadata
# -------------------------------------------------------------------
def add_bronze_metadata(df: DataFrame, source_type:str):

    if source_type == 'file':
        df = df.select(
            "*", 
            "_metadata.file_name", 
            "_metadata.file_path", 
            "_metadata.file_modification_time"
        )
    elif source_type == 'api':
        column_names = df.columns # Compute schema once before the loop
        for col_name in ["url"]:
            if col_name not in column_names:
                raise ValueError(f"Column {col_name} not found in API-sourced dataframe")
    else:
        raise ValueError(f"Invalid source type: {source_type}")
    
    # Add ingesttime
    df = df.withColumn("ingest_time", lit(datetime.now()))
    return df

# -------------------------------------------------------------------
# Bronze dataframe main public function
# -------------------------------------------------------------------
def prep_bronze_file_df(df: DataFrame, source_type:str) -> DataFrame:

    # Clean column names, cast void types to string, and check for duplicate column names
    df = _clean_column_names_bronze(df) # Clean column names
    df = cast_void_columns(df)          # Cast void type columns (causes exception) to string
    df = rename_duplicate_columns(df)   # Find and alert for duplicate column names; rename dupes

    # Add metadata
    df = add_bronze_metadata(df, source_type)
    return df

