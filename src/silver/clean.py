import re
from pyspark.sql.functions import col, lit
from collections import Counter
from datetime import datetime
from delta.tables import DeltaTable
from typing import List
from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass, field

def clean_column_names_silver(df:DataFrame) -> DataFrame:
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