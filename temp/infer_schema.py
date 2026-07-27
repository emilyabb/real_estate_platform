from pyspark.sql import functions as F
import src.utils.helpers
import re
import pandas as pd
from pyspark.sql import functions as F

import pandas as pd
from pyspark.sql import functions as F

def guess_type(column_name: str) -> str:
    """
    Guess an appropriate Silver data type from the standardized column name.
    """

    col = column_name.lower()

    # Explicit rules
    if col in ("period_begin", "period_end"):
        return "date"

    if col.endswith((
        "_price",
        "_value",
        "_ratio",
        "_rate",
        "_index",
        "_score",
        "_median",
        "_average",
        "_avg",
        "perc",
        "ppts"
    )):
        return "decimal(18,6)"

    if col.endswith((
        "_count",
        "_homes",
        "_sales",
        "_listings",
        "_inventory",
        "_days",
        "_months"
    )):
        return "int"

    if col.startswith(("is_", "has_")):
        return "boolean"

    return "string"


def build_redfin_schema_csv(
    spark,
    output_path: str = "/dbfs/tmp/redfin_schema.csv",
    sample_size: int = 3
):
    """
    Reads every Bronze Redfin table, standardizes column names,
    builds a unique list of all columns, guesses a datatype,
    collects sample values, and exports a CSV.
    """

    tables = (
        spark.sql("SHOW TABLES IN bronze_dev.redfin")
        .select("tableName")
        .collect()
    )

    rows = []

    for row in tables:
        table = row.tableName

        print(f"Processing {table}")

        df = spark.table(f"bronze_dev.redfin.{table}")
        df = src.utils.helpers.prep_silver_df(df)

        for col in df.columns:

            # If the column is not a string, it is a metadata type
            if dict(df.dtypes)[col] != "string":
                continue
            else:
                samples = (
                    df.select(col)
                    .where(
                        F.col(col).isNotNull() &
                        (F.col(col) != "NA") &
                        (F.col(col) != "")
                    )
                    .distinct()
                    .orderBy(F.rand()) # Randomize selection
                    .limit(sample_size)
                    .collect()
                )

                sample_values = ", ".join(str(r[0]) for r in samples)

            rows.append({
                "column_name": col,
                "suggested_type": guess_type(col),
                "sample_values": sample_values
            })

    pdf = (
        pd.DataFrame(rows)
        .drop_duplicates(subset=["column_name"])
        .sort_values("column_name")
        .reset_index(drop=True)
    )

    pdf.to_csv(output_path, index=False)

    print(f"\nWrote {len(pdf)} unique columns to {output_path}")

    display(pdf)

    return pdf


import os 
os.makedirs("/dbfs/schemas", exist_ok=True)
build_redfin_schema_csv(spark, "/dbfs/tmp/redfin_schema.csv")