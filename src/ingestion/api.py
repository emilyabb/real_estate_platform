from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
import uuid

def add_api_metadata(df: DataFrame, source:str, endpoint:str, params:dict):

    return (
        df
        .withColumn("source", lit(source))
        .withColumn("api_endpoint", lit(endpoint))
        .withColumn(
            "api_params",
            lit(str(params))
        )
        .withColumn(
            "ingestion_id",
            lit(uuid.uuid4())
        )
    )