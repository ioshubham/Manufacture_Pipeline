from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *
import dlt

SOURCE = dbutils.secrets.get(scope="manufacture", key="s3_product_path")

schema = StructType([
    StructField("dealer_id",        StringType(), True),
    StructField("sgrp_id",          StringType(), True),
    StructField("manufacturer",     StringType(), True),
    StructField("legal_name",       StringType(), True),
    StructField("email",            StringType(), True),
    StructField("system_id",        StringType(), True),
    StructField("vin",              StringType(), True),
    StructField("_corrupt_record",  StringType(), True),
])

@dlt.table(
    name="product_bronze",
    comment="Product Bronze — Streaming Table via Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer":   "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def product_bronze():
    df = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format",         "csv") \
            .option("cloudFiles.schemaLocation", SOURCE + "_schema_hints") \
            .option("header",                    "false") \
            .option("sep",                       "\t") \
            .option("mode",                      "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .schema(schema) \
            .load(SOURCE)

    df = df \
        .withColumn("file_name",               col("_metadata.file_name")) \
        .withColumn("bronze_ingest_timestamp", current_timestamp())

    return df