from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

SOURCE = dbutils.secrets.get(scope="manufacture", key="s3_product_path")

schema = StructType([
    StructField("dealer_id",    StringType(), True),
    StructField("sgrp_id",      StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("legal_name",   StringType(), True),
    StructField("email",        StringType(), True),
    StructField("system_id",    StringType(), True),
    StructField("vin",          StringType(), True),
])

@dlt.table(
    name="Product_Bronze"
)
def Product_Bronze():
    df = spark.read.format("csv")\
            .option("header","false")\
            .schema(schema)\
            .option("sep","\t")\
            .option("mode", "PERMISSIVE") \
            .option("mergeSchema", "true") \
            .option("columnNameOfCorruptRecord", "_corrupt_rec") \
            .load(SOURCE)

    df = df.withColumn("File_name",col("_metadata.file_name"))\
            .withColumn("bronze_ingest_timestamp",current_timestamp())

    return df
