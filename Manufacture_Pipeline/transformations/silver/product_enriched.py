import dlt
from pyspark.sql.functions import (
    regexp_replace, initcap, substring, col,
    current_timestamp, length, lit, when
)

# -----------------------------------------------------------------------
# Step 1 — Bronze streaming read
# -----------------------------------------------------------------------
@dlt.table(
    name="product_enriched",
    comment="Product Silver — valid records only"
)
def product_enriched():
    df_bronze = spark.readStream.table("manufacture.bronze.product_bronze")  # correct way in DLT

    df_silver = df_bronze \
        .withColumn("dealer_id",        regexp_replace(col("dealer_id"), "-", "")) \
        .withColumn("manufacturer",     initcap(col("manufacturer"))) \
        .withColumn("legal_name",       regexp_replace(col("legal_name"), "-", "")) \
        .withColumn("serial_number",    substring(col("dealer_id"), -6, 6)) \
        .withColumn("country_code",     substring(col("dealer_id"), 1, 2)) \
        .withColumn("silver_ingest_ts", current_timestamp())

    df_valid = df_silver.filter(
        col("vin").isNotNull() &       # isNotNull() needs brackets
        (length(col("vin")) == 17)
    )

    return df_valid


# -----------------------------------------------------------------------
# Step 2 — Rejected records
# -----------------------------------------------------------------------
@dlt.table(
    name="product_rejected",
    comment="Product Silver — failed DQ records"
)
def product_rejected():
    df_bronze = dlt.read_stream("product_bronze")  # use dlt.read_stream not spark.read

    df_silver = df_bronze \
        .withColumn("dealer_id",        regexp_replace(col("dealer_id"), "-", "")) \
        .withColumn("manufacturer",     initcap(col("manufacturer"))) \
        .withColumn("legal_name",       regexp_replace(col("legal_name"), "-", "")) \
        .withColumn("serial_number",    substring(col("dealer_id"), -6, 6)) \
        .withColumn("country_code",     substring(col("dealer_id"), 1, 3)) \
        .withColumn("silver_ingest_ts", current_timestamp())

    df_rejected = df_silver.filter(
        col("vin").isNull() |
        (length(col("vin")) != 17)
    ) \
    .withColumn("rejection_reason",
        when(col("vin").isNull(),       lit("VIN is null"))
        .when(length(col("vin")) != 17, lit("VIN length is not 17 digits"))
        .otherwise(                      lit("Unknown"))
    ) \
    .withColumn("rejected_at", current_timestamp())

    return df_rejected
