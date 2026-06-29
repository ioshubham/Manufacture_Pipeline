import dlt
from pyspark.sql.functions import col, broadcast

PRODUCT_TABLE = "manufacture.silver.product_silver"
CUSTOMER_TABLE = "manufacture.silver.customer_enriched"
ORDER_TABLE = "manufacture.silver.payment_enriched"


@dlt.table(
    name="manufacture.gold.curated_360_gold",
    comment="Gold - full 360 view: customer, product, order",
    partition_cols=["dealer_id", "payment_date"],
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.excluded": "true"
    }
)
def curated_360_gold():

    # Streaming fact
    df_order = spark.readStream.table(ORDER_TABLE)

    # Static dimensions
    df_product = spark.read.table(PRODUCT_TABLE)
    df_customer = spark.read.table(CUSTOMER_TABLE)

    df_product_slim = df_product.select(
        "vin", "dealer_id", "sgrp_id", "manufacturer", "legal_name",
        "country_code", "serial_number",
        col("system_id").alias("product_system_id")
    )

    df_customer_slim = df_customer.select(
        "customer_id", "vin", "dealer_id",
        "first_name", "email", "phone_mobile", "city", "country",
        "customer_type", "account_status", "credit_score", "purchase_date",
        col("payment_method").alias("customer_payment_method"),
        col("currency").alias("customer_currency")
    )

    df_step1 = df_order.join(
        broadcast(df_product_slim),
        on=["vin", "dealer_id"],
        how="left"
    )

    df_360 = df_step1.join(
        broadcast(df_customer_slim),
        on=["vin", "customer_id", "dealer_id"],
        how="left"
    )

    df_gold = df_360.select(
        "customer_id",
        "customer_type",
        "account_status",
        "credit_score",
        "customer_payment_method",
        "purchase_date",
        "customer_currency",
        "first_name",
        "email",
        "phone_mobile",
        "city",
        "country",
        "country_code",
        "sgrp_id",
        "manufacturer",
        "legal_name",
        "product_system_id",
        "serial_number",
        "vin",
        "dealer_id",
        "payment_date"
    )

    return df_gold