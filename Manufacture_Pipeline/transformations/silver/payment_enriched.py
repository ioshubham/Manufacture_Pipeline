import dlt
from pyspark.sql.functions import (
    col, current_timestamp, lit, when,
    length, trim, upper, to_date, regexp_replace
)
from pyspark.sql.types import DecimalType, IntegerType


# -----------------------------------------------------------------------
# Shared transformation function
# -----------------------------------------------------------------------
def apply_payment_transformations(df):
    return df .withColumn("amount_paid",
            col("amount_paid").cast(DecimalType(10, 2))) \
        .withColumn("total_cost",
            col("total_cost").cast(DecimalType(10, 2))) \
        .withColumn("parts_cost",
            col("parts_cost").cast(DecimalType(10, 2))) \
        .withColumn("labour_cost",
            col("labour_cost").cast(DecimalType(10, 2))) \
        .withColumn("service_charge",
            col("service_charge").cast(DecimalType(10, 2))) \
        .withColumn("outstanding_balance",
            col("outstanding_balance").cast(DecimalType(10, 2))) \
        .withColumn("payment_date",
            to_date(col("payment_date"), "yyyy-MM-dd")) \
        .withColumn("service_date",
            to_date(col("service_date"), "yyyy-MM-dd")) \
        .withColumn("due_date",
            to_date(col("due_date"),     "yyyy-MM-dd")) \
        .withColumn("service_due_date",
            to_date(col("service_due_date"), "yyyy-MM-dd")) \
        .withColumn("last_payment_date",
            to_date(col("last_payment_date"), "yyyy-MM-dd")) \
        .withColumn("days_overdue",
            col("days_overdue").cast(IntegerType())) \
        .withColumn("mileage_at_service",
            col("mileage_at_service").cast(IntegerType())) \
        .withColumn("payment_status",
            upper(trim(col("payment_status")))) \
        .withColumn("payment_method",
            upper(trim(col("payment_method")))) \
        .withColumn("service_type",
            upper(trim(col("service_type")))) \
        .withColumn("service_status",
            upper(trim(col("service_status")))) \
        .withColumn("currency",
            upper(trim(col("currency")))) \
        .withColumn("card_type",
            upper(trim(col("card_type")))) \
        .withColumn("vin",
            upper(trim(col("vin")))) \
        .withColumn("is_overdue",
            when(col("days_overdue") > 0,  lit("Y"))
            .otherwise(                     lit("N"))) \
        .withColumn("payment_category",
            when(col("payment_method") == "CARD",    lit("CARD"))
            .when(col("payment_method") == "CASH",    lit("CASH"))
            .when(col("payment_method") == "FINANCE", lit("FINANCE"))
            .when(col("payment_method") == "LEASE",   lit("LEASE"))
            .otherwise(                                lit("OTHER"))) \
        .withColumn("silver_ingest_ts", current_timestamp())


# -----------------------------------------------------------------------
# Step 1 — Valid payment records
# -----------------------------------------------------------------------
@dlt.table(
    name="manufacture.silver.payment_enriched",
    comment="Payment Silver — cleansed, typed, DQ passed",
    table_properties={
        "quality": "silver",
        "layer":   "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
def payment_enriched():
    df_bronze = dlt.read_stream("manufacture.bronze.payment_bronze")

    df_transformed = apply_payment_transformations(df_bronze)

    df_valid = df_transformed.filter(
        col("transaction_id").isNotNull() &
        col("payment_date").isNotNull() &
        col("vin").isNotNull() &
        (length(col("vin")) == 17) &
        (col("amount_paid") > 0)
    )

    return df_valid


# -----------------------------------------------------------------------
# Step 2 — Rejected payment records
# -----------------------------------------------------------------------
@dlt.table(
    name="payment_rejected",
    comment="Payment Silver — failed DQ records",
    table_properties={
        "quality": "silver",
        "layer":   "silver"
    }
)
def payment_rejected():
    df_bronze = dlt.read_stream("manufacture.bronze.payment_bronze")

    df_transformed = apply_payment_transformations(df_bronze)

    df_rejected = df_transformed.filter(
        col("transaction_id").isNull() |
        col("payment_date").isNull() |
        col("vin").isNull() |
        (length(col("vin")) != 17) |
        (col("amount_paid") <= 0) |
        col("amount_paid").isNull()
    ) \
    .withColumn("rejection_reason",
        when(col("transaction_id").isNull(), lit("transaction_id is null"))
        .when(col("payment_date").isNull(),  lit("payment_date is null"))
        .when(col("vin").isNull(),           lit("VIN is null"))
        .when(length(col("vin")) != 17,      lit("VIN length not 17"))
        .when(col("amount_paid").isNull(),   lit("amount_paid is null"))
        .when(col("amount_paid") <= 0,       lit("amount_paid must be > 0"))
        .otherwise(                           lit("Unknown DQ failure"))
    ) \
    .withColumn("rejected_at", current_timestamp())

    return df_rejected


# -----------------------------------------------------------------------
# Step 3 — Define streaming target table for CDC
# -----------------------------------------------------------------------
dlt.create_streaming_table(
    name="manufacture.silver.payment_streaming",
    comment="Payment Silver — final SCD Type 1 CDC table",
    table_properties={
        "quality": "silver",
        "layer":   "silver",
        "delta.enableChangeDataFeed": "true"
    }
)


# -----------------------------------------------------------------------
# Step 4 — Apply CDC
# Key = transaction_id (each transaction is unique)
# sequence_by = silver_ingest_ts (latest version wins)
# -----------------------------------------------------------------------
dlt.apply_changes(
    target             = "manufacture.silver.payment_streaming",
    source             = "manufacture.silver.payment_enriched",
    keys               = ["transaction_id"],
    sequence_by        = col("silver_ingest_ts"),
    stored_as_scd_type = 1
)