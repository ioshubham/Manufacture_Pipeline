import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "manufacture.silver.customer_enriched",
    comment ="Transformation and validation completed"
)
def customer_enriched():
    df_bronze = spark.readStream.table("manufacture.bronze.customer_bronze")

    df_silver = df_bronze.withColumn("customer_id",regexp_replace(col("customer_id"),"-",""))\
                .withColumn("national_id",regexp_replace(col("national_id"),"-",""))\
                .withColumn("phone_mobile",regexp_replace(
                                    regexp_replace(col("phone_mobile"), 
                                            r"^(\+\d+)-",    # match +XX- at start
                                            "$1 "            # replace with +XX (space)
                                    ),
                                "-",                 # then remove ALL remaining hyphens
                                ""                   # replace with nothing
                ))\
                .withColumn("phone_home",regexp_replace(
                                    regexp_replace(col("phone_home"), 
                                            r"^(\+\d+)-",    # match +XX- at start
                                            "$1 "            # replace with +XX (space)
                                    ),
                                "-",                 # then remove ALL remaining hyphens
                                ""                   # replace with nothing
                ))\
                .withColumn("address_line2",coalesce(col("address_line2"),lit("Not Available")))\
                .withColumn("country_code",substring(col("dealer_id"),1,2))\
                .withColumn("serial_number",substring(col("dealer_id"),-6,6))\
                .withColumn("dealer_id",regexp_replace("dealer_id","-",""))\
                .withColumn("customer_type",initcap(col("customer_type")))\
                .withColumn("company_name",coalesce(col("company_name"),lit("Not Available")))\
                .withColumn("silver_ingest_timestamp",current_timestamp())

    return df_silver

