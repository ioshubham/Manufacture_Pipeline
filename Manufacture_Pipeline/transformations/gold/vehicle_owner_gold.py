import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(name="manufacture.gold.vehicle_owner",
           comment="Curated table from product_silver & customer_silver join on vin")
def vehicle_owner():

    df_customer = spark.read.table("manufacture.silver.customer_enriched")\
                        .select("vin", "dealer_id", "phone_mobile", "first_name", "country_code","serial_number", "national_id")

    df_product = spark.read.table("manufacture.bronze.product_enriched")\
                    .select("manufacturer", "legal_name", "country_code", "email", "system_id", "sgrp_id","vin"
                )
                    
    df_result = df_customer.join(broadcast(df_product), df_customer["vin"] == df_product["vin"], how="left")\
                    .select(
                        df_customer["*"],
                        df_product["manufacturer"],
                        df_product["legal_name"],
                        df_product["email"],
                        df_product["system_id"],
                        df_product["sgrp_id"]
                    )

    return df_result

dlt.create_sink(
    name="vehicle_owner_s3_sink",
    format="delta",
    options={
        "path": "s3://manufacturar-data-lake-sk1/curated_tables/"
    }
)
@dlt.append_flow(name="vehicle_owner_to_s3", target="vehicle_owner_s3_sink")
def stream_to_s3():
    return spark.readStream.table("manufacture.gold.vehicle_owner")