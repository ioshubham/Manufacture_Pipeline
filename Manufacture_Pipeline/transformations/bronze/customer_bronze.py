from pyspark.sql.types import *
from pyspark.sql.functions import *
import dlt

SOURCE_CUSTOMER = dbutils.secrets.get(scope="manufacture",key="s3_customer_path")

schema = StructType([
    StructField("customer_id",    StringType(), True),
    StructField("first_name",     StringType(), True),
    StructField("last_name",      StringType(), True),
    StructField("date_of_birth",  StringType(), True),
    StructField("national_id",    StringType(), True),
    StructField("email",          StringType(), True),
    StructField("phone_mobile",   StringType(), True),
    StructField("phone_home",     StringType(), True),
    StructField("address_line1",  StringType(), True),
    StructField("address_line2",  StringType(), True),
    StructField("city",           StringType(), True),
    StructField("state",          StringType(), True),
    StructField("zip_code",       StringType(), True),
    StructField("country",        StringType(), True),
    StructField("dealer_id",      StringType(), True),
    StructField("customer_type",  StringType(), True),
    StructField("company_name",   StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("vin",            StringType(), True),
    StructField("purchase_date",  StringType(), True),
    StructField("ownership_type", StringType(), True),
    StructField("credit_score",   StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("currency",       StringType(), True),
    StructField("system_id",      StringType(), True),
    StructField("created_date",   StringType(), True),
    StructField("last_updated",   StringType(), True),
])

@dlt.table(
    name = "Customer_Bronze"
)
def customer_bronze():
    df = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format",         "csv") \
            .option("cloudFiles.schemaLocation", SOURCE_CUSTOMER + "_schema_hints") \
            .option("header","false")\
            .schema(schema)\
            .option("sep","\t")\
            .option("mode","PERMISSIVE")\
            .option("mergeSchema","true")\
            .option("columnNameOfCorruptRecord", "_corrupt_rec")\
            .load(SOURCE_CUSTOMER)

    #df_good = df.filter(col("_corrupt_rec").isNull())
    #df_bad = df.filter(col("_corrupt_rec").isNotNull())

    #df_good = df_good.withColumn("file_name",col("_metadata.filename"))\
                   # .withColumn("bronze_ingest_timestamp",current_timestamp())

    return df



