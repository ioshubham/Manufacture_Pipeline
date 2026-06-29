from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

#SOURCE_PAYMENT = dbutils.secrets.get(scope="Manufacture",key="s3_transaction_path")
SOURCE_PAYMENT="s3://manufacturar-data-lake-sk1/Market/payment"

schema = StructType([
    StructField("transaction_id",      StringType(), True),
    StructField("customer_id",         StringType(), True),
    StructField("vin",                 StringType(), True),
    StructField("dealer_id",           StringType(), True),
    StructField("card_type",           StringType(), True),
    StructField("card_last4",          StringType(), True),
    StructField("amount_paid",         StringType(), True),
    StructField("currency",            StringType(), True),
    StructField("payment_date",        StringType(), True),
    StructField("payment_status",      StringType(), True),
    StructField("payment_method",      StringType(), True),
    StructField("service_id",          StringType(), True),
    StructField("service_type",        StringType(), True),
    StructField("service_date",        StringType(), True),
    StructField("service_due_date",    StringType(), True),
    StructField("mileage_at_service",  StringType(), True),
    StructField("service_charge",      StringType(), True),
    StructField("parts_cost",          StringType(), True),
    StructField("labour_cost",         StringType(), True),
    StructField("total_cost",          StringType(), True),
    StructField("service_status",      StringType(), True),
    StructField("technician_id",       StringType(), True),
    StructField("outstanding_balance", StringType(), True),
    StructField("due_date",            StringType(), True),
    StructField("days_overdue",        StringType(), True),
    StructField("last_payment_date",   StringType(), True),
    StructField("finance_plan_id",     StringType(), True),
    StructField("system_id",           StringType(), True),
    StructField("_corrupt_rec",        StringType(), True),
])

@dlt.table(
    name = "payment_bronze"
)
def payment_bronze():
    df = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","csv")\
               .option("schemaLocation",SOURCE_PAYMENT+"_schema_hints")\
              .option("header","false")\
              .schema(schema)\
              .option("sep","\t")\
              .option("mode","PREMISSIVE")\
              .option("columnNameOfCurruptRecord","_corrupt_rec")\
              .option("mergeSchema","True")\
              .load(SOURCE_PAYMENT)

    return df


