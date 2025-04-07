from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, to_date, date_sub, months_between, when, max as Fmax, min as Fmin, countDistinct, sum as Fsum, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import last_day, date_add
from pyspark.sql import functions as F


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "authkey.json"

GCS_CLICKSTREAM_PATH = 'gs://data-mgmt-bucket/parquet/clickstream_new'

input_path = "gs://data-mgmt-bucket/parquet/"

def spark_init():
    # Create SparkSession
    spark = (SparkSession.builder
        .appName("ETL")  # Set the application name
        .master("local[*]") #Optional - set master to local for local testing
        # .config("spark.jars", "gcs-connector-hadoop3-latest.jar, spark-bigquery-with-dependencies_2.12-0.36.1.jar")
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar, https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.41.1.jar")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "authkey.json") \
        .getOrCreate())
    
    return spark

spark = spark_init()



# Load Data


clickstream = spark.read.format("parquet") \
    .option("header", "true") \
    .load(f'{input_path}/click_stream_new')

print('Ingested Clickstream Table')


customer = spark.read.format("parquet") \
    .option("header", "true") \
    .load(f'{input_path}/customer')

print('Ingested Customer Table')

transaction = spark.read.format("parquet") \
    .option("header", "true") \
    .load(f'{input_path}/transaction_new')

print('Ingested Transaction Table')


#Transform Dates
transaction = transaction.withColumn("created_at", to_date(col("created_at")))
customer = customer.withColumn("first_join_date", to_date(col("first_join_date")))
clickstream = clickstream.withColumn("event_time", to_date(col("event_time")))

transaction = transaction.withColumn("month", col("created_at"))

min_month = transaction.agg(Fmin("month")).collect()[0][0]
max_month = transaction.agg(Fmax("month")).collect()[0][0]

all_months = spark.range(0, (max_month.year - min_month.year) * 12 + max_month.month - min_month.month + 1) \
    .selectExpr("id as month_offset") \
    .withColumn("month_start", F.add_months(F.lit(min_month), col("month_offset"))) \
    .withColumn("month", F.last_day(col("month_start"))) \
    .drop("month_offset").drop("month_start")

unique_customers = transaction.select("customer_id").distinct()
all_combinations = unique_customers.crossJoin(all_months)

first_purchase = transaction.groupBy("customer_id").agg(Fmin("created_at").alias("first_purchase_date"))
filtered_data = all_combinations.join(first_purchase, "customer_id").filter(col("month") >= col("first_purchase_date"))



transaction = transaction.withColumn("purchase_month", 
                                         F.last_day("created_at"))

    # Aggregate data to calculate last purchase, frequency, and monetary values
fm_monthly = transaction.groupBy("customer_id", "purchase_month").agg(
        F.max("created_at").alias("last_purchase"),  # Recency
        F.countDistinct("session_id").alias("frequency"),  # Unique session count
        F.sum("total_amount").alias("monetary")  # Total spending
    )

fm_monthly = fm_monthly.withColumn("last_purchase", F.to_date("last_purchase"))

fm_monthly = fm_monthly.fillna(0)


trans_monthly_data = filtered_data.join(
    fm_monthly,
    (filtered_data.customer_id == fm_monthly.customer_id) & 
    (filtered_data.month == fm_monthly.purchase_month),
    how="left"
).drop(fm_monthly.customer_id)  # Drop duplicate column after join


trans_monthly_data = trans_monthly_data.withColumn(
        "last_purchase",
        F.coalesce("last_purchase", F.lit("1900-01-01").cast("date"))
    )

window_spec = Window.partitionBy("customer_id").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

trans_monthly_data = trans_monthly_data.withColumn(
        "previous_purchase_month",
        F.max("last_purchase").over(window_spec)
    )

trans_monthly_data = trans_monthly_data.withColumn("month", F.to_date("month"))
trans_monthly_data = trans_monthly_data.withColumn("previous_purchase_month", F.to_date("previous_purchase_month"))

rfm_monthly = trans_monthly_data.withColumn(
        "recency",
        F.datediff("month", "previous_purchase_month")
    )





clickstream = clickstream.withColumn("event_time", F.col("event_time").cast("timestamp"))
customer_sessions = transaction.select("customer_id", "session_id").distinct()

clickstream_cust = customer_sessions.join(clickstream, "session_id", "left")

clickstream_cust = clickstream_cust.withColumn(
    "session_month", F.last_day(F.col("event_time"))
)

clickstream_monthly = clickstream_cust.groupBy("customer_id", "session_month").agg(
    F.countDistinct("session_id").alias("session_count"),
    F.when(F.count("event_time") > 1, (F.max("event_time").cast("long") - F.min("event_time").cast("long")) / 60)
    .otherwise(F.lit(0)).alias("avg_session_duration")  # Handle single-session users
)

clickstream_monthly = clickstream_monthly.fillna({"avg_session_duration": 0})


final_data = rfm_monthly.alias("rfm").join(
    clickstream_monthly.alias("cs"),
    (F.col("rfm.customer_id") == F.col("cs.customer_id")) & 
    (F.col("rfm.month") == F.col("cs.session_month")),
    "left"
).drop(F.col("cs.customer_id")).drop(F.col("cs.session_month"))

final_data = final_data.orderBy(["customer_id", "month"], ascending=[True, False])
final_data = final_data.withColumn(
    "last_purchase_naT",
    F.when(F.col("last_purchase") == F.lit("1900-01-01"), F.lit("2100-01-01")).otherwise(F.col("last_purchase"))
)
window_spec = Window.partitionBy("customer_id").orderBy(F.col("month").desc())
final_data = final_data.withColumn(
    "future_purchase_month",
    F.min("last_purchase_naT").over(window_spec)
)



last_three_months = final_data.select(F.max("month")).collect()[0][0] - F.expr("INTERVAL 3 MONTH")

X_train_full = final_data.filter(F.col("month") <= last_three_months)
X_pred_full = final_data.filter(F.col("month") > last_three_months)

X_train_full = X_train_full.withColumn(
    "date_diff", F.datediff(F.col("future_purchase_month"), F.col("month"))
)

X_train_full = X_train_full.withColumn(
    "churn", F.when(F.col("date_diff") > 90, 1).otherwise(0)
)

X_train_full.write.format("bigquery") \
    .option("table", "data-management-project-452400.data_mgmt_project.churn_X_train_full") \
    .option("temporaryGcsBucket", "data-mgmt-bucket") \
    .mode("overwrite") \
    .save()


print(f'\nSuccessfully Loaded churn_X_train_full\n')


X_pred_full.write.format("bigquery") \
    .option("table", "data-management-project-452400.data_mgmt_project.churn_X_pred_full") \
    .option("temporaryGcsBucket", "data-mgmt-bucket") \
    .mode("overwrite") \
    .save()

print(f'\nSuccessfully Loaded churn_X_pred_full\n')


customer.write.format("bigquery") \
    .option("table", "data-management-project-452400.data_mgmt_project.customer_data") \
    .option("temporaryGcsBucket", "data-mgmt-bucket") \
    .mode("overwrite") \
    .save()


print(f'\nSuccessfully Loaded customer_data\n')


