from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import lead, unix_timestamp, when, col, count, sum as spark_sum, row_number, max as spark_max
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

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

clickstream_df = spark.read.format("parquet") \
    .option("header", "true") \
    .load(f'{input_path}/click_stream_new')

print('Ingested Clickstream Table')

clickstream_df = clickstream_df.orderBy("session_id", "event_time")

windowSpec = Window.partitionBy("session_id").orderBy("event_time")

# Create the next_event_time column using the lead() function
df = clickstream_df.withColumn("next_event_time", lead("event_time").over(windowSpec))

# Compute dwell_time as the difference (in seconds) between next_event_time and event_time
df = df.withColumn("dwell_time", unix_timestamp("next_event_time") - unix_timestamp("event_time"))
df = df.filter(col("dwell_time") < 85000)

# 1. Assign a row number per session based on event_time order.
sessionWindow = Window.partitionBy("session_id").orderBy("event_time")
df_with_rn = df.withColumn("rn", row_number().over(sessionWindow))

# 2. For every row, compute a flag over the 3 preceding rows (rowsBetween(-3, -1))
# indicating if any of those rows is an ADD_TO_CART event.
w_preceding = Window.partitionBy("session_id").orderBy("rn").rowsBetween(-3, -1)
df_with_rn = df_with_rn.withColumn(
    "preceding_add_flag",
    spark_max(when(col("event_name") == "ADD_TO_CART", 1).otherwise(0)).over(w_preceding)
)

# 3. Extract valid ADD_TO_CART events.
# Only consider ADD_TO_CART events where none of the 3 preceding rows is ADD_TO_CART.
adds = df_with_rn.filter(col("event_name") == "ADD_TO_CART") \
         .filter(col("preceding_add_flag") == 0) \
         .select("session_id", "rn", col("product_id").alias("add_product_id"))

# 4. Identify candidate rows (irrespective of event_name) that have a null product_id.
candidates = df_with_rn.filter(col("product_id").isNull()) \
             .select("session_id", "rn")

# 5. Alias both DataFrames to avoid ambiguous columns.
cand_alias = candidates.alias("cand")
add_alias = adds.alias("add")

# Join candidates with valid ADD_TO_CART events in the same session where:
# candidate row number is between (add.rn - 3) and (add.rn - 1)
join_condition = (
    (col("cand.session_id") == col("add.session_id")) &
    (col("cand.rn") >= col("add.rn") - 3) &
    (col("cand.rn") < col("add.rn"))
)
joined = cand_alias.join(add_alias, join_condition, "inner") \
                   .select(
                        col("cand.session_id").alias("sess"),
                        col("cand.rn").alias("cand_rn"),
                        col("add.rn").alias("add_rn"),
                        col("add.add_product_id")
                   )

# 6. For each candidate (by session and candidate rn), select the ADD_TO_CART event with the smallest difference.
joined = joined.withColumn("diff", col("add_rn") - col("cand_rn"))
w_candidate = Window.partitionBy("sess", "cand_rn").orderBy("diff")
assignments = joined.withColumn("rn_assign", row_number().over(w_candidate)) \
                    .filter(col("rn_assign") == 1) \
                    .select(
                        col("sess").alias("session_id"),
                        col("cand_rn").alias("rn"),
                        col("add_product_id")
                    )

# 7. Join these assignments back to the original DataFrame.
df_updated = df_with_rn.join(assignments, on=["session_id", "rn"], how="left")

# 8. For candidate rows that got an assignment, update product_id.
df_updated = df_updated.withColumn(
    "final_product_id",
    when(col("add_product_id").isNotNull(), col("add_product_id")).otherwise(col("product_id"))
)

# 9. Clean up temporary columns.
result_df = df_updated.drop("rn", "preceding_add_flag", "add_product_id")

result_df = result_df.na.drop(subset=['final_product_id'])
result_df = result_df.drop("product_id") \
    .withColumnRenamed("final_product_id", "product_id") \
    .withColumn("product_id", col("product_id").cast("int"))

trans_df = spark.read.format("parquet") \
    .option("header", "true") \
    .load(f'{input_path}/transaction_new')\
    .select('created_at', 'session_id', 'payment_status', 'product_id', 'quantity')
print('\nIngested transactions Table\n')


click_agg = result_df.filter(col("event_name") == "CLICK") \
    .groupBy("product_id") \
    .agg(count("*").alias("click_count"))

item_detail_agg = result_df.filter(col("event_name") == "ITEM_DETAIL") \
    .groupBy("product_id") \
    .agg(count("*").alias("item_detail_count"))

add_to_cart_agg = result_df.filter(col("event_name") == "ADD_TO_CART") \
    .groupBy("product_id") \
    .agg(count("*").alias("add_to_cart_count"))

homepage_agg = result_df.filter(col("event_name") == "HOMEPAGE") \
    .groupBy("product_id") \
    .agg(count("*").alias("homepage_count"))

dwell_time_agg = result_df.groupBy("product_id") \
    .agg(spark_sum("dwell_time").alias("total_dwell_time"))

purchases_agg = trans_df.groupBy("product_id") \
    .agg(spark_sum("quantity").alias("purchases"))


popularity_df = click_agg.join(item_detail_agg, "product_id", "outer") \
    .join(add_to_cart_agg, "product_id", "outer") \
    .join(dwell_time_agg, "product_id", "outer") \
    .join(homepage_agg, "product_id", "outer") \
    .join(purchases_agg, "product_id", "outer")


popularity_df = popularity_df.na.fill(0)

product_df = spark.read.format("parquet") \
    .option("header", "true") \
    .load(f'{input_path}/product')

print('\nIngested Product Table\n')

final_df = popularity_df.join(product_df, popularity_df.product_id == product_df.id, "inner")

final_df.write.format("bigquery") \
    .option("table", "data-management-project-452400.data_mgmt_project.click_event_agg_test") \
    .option("temporaryGcsBucket", "data-mgmt-bucket") \
    .mode("overwrite") \
    .save()

print(f'\nSuccessfully Loaded click_event_agg_test\n')
