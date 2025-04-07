from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import requests

sc = SparkContext("local", "analysis")
spark = SparkSession(sc)

path = 'gs://data-mgmt-bucket'

customers = spark.read.format("parquet").option("header", "true").load(f"{path}/parquet/customer")
customers = customers.withColumnRenamed("customer_id", "new_customer_id")

transactions = spark.read.format("parquet").option("header", "true").load(f"{path}/parquet/transaction_new")

products = spark.read.format("parquet").option("header", "true").load(f"{path}/parquet/product")
products = products.withColumnRenamed("gender", "product_gender")

tmp1 = transactions.join(customers, transactions.customer_id==customers.new_customer_id, 'left')
tmp2 = tmp1.join(products, tmp1.product_id==products.id, 'left')
fin_df = tmp2.drop('new_customer_id', 'id')
fin_df = fin_df.withColumn("age", functions.months_between(fin_df["created_at"], fin_df["birthdate"]) / 12)


def get_lat_lon(city_name, api_key):
    # Google Maps Geocoding API URL
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city_name},Indonesia&key={api_key}"
    response = requests.get(url)
    data = response.json()
    if data['status'] == 'OK':
        lat = data['results'][0]['geometry']['location']['lat']
        lon = data['results'][0]['geometry']['location']['lng']
        return lat, lon
    else:
        return None, None


api_key = 'AIzaSyAjThmUgj02YW9ItPPm7Vbt3lfMVQNXbIw'
#customer = spark.read.csv("customer.csv", header=True, inferSchema=True)

@udf(returnType=DoubleType())
def get_lat(city_name):
    lat, _ = get_lat_lon(city_name, api_key)
    return lat

@udf(returnType=DoubleType())
def get_lon(city_name):
    _, lon = get_lat_lon(city_name, api_key)
    return lon

tmp3 = fin_df.select('home_location').distinct()
# Add columns for central latitude and longitude
tmp3 = tmp3.withColumn("central_latitude", get_lat("home_location")) \
                                .withColumn("central_longitude", get_lon("home_location"))
tmp3 = tmp3.withColumnRenamed("home_location", "home_location_new")

fin_df2 = fin_df.join(tmp3, fin_df.home_location==tmp3.home_location_new, 'left')
fin_df2 = fin_df2.drop('home_location_new')

fin_df2.write.format("bigquery") \
    .option("table", "data-management-project-452400:data_mgmt_project.visualization_table") \
    .option("temporaryGcsBucket", "data-mgmt-bucket") \
    .mode("overwrite") \
    .save()

