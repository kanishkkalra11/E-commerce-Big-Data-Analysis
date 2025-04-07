import os
import pandas as pd
from google.cloud import bigquery
import json

from google.cloud import storage


PROJECT_ID = "data-management-project-452400"
BUCKET_NAME = "data-mgmt-bucket"

# BigQuery details
BQ_DATASET_NAME = "data_mgmt_project"
BQ_PRODUCT_POPULARITY_TABLE_NAME = "top10_product_popularity"
BQ_BRAND_POPULARITY_TABLE_NAME = "top10_brand_popularity"
BQ_sub_category_POPULARITY_TABLE_NAME = "top10_sub_category_popularity"
BQ_gender_season_POPULARITY_TABLE_NAME = "top_10_products_by_season_gender"


POP_WEIGHTS_PATH = 'gs://data-mgmt-bucket/feature_importance.json'
# Set up Google Cloud authentication (Ensure you've set up your service account JSON)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'authkey.json'

def load_to_bq(client, df, table_path):
    job = client.load_table_from_dataframe(df, table_path, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    return job.result()

# ============================
# CONNECT TO BIGQUERY
# ============================

# Initialize BigQuery Client
client = bigquery.Client()

# Define your BigQuery table
bq_table = "data-management-project-452400.data_mgmt_project.brands_data"

# Query BigQuery to get the data
query = f"""
SELECT 
    *
FROM `{bq_table}`
"""

# Load Data into Pandas DataFrame
df = client.query(query).to_dataframe()


# Read in the weights for product popularity computation from GCS
bucket_name = BUCKET_NAME # Replace with actual bucket name
file_name = "feature_importance.json"  # JSON file stored in GCS

# Initialize GCS client
client = storage.Client()

# Get the GCS bucket and file
bucket = client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Download JSON file as text and load it as a dictionary
json_data = json.loads(blob.download_as_text())

# Convert to Pandas Series instead of DataFrame
weights = pd.Series(json_data)

df["popularity_score"] = (df["click_count"] * weights["click_count"] +
    df["item_detail_count"] * weights["item_detail_count"] +
    df["add_to_cart_count"] * weights["add_to_cart_count"] +
    df["total_dwell_time"] * weights["total_dwell_time"] +
    df["homepage_count"] * weights["homepage_count"]+
    df["purchases"] * weights["purchases"])/6

df["popularity_score"] = (df["popularity_score"] - df["popularity_score"].min()) / (df["popularity_score"].max() - df["popularity_score"].min())

########## Most Popular Products ##########
# Get top 10 most popular products
df_popular_products = df.sort_values(by="popularity_score", ascending=False).head(10)

try:
    client = bigquery.Client()
    BQ_PATH = f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_PRODUCT_POPULARITY_TABLE_NAME}"
    job = client.load_table_from_dataframe(df_popular_products, BQ_PATH, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    print(f'Succussfully Upload table: {BQ_PRODUCT_POPULARITY_TABLE_NAME}')
    
except Exception as e:
    print(e)
########## Most Popular Brands ##########
brand_popularity = df.groupby("brands", as_index=False)["popularity_score"].sum()
brand_popularity["popularity_score"] = (brand_popularity["popularity_score"] - brand_popularity["popularity_score"].min()) / (brand_popularity["popularity_score"].max() - brand_popularity["popularity_score"].min())
brand_popularity = brand_popularity.sort_values(by="popularity_score", ascending=False).head(10)

try:
    client = bigquery.Client()
    BQ_PATH = f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_BRAND_POPULARITY_TABLE_NAME}"
    job = client.load_table_from_dataframe(brand_popularity, BQ_PATH, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    print (f'Succussfully Upload table: {BQ_BRAND_POPULARITY_TABLE_NAME}')

except Exception as e:
    print("Error loading to BigQuery ", e)
########## Most Popular Sub Categories ##########
subcategory_popularity = df.groupby("subCategory", as_index=False)["popularity_score"].sum()
subcategory_popularity["popularity_score"] = (subcategory_popularity["popularity_score"] - subcategory_popularity["popularity_score"].min()) / (subcategory_popularity["popularity_score"].max() - subcategory_popularity["popularity_score"].min())
subcategory_popularity = subcategory_popularity.sort_values(by="popularity_score", ascending=False).head(10)
try:
    client = bigquery.Client()
    BQ_PATH = f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_sub_category_POPULARITY_TABLE_NAME}"
    job = client.load_table_from_dataframe(subcategory_popularity, BQ_PATH, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    print (f'\nSuccussfully Upload table: {BQ_sub_category_POPULARITY_TABLE_NAME}\n')
except Exception as e:
    print(e)
########## Most Popular Products by Season and Gender ##########
# ============================
# CONNECT TO BIGQUERY
# ============================

# Initialize BigQuery Client
client = bigquery.Client()

# Define your BigQuery table
bq_table = "data-management-project-452400.data_mgmt_project.click_event_agg"

# Query BigQuery to get the data
query = f"""
SELECT 
    product_id, productDisplayName, gender
FROM `{bq_table}`
"""

# Load Data into Pandas DataFrame
df1 = client.query(query).to_dataframe()
df_final = pd.merge(df, df1, left_on="product_id", right_on="product_id", how="outer")

top_10_products_by_season_gender = (
    df_final.groupby(["season", "gender"])
    .apply(lambda x: x.nlargest(10, "popularity_score"))
    .reset_index(drop=True)
)

try:
    client = bigquery.Client()
    BQ_PATH = f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_gender_season_POPULARITY_TABLE_NAME}"
    job = client.load_table_from_dataframe(top_10_products_by_season_gender, BQ_PATH, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
    print (f'Succussfully Upload table: {BQ_gender_season_POPULARITY_TABLE_NAME}')
except Exception as e:
    print(e)
