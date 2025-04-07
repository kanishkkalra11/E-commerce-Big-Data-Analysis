from google.cloud import bigquery
import redis

redis_client = redis.Redis()
bq_client = bigquery.Client()


bq_table = "data-management-project-452400.data_mgmt_project.top10_brand_popularity"
query = f"""
SELECT
    *
FROM `{bq_table}`
"""
brands = bq_client.query(query).to_dataframe()
brands = brands['brands'].tolist()

redis_client.delete('top10_brands')
for brand in brands:
    redis_client.rpush('top10_brands', brand)


bq_table = "data-management-project-452400.data_mgmt_project.top10_product_popularity"
query = f"""
SELECT
    *
FROM `{bq_table}`
"""
products = bq_client.query(query).to_dataframe()
products = products['product_id'].tolist()

redis_client.delete('top10_products')
for product in products:
    redis_client.rpush('top10_products', product)


bq_table = "data-management-project-452400.data_mgmt_project.top10_sub_category_popularity"
query = f"""
SELECT
    *
FROM `{bq_table}`
"""
categories = bq_client.query(query).to_dataframe()
categories = categories['subCategory'].tolist()

redis_client.delete('top10_productCategories')
for category in categories:
    redis_client.rpush('top10_productCategories', category)

