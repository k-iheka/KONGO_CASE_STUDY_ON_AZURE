import psycopg2
import pandas as pd
import io
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient


# Define database parameters
db_params = {
    'user':'internship',
    'password':'10alytics!',
    'host':'10alyticsinternship.postgres.database.azure.com',
    'port': '5432',
    'database':'kongoecommerce'
}

def extract_table_to_dataframe(table_name, conn):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    return df 

# Extracting df
connection = psycopg2.connect(**db_params)
# Extracting Cart data 
cart_df = extract_table_to_dataframe('cart', connection)

# Extracting products data 
products_df = extract_table_to_dataframe('products', connection)

# Extracting sales data 
sales_df = extract_table_to_dataframe('sales', connection)
# Extracting users data 
users_df = extract_table_to_dataframe('users', connection)
users_df.head()

#Save to csv
cart_df.to_csv('dataset/raw_data/cart.csv', index=False)
products_df.to_csv('dataset/raw_data/products.csv', index=False)
sales_df.to_csv('dataset/raw_data/sales.csv', index=False)
users_df.to_csv('dataset/raw_data/users.csv', index=False)
print('tables saved successfully')

r_cart = pd.read_csv(r'dataset/raw_data/cart.csv')
r_product = pd.read_csv(r'dataset/raw_data/products.csv')
r_sales = pd.read_csv(r'dataset/raw_data/sales.csv')
r_user = pd.read_csv(r'dataset/raw_data/users.csv')

cart_df.info()
cart_df['date'] = pd.to_datetime(cart_df['date'])
products_df.rename(columns={'id':'product_id'}, inplace=True)
sales_df['Transaction_Date'] = pd.to_datetime(sales_df['Transaction_Date'])

location_dim = users_df[['address_city','address_street', 'address_number', 'address_zipcode', \
                         'address_geolocation_lat', 'address_geolocation_long']].copy().drop_duplicates().reset_index(drop=True)

location_dim.index.name = 'location_id'
location_dim = location_dim.reset_index()

user_dim = users_df.merge(location_dim, on=['address_city','address_street', 'address_number', 'address_zipcode', \
                                            'address_geolocation_lat', 'address_geolocation_long'], how='left') \
                           [['id', 'email', 'username', 'password', 'phone', 'name_firstname','name_lastname','location_id']]

user_dim.rename(columns={'id':'customer__id'}, inplace=True)
print( 'transformation has been completed')

cart_df.to_csv('dataset/cleaned_data/clean_cart.csv', index=False)
user_dim.to_csv('dataset/cleaned_data/clean_user.csv', index=False)
location_dim.to_csv('dataset/cleaned_data/clean_location.csv', index=False)
products_df.to_csv('dataset/cleaned_data/clean_products.csv', index=False)
sales_df.to_csv('dataset/cleaned_data/clean_sales.csv', index=False)
print('cleaned data successfully saved')

# Setup our connection
load_dotenv()
connection_string = os.getenv('AZURE_CONNECTION_STRING')
container_name = os.getenv('AZURE_CONTAINER_NAME')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

def upload_df_to_blob_container(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob",overwrite=True)
    print(f'{blob_name} uploaded to Blob storage successfully')

upload_df_to_blob_container(r_cart, container_client, 'rawdata/cart.parquet')
upload_df_to_blob_container(r_product, container_client, 'rawdata/product.parquet')
upload_df_to_blob_container(r_sales, container_client, 'rawdata/sales.parquet')
upload_df_to_blob_container(r_user, container_client, 'rawdata/user.parquet')

upload_df_to_blob_container(cart_df, container_client, 'cleandata/cart_dim.parquet')
upload_df_to_blob_container(products_df, container_client, 'cleandata/product_dim.parquet')
upload_df_to_blob_container(sales_df, container_client, 'cleandata/sales_fact.parquet')
upload_df_to_blob_container(user_dim, container_client, 'cleandata/user_dim.parquet')
upload_df_to_blob_container(location_dim, container_client, 'cleandata/location_dim.parquet')