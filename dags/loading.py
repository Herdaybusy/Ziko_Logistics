# Import Necessary libraries
import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv

ziko_df = pd.read_csv('/root/ziko_logistics/cleaned_data/ziko_cleaned_data.csv')

def load_data(df, container_client, blob_name):
    customer = pd.read_csv(r'cleaned_data/customer.csv')
    product = pd.read_csv(r'cleaned_data/product.csv')
    transaction_fact_table = pd.read_csv(r'cleaned_data/transaction_fact_table.csv')
    
    # Data Loading to Azure Blob Storage
    load_dotenv(override=True)
    connect_str = os.getenv('connect_str')
    container_name = os.getenv('container_name')

    # CONNECTION TO AZURE BLOB STORAGE
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Loading data to Azure Blob Storage
    files = [
        (ziko_df, r'Cleaned_Data\ziko_cleaned_data.csv' ),
        (product, r'Cleaned_Data\Products data.csv'),
        (customer, r'Cleaned_Data\Customers data.csv'),
        (transaction_fact_table, r'Cleaned_Data\transaction_fact_table.csv'),
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob Storage')