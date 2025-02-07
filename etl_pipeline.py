# Import Necessary libraries
import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv

# Extraction layer
ziko_df = pd.read_csv(r'ziko_logistics_data.csv')

# Filling the missing values
ziko_df.fillna({
    "Unit_Price" : ziko_df["Unit_Price"].mean(),
    "Total_Cost" : ziko_df["Total_Cost"].mean(),
    "Discount_Rate" : 0.0,
    "Return_Reason" : "Unknown"
}, inplace=True)

# Changing the date format to datetime 
ziko_df['Date'] = pd.to_datetime(ziko_df['Date'])
ziko_df.info()

# Creating the Customer table
customer = ziko_df[['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().reset_index(drop=True).drop_duplicates()

# Product Table
product = ziko_df[['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price']].copy().reset_index(drop=True).drop_duplicates()

# Transaction Fact Table
transaction_fact_table = ziko_df.merge(customer, on=['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address'], how = 'left')\
                                .merge(product, on=['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price'], how = 'left')\
                                [['Transaction_ID','Customer_ID', 'Product_ID', 'Date', 'Total_Cost', 'Discount_Rate', 'Sales_Channel','Order_Priority', 'Warehouse_Code'\
                                , 'Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 'Item_Returned', 'Return_Reason', 'Payment_Type', 'Taxable', 'Region', 'Country']]

# Save tables created to local machine
customer.to_csv(r'cleaned_data/customer.csv', index=False)
product.to_csv(r'cleaned_data/product.csv', index=False)
transaction_fact_table.to_csv(r'cleaned_data/transaction_fact_table.csv', index=False)

print('File loaded to local machine Successfully')

# Data Loading to Azure Blob Storage
load_dotenv(override=True)
connect_str = os.getenv('connect_str')
container_name = os.getenv('container_name')

# CONNECTION TO AZURE BLOB STORAGE
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# function that would upload files as paquet to azure blob storage
def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    block_client = container_client.get_blob_client(blob_name)
    block_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f'{blob_name} uploaded to Azure Blob Storage successfully')

# %%
upload_df_to_blob_as_parquet(customer, container_client, 'rawdata/customer.parquet') 
upload_df_to_blob_as_parquet(product, container_client, 'rawdata/product.parquet') 
upload_df_to_blob_as_parquet(transaction_fact_table, container_client, 'rawdata/transaction_fact_table.parquet') 
