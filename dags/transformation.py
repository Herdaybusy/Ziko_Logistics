import pandas as pd

def transform_data(): 
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
    
    # save the data
    ziko_df.to_csv(r'cleaned_data/ziko_cleaned_data.csv', index=False)
    
    # Creating the Customers table
    customer = ziko_df[['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().reset_index(drop=True).drop_duplicates()
    if customer is None:
        raise ValueError('Customers table not created')
    
    # Products Table
    product = ziko_df[['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price']].copy().reset_index(drop=True).drop_duplicates()
    if product is None:
        raise ValueError('Products table not created')
    
    # Transaction Fact Table
    transaction_fact_table = ziko_df.merge(customer, on=['Customer_ID', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address'], how = 'left')\
                                    .merge(product, on=['Product_ID', 'Product_List_Title', 'Quantity', 'Unit_Price'], how = 'left')\
                                    [['Transaction_ID','Customer_ID', 'Product_ID', 'Date', 'Total_Cost', 'Discount_Rate', 'Sales_Channel','Order_Priority', 'Warehouse_Code'\
                                    , 'Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 'Item_Returned', 'Return_Reason', 'Payment_Type', 'Taxable', 'Region', 'Country']]
    if transaction_fact_table is None:
        raise ValueError('transaction_fact_table not created')
    
    # Save tables created to local machine
    customer.to_csv(r'cleaned_data/customer.csv', index=False)
    product.to_csv(r'cleaned_data/product.csv', index=False)
    transaction_fact_table.to_csv(r'cleaned_data/transaction_fact_table.csv', index=False)
    print('Data cleaned, transformed and loaded to local machine Successfully')