import pandas as pd

def extract_data():
    try:
        ziko_df = pd.read_csv(r'ziko_logistics_data.csv')
        print('Data Extracted successfully')
    except Exception as e:
        print (f'an error occured: {e}')