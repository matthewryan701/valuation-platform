from fredapi import Fred
from supabase import create_client
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# initialize api keys, supabase
fred = Fred(api_key=os.getenv('FRED_API_KEY'))

supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

# get cpi (all urban consumers) data from fred
cpi_data = fred.get_series('CPIAUCSL', observation_start='2010-10-28')
df_cpi = cpi_data.reset_index()
df_cpi.columns = ['date', 'cpi_value']
df_cpi['date'] = df_cpi['date'].astype(str)

# insert to supabase
data_to_insert = df_cpi.to_dict('records')
response = supabase.table('cpi_data').insert(data_to_insert).execute()

