from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import yfinance as yf

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

def upload_to_supabase(df: pd.DataFrame, table_name: str):
    df.drop_duplicates(subset=['ticker', 'date'], inplace=True)
    df.to_sql(stock_prices, engine, if_exists="append", index=False)
