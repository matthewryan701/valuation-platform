import pandas as pd 
from yahoo_fin import stock_info as si
from src.database import engine
import requests
import yfinance as yf
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()  

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    response = requests.get(url, headers=headers)
    tables = pd.read_html(response.text)
    df = tables[0]
    tickers = df['Symbol'].tolist()
    tickers = [t.replace(".", "-") for t in tickers]  
    return tickers

def fetch_multiple_stocks(tickers, period="5y"):
    data = yf.download(tickers, period=period, group_by='ticker', threads=True)
    records = []
    for ticker in tickers:
        try:
            df = data[ticker].reset_index()
            df['ticker'] = ticker
            df = df[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }, inplace=True)
            records.append(df)
        except Exception as e:
            print(f"⚠️ Error processing {ticker}: {e}")
    combined = pd.concat(records)
    combined['date'] = pd.to_datetime(combined['date'])
    combined.drop_duplicates(subset=['ticker', 'date'], inplace=True)
    print(f"✅ Downloaded {len(combined)} rows for {len(tickers)} tickers")
    return combined

def upload_to_supabase(df: pd.DataFrame, table_name: str = "stock_prices"):
    if df.empty:
        print("⚠️ No data to upload.")
        return
    
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    
    data = df.to_dict(orient="records")
    for chunk_start in range(0, len(data), 500):
        chunk = data[chunk_start:chunk_start + 500]
        supabase.table(table_name).insert(chunk).execute()
    
    print(f"✅ Uploaded {len(df)} rows to '{table_name}' via Supabase client.")


def batch_download_and_upload(all_tickers, batch_size=20):
    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1}: {batch}")
        try:
            df = fetch_multiple_stocks(batch)
            upload_to_supabase(df, "stock_prices")
        except Exception as e:
            print(f"Error in batch {i // batch_size + 1}: {e}")

if __name__ == "__main__":
    all_tickers = get_sp500_tickers()
    batch_download_and_upload(all_tickers, batch_size=10)