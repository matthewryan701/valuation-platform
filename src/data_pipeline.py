import pandas as pd 
import yfinance as yf
from src.database import engine

def fetch_multiple_stocks(tickers, period="5y"):
    data = yf.download(tickers, period=period, group_by='ticker', threads=True)
    records = []
    for ticker in tickers:
        df = data[ticker].reset_index()
        df['ticker'] = ticker
        df.rename(columns={"Adj Close": "adj_close"}, inplace=True)
        df = df[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'adj_close', 'Volume']]
        df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)
        records.append(df)
    combined = pd.concat(records)
    combined['date'] = pd.to_datetime(combined['date'])
    combined.drop_duplicates(subset=['ticker', 'date'], inplace=True)
    return combined

def upload_to_supabase(df: pd.DataFrame, table_name: str):
    df.drop_duplicates(subset=['ticker', 'date'], inplace=True)
    df.to_sql("stock_prices", engine, if_exists="append", index=False)