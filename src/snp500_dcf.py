import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import os
from datetime import datetime
import time
import numpy as np

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_sp500_tickers():
    """Fetch current S&P 500 tickers from Wikipedia"""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    sp500_table = tables[0]
    tickers = sp500_table['Symbol'].tolist()
    # Clean tickers (replace dots with dashes for yfinance)
    tickers = [ticker.replace('.', '-') for ticker in tickers]
    return tickers

def calculate_data_quality_score(data):
    """Calculate completeness score based on critical fields"""
    critical_fields = [
        'operating_cash_flow', 'capital_expenditure', 'free_cash_flow',
        'revenue', 'total_debt', 'beta', 'shares_outstanding'
    ]
    
    score = 0
    for field in critical_fields:
        if field in data and data[field] is not None:
            if isinstance(data[field], list):
                # For arrays, check if we have at least 3 years
                if len([x for x in data[field] if x is not None]) >= 3:
                    score += 1
            else:
                score += 1
    
    return score / len(critical_fields)

def scrape_company_dcf_data(ticker):
    """Scrape DCF-relevant data for a single company"""
    try:
        stock = yf.Ticker(ticker)
        
        # Get basic info
        info = stock.info
        
        # Initialize data dictionary
        data = {
            'ticker': ticker,
            'company_name': info.get('longName', ticker),
            'sector': info.get('sector'),
            'last_updated': datetime.now().isoformat(),
            'current_price': info.get('currentPrice') or info.get('regularMarketPrice'),
            'shares_outstanding': info.get('sharesOutstanding'),
            'market_cap': info.get('marketCap'),
            'beta': info.get('beta'),
            'tax_rate': info.get('effectiveTaxRate'),
        }
        
        # Get financial statements
        try:
            # Income statement
            income_stmt = stock.financials
            if not income_stmt.empty:
                # Most recent year
                latest = income_stmt.columns[0]
                data['revenue'] = float(income_stmt.loc['Total Revenue', latest]) if 'Total Revenue' in income_stmt.index else None
                data['operating_income'] = float(income_stmt.loc['Operating Income', latest]) if 'Operating Income' in income_stmt.index else None
                data['net_income'] = float(income_stmt.loc['Net Income', latest]) if 'Net Income' in income_stmt.index else None
        except Exception as e:
            print(f"  Warning: Could not fetch income statement for {ticker}: {e}")
        
        # Balance sheet
        try:
            balance_sheet = stock.balance_sheet
            if not balance_sheet.empty:
                latest = balance_sheet.columns[0]
                data['total_debt'] = float(balance_sheet.loc['Total Debt', latest]) if 'Total Debt' in balance_sheet.index else None
                data['cash_and_equivalents'] = float(balance_sheet.loc['Cash And Cash Equivalents', latest]) if 'Cash And Cash Equivalents' in balance_sheet.index else None
                data['total_equity'] = float(balance_sheet.loc['Stockholders Equity', latest]) if 'Stockholders Equity' in balance_sheet.index else None
        except Exception as e:
            print(f"  Warning: Could not fetch balance sheet for {ticker}: {e}")
        
        # Cash flow statement (historical arrays)
        try:
            cashflow = stock.cashflow
            if not cashflow.empty:
                # Get up to 5 years of data
                years = min(5, len(cashflow.columns))
                
                # Operating cash flow
                if 'Operating Cash Flow' in cashflow.index:
                    data['operating_cash_flow'] = [float(cashflow.loc['Operating Cash Flow', cashflow.columns[i]]) 
                                                   for i in range(years)]
                
                # Capital expenditure (usually negative)
                if 'Capital Expenditure' in cashflow.index:
                    data['capital_expenditure'] = [float(cashflow.loc['Capital Expenditure', cashflow.columns[i]]) 
                                                   for i in range(years)]
                
                # Free cash flow
                if 'Free Cash Flow' in cashflow.index:
                    data['free_cash_flow'] = [float(cashflow.loc['Free Cash Flow', cashflow.columns[i]]) 
                                             for i in range(years)]
                elif data.get('operating_cash_flow') and data.get('capital_expenditure'):
                    # Calculate FCF if not directly available
                    data['free_cash_flow'] = [ocf + capex for ocf, capex in 
                                             zip(data['operating_cash_flow'], data['capital_expenditure'])]
        except Exception as e:
            print(f"  Warning: Could not fetch cash flow for {ticker}: {e}")
        
        # Calculate growth metrics
        try:
            if data.get('free_cash_flow') and len(data['free_cash_flow']) >= 3:
                fcf_recent = data['free_cash_flow'][0]
                fcf_3y_ago = data['free_cash_flow'][min(2, len(data['free_cash_flow'])-1)]
                if fcf_3y_ago and fcf_3y_ago > 0:
                    data['fcf_growth_3y'] = (fcf_recent / fcf_3y_ago) ** (1/3) - 1
        except:
            pass
        
        # Risk-free rate (use 10-year Treasury as proxy, ~4.5% as of late 2024)
        data['risk_free_rate'] = 0.045  # Update this periodically or fetch from FRED API
        
        # Calculate data quality score
        data['data_quality_score'] = calculate_data_quality_score(data)
        data['scrape_status'] = 'success' if data['data_quality_score'] > 0.5 else 'partial'
        data['error_message'] = None
        
        return data
        
    except Exception as e:
        print(f"  Error scraping {ticker}: {str(e)}")
        return {
            'ticker': ticker,
            'scrape_status': 'failed',
            'error_message': str(e),
            'last_updated': datetime.now().isoformat()
        }

def upload_to_supabase(data):
    """Upload data to Supabase table"""
    try:
        # Upsert (insert or update if ticker exists)
        response = supabase.table('SNP500_DCF_Variables').upsert(data).execute()
        return True
    except Exception as e:
        print(f"  Error uploading to Supabase: {e}")
        return False

def main():
    print("Fetching S&P 500 tickers...")
    tickers = get_sp500_tickers()
    print(f"Found {len(tickers)} tickers")
    
    successful = 0
    failed = 0
    
    for i, ticker in enumerate(tickers):
        print(f"\n[{i+1}/{len(tickers)}] Processing {ticker}...")
        
        # Scrape data
        data = scrape_company_dcf_data(ticker)
        
        # Upload to Supabase
        if upload_to_supabase(data):
            if data['scrape_status'] == 'success':
                successful += 1
                print(f"  ✓ Successfully scraped and uploaded (quality: {data['data_quality_score']:.2f})")
            else:
                print(f"  ⚠ Partial data uploaded")
        else:
            failed += 1
            print(f"  ✗ Failed to upload")
        
        # Rate limiting (be nice to Yahoo Finance)
        time.sleep(0.5)
    
    print(f"\n{'='*50}")
    print(f"Scraping complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()