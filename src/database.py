from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import yfinance as yf

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)