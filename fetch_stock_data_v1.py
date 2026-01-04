# ---------------------------------------------
# EARLIER VERSION (ARCHIVED)
# ---------------------------------------------
# This script is the initial implementation of the
# stock data collection system.
# It stores stock OHLC data on a day-to-day basis.
#
# This version is kept for reference and learning
# purposes and is available on GitHub.
# ---------------------------------------------

import os
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()
# -------------------------------
# DATABASE CONFIG
# -------------------------------
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

# -------------------------------
# STOCKS
# -------------------------------
STOCKS = ["NESTLEIND.NS"]

# -------------------------------
# FETCH & STORE
# -------------------------------
for symbol in STOCKS:
    try:
        df = yf.download(symbol, period="1d", interval="1d", auto_adjust=False)

        if df.empty:
            print(f"No data for {symbol}")
            continue

        # 🔴 FIX 1: REMOVE MULTI-INDEX
        df.columns = df.columns.get_level_values(0)

        df.reset_index(inplace=True)

        df = df[['Date', 'Open', 'High', 'Low', 'Close']]
        df['symbol'] = symbol

        df.rename(columns={
            'Date': 'trade_date',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price'
        }, inplace=True)

        df.to_sql(
            'stock_ohlc',
            con=engine,
            if_exists='append',
            index=False
        )

        print(f" Inserted data for {symbol}")

    except Exception as e:

        print(f" Error for {symbol}: {e}")
