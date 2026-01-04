import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import os, logging
from dotenv import load_dotenv

# ---------- CONFIG ----------
load_dotenv()
STOCKS = ["NESTLEIND.NS"]
DATABASE_URL = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

# Force logging to show in terminal
logging.basicConfig(level=logging.INFO, format="%(message)s")

def get_existing_dates(symbol):
    query = text("SELECT trade_date FROM stock_ohlc WHERE symbol = :symbol")
    with engine.connect() as conn:
        result = conn.execute(query, {"symbol": symbol})
        return {row[0] for row in result}

for symbol in STOCKS:
    print(f"\n>>> Checking {symbol}...")
    
    # 1. Download Data
    # We use auto_adjust=True to simplify columns
    df = yf.download(symbol, period="35d", interval="1d", auto_adjust=True, progress=False)
    
    if df.empty:
        print(f"❌ ERROR: Yahoo Finance returned NO data for {symbol}. (Check internet or symbol)")
        continue

    # 2. Fix yfinance MultiIndex (This is likely why your previous code failed)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # 3. Filter for NEW dates
    df.index = pd.to_datetime(df.index).date
    existing = get_existing_dates(symbol)
    new_data = df[~df.index.isin(existing)].copy()

    print(f"   Found {len(df)} total rows. {len(new_data)} are NEW.")

    if not new_data.empty:
        # 4. Format for Database
        new_data = new_data.reset_index().rename(columns={
            'index': 'trade_date', 'Open': 'open_price', 
            'High': 'high_price', 'Low': 'low_price', 'Close': 'close_price'
        })
        new_data['symbol'] = symbol
        
        # 5. Insert
        new_data[['symbol', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price']].to_sql(
            'stock_ohlc', con=engine, if_exists='append', index=False, method='multi'
        )
        print(f"   ✅ SUCCESS: {len(new_data)} rows inserted.")
    else:
        print("   ⏭️ SKIPPED: All data already exists in DB.")

print("\nAll tasks finished.")