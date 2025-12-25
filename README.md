# Stock OHLC Data Pipeline

An automated system to collect and store daily OHLC (Open, High, Low, Close)
stock market data for analysis and research.

## Features
- Automated daily OHLC data collection
- Supports configurable stock symbols
- Stores data in CSV format
- Designed to run on a scheduled basis

## Tech Stack
- Python
- Stock market data API
- Cron / Task Scheduler
- CSV storage

## How It Works
1. A scheduler triggers the script daily
2. OHLC data is fetched from the market API
3. Data is cleaned and validated
4. Data is stored locally for future analysis

## Setup
```bash
pip install -r requirements.txt
python src/fetch_ohlc.py

