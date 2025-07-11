import json
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import logging
import time
from datetime import datetime
import os
import csv

from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

from options_scraper.scraper import NASDAQOptionsScraper

# --- Configuration ---
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s :: [%(levelname)s] :: %(message)s")

TICKERS_TO_SCRAPE = ["AMD"]
DATA_STORE_DIR = "data_store"

# --- Helper Functions for Safe Data Conversion ---

def safe_to_int(value):
    """Safely converts a value to an integer, returning 0 on failure."""
    try:
        return int(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return 0

def safe_to_float(value):
    """Safely converts a value to a float, returning 0.0 on failure."""
    try:
        return float(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return 0.0

# --- Background Scheduler Functions ---

def scrape_and_save(market_session: str):
    """
    Fetches options data for a list of tickers and appends it to a persistent CSV store.
    """
    log_prefix = f"[Scheduler - {market_session}]"
    LOG.info(f"{log_prefix} --- Starting scrape for tickers: {TICKERS_TO_SCRAPE} ---")
    scraper = NASDAQOptionsScraper()
    scrape_timestamp = datetime.now()

    for ticker in TICKERS_TO_SCRAPE:
        LOG.info(f"{log_prefix} Processing {ticker}...")
        # We handle errors for individual dates inside process_single_expiry,
        # so this outer try/except is for more general network/API failures.
        try:
            expiration_dates = scraper.get_expiration_dates(ticker)
            if not expiration_dates:
                LOG.warning(f"{log_prefix} No expiration dates found for {ticker}.")
                continue

            LOG.info(f"{log_prefix} Found {len(expiration_dates)} expiration dates for {ticker}.")
            for expiry in expiration_dates:
                # The core logic is now handled in the helper function
                process_single_expiry(scraper, ticker, expiry, scrape_timestamp, market_session, log_prefix)
        
        except Exception as e:
            # This will catch broader issues, like failing to get the expiration dates list
            LOG.error(f"{log_prefix} A critical error occurred while processing {ticker}: {e}")

    LOG.info(f"{log_prefix} --- Completed scrape for all tickers ---")


def process_single_expiry(scraper, ticker, expiry, scrape_timestamp, market_session, log_prefix):
    """
    Helper function to scrape and save data for a single expiration date.
    This function now contains its own error handling.
    """
    ticker_dir = os.path.join(DATA_STORE_DIR, ticker)
    if not os.path.exists(ticker_dir):
        os.makedirs(ticker_dir)
    file_path = os.path.join(ticker_dir, f"{expiry}.csv")

    try:
        # --- THIS IS THE KEY CHANGE ---
        # The call to the scraper is now wrapped in a try/except block.
        records = list(scraper(ticker, expiry=expiry))
        if not records:
            LOG.warning(f"{log_prefix} No records found for {ticker} on {expiry}.")
            return
            
    except TypeError:
        # This specifically catches the 'NoneType' is not iterable error.
        LOG.error(f"{log_prefix} Corrupted data returned from API for {ticker} (Expiry: {expiry}). Skipping this date.")
        return # Exit this function for this date and allow the main loop to continue

    file_exists = os.path.isfile(file_path)
    with open(file_path, 'a', newline='') as csv_file:
        # ... (the rest of the file writing logic remains the same)
        headers = ['timestamp', 'market_session', 'strike', 'type', 'open_interest', 'volume', 'bid', 'ask']
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        
        for record in records:
            option_type = 'call' if record.get('Calls') else 'put'
            writer.writerow({
                'timestamp': scrape_timestamp.isoformat(),
                'market_session': market_session,
                'strike': safe_to_float(record.get('Strike')),
                'type': option_type,
                'open_interest': safe_to_int(record.get('Open Int')),
                'volume': safe_to_int(record.get('Vol')),
                'bid': safe_to_float(record.get('Bid')),
                'ask': safe_to_float(record.get('Ask')),
            })
            
    LOG.info(f"{log_prefix} Appended {len(records)} records for {ticker} (Expiry: {expiry})")


# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)
api_scraper = NASDAQOptionsScraper() # Instance for on-demand API calls

# --- UI Data Processing and API Endpoints ---

def preprocess_for_chart(raw_data, last_price=None):
    """Aggregates raw options data and calculates walls for visualization."""
    if not raw_data: return {}
    df = pd.DataFrame(raw_data)
    for col in ['volume', 'open_interest']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    agg_df = df.groupby(['strike', 'type']).sum().unstack(fill_value=0)
    agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]
    for col in ['open_interest_call', 'open_interest_put', 'volume_call', 'volume_put']:
        if col not in agg_df.columns:
            agg_df[col] = 0
    agg_df = agg_df.sort_index()
    call_wall_strike = agg_df['open_interest_call'].idxmax() if not agg_df['open_interest_call'].empty else 0
    put_wall_strike = agg_df['open_interest_put'].idxmax() if not agg_df['open_interest_put'].empty else 0
    agg_df['cumulative_oi'] = (agg_df['open_interest_call'] + agg_df['open_interest_put']).cumsum()
    agg_df['cumulative_vol'] = (agg_df['volume_call'] + agg_df['volume_put']).cumsum()
    return {
        'labels': [f"{s:.2f}" for s in agg_df.index.tolist()],
        'call_oi': agg_df['open_interest_call'].tolist(),
        'put_oi': (-agg_df['open_interest_put']).tolist(),
        'call_vol': agg_df['volume_call'].tolist(),
        'put_vol': (-agg_df['volume_put']).tolist(),
        'cumulative_oi': agg_df['cumulative_oi'].tolist(),
        'cumulative_vol': agg_df['cumulative_vol'].tolist(),
        'last_price': last_price, 'call_wall_strike': call_wall_strike, 'put_wall_strike': put_wall_strike,
    }

@app.route('/api/expirations/<string:ticker>', methods=['GET'])
def get_expirations(ticker):
    """API endpoint to fetch available expiration dates for a ticker."""
    if not ticker:
        return jsonify({"error": "Ticker symbol is required."}), 400
    dates = api_scraper.get_expiration_dates(ticker)
    if dates is None:
        return jsonify({"error": "Failed to fetch expiration dates from NASDAQ."}), 500
    return jsonify(dates)

@app.route('/api/chart_data/<string:ticker>/<string:expiry>', methods=['GET'])
def get_chart_data(ticker, expiry):
    """API endpoint to fetch and process options data for the chart."""
    if not ticker or not expiry:
        return jsonify({"error": "Ticker and expiry date are required."}), 400
    
    stock_info = api_scraper.get_stock_info(ticker)
    last_price = stock_info.get('last_price') if stock_info else None

    raw_data = []
    for record in api_scraper(ticker, expiry=expiry):
        if record.get('Calls'):
            raw_data.append({'type': 'call', 'strike': safe_to_float(record.get('Strike')), 'volume': safe_to_int(record.get('Vol')), 'open_interest': safe_to_int(record.get('Open Int'))})
        elif record.get('Puts'):
             raw_data.append({'type': 'put', 'strike': safe_to_float(record.get('Strike')), 'volume': safe_to_int(record.get('Vol')), 'open_interest': safe_to_int(record.get('Open Int'))})

    if not raw_data:
        return jsonify({"error": "No data found for the selected criteria."}), 404
        
    processed_data = preprocess_for_chart(raw_data, last_price=last_price)
    return jsonify(processed_data)

# --- Main Execution ---
if __name__ == '__main__':
    scheduler = BackgroundScheduler(timezone=timezone('US/Eastern'))
    scheduler.add_job(scrape_and_save, 'cron', day_of_week='mon-fri', hour=9, minute=30, args=['pre_market'])
    scheduler.add_job(scrape_and_save, 'cron', day_of_week='mon-fri', hour=16, minute=0, args=['post_market'])
    scheduler.start()
    LOG.info("Background scheduler started.")
    
    app.run(debug=True, use_reloader=False, port=5000)