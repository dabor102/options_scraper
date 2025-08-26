#server.py
import json
from flask import Flask, jsonify
from flask_cors import CORS
import logging
from datetime import datetime
import yfinance as yf
from py_vollib.black_scholes.implied_volatility import implied_volatility as iv
from py_vollib.black_scholes.greeks.analytical import delta as calculate_delta, gamma as calculate_gamma

from options_scraper.scraper import NASDAQOptionsScraper


# --- Configuration ---
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s :: [%(levelname)s] :: %(message)s")

# --- Flask App Initialization ---
app = Flask(__name__)
CORS(app)
api_scraper = NASDAQOptionsScraper() # Instance for on-demand API calls

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
    

# --- Helper Function to get risk free rate ---

def get_risk_free_rate(time_to_expiration):
    """
    Fetches an appropriate risk-free rate from yfinance.
    - Uses 13-Week Treasury Bill (^IRX) for options expiring in < 1 year.
    - Uses 10-Year Treasury Note (^TNX) for options expiring in >= 1 year.
    """
    # Choose the ticker based on the option's duration
    if time_to_expiration < 1.0:
        rate_ticker = "^IRX"
        rate_name = "13-Week Treasury Bill"
    else:
        rate_ticker = "^TNX"
        rate_name = "10-Year Treasury Note"
    
    try:
        ticker_obj = yf.Ticker(rate_ticker)
        hist = ticker_obj.history(period="5d")
        if not hist.empty:
            rate = hist['Close'].iloc[-1] / 100
            LOG.info(f"Using {rate_name} for risk-free rate: {rate:.4f}")
            return rate
    except Exception as e:
        LOG.warning(f"Could not fetch live rate for {rate_ticker}: {e}. Falling back.")
    
    return 0.045 # Fallback to a default value

# --- API Endpoints ---

@app.route('/api/expirations/<string:ticker>', methods=['GET'])
def get_expirations(ticker):
    """API endpoint to fetch available expiration dates for a ticker."""
    LOG.info(f"--- Endpoint /api/expirations/{ticker} HIT ---") # <-- ADD THIS LINE
    if not ticker:
        return jsonify({"error": "Ticker symbol is required."}), 400
    try:
        dates = api_scraper.get_expiration_dates(ticker)
        if dates is None:
            return jsonify({"error": "Failed to fetch expiration dates from NASDAQ."}), 500
        return jsonify(dates)
    except Exception as e:
        LOG.error(f"Error fetching expirations for {ticker}: {e}")
        return jsonify({"error": "An internal error occurred."}), 500

@app.route('/api/stock_info/<string:ticker>', methods=['GET'])
def get_stock_price(ticker):
    """API endpoint to fetch the last price for a ticker."""
    if not ticker:
        return jsonify({"error": "Ticker symbol is required."}), 400
    try:
        stock_info = api_scraper.get_stock_info(ticker)
        if not stock_info or 'last_price' not in stock_info:
             return jsonify({"error": "Failed to fetch stock info from NASDAQ."}), 500
        return jsonify({"last_price": stock_info['last_price']})
    except Exception as e:
        LOG.error(f"Error fetching stock info for {ticker}: {e}")
        return jsonify({"error": "An internal error occurred."}), 500

@app.route('/api/options_chain/<string:ticker>/<string:expiry>', methods=['GET'])
def get_options_chain(ticker, expiry):
    """
    API endpoint to fetch the full options chain, calculate Implied Volatility,
    Delta, Gamma, and return the formatted data.
    """
    if not ticker or not expiry:
        return jsonify({"error": "Ticker and expiry date are required."}), 400

    try:
        LOG.info(f"Fetching chain for {ticker} expiring on {expiry}.")
        records = list(api_scraper(ticker, expiry=expiry))
        stock_info = api_scraper.get_stock_info(ticker)

        if not records or not stock_info:
            return jsonify({"error": "No data found for the selected criteria."}), 404

        S = stock_info['last_price']
        expiry_date = datetime.strptime(expiry, '%Y-%m-%d')
        time_to_expiration = (expiry_date - datetime.utcnow()).days / 365.0
        if time_to_expiration <= 0: time_to_expiration = 0.00001
        r = get_risk_free_rate(time_to_expiration)

        calls_data, puts_data = [], []

        for record in records:
            bid, ask = safe_to_float(record.get('Bid')), safe_to_float(record.get('Ask'))
            if bid <= 0 or ask <= 0: continue

            market_price = (bid + ask) / 2
            K = safe_to_float(record.get('Strike'))
            option_type_flag = 'p' if record.get('Puts') is not None else 'c'
            
            calculated_iv, delta, gamma = 0.0, 0.0, 0.0
            try:
                calculated_iv = iv(market_price, S, K, time_to_expiration, r, option_type_flag)
                delta = calculate_delta(option_type_flag, S, K, time_to_expiration, r, calculated_iv)
                gamma = calculate_gamma(option_type_flag, S, K, time_to_expiration, r, calculated_iv)
            except Exception: pass
            
            option_data = {
                'strike': K, 'lastPrice': safe_to_float(record.get('Last')),
                'bid': bid, 'ask': ask, 'volume': safe_to_int(record.get('Vol')),
                'openInterest': safe_to_int(record.get('Open Int')),
                'impliedVolatility': calculated_iv, 'delta': delta, 'gamma': gamma
            }

            if option_type_flag == 'c': calls_data.append(option_data)
            else: puts_data.append(option_data)

        return jsonify({"calls": calls_data, "puts": puts_data})

    except Exception as e:
        LOG.error(f"Unexpected error in get_options_chain for {ticker}/{expiry}: {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500
    
# --- Main Execution ---
if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=5000)