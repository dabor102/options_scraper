import json
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import requests


# Assuming the scraper is in the options_scraper directory
from options_scraper.scraper import NASDAQOptionsScraper

app = Flask(__name__)
# This enables CORS, allowing your visualizer.html to make requests to this server
CORS(app)

# Instantiate the scraper once to reuse the session
scraper = NASDAQOptionsScraper()

def preprocess_for_chart(raw_data):
    """
    Aggregates raw options data by strike price for visualization.
    This function is moved from the visualizer to the backend.
    """
    if not raw_data:
        return {}

    df = pd.DataFrame(raw_data)

    # Convert to numeric, coercing errors to NaN, then fill NaN with 0
    for col in ['volume', 'open_interest']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Aggregate data by strike and type
    agg_df = df.groupby(['strike', 'type']).sum().unstack(fill_value=0)
    agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]
    
    # Ensure all required columns exist
    for col in ['open_interest_call', 'open_interest_put', 'volume_call', 'volume_put']:
        if col not in agg_df.columns:
            agg_df[col] = 0

    agg_df = agg_df.sort_index()

    # --- NEW: Find the Call and Put Walls ---
    # Find the strike price (index) with the maximum open interest.
    # Use .get() to avoid errors if a column is all zeros.
    call_wall_strike = agg_df['open_interest_call'].idxmax() if not agg_df['open_interest_call'].empty else 0
    put_wall_strike = agg_df['open_interest_put'].idxmax() if not agg_df['open_interest_put'].empty else 0
    
    # Calculate cumulative values
    agg_df['cumulative_oi'] = (agg_df['open_interest_call'] + agg_df['open_interest_put']).cumsum()
    agg_df['cumulative_vol'] = (agg_df['volume_call'] + agg_df['volume_put']).cumsum()

    # Prepare data for JSON serialization
    chart_data = {
        'labels': [f"{s:.2f}" for s in agg_df.index.tolist()],
        'call_oi': agg_df['open_interest_call'].tolist(),
        'put_oi': (-agg_df['open_interest_put']).tolist(), # Negative for chart display
        'call_vol': agg_df['volume_call'].tolist(),
        'put_vol': (-agg_df['volume_put']).tolist(), # Negative for chart display
        'cumulative_oi': agg_df['cumulative_oi'].tolist(),
        'cumulative_vol': agg_df['cumulative_vol'].tolist(),
        # --- NEW: Add walls to the response ---
        'call_wall_strike': f"{call_wall_strike:.2f}",
        'put_wall_strike': f"{put_wall_strike:.2f}"
    }
    return chart_data
    """
    Aggregates raw options data by strike price for visualization.
    This function is moved from the visualizer to the backend.
    """
    if not raw_data:
        return {}

    df = pd.DataFrame(raw_data)

    # Convert to numeric, coercing errors to NaN, then fill NaN with 0
    for col in ['volume', 'open_interest']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Aggregate data by strike and type
    agg_df = df.groupby(['strike', 'type']).sum().unstack(fill_value=0)
    agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]
    
    # Ensure all required columns exist
    for col in ['open_interest_call', 'open_interest_put', 'volume_call', 'volume_put']:
        if col not in agg_df.columns:
            agg_df[col] = 0

    agg_df = agg_df.sort_index()

    # Calculate cumulative values
    agg_df['cumulative_oi'] = (agg_df['open_interest_call'] + agg_df['open_interest_put']).cumsum()
    agg_df['cumulative_vol'] = (agg_df['volume_call'] + agg_df['volume_put']).cumsum()

    # Prepare data for JSON serialization
    chart_data = {
        'labels': [f"{s:.2f}" for s in agg_df.index.tolist()],
        'call_oi': agg_df['open_interest_call'].tolist(),
        'put_oi': (-agg_df['open_interest_put']).tolist(), # Negative for chart display
        'call_vol': agg_df['volume_call'].tolist(),
        'put_vol': (-agg_df['volume_put']).tolist(), # Negative for chart display
        'cumulative_oi': agg_df['cumulative_oi'].tolist(),
        'cumulative_vol': agg_df['cumulative_vol'].tolist(),
    }
    return chart_data


@app.route('/api/expirations/<string:ticker>', methods=['GET'])
def get_expirations(ticker):
    """API endpoint to fetch available expiration dates for a ticker."""
    if not ticker:
        return jsonify({"error": "Ticker symbol is required."}), 400
    
    dates = scraper.get_expiration_dates(ticker)
    if dates is None:
        return jsonify({"error": "Failed to fetch expiration dates from NASDAQ."}), 500
        
    return jsonify(dates)


def to_int(value):
    """Safely converts a string to an int, handling '--' and commas."""
    if value is None or value == '--':
        return 0
    return int(str(value).replace(',', ''))


@app.route('/api/chart_data/<string:ticker>/<string:expiry>', methods=['GET'])
def get_chart_data(ticker, expiry):
    """API endpoint to fetch and process options data for the chart."""
    if not ticker or not expiry:
        return jsonify({"error": "Ticker and expiry date are required."}), 400
    
    stock_info = scraper.get_stock_info(ticker)
    last_price = stock_info.get('last_price') if stock_info else None
    
    raw_data = []
    # The scraper yields records; some may have '--' for volume or open interest.
    for record in scraper(ticker, expiry=expiry):
        # We now use our safe to_int() converter here.
        if record.get('Calls'):
            raw_data.append({
                'type': 'call',
                'strike': float(record.get('Strike')),
                'volume': to_int(record.get('Vol')),
                'open_interest': to_int(record.get('Open Int'))
            })
        elif record.get('Puts'):
             raw_data.append({
                'type': 'put',
                'strike': float(record.get('Strike')),
                'volume': to_int(record.get('Vol')),
                'open_interest': to_int(record.get('Open Int'))
            })

    if not raw_data:
        return jsonify({"error": "No data found for the selected criteria."}), 404
        
    processed_data = preprocess_for_chart(raw_data)

    if last_price:
        processed_data['last_price'] = last_price

    return jsonify(processed_data)


if __name__ == '__main__':
    # Runs the server on http://127.0.0.1:5000
    app.run(debug=True, port=5000)