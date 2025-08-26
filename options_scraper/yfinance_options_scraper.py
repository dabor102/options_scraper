import streamlit as st
import yfinance as yf
from FOC import FOC # --- Added for the new data source
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

# Set the default style for matplotlib plots for a dark theme
plt.style.use('dark_background')

# --- Data Fetching and Caching ---

# --- yfinance Data Functions ---
@st.cache_data(ttl=300) # Cache data for 5 minutes
def get_spot_price_yf(ticker_symbol):
    """Fetches the most recent spot price from yfinance."""
    try:
        ticker_obj = yf.Ticker(ticker_symbol)
        hist = ticker_obj.history(period='1d')
        if not hist.empty:
            return hist['Close'].iloc[-1]
        data = ticker_obj.fast_info
        return data.get('last_price', 0)
    except Exception:
        return 0

@st.cache_data(ttl=3600) # Cache expiration dates for 1 hour
def get_available_expiration_dates_yf(ticker_symbol):
    """Gets all available expiration dates from yfinance."""
    try:
        ticker_obj = yf.Ticker(ticker_symbol)
        return ticker_obj.options
    except Exception:
        return None

@st.cache_data(ttl=300) # Cache options data for 5 minutes
def get_options_data_yf(ticker_symbol, expiration_date):
    """Fetches and preprocesses options data from yfinance."""
    try:
        ticker_obj = yf.Ticker(ticker_symbol)
        option_chain = ticker_obj.option_chain(expiration_date)
        calls = option_chain.calls
        puts = option_chain.puts
        if calls.empty or puts.empty:
            return None, None
        for df in [calls, puts]:
            for col in ['openInterest', 'volume', 'impliedVolatility', 'strike']:
                if col not in df.columns: df[col] = 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return calls, puts
    except Exception:
        return None, None

# --- freeoptionschain (FOC) Data Functions ---
@st.cache_data(ttl=300)
def get_spot_price_foc(ticker_symbol):
    """Fetches the most recent spot price from FOC."""
    try:
        # FOC returns a DataFrame, so we must extract the scalar value
        price_df = FOC().get_stock_price(ticker_symbol)
        if not price_df.empty:
            # Extract the single price value from the 'Price' column
            return price_df['Price'].item()
        return 0
    except Exception:
        return 0
    
@st.cache_data(ttl=3600)
def get_available_expiration_dates_foc(ticker_symbol):
    """Gets all available expiration dates from FOC."""
    try:
        return FOC().get_expiration_dates(ticker_symbol)
    except Exception:
        return None

@st.cache_data(ttl=300)
def get_options_data_foc(ticker_symbol, expiration_date):
    """Fetches and preprocesses options data from FOC."""
    try:
        foc_chain = FOC().get_options_chain(ticker_symbol, expiration_date)
        if foc_chain.empty:
            return None, None

        # FOC returns one df; split and rename columns to match yfinance structure
        calls_df = foc_chain[['Strike', 'c_Last', 'c_Bid', 'c_Ask', 'c_Volume', 'c_Open Interest', 'c_IV']].copy()
        calls_df.rename(columns={
            'Strike': 'strike', 'c_Last': 'lastPrice', 'c_Bid': 'bid', 'c_Ask': 'ask',
            'c_Volume': 'volume', 'c_Open Interest': 'openInterest', 'c_IV': 'impliedVolatility'
        }, inplace=True)
        # FOC IV is a percentage (e.g., 25.5), convert to decimal
        calls_df['impliedVolatility'] /= 100

        puts_df = foc_chain[['Strike', 'p_Last', 'p_Bid', 'p_Ask', 'p_Volume', 'p_Open Interest', 'p_IV']].copy()
        puts_df.rename(columns={
            'Strike': 'strike', 'p_Last': 'lastPrice', 'p_Bid': 'bid', 'p_Ask': 'ask',
            'p_Volume': 'volume', 'p_Open Interest': 'openInterest', 'p_IV': 'impliedVolatility'
        }, inplace=True)
        puts_df['impliedVolatility'] /= 100

        # Standardize data types
        for df in [calls_df, puts_df]:
            for col in ['openInterest', 'volume', 'impliedVolatility', 'strike']:
                if col not in df.columns: df[col] = 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return calls_df, puts_df
    except Exception:
        return None, None

class AdvancedOptionsAnalyzer:
    """
    A class to analyze and visualize options data.
    (This class remains unchanged as it works with the standardized DataFrame format)
    """
    def __init__(self, ticker_symbol, spot_price):
        self.ticker_symbol = ticker_symbol
        self.spot_price = spot_price

    def analyze_options_overview(self, calls_df, puts_df):
        total_call_oi = calls_df['openInterest'].sum()
        total_put_oi = puts_df['openInterest'].sum()
        total_call_vol = calls_df['volume'].sum()
        total_put_vol = puts_df['volume'].sum()

        pcr_oi = total_put_oi / total_call_oi if total_call_oi > 0 else 0
        pcr_vol = total_put_vol / total_call_vol if total_call_vol > 0 else 0

        calls_df['oi_vol_product'] = calls_df['openInterest'] * calls_df['volume']
        puts_df['oi_vol_product'] = puts_df['openInterest'] * puts_df['volume']

        top_calls = calls_df.nlargest(5, 'oi_vol_product')[['strike', 'openInterest', 'volume', 'impliedVolatility']]
        top_puts = puts_df.nlargest(5, 'oi_vol_product')[['strike', 'openInterest', 'volume', 'impliedVolatility']]
        
        return {
            "Total Call OI": total_call_oi, "Total Put OI": total_put_oi,
            "Total Call Vol": total_call_vol, "Total Put Vol": total_put_vol,
            "Put/Call Ratio (OI)": f"{pcr_oi:.2f}", "Put/Call Ratio (Vol)": f"{pcr_vol:.2f}",
            "Top 5 Calls": top_calls, "Top 5 Puts": top_puts
        }

    def plot_volume_oi_profile(self, calls_df, puts_df, expiration_date, strike_range_pct=0.25):
        min_strike = self.spot_price * (1 - strike_range_pct)
        max_strike = self.spot_price * (1 + strike_range_pct)

        calls = calls_df[(calls_df['strike'] >= min_strike) & (calls_df['strike'] <= max_strike)]
        puts = puts_df[(puts_df['strike'] >= min_strike) & (puts_df['strike'] <= max_strike)]

        call_data = calls.groupby('strike')[['openInterest', 'volume']].sum()
        put_data = puts.groupby('strike')[['openInterest', 'volume']].sum() * -1
        
        all_strikes = sorted(list(set(call_data.index.tolist() + put_data.index.tolist())))
        profile_df = pd.DataFrame(index=all_strikes)
        profile_df = profile_df.join(call_data.rename(columns={'openInterest': 'call_oi', 'volume': 'call_vol'}))
        profile_df = profile_df.join(put_data.rename(columns={'openInterest': 'put_oi', 'volume': 'put_vol'}))
        profile_df.fillna(0, inplace=True)

        call_wall_strike = profile_df['call_oi'].idxmax() if not profile_df['call_oi'].empty else 0
        put_wall_strike = profile_df['put_oi'].abs().idxmax() if not profile_df['put_oi'].empty else 0
        
        profile_df['cum_oi'] = (profile_df['call_oi'] + profile_df['put_oi'].abs()).cumsum()
        profile_df['cum_vol'] = (profile_df['call_vol'] + profile_df['put_vol'].abs()).cumsum()
        
        fig, ax = plt.subplots(figsize=(14, 8))
        bar_width = 0.8 * (profile_df.index[1] - profile_df.index[0] if len(profile_df.index) > 1 else 1)
        
        ax.bar(profile_df.index, profile_df['call_oi'], width=bar_width, color='blue', label='Call OI')
        ax.bar(profile_df.index, profile_df['put_oi'], width=bar_width, color='red', label='Put OI')
        ax.bar(profile_df.index, profile_df['call_vol'], width=bar_width, color='cyan', alpha=0.7, label='Call Volume')
        ax.bar(profile_df.index, profile_df['put_vol'], width=bar_width, color='orange', alpha=0.7, label='Put Volume')

        ax.set_title(f'{self.ticker_symbol} Open Interest & Volume Profile | Exp: {expiration_date}', color='white', fontsize=16)
        ax.set_ylabel('Contracts per Strike', color='deepskyblue')
        ax.set_xlabel('Strike Price', color='white')
        
        ax2 = ax.twinx()
        ax2.plot(profile_df.index, profile_df['cum_oi'], color='yellow', linestyle='-', linewidth=2, label='Cumulative OI')
        ax2.plot(profile_df.index, profile_df['cum_vol'], color='gold', linestyle='--', linewidth=2, label='Cumulative Volume')
        ax2.set_ylabel('Cumulative Contracts', color='yellow')
        
        ax.axvline(self.spot_price, color='white', linestyle=':', linewidth=2, label=f'Spot: ${self.spot_price:.2f}')
        ax.axvline(call_wall_strike, color='lime', linestyle='--', linewidth=2, label=f'Call Wall: ${call_wall_strike:.2f}')
        ax.axvline(put_wall_strike, color='fuchsia', linestyle='--', linewidth=2, label=f'Put Wall: ${put_wall_strike:.2f}')
        
        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc='upper left')

        fig.tight_layout()
        return fig

    def plot_iv_skew(self, calls_df, puts_df, expiration_date):
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.plot(calls_df['strike'], calls_df['impliedVolatility'], 'o-', label='Call IV', color='deepskyblue')
        ax.plot(puts_df['strike'], puts_df['impliedVolatility'], 'o-', label='Put IV', color='orangered')
        
        ax.set_title(f'Implied Volatility Skew | Exp: {expiration_date}', color='white', fontsize=16)
        ax.set_xlabel('Strike Price', color='white')
        ax.set_ylabel('Implied Volatility', color='white')
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
        ax.axvline(self.spot_price, color='yellow', linestyle='--', linewidth=2, label=f'Spot: ${self.spot_price:.2f}')
        
        ax.legend()
        fig.tight_layout()
        return fig

# --- Streamlit Front End ---

st.set_page_config(layout="wide", page_title="Options Analyzer")
st.title('🎯 Advanced Options Analyzer')

# --- User Input in Sidebar ---
with st.sidebar:
    st.header('⚙️ Configuration')
    # --- NEW: Data source selection ---
    data_source = st.selectbox('Select Data Source:', ('yfinance', 'freeoptionschain'), help="Choose the source for options data. FOC may provide more accurate volume/OI.")
    ticker_symbol = st.text_input('Enter Ticker Symbol (e.g., SPY, TSLA):', 'SPY').upper()

if ticker_symbol:
    # --- Abstracted Data Fetching ---
    if data_source == 'yfinance':
        spot_price = get_spot_price_yf(ticker_symbol)
        exp_dates = get_available_expiration_dates_yf(ticker_symbol)
    else: # freeoptionschain
        spot_price = get_spot_price_foc(ticker_symbol)
        exp_dates = get_available_expiration_dates_foc(ticker_symbol)

    if spot_price == 0:
        st.error(f'Could not fetch data for {ticker_symbol} from {data_source}. Please check the ticker symbol.')
    else:
        analyzer = AdvancedOptionsAnalyzer(ticker_symbol, spot_price)
        with st.sidebar:
            if exp_dates:
                selected_exp = st.selectbox('Select an Expiration Date:', exp_dates)
                analyze_button = st.button('Analyze ✨', use_container_width=True)
            else:
                st.warning(f'No expiration dates found for this ticker from {data_source}.')
                analyze_button = False

        if analyze_button:
            with st.spinner(f'Fetching and analyzing data for {ticker_symbol} from {data_source}...'):
                # --- Fetch options data from the selected source ---
                if data_source == 'yfinance':
                    calls, puts = get_options_data_yf(ticker_symbol, selected_exp)
                else: # freeoptionschain
                    calls, puts = get_options_data_foc(ticker_symbol, selected_exp)

            if calls is not None and puts is not None:
                st.header(f'Analysis for {ticker_symbol} | Spot Price: ${analyzer.spot_price:,.2f}')
                st.subheader(f'Expiration: {selected_exp}')
                
                overview_data = analyzer.analyze_options_overview(calls, puts)
                
                st.write("---")
                col1, col2, col3, col4, col5, col6 = st.columns(6)
                col1.metric("Call OI", f"{overview_data['Total Call OI']:,.0f}")
                col2.metric("Put OI", f"{overview_data['Total Put OI']:,.0f}")
                col3.metric("Call Vol", f"{overview_data['Total Call Vol']:,.0f}")
                col4.metric("Put Vol", f"{overview_data['Total Put Vol']:,.0f}")
                col5.metric("PCR (OI)", overview_data['Put/Call Ratio (OI)'])
                col6.metric("PCR (Vol)", overview_data['Put/Call Ratio (Vol)'])
                st.write("---")

                fig_oi_profile = analyzer.plot_volume_oi_profile(calls, puts, selected_exp)
                st.pyplot(fig_oi_profile)

                fig_iv_skew = analyzer.plot_iv_skew(calls, puts, selected_exp)
                st.pyplot(fig_iv_skew)

                st.write("---")
                c1, c2 = st.columns(2)
                with c1:
                    st.write("**Top 5 Calls (by OI & Vol)**")
                    st.dataframe(overview_data['Top 5 Calls'])
                with c2:
                    st.write("**Top 5 Puts (by OI & Vol)**")
                    st.dataframe(overview_data['Top 5 Puts'])

            else:
                st.error(f'Could not retrieve options data for the selected date from {data_source}.')
else:
    st.info("Please enter a stock ticker symbol in the sidebar to begin.")