#options.py
import streamlit as st
import yfinance as yf
from FOC import FOC
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import requests

# Set the default style for matplotlib plots for a dark theme
plt.style.use('dark_background')

# --- Constants ---
API_BASE_URL = "http://127.0.0.1:5000/api"

# --- Data Fetching and Caching ---

# --- yfinance Data Functions ---
@st.cache_data(ttl=60)
def get_spot_price_yf(ticker_symbol):
    try:
        ticker_obj = yf.Ticker(ticker_symbol)
        data = ticker_obj.fast_info
        return data.get('last_price', 0)
    except Exception: return 0

@st.cache_data(ttl=3600)
def get_available_expiration_dates_yf(ticker_symbol):
    try: return yf.Ticker(ticker_symbol).options
    except Exception: return None

@st.cache_data(ttl=60)
def get_options_data_yf(ticker_symbol, expiration_date):
    try:
        option_chain = yf.Ticker(ticker_symbol).option_chain(expiration_date)
        calls, puts = option_chain.calls, option_chain.puts
        for df in [calls, puts]:
            for col in ['openInterest', 'volume', 'impliedVolatility', 'strike', 'delta', 'gamma']:
                if col not in df.columns: df[col] = 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return calls, puts
    except Exception: return None, None

# --- NASDAQ API Data Functions ---
@st.cache_data(ttl=60)
def get_spot_price_nasdaq(ticker_symbol):
    try:
        response = requests.get(f"{API_BASE_URL}/stock_info/{ticker_symbol}")
        response.raise_for_status()
        return response.json().get('last_price', 0)
    except requests.exceptions.RequestException as e:
        st.session_state.api_error = f"NASDAQ API connection failed: {e}"
        return 0

@st.cache_data(ttl=3600)
def get_available_expiration_dates_nasdaq(ticker_symbol):
    try:
        response = requests.get(f"{API_BASE_URL}/expirations/{ticker_symbol}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.session_state.api_error = f"NASDAQ API connection failed: {e}"
        return None

@st.cache_data(ttl=60)
def get_options_data_nasdaq(ticker_symbol, expiration_date):
    try:
        response = requests.get(f"{API_BASE_URL}/options_chain/{ticker_symbol}/{expiration_date}")
        response.raise_for_status()
        data = response.json()
        calls_df = pd.DataFrame(data.get('calls', []))
        puts_df = pd.DataFrame(data.get('puts', []))
        return (None, None) if calls_df.empty or puts_df.empty else (calls_df, puts_df)
    except requests.exceptions.RequestException as e:
        st.session_state.api_error = f"NASDAQ API connection failed: {e}"
        return None, None

class AdvancedOptionsAnalyzer:
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

        # Create the profile DataFrame to find the walls
        all_strikes = sorted(list(set(calls_df['strike'].tolist() + puts_df['strike'].tolist())))
        profile_df = pd.DataFrame(index=all_strikes)
        profile_df = profile_df.join(calls_df.groupby('strike')['openInterest'].sum().rename('call_oi'))
        profile_df = profile_df.join(puts_df.groupby('strike')['openInterest'].sum().rename('put_oi')).fillna(0)
        
        call_wall = profile_df['call_oi'].idxmax()
        put_wall = profile_df['put_oi'].idxmax()
        
        # Calculate Top 5 based on OI*Vol
        calls_df['oi_vol_product'] = calls_df['openInterest'] * calls_df['volume']
        puts_df['oi_vol_product'] = puts_df['openInterest'] * puts_df['volume']
        top_calls = calls_df.nlargest(5, 'oi_vol_product')[['strike', 'openInterest', 'volume', 'impliedVolatility']]
        top_puts = puts_df.nlargest(5, 'oi_vol_product')[['strike', 'openInterest', 'volume', 'impliedVolatility']]
        
        return {
            "Total Call OI": total_call_oi, "Total Put OI": total_put_oi,
            "Total Call Vol": total_call_vol, "Total Put Vol": total_put_vol,
            "Put/Call Ratio (OI)": f"{pcr_oi:.2f}", "Put/Call Ratio (Vol)": f"{pcr_vol:.2f}",
            "Top 5 Calls": top_calls, "Top 5 Puts": top_puts,
            "call_wall": call_wall, "put_wall": put_wall
        }

    def calculate_max_pain(self, calls_df, puts_df):
        strikes = sorted(list(set(calls_df['strike'].tolist() + puts_df['strike'].tolist())))
        total_losses = []
        for test_price in strikes:
            call_loss = ((test_price - calls_df[calls_df['strike'] < test_price]['strike']) * calls_df[calls_df['strike'] < test_price]['openInterest']).sum()
            put_loss = ((puts_df[puts_df['strike'] > test_price]['strike'] - test_price) * puts_df[puts_df['strike'] > test_price]['openInterest']).sum()
            total_losses.append(call_loss + put_loss)
        min_loss_index = np.argmin(total_losses)
        return strikes[min_loss_index] if strikes else 0

    def calculate_exposure_profiles(self, calls_df, puts_df):
        required_cols = ['gamma', 'delta', 'openInterest', 'strike']
        if not all(col in calls_df.columns and not calls_df[col].isnull().all() for col in required_cols) or \
           not all(col in puts_df.columns and not puts_df[col].isnull().all() for col in required_cols):
            st.warning("Greek data not available from this source. Cannot calculate exposure profiles.")
            return pd.DataFrame(), 0, 0

        calls_df['gex'] = calls_df['gamma'] * calls_df['openInterest'] * 100
        calls_df['dex'] = calls_df['delta'] * calls_df['openInterest'] * 100
        puts_df['gex'] = puts_df['gamma'] * puts_df['openInterest'] * 100 * -1
        puts_df['dex'] = puts_df['delta'] * puts_df['openInterest'] * 100
        
        call_exposure = calls_df.groupby('strike')[['gex', 'dex']].sum()
        put_exposure = puts_df.groupby('strike')[['gex', 'dex']].sum()
        profile = pd.concat([call_exposure, put_exposure], axis=1, keys=['call', 'put']).fillna(0)
        profile['net_gex'] = profile['call']['gex'] + profile['put']['gex']
        profile['net_dex'] = profile['call']['dex'] + profile['put']['dex']

        try:
            net_gex_cumsum = profile['net_gex'].cumsum()
            gamma_flip_point = net_gex_cumsum[net_gex_cumsum > 0].index[0]
        except IndexError:
            gamma_flip_point = 0
        
        hvl_strike = profile['net_gex'].abs().idxmax()
        return profile, gamma_flip_point, hvl_strike

    def plot_exposure_profile(self, profile_df, gamma_flip, hvl, call_resistance, put_support, expiration_date):
        if profile_df.empty: return None

        strike_range_pct = 0.25
        min_strike = self.spot_price * (1 - strike_range_pct)
        max_strike = self.spot_price * (1 + strike_range_pct)
        profile_df = profile_df[(profile_df.index >= min_strike) & (profile_df.index <= max_strike)]
        
        if profile_df.empty:
            st.warning("No options data available in the selected strike range to plot.")
            return None

        fig, ax = plt.subplots(figsize=(14, 10))
        
        strike_gap = profile_df.index[1] - profile_df.index[0] if len(profile_df.index) > 1 else 1
        bar_height = 0.8 * strike_gap

        gex_values = profile_df['net_gex'] / 1_000_000
        colors = ['limegreen' if g >= 0 else 'red' for g in gex_values]
        ax.barh(profile_df.index, gex_values, color=colors, height=bar_height, label='Net GEX')
        ax.axvline(0, color='gray', linestyle='--', linewidth=1)
        
        ax.set_xlabel('GEX ($ Millions per 1% move)', color='white', fontsize=12)
        ax.set_ylabel('Strike Price', color='white', fontsize=12)
        ax.set_ylim(profile_df.index.min() - strike_gap, profile_df.index.max() + strike_gap)
        ax.invert_yaxis()
        
        ax.axhline(self.spot_price, color='white', linestyle=':', linewidth=2, label=f'Spot: ${self.spot_price:.2f}')
        ax.axhline(call_resistance, color='red', linestyle='--', linewidth=2, label=f'Call Resistance: ${call_resistance:.2f}')
        ax.axhline(put_support, color='saddlebrown', linestyle='--', linewidth=2, label=f'Put Support: ${put_support:.2f}')
        ax.axhline(hvl, color='lime', linestyle='--', linewidth=2, label=f'HVL: ${hvl:.2f}')
        if gamma_flip > 0:
            ax.axhline(gamma_flip, color='yellow', linestyle='-', linewidth=2, label=f'Gamma Flip: ${gamma_flip:.2f}')

        ax2 = ax.twiny()
        gex_cumsum = profile_df['net_gex'].cumsum() / 1_000_000
        dex_cumsum = profile_df['net_dex'].cumsum() / 1_000_000
        ax2.plot(gex_cumsum, profile_df.index, color='gold', label='GEX Profile (Cumulative)')
        ax2.plot(dex_cumsum, profile_df.index, color='deepskyblue', label='DEX Profile (Cumulative)')
        ax2.set_xlabel('Cumulative Exposure ($ Millions)', color='white', fontsize=12)
        
        # --- Center both x-axes on zero ---
        # Find the max absolute value for each dataset to create a symmetrical range
        max_gex_val = gex_values.abs().max()
        ax.set_xlim(-max_gex_val * 1.1, max_gex_val * 1.1)

        max_cum_val = max(gex_cumsum.abs().max(), dex_cumsum.abs().max())
        ax2.set_xlim(-max_cum_val * 1.1, max_cum_val * 1.1)
        
        fig.suptitle(f'Exposure Profile for {self.ticker_symbol} | Exp: {expiration_date}', fontsize=16)
        
        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax2.legend(lines + lines2, labels + labels2, loc='upper right')
        
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        return fig

    def plot_volume_oi_profile(self, calls_df, puts_df, expiration_date, max_pain_price, strike_range_pct=0.25):
        # This function remains largely the same but ensure it still works
        min_strike = self.spot_price * (1 - strike_range_pct)
        max_strike = self.spot_price * (1 + strike_range_pct)
        calls = calls_df[(calls_df['strike'] >= min_strike) & (calls_df['strike'] <= max_strike)]
        puts = puts_df[(puts_df['strike'] >= min_strike) & (puts_df['strike'] <= max_strike)]
        call_data = calls.groupby('strike')[['openInterest', 'volume']].sum()
        put_data = puts.groupby('strike')[['openInterest', 'volume']].sum() * -1
        all_strikes = sorted(list(set(call_data.index.tolist() + put_data.index.tolist())))
        profile_df = pd.DataFrame(index=all_strikes).join(call_data.rename(columns={'openInterest': 'call_oi', 'volume': 'call_vol'})).join(put_data.rename(columns={'openInterest': 'put_oi', 'volume': 'put_vol'})).fillna(0)
        call_wall_strike = profile_df['call_oi'].idxmax() if not profile_df['call_oi'].empty else 0
        put_wall_strike = profile_df['put_oi'].abs().idxmax() if not profile_df['put_oi'].empty else 0
        
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
        ax.axvline(self.spot_price, color='white', linestyle=':', linewidth=2, label=f'Spot: ${self.spot_price:.2f}')
        ax.axvline(call_wall_strike, color='lime', linestyle='--', linewidth=2, label=f'Call Wall: ${call_wall_strike:.2f}')
        ax.axvline(put_wall_strike, color='fuchsia', linestyle='--', linewidth=2, label=f'Put Wall: ${put_wall_strike:.2f}')
        ax.axvline(max_pain_price, color='magenta', linestyle='-.', linewidth=2, label=f'Max Pain: ${max_pain_price:.2f}')
        lines, labels = ax.get_legend_handles_labels()
        ax.legend(lines, labels, loc='upper left')
        fig.tight_layout(); return fig

    def plot_iv_skew(self, calls_df, puts_df, expiration_date):
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.plot(calls_df['strike'], calls_df['impliedVolatility'], 'o-', label='Call IV', color='deepskyblue')
        ax.plot(puts_df['strike'], puts_df['impliedVolatility'], 'o-', label='Put IV', color='orangered')
        ax.set_title(f'Implied Volatility Skew | Exp: {expiration_date}', color='white', fontsize=16)
        ax.set_xlabel('Strike Price', color='white')
        ax.set_ylabel('Implied Volatility', color='white')
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
        ax.axvline(self.spot_price, color='yellow', linestyle='--', linewidth=2, label=f'Spot: ${self.spot_price:.2f}')
        ax.legend(); fig.tight_layout(); return fig

# --- Streamlit Front End ---
st.set_page_config(layout="wide", page_title="Options Analyzer")
st.title('🎯 Advanced Options Analyzer')

with st.sidebar:
    st.header('⚙️ Configuration')
    data_source = st.selectbox('Select Data Source:', ('NASDAQ API', 'yfinance', 'freeoptionschain'), help="NASDAQ API is recommended for quality.")
    ticker_symbol = st.text_input('Enter Ticker Symbol (e.g., SPY, TSLA):', 'SPY').upper()
    
if 'api_error' in st.session_state: del st.session_state.api_error

if ticker_symbol:
    if data_source == 'yfinance':
        spot_price = get_spot_price_yf(ticker_symbol)
        exp_dates = get_available_expiration_dates_yf(ticker_symbol)
    elif data_source == 'freeoptionschain':
        spot_price = get_spot_price_foc(ticker_symbol)
        exp_dates = get_available_expiration_dates_foc(ticker_symbol)
    else: # NASDAQ API
        spot_price = get_spot_price_nasdaq(ticker_symbol)
        exp_dates = get_available_expiration_dates_nasdaq(ticker_symbol)

    if 'api_error' in st.session_state: st.error(st.session_state.api_error)

    if spot_price == 0 and 'api_error' not in st.session_state:
        st.error(f'Could not fetch data for {ticker_symbol}.')
    elif spot_price > 0:
        analyzer = AdvancedOptionsAnalyzer(ticker_symbol, spot_price)
        with st.sidebar:
            if exp_dates:
                selected_exp = st.selectbox('Select an Expiration Date:', exp_dates)
                analyze_button = st.button('Analyze ✨', use_container_width=True)
            else:
                if 'api_error' not in st.session_state: st.warning('No expiration dates found.')
                analyze_button = False

        if analyze_button:
            with st.spinner(f'Fetching and analyzing data for {ticker_symbol}...'):
                if data_source == 'yfinance':
                    calls, puts = get_options_data_yf(ticker_symbol, selected_exp)
                elif data_source == 'freeoptionschain':
                    calls, puts = get_options_data_foc(ticker_symbol, selected_exp)
                else: # NASDAQ API
                    calls, puts = get_options_data_nasdaq(ticker_symbol, selected_exp)

            if calls is not None and puts is not None and not calls.empty and not puts.empty:
                st.header(f'Analysis for {ticker_symbol} | Spot Price: ${analyzer.spot_price:,.2f}')
                st.subheader(f'Expiration: {selected_exp}')
                
                overview = analyzer.analyze_options_overview(calls, puts)
                max_pain_price = analyzer.calculate_max_pain(calls, puts)
                exposure_profile, gamma_flip, hvl = analyzer.calculate_exposure_profiles(calls, puts)

                st.write("---")
                col1, col2, col3, col4, col5, col6 = st.columns(6)
                col1.metric("Spot Price", f"${analyzer.spot_price:,.2f}")
                col2.metric("PCR (OI)", overview['Put/Call Ratio (OI)'])
                col2.metric("Total Call OI", overview['Total Call OI'])
                col2.metric("Total Put OI", overview['Total Put OI'])
                col3.metric("PCR (Vol)", overview['Put/Call Ratio (Vol)'])
                col3.metric("Total Call Vol", overview['Total Call Vol'])
                col3.metric("Total Put Vol", overview['Total Put Vol'])
                col5.metric("Call Resistance", f"${overview['call_wall']:,.2f}")
                col5.metric("Put Support", f"${overview['put_wall']:,.2f}")
                col6.metric("Max Pain", f"${max_pain_price:,.2f}")
                col6.metric("Gamma Flip", f"${gamma_flip:,.2f}" if gamma_flip > 0 else "N/A")

                st.write("---")
                exposure_fig = analyzer.plot_exposure_profile(exposure_profile, gamma_flip, hvl, overview['call_wall'], overview['put_wall'], selected_exp)
                if exposure_fig:
                    st.pyplot(exposure_fig)
                st.pyplot(analyzer.plot_volume_oi_profile(calls, puts, selected_exp, max_pain_price))
                st.pyplot(analyzer.plot_iv_skew(calls, puts, selected_exp))

                st.write("---")
                c1, c2 = st.columns(2)
                with c1:
                    st.write("**Top 5 Calls (by OI & Vol)**"); st.dataframe(overview['Top 5 Calls'])
                with c2:
                    st.write("**Top 5 Puts (by OI & Vol)**"); st.dataframe(overview['Top 5 Puts'])
            else:
                if 'api_error' not in st.session_state:
                    st.error(f'Could not retrieve options data.')
else:
    st.info("Please enter a stock ticker symbol to begin.")