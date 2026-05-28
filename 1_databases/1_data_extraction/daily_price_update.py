from datetime import datetime, timedelta
import os
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from dotenv import load_dotenv
import time

# ==============================================================================
# 1. TICKER CONFIGURATION
# ==============================================================================

# Cross-asset portfolio tracking: Crypto, Commodities, Fixed Income, and Global Equities.
# Maintained as a flat list to pass directly to the yfinance batch downloader.
tickers = [
    "BTC-USD", "ETH-USD", "SOL-USD", "LINK-USD", "GC=F", "SI=F", "BZ=F", "NG=F", "HG=F", 
    "ZC=F", "KC=F", "PA=F", "TLT", "IEF", "SHY", "TIP", "BNDX", "EMB", "VTC", "JNK", 
    "IBGL.L", "MUB", "AAPL", "MSFT", "AMZN", "JNJ", "JPM", "XOM", "PG", "TSLA", 
    "UNH", "BRK-B", "SAN.MC", "ITX.MC", "IBE.MC", "MC.PA", "SAP.DE", "ASML.AS", 
    "SIE.DE", "NESN.SW", "AZN.L", "HSBA.L", "2330.TW", "7203.T", "BABA", 
    "TCEHY", "RELIANCE.NS", "VALE", "BHP"
]

# ==============================================================================
# 2. TEMPORAL ANCHORING
# ==============================================================================

# Calculate strict 24-hour delta dates. Because the yfinance 'end' parameter is 
# exclusive, setting 'start' to yesterday and 'end' to today isolates exactly 
# yesterday's finalized trading session data.
hoy = datetime.now()
ayer_str = (hoy - timedelta(days=1)).strftime("%Y-%m-%d")
hoy_str = hoy.strftime("%Y-%m-%d")

# ==============================================================================
# 3. DATA INGESTION
# ==============================================================================

# Download single-day incremental data block.
# `group_by="ticker"` outputs a MultiIndex column structure (Ticker -> Market Metric).
# `auto_adjust=False` preserves raw, unadjusted close prices to maintain data lineage.
data = yf.download(
    tickers,
    start=ayer_str,  
    end=hoy_str,     
    group_by="ticker",
    auto_adjust=False
)

# ==============================================================================
# 4. OPTIMIZED TRANSFORMATION (WIDE TO LONG FORMAT)
# ==============================================================================

# Pivot the MultiIndex columns from wide format to a clean, long format database structure.
# `level=0` targets the ticker symbol, and `future_stack=True` ensures compliance 
# with upcoming pandas internal behavior modifications.
final_df = data.stack(level=0, future_stack=True)

# Explicitly assign names to the resulting MultiIndex before resetting to guarantee
# seamless column mapping when flattening the index.
final_df.index.names = ["date", "ticker"]
final_df = final_df.reset_index()

# ==============================================================================
# 5. CLEANING & FILTERING
# ==============================================================================

# Enforce strict data completeness. Drop any rows where core price metrics are missing,
# which effectively filters out non-trading days, exchange holidays, and tracking gaps.
final_df = final_df.dropna(subset=["Open", "High", "Low", "Close"])

# ==============================================================================
# 6. UNIQUE ID GENERATION & SORTING
# ==============================================================================

# Construct a composite business key (asset_id) combining the ticker and the ISO date.
# This serves as a reliable primary key for downstream relational databases or analytical pipelines.
final_df["asset_id"] = final_df["ticker"] + "_" + final_df["date"].dt.strftime("%Y-%m-%d")

# Enforce a strict schema layout by explicitly ordering columns, then sort the dataset
# chronologically per asset to ensure structural consistency for time-series operations.
final_df = final_df[
    ["asset_id", "ticker", "date", "Open", "High", "Low", "Close", "Volume"]
].sort_values(["ticker", "date"])

# Initialize downstream analytical placeholders to fulfill the target database schema structure
nuevas_columnas = ['return_3m', 'return_6m', 'momentum_score', 'returns', 'volatility', 'low_vol_score']

# Mass initialization of analytical columns with None values to prevent structural mismatches
final_df[nuevas_columnas] = None

# ==============================================================================
# 7. ENVIRONMENT CONFIGURATION & ENVIRONMENT SETUP
# ==============================================================================
# 1. Environment Configuration
# Intentará cargar el .env si existe (local), si no, os.getenv leerá los Secrets (GitHub)
load_dotenv(override=True)

# 2. Database Connection Setup
# Forzamos que si el puerto viene vacío o no existe, use '5432' por defecto
db_port = os.getenv('port')
if not db_port:
    db_port = '5432'

DATABASE_URL = f"postgresql://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{db_port}/{os.getenv('dbname')}"
engine = create_engine(DATABASE_URL)

# ==============================================================================
# 8. DATABASE SCHEMA MAPPING & RENAMING
# ==============================================================================

# Normalize column names to lowercase to align with standard PostgreSQL naming conventions
final_df.columns = final_df.columns.str.lower()

# Map Pandas DataFrame column labels to match destination relational database column definitions
final_df = final_df.rename(columns={
    'ticker': 'ticker',
    'date': 'date',
    'open': 'open_price',
    'high': 'high_price',
    'low': 'low_price',
    'close': 'close_price',
    'volume': 'volume',
    'return_3m': 'return_3m',
    'return_6m': 'return_6m',
    'momentum_score': 'momentum_score',
    'returns': 'returns',
    'volatility': 'volatility',
    'low_vol_score': 'low_vol_score'
})

# Standardize date objects to datetime.date format to prevent timestamp/timezone serialization issues
final_df['date'] = pd.to_datetime(final_df['date']).dt.date

# Filter for target database destination columns only, discarding metadata columns like 'asset_id'
valid_columns = [
    'ticker', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 
    'volume', 'return_3m', 'return_6m', 'momentum_score', 'returns', 'volatility', 'low_vol_score'
]
df_final = final_df[valid_columns]

# Initialize execution timer to monitor data ingestion pipeline latency metrics
start_time = time.time()

# ==============================================================================
# 9. RELATIONAL PERSISTENCE (BULK INGESTION)
# ==============================================================================

# Append transformed data records into the relational database 'price_history' table.
# Using 'method=multi' minimizes the roundtrips to the database by batching multiple rows per statement.
df_final.to_sql(
    'price_history', 
    engine, 
    if_exists='append', 
    index=False, 
    method='multi', 
)
