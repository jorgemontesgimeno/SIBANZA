import yfinance as yf
import pandas as pd
import os

# ---------------------------
# TICKERS
# ---------------------------
tickers = [
    "BTC-USD", "ETH-USD", "SOL-USD", "LINK-USD",
    "GC=F", "SI=F", "BZ=F", "NG=F", "HG=F", "ZC=F", "KC=F", "PA=F",
    "TLT", "IEF", "SHY", "TIP", "BNDX", "EMB", "VTC", "JNK", "IBGL.L", "BTP.MI", "MUB",
    "AAPL", "MSFT", "AMZN", "JNJ", "JPM", "XOM", "PG", "TSLA", "UNH", "BRK-B",
    "SAN.MC", "ITX.MC", "IBE.MC", "MC.PA", "SAP.DE", "ASML.AS", "SIE.DE", "NESN.SW",
    "AZN.L", "HSBA.L",
    "2330.TW", "7203.T", "BABA", "TCEHY", "RELIANCE.NS", "VALE", "BHP"
]

# ---------------------------
# DESCARGA ÚLTIMOS DATOS
# ---------------------------
data = yf.download(
    tickers,
    period="5d",
    interval="1d",
    group_by="ticker",
    auto_adjust=False,
    progress=False
)

if data.empty:
    print("No hay datos disponibles.")
    exit()

rows = []

# ---------------------------
# PROCESAR CADA TICKER
# ---------------------------
for ticker in tickers:
    if ticker not in data.columns.levels[0]:
        continue

    df_ticker = data[ticker].copy().dropna()

    if df_ticker.empty:
        continue

    # Última fila disponible
    last_row = df_ticker.tail(1)

    date = last_row.index[0]
    date_str = date.strftime("%Y-%m-%d")

    row_dict = {
        "asset_id": f"{ticker}_{date_str}",
        "ticker": ticker,
        "date": date_str,
        "Open": last_row["Open"].values[0],
        "High": last_row["High"].values[0],
        "Low": last_row["Low"].values[0],
        "Close": last_row["Close"].values[0],
        "Volume": last_row["Volume"].values[0]
    }

    rows.append(row_dict)

new_data = pd.DataFrame(rows)

# ---------------------------
# CARGAR HISTÓRICO
# ---------------------------
file_path = "historico/historical_assets.csv"

if os.path.exists(file_path):
    historical_df = pd.read_csv(file_path)
else:
    historical_df = pd.DataFrame()

# ---------------------------
# CONCATENAR + LIMPIAR DUPLICADOS
# ---------------------------
final_df = pd.concat([historical_df, new_data], ignore_index=True)

# Eliminar duplicados por ticker + fecha
final_df = final_df.drop_duplicates(subset=["ticker", "date"], keep="last")

# Ordenar
final_df = final_df.sort_values(["ticker", "date"])

# ---------------------------
# GUARDAR
# ---------------------------
final_df.to_csv(file_path, index=False)

print("Datos actualizados correctamente ✅")