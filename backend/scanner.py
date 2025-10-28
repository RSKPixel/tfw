from framework.data.ohlc import fetch_ta_data, symbols
from backfill import wait_until_next, check_market_hours
import config
import pandas as pd
from time import time
import requests
from datetime import datetime
import time
from rich.console import Console
from rich.table import Table
from rich.columns import Columns
import os


console = Console()


def scan_squeeze_symbols(from_date="", to_date="", timeframe="15min", conn=None):
    if conn is None or conn.closed:
        return {"status": "error", "error": "Database connection is not provided."}

    symbol_list = symbols(conn=conn)
    squeeze_symbols = []

    for symbol in symbol_list:
        params = {
            "symbol": symbol,
            "from_date": from_date,
            "to_date": to_date,
            "timeframe": timeframe,
            "conn": conn,
        }
        ta_data = fetch_ta_data(**params)
        if isinstance(ta_data, dict) and ta_data.get("status") == "error":
            continue

        df = pd.DataFrame(ta_data)
        if df is not None and not df.empty:
            latest_row = df.iloc[-1]
            if latest_row.get("in_squeeze"):
                squeeze_symbols.append(symbol)

    return squeeze_symbols


def scan_intraday_signal(from_date="", to_date="", timeframe="15min", conn=None):
    if conn is None or conn.closed:
        return {"status": "error", "error": "Database connection is not provided."}

    symbol_list = symbols(conn=conn)
    intraday_buy_symbols = pd.DataFrame()
    intraday_sell_symbols = pd.DataFrame()
    total_symbols = len(symbol_list)
    for idx, symbol in enumerate(symbol_list, start=1):
        print(f"Scanning {idx}/{total_symbols} symbols...", end="\r", flush=True)
        params = {
            "symbol": symbol,
            "from_date": from_date,
            "to_date": to_date,
            "timeframe": timeframe,
            "conn": conn,
        }
        ta_data = fetch_ta_data(**params)

        if isinstance(ta_data, dict) and ta_data.get("status") == "error":
            continue

        df = pd.DataFrame(ta_data)
        pivots = df[["date", "pivot_high", "pivot_low"]]
        if symbol == "FEDERALBNK-I":
            pivots.to_clipboard(index=False)
        df = df[pd.to_datetime(df["date"]).dt.date == datetime.now().date()]

        for index, row in df.iterrows():
            if row["intraday_buy"]:
                # Identify 1 pivot low and pivot high to get fibo levels
                pivot_data = pivots[pivots["date"] <= row["date"]]

                last_high = pivot_data.loc[pivot_data["pivot_high"].notna(), "pivot_high"].iloc[-1]
                last_low = pivot_data.loc[pivot_data["pivot_low"].notna(), "pivot_low"].iloc[-1]

                if symbol == "FEDERALBNK-I":
                    print(last_high, last_low)
                fibo_levels = {}
                if last_high is not None and last_low is not None:
                    high_price = last_high
                    low_price = last_low
                    diff = high_price - low_price
                    fibo_levels = {
                        "0.0%": high_price,
                        "23.6%": high_price - 0.236 * diff,
                        "38.2%": high_price - 0.382 * diff,
                        "50.0%": high_price - 0.5 * diff,
                        "61.8%": high_price - 0.618 * diff,
                        "100.0%": low_price,
                        "161.8%": low_price - 0.618 * diff,
                    }

                signal = {
                    "symbol": symbol,
                    "date": pd.to_datetime(row["date"]).strftime("%d-%m-%Y %H:%M:%S"),
                    "last_high": last_high,
                    "last_low": last_low,
                    "0.0%": fibo_levels.get("0.0%"),
                    "23.6%": fibo_levels.get("23.6%"),
                    "38.2%": fibo_levels.get("38.2%"),
                    "50.0%": fibo_levels.get("50.0%"),
                    "61.8%": fibo_levels.get("61.8%"),
                    "100.0%": fibo_levels.get("100.0%"),
                    "161.8%": fibo_levels.get("161.8%"),
                }

                intraday_buy_symbols = pd.concat(
                    [intraday_buy_symbols, pd.DataFrame([signal])], ignore_index=True
                )

            if row["intraday_sell"]:
                signal = {
                    "symbol": symbol,
                    "date": pd.to_datetime(row["date"]).strftime("%d-%m-%Y %H:%M:%S"),
                    "value": float(row["close"]),
                }
                intraday_sell_symbols = pd.concat(
                    [intraday_sell_symbols, pd.DataFrame([signal])], ignore_index=True
                )

    intraday_buy_symbols = intraday_buy_symbols.sort_values(by=["date"]).to_dict(orient="records")[
        -1000:
    ]
    intraday_sell_symbols = intraday_sell_symbols.sort_values(by=["date"]).to_dict(
        orient="records"
    )[-1000:]
    return intraday_buy_symbols, intraday_sell_symbols


def scan(notify_telegram=False):
    conn = config.db_conn()
    starttime = datetime.now()
    print("Starting intraday signal scan...")
    buy_signals, sell_signals = scan_intraday_signal(
        from_date="2025-10-01", to_date="2025-10-31", timeframe="15min", conn=conn
    )
    endtime = datetime.now()
    print(f"Scan completed in {(endtime - starttime).total_seconds():.2f} seconds.")

    def fmt(v):
        if v is None or pd.isna(v):
            return "-"
        if isinstance(v, (float, int)):
            return f"{v:.2f}"
        return str(v)

    # print in rich format tabular
    buy_signal_tab = Table(title="Intraday Buy Signals", show_lines=False, title_style="bold green")
    buy_signal_tab.add_column("Symbol", justify="left")
    buy_signal_tab.add_column("Signal Candle", justify="left")
    buy_signal_tab.add_column("Last High", justify="right")
    buy_signal_tab.add_column("Last Low", justify="right")
    buy_signal_tab.add_column("0.0%", justify="right")
    buy_signal_tab.add_column("23.6%", justify="right")
    buy_signal_tab.add_column("38.2%", justify="right")
    buy_signal_tab.add_column("50.0%", justify="right")
    buy_signal_tab.add_column("61.8%", justify="right")
    buy_signal_tab.add_column("100.0%", justify="right")
    # buy_signal_tab.add_column("161.8%", justify="right")

    for signal in buy_signals:
        buy_signal_tab.add_row(
            fmt(signal["symbol"]),
            fmt(signal["date"]),
            fmt(signal["last_high"]),
            fmt(signal["last_low"]),
            fmt(signal["0.0%"]),
            fmt(signal["23.6%"]),
            fmt(signal["38.2%"]),
            fmt(signal["50.0%"]),
            fmt(signal["61.8%"]),
            fmt(signal["100.0%"]),
            # fmt(signal["161.8%"]),
        )

    sell_signal_tab = Table(
        title="Intraday Sell Signals",
        show_lines=False,
        title_style="bold red",
    )
    sell_signal_tab.add_column("Symbol", justify="left")
    sell_signal_tab.add_column("Signal Candle", justify="left")

    for signal in sell_signals:
        sell_signal_tab.add_row(signal["symbol"], signal["date"])

    if buy_signals or sell_signals:
        console.print(Columns([buy_signal_tab, sell_signal_tab]))

    BOT_TOKEN = "8341158966:AAGtpv713A71zMwxHkAlhI08JbElB480zIw"
    CHAT_ID = "7184769936"


if __name__ == "__main__":

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        scan(notify_telegram=False)
        if not check_market_hours():
            break

        wait_until_next(waiting_minutes=15, seconds=30)
