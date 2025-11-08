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
        # if symbol == "JSWENERGY-I":
        #     df.to_clipboard()

        pivots = df[["date", "pivot_high", "pivot_low"]]

        dates = df["date"].unique()
        latest_date = max(pd.to_datetime(dates)).date()
        df = df[pd.to_datetime(df["date"]).dt.date == latest_date]

        for index, row in df.iterrows():
            pivot_data = pivots[pivots["date"] < row["date"]]

            last_high = pivot_data.loc[pivot_data["pivot_high"].notna(), "pivot_high"].iloc[-1]
            last_high_date = pivot_data.loc[pivot_data["pivot_high"].notna(), "date"].iloc[-1]
            last_low = pivot_data.loc[pivot_data["pivot_low"].notna(), "pivot_low"].iloc[-1]
            last_low_date = pivot_data.loc[pivot_data["pivot_low"].notna(), "date"].iloc[-1]

            fibo_levels = {}

            if row["intraday_buy"]:
                # Identify 1 pivot low and pivot high to get fibo levels
                if last_high is not None and last_low is not None:
                    high = last_high
                    low = last_low
                    diff = high - low
                    fibo_levels = {
                        "0.0%": low,
                        "23.6%": low + 0.236 * diff,
                        "38.2%": low + 0.382 * diff,
                        "50.0%": low + 0.5 * diff,
                        "61.8%": low + 0.618 * diff,
                        "78.6%": low + 0.786 * diff,
                        "100.0%": high,
                        "161.8%": high + 0.618 * diff,
                    }
                signal = {
                    "symbol": symbol,
                    "date": pd.to_datetime(row["date"]).strftime("%d-%m-%Y %H:%M:%S"),
                    "ltp": float(row["close"]),
                    "last_high": last_high,
                    "last_high_date": last_high_date,
                    "last_low": last_low,
                    "last_low_date": last_low_date,
                    "ema13": float(row["ema_13"]),
                    "ema50": float(row["ema_50"]),
                    "ema200": float(row["ema_200"]),
                    "rsi3": float(row["rsi_3"]),
                    "entry": fibo_levels.get("50.0%"),
                    "sl": fibo_levels.get("38.2%"),
                    "target_1": fibo_levels.get("78.6%"),
                    "target_2": fibo_levels.get("100.0%"),
                    "target_3": fibo_levels.get("161.8%"),
                }

                intraday_buy_symbols = pd.concat(
                    [intraday_buy_symbols, pd.DataFrame([signal])], ignore_index=True
                )

            if row["intraday_sell"]:

                if last_high is not None and last_low is not None:
                    high = last_high
                    low = last_low
                    diff = high - low
                    fibo_levels = {
                        "0.0%": high,
                        "23.6%": high - 0.236 * diff,
                        "38.2%": high - 0.382 * diff,
                        "50.0%": high - 0.5 * diff,
                        "61.8%": high - 0.618 * diff,
                        "78.6%": high - 0.786 * diff,
                        "100.0%": low,
                        "161.8%": low - 0.618 * diff,
                    }

                    signal = {
                        "symbol": symbol,
                        "date": pd.to_datetime(row["date"]).strftime("%d-%m-%Y %H:%M:%S"),
                        "ltp": float(row["close"]),
                        "last_high": last_high,
                        "last_high_date": last_high_date,
                        "last_low": last_low,
                        "last_low_date": last_low_date,
                        "ema13": float(row["ema_13"]),
                        "ema50": float(row["ema_50"]),
                        "ema200": float(row["ema_200"]),
                        "rsi3": float(row["rsi_3"]),
                        "entry": fibo_levels.get("50.0%"),
                        "sl": fibo_levels.get("38.2%"),
                        "target_1": fibo_levels.get("78.6%"),
                        "target_2": fibo_levels.get("100.0%"),
                        "target_3": fibo_levels.get("161.8%"),
                    }
                intraday_sell_symbols = pd.concat(
                    [intraday_sell_symbols, pd.DataFrame([signal])], ignore_index=True
                )

    intraday_buy_symbols = intraday_buy_symbols.sort_values(by=["date"]).to_dict(orient="records")
    intraday_sell_symbols = intraday_sell_symbols.sort_values(by=["date"]).to_dict(orient="records")
    return intraday_buy_symbols, intraday_sell_symbols


def scan_donchian_signal(from_date="", to_date="", timeframe="15min", conn=None):
    if conn is None or conn.closed:
        return {"status": "error", "error": "Database connection is not provided."}

    symbol_list = symbols(conn=conn)
    total_symbols = len(symbol_list)
    buy_signals = pd.DataFrame()
    sell_signals = pd.DataFrame()

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
        unique_dates = pd.to_datetime(df["date"]).dt.date.unique()
        latest_date = max(unique_dates)
        df = df[pd.to_datetime(df["date"]).dt.date == latest_date]

        signals = df[((df["signal"] == 1) | (df["signal"] == -1))]

        for index, row in signals.iterrows():
            signal = {
                "symbol": symbol,
                "date": pd.to_datetime(row["date"]).strftime("%d-%m-%Y %H:%M:%S"),
                "buy_signal": float(row["donchian_upper"]) if row["signal"] == 1 else None,
                "sell_signal": float(row["donchian_lower"]) if row["signal"] == -1 else None,
            }
            if row["signal"] == 1:
                buy_signals = pd.concat([buy_signals, pd.DataFrame([signal])], ignore_index=True)
            elif row["signal"] == -1:
                sell_signals = pd.concat([sell_signals, pd.DataFrame([signal])], ignore_index=True)

    return buy_signals.to_dict(orient="records"), sell_signals.to_dict(orient="records")


def scan(notify_telegram=False):
    conn = config.db_conn()
    starttime = datetime.now()
    print("Starting intraday signal scan...")
    buy_signals, sell_signals = scan_donchian_signal(
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
    buy_signal_tab.add_column("Entry", justify="right")
    buy_signal_tab.add_column("SL", justify="right")
    buy_signal_tab.add_column("Target 1", justify="right")
    buy_signal_tab.add_column("Target 2", justify="right")
    buy_signal_tab.add_column("Target 3", justify="right")

    for signal in buy_signals:
        # if signal["symbol"] == "ASHOKLEY-I":
        buy_signal_tab.add_row(
            fmt(signal["symbol"]),
            fmt(signal["date"]),
            fmt(signal["last_high"]),
            fmt(signal["last_low"]),
            fmt(signal["entry"]),
            fmt(signal["sl"]),
            fmt(signal["target_1"]),
            fmt(signal["target_2"]),
            fmt(signal["target_3"]),
        )

    sell_signal_tab = Table(
        title="Intraday Sell Signals",
        show_lines=False,
        title_style="bold red",
    )
    sell_signal_tab.add_column("Symbol", justify="left")
    sell_signal_tab.add_column("Signal Candle", justify="left")

    # for signal in sell_signals:
    #     sell_signal_tab.add_row(signal["symbol"], signal["date"])

    if buy_signals or sell_signals:
        console.print(Columns([buy_signal_tab, sell_signal_tab]))

    BOT_TOKEN = "8341158966:AAGtpv713A71zMwxHkAlhI08JbElB480zIw"
    CHAT_ID = "7184769936"


if __name__ == "__main__":

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        # scan(notify_telegram=False)
        buy, sell = scan_donchian_signal(
            from_date="2025-10-01", to_date="2025-11-07", timeframe="15min", conn=config.db_conn()
        )

        print(buy, sell)
        if not check_market_hours():
            break

        wait_until_next(waiting_minutes=15, seconds=30)
