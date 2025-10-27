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
        df = df[pd.to_datetime(df["date"]).dt.date == datetime.now().date()]

        for index, row in df.iterrows():
            if row["intraday_buy"]:
                signal = {
                    "symbol": symbol,
                    "date": pd.to_datetime(row["date"]).strftime("%d-%m-%Y %H:%M:%S"),
                    "value": float(row["close"]),
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

    intraday_buy_symbols = intraday_buy_symbols.sort_values(by=["date"]).to_dict(
        orient="records"
    )[-1000:]
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

    # print in rich format tabular
    buy_signal_tab = Table(
        title="Intraday Buy Signals", show_lines=False, title_style="bold green"
    )
    buy_signal_tab.add_column("Symbol", justify="left")
    buy_signal_tab.add_column("Signal Candle", justify="left")

    for signal in buy_signals:
        buy_signal_tab.add_row(signal["symbol"], signal["date"])

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

    def format_signal_message(signals, title="Intraday Buy Signals"):
        if not signals:
            return f"📊 <b>{title}</b>\nNo signals for now."

        date = signals[0]["date"]
        msg = f"📈 <b>{title}</b>\n🕒 {date}\n\n<pre>Symbol          Price\n----------------------\n"

        for s in signals:
            msg += f"{s['symbol']:<15}{s['value']:>8.2f}\n"

        msg += "</pre>"
        return msg

    msg = format_signal_message(buy_signals, "Intraday Buy Signals")
    sell_msg = format_signal_message(sell_signals, "Intraday Sell Signals")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    if notify_telegram:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"})
        requests.post(
            url, data={"chat_id": CHAT_ID, "text": sell_msg, "parse_mode": "HTML"}
        )
    print("Signals sent to Telegram.")


if __name__ == "__main__":

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        scan(notify_telegram=False)
        if not check_market_hours():
            break

        wait_until_next(waiting_minutes=15, seconds=30)
