from framework.data.ohlc import fetch_ta_data, symbols
import config
import pandas as pd
from time import time
import requests


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
            "conn": conn
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
    intraday_buy_symbols = []
    intraday_sell_symbols = []

    for symbol in symbol_list:
        params = {
            "symbol": symbol,
            "from_date": from_date,
            "to_date": to_date,
            "timeframe": timeframe,
            "conn": conn
        }
        ta_data = fetch_ta_data(**params)
        if isinstance(ta_data, dict) and ta_data.get("status") == "error":
            continue

        df = pd.DataFrame(ta_data)
        if df is not None and not df.empty:
            latest_row = df.iloc[-1]

            if latest_row["intraday_buy"]:
                signal = {
                    "symbol": symbol, "date": pd.to_datetime(latest_row["date"]).strftime("%d-%m-%Y %H:%M:%S"), "value": float(latest_row["close"])}
                intraday_buy_symbols.append(signal)

            if latest_row["intraday_sell"]:
                signal = {
                    "symbol": symbol, "date": pd.to_datetime(latest_row["date"]).strftime("%d-%m-%Y %H:%M:%S"), "value": float(latest_row["close"])}
                intraday_sell_symbols.append(signal)

    return intraday_buy_symbols, intraday_sell_symbols


if __name__ == "__main__":
    conn = config.db_conn()
    starttime = time()
    buy_signals, sell_signals = scan_intraday_signal(
        from_date="2025-10-01", to_date="2025-10-24", timeframe="15min", conn=conn)
    endtime = time()
    print(f"Scan completed in {endtime - starttime:.2f} seconds.")
    print("Symbols in intraday buy signals:", buy_signals)
    print("Symbols in intraday sell signals:", sell_signals)

    BOT_TOKEN = "8341158966:AAGtpv713A71zMwxHkAlhI08JbElB480zIw"
    CHAT_ID = "7184769936"

    def format_signal_message(signals, title="Intraday Buy Signals"):
        if not signals:
            return f"📊 <b>{title}</b>\nNo signals today."

        date = signals[0]["date"]
        msg = f"📈 <b>{title}</b>\n🕒 {date}\n\n<pre>Symbol          Price\n----------------------\n"

        for s in signals:
            msg += f"{s['symbol']:<15}{s['value']:>8.2f}\n"

        msg += "</pre>"
        return msg

    msg = format_signal_message(buy_signals, "Intraday Buy Signals")
    sell_msg = format_signal_message(sell_signals, "Intraday Sell Signals")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID,
                  "text": msg, "parse_mode": "HTML"})
    requests.post(url, data={"chat_id": CHAT_ID,
                  "text": sell_msg, "parse_mode": "HTML"})
