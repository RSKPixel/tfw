from framework.backfiller.core import banknifty_options_chain, api_request
import config
import requests
import webbrowser
from kiteconnect import KiteConnect
import pandas as pd
import talib as ta
import numpy as np
import os
from datetime import datetime


def kite_connect() -> KiteConnect | None:
    api_key = config.KITE_API_KEY
    api_secret = config.KITE_API_SECRET
    access_token_api_url = config.ACCESS_TOKEN_API_URL

    request = requests.get(access_token_api_url)
    access_token = request.json().get("access_token", "")

    kite = KiteConnect(api_key=api_key)
    try:
        kite.set_access_token(access_token)
        profile = kite.profile()
    except Exception as e:
        print("Error setting access token:", e)

        loginurl = kite.login_url()
        kite = None
        print("Login URL:", loginurl)
        webbrowser.open(loginurl)
        return None

    return kite


def scan():
    options_data = banknifty_options_chain()
    kite = kite_connect()
    if kite is None:
        print("Kite connection could not be established.")
        return

    # Add NFO prefix for Kite LTP API
    instrument_list = [
        f"NFO:{symbol}" for symbol in options_data["tradingsymbol"]]
    ltp_data = kite.ltp(instrument_list)

    # Convert LTP dict → DataFrame
    ltp_data = pd.DataFrame.from_dict(ltp_data, orient="index")
    ltp_data.reset_index(inplace=True)
    ltp_data.rename(columns={"index": "tradingsymbol",
                    "last_price": "ltp"}, inplace=True)
    ltp_data["ltp"] = ltp_data["ltp"].astype(float)

    # Remove "NFO:" prefix to match original column
    ltp_data["tradingsymbol"] = ltp_data["tradingsymbol"].str.replace(
        "NFO:", "", regex=False)

    # Merge correctly by tradingsymbol
    options_data = pd.merge(
        options_data,
        ltp_data[["tradingsymbol", "ltp"]],
        on="tradingsymbol",
        how="inner"
    )

    # Filter by LTP range
    options_data = options_data[
        (options_data["ltp"] > 70) & (options_data["ltp"] < 140)
    ].reset_index(drop=True)

    options_data = banknifty_options_chain()
    kite = kite_connect()
    if kite is None:
        print("Kite connection could not be established.")
        return

    # Add NFO prefix for Kite LTP API
    instrument_list = [
        f"NFO:{symbol}" for symbol in options_data["tradingsymbol"]]
    ltp_data = kite.ltp(instrument_list)

    # Convert LTP dict → DataFrame
    ltp_data = pd.DataFrame.from_dict(ltp_data, orient="index")
    ltp_data.reset_index(inplace=True)
    ltp_data.rename(columns={"index": "tradingsymbol",
                    "last_price": "ltp"}, inplace=True)
    ltp_data["ltp"] = ltp_data["ltp"].astype(float)

    # Remove "NFO:" prefix to match original column
    ltp_data["tradingsymbol"] = ltp_data["tradingsymbol"].str.replace(
        "NFO:", "", regex=False)

    # Merge correctly by tradingsymbol
    options_data = pd.merge(
        options_data,
        ltp_data[["tradingsymbol", "ltp"]],
        on="tradingsymbol",
        how="inner"
    )

    # Filter by LTP range
    options_data = options_data[
        (options_data["ltp"] >= 70) & (options_data["ltp"] <= 140)
    ].reset_index(drop=True)

    today = datetime.now()

    historical_data = api_request(
        api=kite,
        instrument_list=options_data,
        interval="3minute",
        from_date=(today - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        to_date=today.strftime("%Y-%m-%d"),
    )
    historical_data = historical_data.sort_values(by=["tradingsymbol", "date"])

    signals = pd.DataFrame()

    for tradingsymbol, group in historical_data.groupby("tradingsymbol"):
        group = group.reset_index(drop=True)
        group["rsi3"] = ta.RSI(group["close"], timeperiod=3)
        group["previous_low"] = group["low"].shift(2)

        # Define boolean masks
        cond_rsi = group["rsi3"] > 50
        cond_bull1 = group["close"] > group["open"]
        cond_bull2 = cond_bull1.shift(1)
        cond_bear3 = (group["close"].shift(2) < group["open"].shift(2))
        cond_break = group["close"] > group["high"].shift(1)

        # Combine conditions
        group["signal"] = np.where(
            cond_rsi & cond_bull1 & cond_bull2 & cond_bear3 & cond_break,
            "BUY",
            None
        )

        group_signals = group[group["signal"] == "BUY"].copy()

        if not group_signals.empty:
            group_signals["tradingsymbol"] = tradingsymbol
            group_signals["date"] = group_signals["date"].dt.strftime(
                "%Y-%m-%d %H:%M")
            group_signals["entry_price"] = group_signals["high"] + 1
            group_signals["stop_loss"] = group_signals["previous_low"] - 1
            group_signals["risk"] = np.where(
                group_signals["stop_loss"] > 15, "HIGH", "LOW")
            group_signals["target_price_1"] = group_signals["high"] + \
                ((group_signals["high"] - group_signals["previous_low"]) * 2)
            group_signals["target_price_2"] = group_signals["high"] + \
                ((group_signals["high"] - group_signals["previous_low"]) * 3)

            group_signals[["tradingsymbol", "date", "entry_price",
                           "stop_loss", "target_price_1", "target_price_2", "signal", "risk"]]
            signals = pd.concat([signals, group_signals], ignore_index=True)

    if not signals.empty:
        signals = signals.sort_values(by=["date"]).reset_index()

        print("🚀 Buy Signals:")
        print(signals.iloc[-10:][["tradingsymbol", "date", "close", "entry_price",
                                  "stop_loss", "target_price_1", "target_price_2", "signal", "risk"]])
    else:
        print("No buy signals found.")
    return None


if __name__ == "__main__":
    # clear console
    os.system('cls' if os.name == 'nt' else 'clear')
    scan()
