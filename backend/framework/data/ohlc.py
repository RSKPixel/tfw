import json
from datetime import datetime, timedelta
import pandas as pd
import talib as ta
import pytz
IST = pytz.timezone("Asia/Kolkata")

table_name = {
    "3min": "idata_3min",
    "5min": "idata_5min",
    "15min": "idata_15min",
    "60min": "idata_60min",
    "1day": "idata_1day"
}


def symbols(conn=None):
    if conn is None or conn.closed:
        return {"status": "error", "error": "Database connection is not provided."}

    query = "SELECT DISTINCT symbol FROM idata_1day ORDER BY symbol;"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            return []

        return [row[0] for row in rows]
    except Exception as e:
        return {"status": "error", "error": str(e)}
    finally:
        if cursor:
            cursor.close()


def fetch_ohlc_data(symbol="", from_date="", to_date="", timeframe="15min", conn=None):
    if conn is None or conn.closed:
        return {"status": "error", "error": "Database connection is not provided."}

    if not symbol:
        return {"status": "error", "error": "Symbol parameter is required."}

    query_table = table_name.get(timeframe)
    if not query_table:
        return {"status": "error", "error": "Invalid timeframe specified."}

    if not from_date:
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = datetime.now().strftime("%Y-%m-%d")

    query = f"""
        SELECT date AT TIME ZONE 'Asia/Kolkata' AS local_time, *
        FROM {query_table}
        WHERE symbol = %s AND date BETWEEN %s AND %s
        ORDER BY date ASC;
    """
    params = (symbol, from_date, to_date)

    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not rows:
            return []  # ✅ return list, not json string

        df = pd.DataFrame(rows, columns=columns)
        df['date'] = df["local_time"]
        df.drop(columns=["local_time", "id"], inplace=True)

        return json.loads(df.to_json(orient="records", date_format="iso"))
    except Exception as e:
        return {"status": "error", "error": str(e)}

    finally:
        if cursor:
            cursor.close()


def fetch_ta_data(symbol="", from_date="", to_date="", timeframe="1day", conn=None):
    if conn is None or conn.closed:
        return {"status": "error", "error": "Database connection is not provided."}

    if not symbol:
        return {"status": "error", "error": "Symbol parameter is required."}

    query_table = table_name.get(timeframe)
    if not query_table:
        return {"status": "error", "error": "Invalid timeframe specified."}

    if not from_date:
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = datetime.now().strftime("%Y-%m-%d")

    query = f"""
        SELECT date AT TIME ZONE 'Asia/Kolkata' AS local_time, *
        FROM {query_table}
        WHERE symbol = %s AND date(date) >= %s AND date(date) <= %s
        ORDER BY date ASC;
    """
    params = (symbol, from_date, to_date)

    print(params)

    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not rows:
            return []

        df = pd.DataFrame(rows, columns=columns)
        df['date'] = df["local_time"]
        df.drop(columns=["local_time", "id"], inplace=True)

        df["ema_13"] = ta.EMA(df["close"], timeperiod=13)
        df["ema_20"] = ta.EMA(df["close"], timeperiod=20)
        df["ema_50"] = ta.EMA(df["close"], timeperiod=50)
        df["ema_100"] = ta.EMA(df["close"], timeperiod=100)
        df["ema_200"] = ta.EMA(df["close"], timeperiod=200)
        df["rsi_3"] = ta.RSI(df["close"], timeperiod=3)
        df["rsi_13"] = ta.RSI(df["close"], timeperiod=13)

        df["intraday_buy"] = (
            (df["ema_13"] > df["ema_50"]) &
            (df["ema_50"] > df["ema_200"]) &
            (df["rsi_3"] > 80) &
            (df.iloc[-1]["close"] > df.iloc[-1]["open"]) &
            (df.iloc[-2]["close"] > df.iloc[-2]["open"]) &
            (df.iloc[-1]["close"] > df.iloc[-2]["high"])
        )
        df["intraday_sell"] = (
            (df["ema_13"] < df["ema_50"]) &
            (df["ema_50"] < df["ema_200"]) &
            (df["rsi_3"] < 20) &
            (df.iloc[-1]["close"] < df.iloc[-1]["open"]) &
            (df.iloc[-2]["close"] < df.iloc[-2]["open"]) &
            (df.iloc[-1]["close"] < df.iloc[-2]["low"])
        )

        df['sma20'] = ta.SMA(df['close'], timeperiod=20)
        df["bb_upper"] = ta.BBANDS(df["close"], timeperiod=20)[0]
        df["bb_middle"] = ta.BBANDS(df["close"], timeperiod=20)[1]
        df["bb_lower"] = ta.BBANDS(df["close"], timeperiod=20)[2]

        df["atr_20"] = ta.ATR(df["high"], df["low"],
                              df["close"], timeperiod=20)

        df["keltner_upper"] = (df["ema_20"] + 1.5 * df["atr_20"])
        df["keltner_lower"] = (df["ema_20"] - 1.5 * df["atr_20"])

        def in_squeeze(df):
            return df["bb_lower"] > df["keltner_lower"] and df["bb_upper"] < df["keltner_upper"]
        df["in_squeeze"] = df.apply(in_squeeze, axis=1)

        df["macd"], df["macd_signal"], df["macd_hist"] = ta.MACD(
            df["close"], fastperiod=12, slowperiod=26, signalperiod=9
        )

        df["bull_candle"] = df["close"] > df["open"]
        df["bear_candle"] = df["open"] > df["close"]

        df.dropna(inplace=True)
        df.to_clipboard()
        df = df.round(2)

        return json.loads(df.to_json(orient="records", date_format="iso"))
    except Exception as e:
        return {"status": "error", "error": str(e)}

    finally:
        if cursor:
            cursor.close()


def compute_rsi(series, period):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi
