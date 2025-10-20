import sys
import os
import json
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from framework import config
import pytz
IST = pytz.timezone("Asia/Kolkata")

table_name = {
    "3min": "idata_3min",
    "5min": "idata_5min",
    "15min": "idata_15min",
    "60min": "idata_60min",
    "1day": "idata_1day"
}


def fetch_ohlc_data(symbol="All", from_date="", to_date="", timeframe="15min"):
    conn = config.db_conn()
    cursor = conn.cursor()
    query_table = table_name.get(timeframe)

    if not query_table:
        raise ValueError("Invalid timeframe specified.")

    if from_date == "":
        from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if to_date == "":
        to_date = datetime.now().strftime("%Y-%m-%d")

    if symbol == "All":
        query = f"""
            SELECT *
            FROM {query_table}
            WHERE date BETWEEN %s AND %s
            ORDER BY date DESC;
        """
        parms = (from_date, to_date)
    else:
        query = f"""
            SELECT *
            FROM {query_table}
            WHERE symbol = %s AND date BETWEEN %s AND %s
            ORDER BY date DESC;
        """
        parms = (symbol, from_date, to_date)

    cursor.execute(query, parms)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    df["date"] = df["date"].dt.tz_convert(IST)
    df['symbol'] = symbol
    return df.to_json(orient='records', date_format='iso')


if __name__ == "__main__":
    symbol = "NIFTY-I"
    from_date = "2025-10-01"
    to_date = "2025-10-10"
    timeframe = "15min"
    ohlc_json = fetch_ohlc_data(symbol, from_date, to_date, timeframe)
    ohlc_json = json.dumps(json.loads(ohlc_json), indent=2)
    print(ohlc_json)
