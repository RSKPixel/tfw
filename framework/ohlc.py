import sys
import os
import json
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
import pytz
IST = pytz.timezone("Asia/Kolkata")

table_name = {
    "3min": "idata_3min",
    "5min": "idata_5min",
    "15min": "idata_15min",
    "60min": "idata_60min",
    "1day": "idata_1day"
}


def fetch_ohlc_data(symbol="", from_date="", to_date="", timeframe="15min", conn=None):
    if conn is None:
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
        ORDER BY date DESC;
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
