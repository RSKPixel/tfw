# from turtle import pd
from fastapi import FastAPI, Request, responses
from framework.data.ohlc import fetch_ohlc_data
from framework.data.ohlc import fetch_ta_data, symbols
from scanner import scan_intraday_signal, scan_donchian_signal
from datetime import datetime

import pandas as pd
import pytz
import config
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:8000",
    "http://tfw2.trialnerror.in",
    "https://tfw2.trialnerror.in",
    "http://tfw.trialnerror.in",
    "https://tfw.trialnerror.in",
]


app = FastAPI()
conn = config.db_conn()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # your frontend URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/ohlc")
async def ohlc(symbol: str = "", from_date: str = "", to_date: str = "", timeframe: str = ""):
    params = {
        "symbol": symbol,
        "from_date": from_date,
        "to_date": to_date,
        "timeframe": timeframe,
        "conn": conn,
    }

    json_data = fetch_ohlc_data(**params)
    return responses.JSONResponse(content=json_data)


@app.get("/ta")
async def ta(symbol: str = "", from_date: str = "", to_date: str = "", timeframe: str = ""):

    params = {
        "symbol": symbol,
        "from_date": from_date,
        "to_date": to_date,
        "timeframe": timeframe,
        "conn": conn,
    }

    ta_data = fetch_ta_data(**params)
    return responses.JSONResponse(content=ta_data)


@app.get("/symbols")
async def fetch_symbols(timeframe: str = ""):
    symbol_list = symbols(conn=conn, timeframe=timeframe)
    return {"status": "success", "data": symbol_list}


@app.get("/scanner/intraday-signals")
async def scanner_intraday_signals():
    conn = config.db_conn()
    today = datetime.now().date()
    from_date = today - pd.Timedelta(days=30)
    buy_signals, sell_signals = scan_intraday_signal(
        from_date=from_date, to_date=today, timeframe="15min", conn=conn
    )
    return {"status": "success", "buy_signals": buy_signals, "sell_signals": sell_signals}


@app.get("/scanner/donchian-signals")
async def scanner_donchian_symbols():
    conn = config.db_conn()
    today = datetime.now().date()
    from_date = today - pd.Timedelta(days=60)
    buy_signals, sell_signals = scan_donchian_signal(
        from_date=from_date, to_date=today, timeframe="15min", conn=conn
    )
    return {"status": "success", "buy_signals": buy_signals, "sell_signals": sell_signals}


@app.get("/scanner/intraday-test-data")
async def scanner_intraday_test_data():
    # Load test data from a local JSON file
    import json

    with open('signals.json') as f:
        test_data = json.load(f)
    return responses.JSONResponse(content=test_data)


if __name__ == "__main__":
    uvicorn.run("fastapiapp:app", host="127.0.0.1", port=8000, reload=True)
