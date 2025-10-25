from fastapi import FastAPI, Request, responses
from framework.data.ohlc import fetch_ohlc_data
from framework.data.ohlc import fetch_ta_data, symbols
import pytz
import config
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:8000",
]


app = FastAPI()
conn = config.db_conn()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,          # your frontend URLs
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
        "conn": conn
    }
    print(params)

    json_data = fetch_ohlc_data(**params)
    return responses.JSONResponse(content=json_data)


@app.get("/ta")
async def ta(symbol: str = "", from_date: str = "", to_date: str = "", timeframe: str = ""):

    params = {
        "symbol": symbol,
        "from_date": from_date,
        "to_date": to_date,
        "timeframe": timeframe,
        "conn": conn
    }

    ta_data = fetch_ta_data(**params)
    return responses.JSONResponse(content=ta_data)


@app.get("/symbols")
async def fetch_symbols(timeframe: str = ""):
    symbol_list = symbols(conn=conn, timeframe=timeframe)
    return responses.JSONResponse(content=symbol_list)

if __name__ == "__main__":
    uvicorn.run("fastapiapp:app", host="127.0.0.1", port=8000, reload=True)
