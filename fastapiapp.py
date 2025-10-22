from fastapi import FastAPI, Request, responses
from framework.ohlc import fetch_ohlc_data
import pytz
import config
import uvicorn


app = FastAPI()
conn = config.db_conn()


@app.get("/ohlc")
async def ohlc(request: Request):
    symbol = request.query_params.get("symbol", "")
    from_date = request.query_params.get("from_date", "")
    to_date = request.query_params.get("to_date", "")
    timeframe = request.query_params.get("timeframe", "1day")
    params = {
        "symbol": symbol,
        "from_date": from_date,
        "to_date": to_date,
        "timeframe": timeframe,
        "conn": conn
    }

    json_data = fetch_ohlc_data(**params)
    return responses.JSONResponse(content=json_data)

if __name__ == "__main__":
    uvicorn.run("fastapiapp:app", host="127.0.0.1", port=8000, reload=True)
