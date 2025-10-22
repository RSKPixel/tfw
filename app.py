from flask import Flask
from framework.ohlc import fetch_ohlc_data
import pytz
import config

app = Flask(__name__)
conn = config.db_conn()


@app.route("/ohlc")
def ohlc(request):
    symbol = request.args.get("symbol", "")
    from_date = request.args.get("from_date", "2025-10-01")
    to_date = request.args.get("to_date", "2025-10-17")
    timeframe = request.args.get("timeframe", "15min")
    params = {
        "symbol": symbol,
        "from_date": from_date,
        "to_date": to_date,
        "timeframe": timeframe,
        "conn": conn
    }
    json_data = fetch_ohlc_data(**params)
    return json_data


if __name__ == "__main__":
    app.run(debug=True, port=5000)
