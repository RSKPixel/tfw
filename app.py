from flask import Flask
from framework.ohlc import fetch_ohlc_data
import pytz

app = Flask(__name__)


@app.route("/")
def home():
    params = {
        "symbol": "All",
        "from_date": "2025-10-01",
        "to_date": "2025-10-17",
        "timeframe": "15min"
    }
    json_data = fetch_ohlc_data(**params)
    return json_data
    return


if __name__ == "__main__":
    app.run(debug=True, port=5000)
