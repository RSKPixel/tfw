import React, { useEffect, useState } from "react";
import CandleChart from "./components/CandleChart";

function App() {
  const [data, setData] = useState([]);

  useEffect(() => {
    fetch(
      "http://127.0.0.1:8000/ohlc?symbol=NIFTY-I&from_date=2025-10-01&to_date=2025-10-22&timeframe=15min"
    )
      .then((res) => res.json())
      .then((json) => setData(json))
      .catch((err) => console.error("Error fetching data:", err));
  }, []);

  return (
    <div>
      <h2>NIFTY-I Candlestick Chart</h2>
      <CandleChart data={data} />
    </div>
  );
}

export default App;
