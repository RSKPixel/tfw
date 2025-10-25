import React, { useEffect, useState } from "react";
import CandleChart from "./components/CandleChart";
import Basetemplate from "./template/Basetemplate";
import GlobalContext from "./template/GlobalContext";

function App() {
  const [data, setData] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState("");
  const [timeframe, setTimeframe] = useState("15min");

  const api = "http://127.0.0.1:8000";
  const provider = { api };

  useEffect(() => {
    if (!selectedSymbol) return;

    fetch(
      `${api}/ta?symbol=${encodeURIComponent(
        selectedSymbol
      )}&from_date=2025-10-01&timeframe=${timeframe}`
    )
      .then((res) => res.json())
      .then((json) => setData(json))
      .catch((err) => console.error("Error fetching data:", err));
  }, [selectedSymbol, timeframe]);

  return (
    <GlobalContext.Provider value={provider}>
      <Basetemplate
        setSelectedSymbol={setSelectedSymbol}
        setTimeframe={setTimeframe}
        timeframe={timeframe}
      >
        {data.length === 0 && (
          <p className="text-stone-400">No data available</p>
        )}

        {data.length > 0 && <CandleChart data={data} symbol={selectedSymbol} />}
      </Basetemplate>
    </GlobalContext.Provider>
  );
}

export default App;
