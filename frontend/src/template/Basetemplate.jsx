import React, { useContext, useEffect, useState } from "react";
import GlobalContext from "./GlobalContext";

const Basetemplate = ({
  children,
  setSelectedSymbol,
  setTimeframe,
  timeframe,
}) => {
  const [symbols, setSymbols] = useState([]);
  const { api } = useContext(GlobalContext);
  const frames = {
    "3min": "3m",
    "5min": "5m",
    "15min": "15m",
    "60min": "1hr",
    "1day": "1d",
  };

  useEffect(() => {
    fetch(`${api}/symbols?timeframe=${timeframe}`)
      .then((res) => res.json())
      .then((data) => setSymbols(data))
      .catch((err) => console.error("Error fetching symbols:", err));
  }, [api, timeframe]);

  const handleSelection = (event) => {
    const selectedOptions = Array.from(event.target.selectedOptions).map(
      (option) => option.value
    );
    setSelectedSymbol(selectedOptions[0] || ""); // Set to first selected symbol or empty string
  };

  return (
    <div className="flex flex-col h-screen w-full">
      {/* Top Bar */}
      <div className="flex flex-row fixed w-full shadow-2xl justify-between items-center bg-gray-950 px-4 py-1  text-stone-100 border-b border-sky-900">
        <div className="flex flex-col cursor-pointer hover:text-yellow-300">
          <h1 className="text-xl text-center font-bold ">Trader's Framework</h1>
        </div>
        <div className="ms-auto" />
        <div className="flex flex-col">
          <span>Login</span>
        </div>
      </div>

      {/* Main Layout */}
      <div className="flex flex-row text-stone-200 w-full flex-1 overflow-hidden pt-9">
        {/* Sidebar */}
        <div className="flex flex-col w-[350px] gap-2 h-full text-sm font-medium border-r border-gray-900 bg-gray-900 overflow-y-auto">
          <div className="flex flex-row p-1 bg-amber-900">
            {Object.entries(frames).map(([label, key]) => (
              <span
                key={key}
                onClick={() => setTimeframe(label)}
                className={`p-1 text-sm font-medium hover:bg-amber-800 cursor-pointer rounded-md mx-1 ${
                  timeframe === label ? "bg-amber-800" : ""
                }`}
              >
                {label}
              </span>
            ))}
          </div>

          <select
            className="bg-gray-900 pt-2 pb-4 rounded-md h-full focus:outline-none scroll-none scrollbar-thin scrollbar-thumb-sky-900 scrollbar-track-gray-800"
            multiple={true}
            size={1}
            onChange={handleSelection}
          >
            {/* <option value="">Select Symbol</option> */}
            {symbols.map((symbol) => (
              <option key={symbol} value={symbol} className="p-2">
                {symbol}
              </option>
            ))}
          </select>
        </div>

        {/* Main Content */}
        <div className="flex flex-col w-full bg-gray-800 p-4 items-center overflow-y-scroll scrollbar-thin scrollbar-track-gray-800 scrollbar-thumb-sky-900">
          {children}
        </div>
      </div>
    </div>
  );
};

export default Basetemplate;
