import React, { useContext, useEffect, useState } from "react";
import GlobalContext from "./GlobalContext";
import { Link } from "react-router-dom";

const Basetemplate = ({
  children,
  setSelectedSymbol,
  setTimeframe,
  timeframe,
}) => {
  const [symbols, setSymbols] = useState([]);
  const { api } = useContext(GlobalContext);

  return (
    <div className="flex flex-col h-screen w-full">
      {/* Top Bar */}
      <div className="flex flex-row fixed w-full shadow-2xl justify-between items-center bg-gray-950 px-4 py-1  text-stone-100 border-b border-sky-900">
        <div className="flex flex-col cursor-pointer hover:text-yellow-300">
          <h1 className="text-xl text-center font-bold ">
            <Link to="/">Trader's Framework</Link>
          </h1>
        </div>
        <div className="ms-auto" />
        <div className="flex flex-row gap-6 text-sm font-medium">
          <span>Login</span>
          <span>
            <Link to="/intraday-signals">Intraday Signals</Link>
          </span>
          <span>
            <Link to="/intraday-donchian-signals">
              Intraday Donchian Signals
            </Link>
          </span>
        </div>
      </div>

      {/* Main Layout */}
      <div className="flex flex-row text-stone-200 bg-gray-900 w-full flex-1 overflow-hidden pt-9">
        {children}
      </div>
    </div>
  );
};

export default Basetemplate;
