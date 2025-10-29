import React, { useEffect, useState } from "react";
import CandleChart from "./components/CandleChart";
import Basetemplate from "./template/Basetemplate";
import GlobalContext from "./template/GlobalContext";
import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import IntradaySignals from "./pages/IntradaySignals";

function App() {
  const api = "http://127.0.0.1:8000";
  const provider = { api };

  return (
    <Router>
      <GlobalContext.Provider value={provider}>
        <Basetemplate provider={provider}>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/intraday-signals" element={<IntradaySignals />} />
          </Routes>
        </Basetemplate>
      </GlobalContext.Provider>
    </Router>
  );
}

export default App;
