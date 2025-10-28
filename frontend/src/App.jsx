import React, { useEffect, useState } from "react";
import CandleChart from "./components/CandleChart";
import Basetemplate from "./template/Basetemplate";
import GlobalContext from "./template/GlobalContext";
import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";

function App() {
  const api = "http://127.0.0.1:8000";
  const provider = { api };

  return (
    <Router>
      <GlobalContext.Provider value={provider}>
        <Basetemplate provider={provider}>
          <Routes>
            <Route path="/" element={<div />} />
            <Route
              path="/intraday-signals"
              element={<div>Intraday Signals Page</div>}
            />
          </Routes>
        </Basetemplate>
      </GlobalContext.Provider>
    </Router>
  );
}

export default App;
