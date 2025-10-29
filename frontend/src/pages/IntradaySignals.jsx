import React, { useContext, useEffect, useState } from "react";
import GlobalContext from "../template/GlobalContext";
import Loader from "../components/Loader";
import numeral from "numeral";
import moment from "moment";
import CandleChart from "../components/CandleChart";

numeral.defaultFormat("0,0.00");

const IntradaySignals = () => {
  const { api } = useContext(GlobalContext);
  const [buySignals, setBuySignals] = useState([]);
  const [sellSignals, setSellSignals] = useState([]);
  const [refresh, setRefresh] = useState(false);
  const [loading, setLoading] = useState(false);
  const [testing, setTesting] = useState(false);
  const [selectedCall, setSelectedCall] = useState("buy");
  const [selectedSymbol, setSelectedSymbol] = useState(null);
  const [timeframe, setTimeframe] = useState("15min");
  const [groupedSignals, setGroupedSignals] = useState({});

  useEffect(() => {
    setLoading(true);
    const endpoint = testing
      ? `${api}/scanner/intraday-test-data`
      : `${api}/scanner/intraday-signals`;

    fetch(endpoint)
      .then((res) => res.json())
      .then((data) => {
        if (data.status === "error") {
          return;
        }
        setBuySignals(data.buy_signals);
        setSellSignals(data.sell_signals);
      })
      .catch((err) => console.error("Error fetching intraday signals:", err))
      .finally(() => setLoading(false));
  }, [refresh]);

  useEffect(() => {
    let gbs = {};
    if (selectedCall === "buy") {
      gbs = buySignals.reduce((groups, signal) => {
        const symbol = signal.symbol;
        if (!groups[symbol]) {
          groups[symbol] = [];
        }
        groups[symbol].push(signal);
        return groups;
      }, {});
    } else {
      gbs = sellSignals.reduce((groups, signal) => {
        const symbol = signal.symbol;
        if (!groups[symbol]) {
          groups[symbol] = [];
        }
        groups[symbol].push(signal);
        return groups;
      }, {});
    }
    setGroupedSignals(gbs);
  }, [buySignals, sellSignals, selectedCall]);

  const handleCall = (callType) => {
    setSelectedCall(callType);
    setSelectedSymbol(null);
  };
  return (
    <div className="flex flex-row w-full overflow-auto overflow-y-scroll h-full">
      {loading && <Loader />}
      {/* side bar */}
      <div className="flex flex-col w-[300px] border-r border-gray-600 bg-gray-800">
        <div className="flex flex-row gap-2 justify-between w-full p-1">
          <button
            onClick={() => handleCall("buy")}
            className={`${
              selectedCall === "buy"
                ? "bg-blue-800 border font-bold"
                : "bg-amber-700"
            } w-full cursor-pointer p-1`}
          >
            Buy Signals
          </button>
          <button
            onClick={() => handleCall("sell")}
            className={`${
              selectedCall === "sell"
                ? "bg-blue-800 border font-bold"
                : "bg-amber-700"
            } w-full cursor-pointer p-1`}
          >
            Sell Signals
          </button>
        </div>

        <select
          className="bg-gray-900 pt-2 pb-4 text-sm rounded-md h-full focus:outline-none scroll-none scrollbar-thin scrollbar-thumb-sky-900 scrollbar-track-gray-800"
          multiple={true}
          size={1}
          onChange={(event) => {
            setSelectedSymbol(event.target.value || null);
          }}
        >
          {Object.keys(groupedSignals).map((symbol) => (
            <option key={symbol} value={symbol} className="p-2">
              {symbol} ({groupedSignals[symbol].length})
            </option>
          ))}
        </select>
      </div>
      {/* main content */}
      <div className="flex flex-col gap-2 w-full p-4 mb-5 overflow-y-scroll scrollbar-thin scrollbar-track-gray-700 scrollbar-thumb-sky-900">
        <span className="text-start">Intraday Signals</span>
        {selectedSymbol ? (
          <>
            <h2 className="text-xl font-bold mb-4 text-stone-200">
              {selectedSymbol} - {selectedCall.toUpperCase()} Signals
            </h2>
            <table className="min-w-full table-auto border-collapse border border-gray-600">
              <thead className="bg-gray-950">
                <tr>
                  <th className="border border-gray-600 px-4 py-2 w-[15%]">
                    Date
                  </th>
                  <th className="border border-gray-600 px-4 py-2 w-[15%]">
                    LTP
                  </th>
                  <th className="border border-gray-600 px-4 py-2 w-[15%]">
                    Entry
                  </th>
                  <th className="border border-gray-600 px-4 py-2 w-[15%]">
                    Stop loss
                  </th>
                  <th className="border border-gray-600 px-4 py-2 w-[15%]">
                    Targets 1
                  </th>
                  <th className="border border-gray-600 px-4 py-2 w-[15%]">
                    Targets 2
                  </th>
                  <th className="border border-gray-600 px-4 py-2 w-[15%]">
                    Targets 3
                  </th>
                </tr>
              </thead>
              <tbody>
                {groupedSignals[selectedSymbol].map((signal, index) => (
                  <tr
                    key={index}
                    className="text-center hover:bg-gray-800 cursor-pointer"
                  >
                    <td className="border border-gray-600 px-4 py-2">
                      {moment(signal.date, "DD-MM-YYYY HH:mm:ss").format(
                        "DD-MM-YYYY HH:mm"
                      )}
                    </td>
                    <td className="border border-gray-600 px-4 py-2">
                      {numeral(signal.ltp).format()}
                    </td>
                    <td className="border border-gray-600 px-4 py-2">
                      {numeral(signal.entry).format()}
                    </td>
                    <td className="border border-gray-600 px-4 py-2">
                      {numeral(signal.sl).format()}
                    </td>
                    <td className="border border-gray-600 px-4 py-2">
                      {numeral(signal.target_1).format()}
                    </td>
                    <td className="border border-gray-600 px-4 py-2">
                      {numeral(signal.target_2).format()}
                    </td>
                    <td className="border border-gray-600 px-4 py-2">
                      {numeral(signal.target_3).format()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* <div className="border border-gray-800 w-full h-[600px] mb-12"> */}
            {/* <CandleChart symbol={selectedSymbol} timeframe={timeframe} /> */}
            {/* </div> */}
          </>
        ) : (
          <div className="text-stone-300 mt-20">
            Please select a symbol to view the signals.
          </div>
        )}
      </div>
    </div>
  );
};

export default IntradaySignals;
