import React, { useLayoutEffect, useRef, useEffect, useState } from "react";
import {
  createChart,
  ColorType,
  CandlestickSeries,
  LineSeries,
} from "lightweight-charts";

export default function CandleChart({ data = [] }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const [hoverData, setHoverData] = useState(null);
  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    // v5 chart creation
    const chart = createChart(el, {
      width: el.clientWidth,
      height: el.clientHeight * 1 || 600,
      borderVisible: true,
      borderColor: "#333",

      layout: {
        background: { type: ColorType.Solid, color: "#111" },
        textColor: "#ddd",
      },
      grid: {
        vertLines: { color: "#222" },
        horzLines: { color: "#222" },
      },
      timeScale: {
        borderColor: "#333",
        timeVisible: true,
        secondsVisible: true,
      },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
    });

    chart.subscribeCrosshairMove((param) => {
      if (!param?.time || !param.seriesData) {
        setHoverData(null);
        return;
      }

      const cData = param.seriesData.get(candleSeries);

      if (cData) {
        setHoverData({
          time: param.time,
          open: cData.open,
          high: cData.high,
          low: cData.low,
          close: cData.close,
        });
      }
    });

    const ema13 = chart.addSeries(LineSeries, {
      color: "#2196f3",
      lineWidth: 2,
    });
    chartRef.current = chart;
    seriesRef.current = candleSeries;

    const handleResize = () => chart.applyOptions({ width: el.clientWidth });
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, []);

  useEffect(() => {
    if (!seriesRef.current) return;
    const formatted = data.map((d) => {
      const date = new Date(d.date);
      return {
        time: Math.floor(
          (date.getTime() - date.getTimezoneOffset() * 60 * 1000) / 1000
        ),
        open: +d.open,
        high: +d.high,
        low: +d.low,
        close: +d.close,
      };
    });

    seriesRef.current.setData(formatted);
    chartRef.current.timeScale().fitContent();
  }, [data]);

  const current = hoverData || data[data.length - 1];

  return (
    <div className="h-96 w-full bg-[#111] rounded-xl text-gray-300">
      {/* --- HEADER / INFO BAR --- */}
      <div className="flex flex-wrap gap-2 text-sm px-3 py-2 border-b border-gray-800 bg-gray-900/70">
        {/* <span className="font-bold text-blue-400">{symbol}</span> */}
        {current && (
          <>
            {/* <span>{new Date(current.time).toDateString()}</span> */}
            <span>O: {current.open?.toFixed(2)}</span>
            <span>H: {current.high?.toFixed(2)}</span>
            <span>L: {current.low?.toFixed(2)}</span>
            <span>C: {current.close?.toFixed(2)}</span>
            {current.ema13 && (
              <span className="text-blue-400">
                EMA13: {current.ema13.toFixed(2)}
              </span>
            )}
            {current.rsi && (
              <span className="text-amber-400">
                RSI: {current.rsi.toFixed(2)}
              </span>
            )}
          </>
        )}
      </div>
      <div className="flex flex-1 w-full h-full" ref={containerRef} />
    </div>
  );
}
