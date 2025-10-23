import React, { useLayoutEffect, useRef, useEffect } from "react";
import { createChart, ColorType, CandlestickSeries } from "lightweight-charts";

export default function CandleChart({ data = [] }) {
  const containerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    // v5 chart creation
    const chart = createChart(el, {
      width: el.clientWidth,
      height: "400",
      layout: {
        background: { type: ColorType.Solid, color: "#111" },
        textColor: "#ddd",
      },
      grid: {
        vertLines: { color: "#222" },
        horzLines: { color: "#222" },
      },
    });

    // v5 way to create a candlestick series
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderVisible: false,
      wickUpColor: "#26a69a",
      wickDownColor: "#ef5350",
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
    const formatted = data.map((d) => ({
      time: Math.floor(new Date(d.date).getTime() / 1000),
      open: Number(d.open),
      high: Number(d.high),
      low: Number(d.low),
      close: Number(d.close),
    }));
    seriesRef.current.setData(formatted);
  }, [data]);

  return (
    <div
      ref={containerRef}
      style={{
        width: "100%",
        height: "100%",
        borderRadius: 8,
        overflow: "hidden",
      }}
    />
  );
}
