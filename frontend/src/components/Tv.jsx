import { useEffect } from "react";

const Tv = () => {
  useEffect(() => {
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/tv.js";
    script.async = true;
    script.onload = () => {
      new window.TradingView.widget({
        symbol: "NSE:INDIANB",
        interval: "15",
        theme: "dark",
        container_id: "tv_chart",
        autosize: true,
      });
    };
    document.body.appendChild(script);
  }, []);

  return <div id="tv_chart" style={{ height: "500px", width: "100%" }} />;
};

export default Tv;
