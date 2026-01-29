import parquet from "parquetjs-lite";

export const schema = new parquet.ParquetSchema({
  symbol: { type: "UTF8" },
  instrument_key: { type: "UTF8" },
  timeframe: { type: "UTF8" },
  timestamp: { type: "UTF8" },
  open: { type: "DOUBLE" },
  high: { type: "DOUBLE" },
  low: { type: "DOUBLE" },
  close: { type: "DOUBLE" },
  volume: { type: "INT64" },
  source: { type: "UTF8" } // historical | intraday
});