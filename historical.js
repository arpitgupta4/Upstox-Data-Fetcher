// historical.js
import fs from "fs";
import path from "path";
import axios from "axios";
import dotenv from "dotenv";
import parquet from "parquetjs-lite";

dotenv.config();

/* =========================================================
   CONFIG
========================================================= */

const BASE_URL = "https://api.upstox.com/v3";
const DATA_DIR = "./data";

const DAILY_FROM = "2022-01-01";
const HTF_FROM = "2022-01-01";       // weekly & monthly
const INTRADAY_FROM = "2026-01-20";  // logical start for 15m

const SLEEP_MS = 120; // Cloudflare-safe delay
const TOKEN = process.env.UPSTOX_ACCESS_TOKEN;

/* =========================================================
   AXIOS (NO keepAlive — VERY IMPORTANT)
========================================================= */

const client = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
  headers: {
    Accept: "application/json",
    Authorization: `Bearer ${TOKEN}`
  }
});

/* =========================================================
   HELPERS
========================================================= */

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function todayIST() {
  const d = new Date();
  const utc = d.getTime() + d.getTimezoneOffset() * 60000;
  const ist = new Date(utc + 5.5 * 3600000);
  if (ist.getHours() < 16) ist.setDate(ist.getDate() - 1);
  return ist.toISOString().slice(0, 10);
}

function loadSymbolMap() {
  const rows = fs.readFileSync("symbol_isin.csv", "utf-8").split("\n");
  const map = new Map();
  for (let i = 1; i < rows.length; i++) {
    const [sym, isin] = rows[i].split(",");
    if (sym && isin) {
      map.set(sym.trim().toUpperCase(), `NSE_EQ|${isin.trim()}`);
    }
  }
  return map;
}

/* =========================================================
   PARQUET MERGE WRITER (SYMBOL-CENTRIC)
========================================================= */

const schema = new parquet.ParquetSchema({
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

async function writeParquet(timeframe, symbol, rows) {
  if (!rows.length) return;

  const dir = path.join(DATA_DIR, `timeframe=${timeframe}`, `symbol=${symbol}`);
  fs.mkdirSync(dir, { recursive: true });

  const file = path.join(dir, "data.parquet");
  const map = new Map();

  if (fs.existsSync(file)) {
    const reader = await parquet.ParquetReader.openFile(file);
    const cursor = reader.getCursor();
    let r;
    while ((r = await cursor.next())) map.set(r.timestamp, r);
    await reader.close();
  }

  for (const r of rows) {
    if (r.source === "historical" || !map.has(r.timestamp)) {
      map.set(r.timestamp, r);
    }
  }

  const finalRows = [...map.values()].sort(
    (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
  );

  const tmp = file + ".tmp";
  const writer = await parquet.ParquetWriter.openFile(schema, tmp);
  for (const r of finalRows) await writer.appendRow(r);
  await writer.close();

  if (fs.existsSync(file)) fs.unlinkSync(file);
  fs.renameSync(tmp, file);
}

/* =========================================================
   API FETCHERS
========================================================= */

async function fetchHistorical(key, unit, interval, from, to) {
  const url = `/historical-candle/${encodeURIComponent(
    key
  )}/${unit}/${interval}/${to}/${from}`;

  const r = await client.get(url);
  return r.data?.data?.candles || [];
}

async function fetchIntraday15m(key) {
  const url = `/historical-candle/intraday/${encodeURIComponent(
    key
  )}/minutes/15`;

  const r = await client.get(url);
  return r.data?.data?.candles || [];
}

/* =========================================================
   MAIN
========================================================= */

async function run() {
  const symbols = loadSymbolMap();
  const today = todayIST();
  const total = symbols.size;

  console.log(`🚀 HISTORICAL START | Symbols: ${total}`);
  let done = 0;
  const startTs = Date.now();

  for (const [symbol, key] of symbols.entries()) {
    done++;

    try {
      // ── Monthly
      await processHTF(symbol, key, "1m", "months", 1, HTF_FROM, today);

      // ── Weekly
      await processHTF(symbol, key, "1w", "weeks", 1, HTF_FROM, today);

      // ── Daily
      await processHTF(symbol, key, "1d", "days", 1, DAILY_FROM, today);

      // ── Intraday 15m (accumulating from 20-01-26)
      const candles = await fetchIntraday15m(key);
      const rows = candles
        .filter(c => c[0].slice(0, 10) >= INTRADAY_FROM)
        .map(c => ({
          symbol,
          instrument_key: key,
          timeframe: "15m",
          timestamp: c[0],
          open: c[1],
          high: c[2],
          low: c[3],
          close: c[4],
          volume: BigInt(c[5]),
          source: "intraday"
        }));

      await writeParquet("15m", symbol, rows);

    } catch (e) {
      if (e.response?.status === 429) {
        const retry = Number(e.response.headers["retry-after"] || 300);
        console.log(`⛔ 429 | sleeping ${retry}s`);
        await sleep(retry * 1000);
        done--;
        continue;
      }
      console.error(`❌ ${symbol}`, e.message);
    }

    if (done % 20 === 0 || done === total) {
      const elapsed = Math.round((Date.now() - startTs) / 1000);
      console.log(`⚡ ${done}/${total} | ${symbol} | ${elapsed}s`);
    }

    await sleep(SLEEP_MS + Math.random() * 80);
  }

  console.log("🏁 HISTORICAL COMPLETE");
}

/* =========================================================
   HTF PROCESSOR
========================================================= */

async function processHTF(symbol, key, tf, unit, interval, from, to) {
  const candles = await fetchHistorical(key, unit, interval, from, to);
  if (!candles.length) return;

  const rows = candles.map(c => ({
    symbol,
    instrument_key: key,
    timeframe: tf,
    timestamp: c[0],
    open: c[1],
    high: c[2],
    low: c[3],
    close: c[4],
    volume: BigInt(c[5]),
    source: "historical"
  }));

  await writeParquet(tf, symbol, rows);
  await sleep(60); // HTF burst protection
}

/* =========================================================
   RUN
========================================================= */

run().catch(err => {
  console.error("❌ Fatal", err);
  process.exit(1);
});