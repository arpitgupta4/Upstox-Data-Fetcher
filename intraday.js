// intraday.js
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

const INTRADAY_FROM = "2026-01-20";
const TOKEN = process.env.UPSTOX_ACCESS_TOKEN;

// SPEED SETTINGS (UNCHANGED)
const CONCURRENCY = 5;
const SLEEP_MS = 200;
const RETRY_LIMIT = 3;

/* =========================================================
   AXIOS
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

const sleep = ms => new Promise(r => setTimeout(r, ms));

function isAfterMarketCloseIST() {
  const now = new Date();
  const utc = now.getTime() + now.getTimezoneOffset() * 60000;
  const ist = new Date(utc + 5.5 * 60 * 60 * 1000);

  const h = ist.getHours();
  const m = ist.getMinutes();
  return h > 15 || (h === 15 && m >= 30);
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
   PARQUET MERGE WRITER
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
  source: { type: "UTF8" }
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
    map.set(r.timestamp, r); // overwrite same day if exists
  }

  const tmp = file + ".tmp";
  const writer = await parquet.ParquetWriter.openFile(schema, tmp);
  for (const r of [...map.values()].sort((a, b) =>
    a.timestamp.localeCompare(b.timestamp)
  )) {
    await writer.appendRow(r);
  }
  await writer.close();

  if (fs.existsSync(file)) fs.unlinkSync(file);
  fs.renameSync(tmp, file);
}

/* =========================================================
   API FETCHERS
========================================================= */

async function fetchIntraday15m(key) {
  const r = await client.get(
    `/historical-candle/intraday/${encodeURIComponent(key)}/minutes/15`
  );
  return r.data?.data?.candles || [];
}

async function fetchIntradayDay(key) {
  const r = await client.get(
    `/historical-candle/intraday/${encodeURIComponent(key)}/days/1`
  );
  return r.data?.data?.candles || [];
}

/* =========================================================
   CORE PROCESSOR
========================================================= */

async function processSymbol(symbol, key) {

  /* ── 15 MIN DATA (UNCHANGED) ───────────────────────── */
  let candles15m = await fetchIntraday15m(key);
  candles15m = candles15m.filter(
    c => c[0].slice(0, 10) >= INTRADAY_FROM
  );

  await writeParquet("15m", symbol, candles15m.map(c => ({
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
  })));

  /* ── DAILY CANDLE → APPEND TO HISTORICAL 1D ───────── */
  if (isAfterMarketCloseIST()) {
    const candles1d = await fetchIntradayDay(key);

    await writeParquet("1d", symbol, candles1d.map(c => ({
      symbol,
      instrument_key: key,
      timeframe: "1d",
      timestamp: c[0],
      open: c[1],
      high: c[2],
      low: c[3],
      close: c[4],
      volume: BigInt(c[5]),
      source: "intraday_final"
    })));
  }
}

/* =========================================================
   MAIN RUNNER (UNCHANGED)
========================================================= */

async function run() {
  const symbols = loadSymbolMap();
  const queue = Array.from(symbols.entries());

  console.log(
    `⚡ INTRADAY START | Symbols: ${queue.length} | AfterClose=${isAfterMarketCloseIST()}`
  );

  let completed = 0;

  const worker = async () => {
    while (queue.length) {
      const [symbol, key] = queue.shift();
      let attempts = 0;

      while (attempts < RETRY_LIMIT) {
        try {
          await processSymbol(symbol, key);
          break;
        } catch (e) {
          attempts++;
          if (e.response?.status === 429) {
            const t = Number(e.response.headers["retry-after"] || 2);
            await sleep(t * 1000);
            attempts--;
          } else if (attempts >= RETRY_LIMIT) {
            console.error(`❌ ${symbol}`, e.message);
          }
        }
      }

      completed++;
      if (completed % 10 === 0) {
        console.log(`⚡ ${completed}/${symbols.size}`);
      }

      await sleep(SLEEP_MS);
    }
  };

  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  console.log("🏁 INTRADAY COMPLETE");
}

run().catch(err => {
  console.error("❌ Fatal", err);
  process.exit(1);
});