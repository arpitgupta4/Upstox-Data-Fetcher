// runner.js
import fs from "fs";
import path from "path";
import axios from "axios";
import https from "https";
import dotenv from "dotenv";
import pLimit from "p-limit";
import parquet from "parquetjs-lite";

dotenv.config();

/* =========================================================
   CONFIG
========================================================= */

const BASE_URL = "https://api.upstox.com/v3";
const DATA_DIR = "./data";

const DAILY_FROM = "2022-01-01";
const INTRADAY_DAYS = 30;

// Concurrency controls (ONLY mechanism used)
const FETCH_CONCURRENCY = 20;
const WRITE_CONCURRENCY = 8;
const RETRIES = 3;

/* =========================================================
   SYMBOL LOCKS (ONE WRITER PER SYMBOL)
========================================================= */

const symbolLocks = new Map();

function getSymbolLock(symbol) {
  if (!symbolLocks.has(symbol)) {
    symbolLocks.set(symbol, pLimit(1));
  }
  return symbolLocks.get(symbol);
}

/* =========================================================
   NETWORK
========================================================= */

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 50
});

const client = axios.create({
  baseURL: BASE_URL,
  httpsAgent,
  timeout: 30000, // prevents silent hangs
  headers: {
    Accept: "application/json",
    Authorization: `Bearer ${process.env.UPSTOX_ACCESS_TOKEN}`
  }
});

/* =========================================================
   TIME UTILS
========================================================= */

function todayIST() {
  const d = new Date();
  const utc = d.getTime() + d.getTimezoneOffset() * 60000;
  const ist = new Date(utc + 5.5 * 3600000);
  if (ist.getHours() < 16) ist.setDate(ist.getDate() - 1);
  return ist.toISOString().slice(0, 10);
}

/* =========================================================
   SYMBOL LOADER
========================================================= */

function loadSymbolIsin() {
  const map = new Map();
  const rows = fs.readFileSync("symbol_isin.csv", "utf-8").split("\n");

  for (let i = 1; i < rows.length; i++) {
    const [symbol, isin] = rows[i].split(",");
    if (symbol && isin) {
      map.set(symbol.trim().toUpperCase(), `NSE_EQ|${isin.trim()}`);
    }
  }
  return map;
}

/* =========================================================
   PARQUET SCHEMA
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
  volume: { type: "INT64" }
});

const writeQueue = pLimit(WRITE_CONCURRENCY);

/* =========================================================
   PARQUET WRITER (SYMBOL-CENTRIC, SAFE)
========================================================= */

async function writeParquet(timeframe, rows, symbol) {
  if (!rows.length) return;

  const dir = path.join(
    DATA_DIR,
    `timeframe=${timeframe}`,
    `symbol=${symbol}`
  );
  fs.mkdirSync(dir, { recursive: true });

  const file = path.join(dir, "data.parquet");

  // Load existing data (if any)
  const rowMap = new Map();

  if (fs.existsSync(file)) {
    const reader = await parquet.ParquetReader.openFile(file);
    const cursor = reader.getCursor();
    let record;
    while ((record = await cursor.next())) {
      rowMap.set(record.timestamp, record);
    }
    await reader.close();
  }

  // Merge new rows
  for (const r of rows) {
    rowMap.set(r.timestamp, r);
  }

  const finalRows = Array.from(rowMap.values()).sort(
    (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
  );

  // Atomic rewrite
  const tmp = file + ".tmp";
  const writer = await parquet.ParquetWriter.openFile(schema, tmp, {
    compression: "SNAPPY"
  });

  for (const r of finalRows) {
    await writer.appendRow(r);
  }

  await writer.close();

  if (fs.existsSync(file)) fs.unlinkSync(file);
  fs.renameSync(tmp, file);
}

/* =========================================================
   API FETCH
========================================================= */

async function fetchHistorical(unit, interval, key, from, to, attempt = 1) {
  try {
    const url = `/historical-candle/${encodeURIComponent(
      key
    )}/${unit}/${interval}/${to}/${from}`;

    const r = await client.get(url);
    return r.data?.data?.candles || [];
  } catch (e) {
    if (attempt >= RETRIES) return [];
    if (e.response?.status === 429) {
      await new Promise(r => setTimeout(r, 15000));
    }
    return fetchHistorical(unit, interval, key, from, to, attempt + 1);
  }
}

/* =========================================================
   MAIN RUNNER
========================================================= */

async function run() {
  fs.mkdirSync(DATA_DIR, { recursive: true });

  const symbolMap = loadSymbolIsin();
  const symbols = [...symbolMap.keys()];
  const today = todayIST();

  console.log(`🚀 RUN START | Symbols: ${symbols.length} | Today: ${today}`);

  const limit = pLimit(FETCH_CONCURRENCY);

  const TIMEFRAMES = [
    { name: "1d", unit: "days", interval: 1, from: DAILY_FROM },
    { name: "15m", unit: "minutes", interval: 15, days: INTRADAY_DAYS },
    { name: "1h", unit: "hours", interval: 1, days: INTRADAY_DAYS }
  ];

  for (const tf of TIMEFRAMES) {
    console.log(`\n📊 ${tf.name.toUpperCase()} START`);
    let completed = 0;

    await Promise.all(
      symbols.map(sym =>
        limit(async () => {
          const key = symbolMap.get(sym);
          if (!key) return;

          let from = tf.from;
          if (tf.days) {
            const d = new Date();
            d.setDate(d.getDate() - tf.days);
            from = d.toISOString().slice(0, 10);
          }

          const candles = await fetchHistorical(
            tf.unit,
            tf.interval,
            key,
            from,
            today
          );

          if (!candles.length) {
            completed++;
            return;
          }

          // Deduplicate + normalize
          const seen = new Set();
          const rows = [];

          for (const c of candles) {
            if (seen.has(c[0])) continue;
            seen.add(c[0]);

            rows.push({
              symbol: sym,
              instrument_key: key,
              timeframe: tf.name,
              timestamp: c[0],
              open: c[1],
              high: c[2],
              low: c[3],
              close: c[4],
              volume: BigInt(c[5])
            });
          }

          rows.sort(
            (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
          );

          const lock = getSymbolLock(sym);
          await lock(() =>
            writeQueue(() => writeParquet(tf.name, rows, sym))
          );

          completed++;
          if (completed % 25 === 0) {
            console.log(
              `   ⚡ ${tf.name} progress: ${completed}/${symbols.length}`
            );
          }
        })
      )
    );

    console.log(`✅ ${tf.name.toUpperCase()} DONE`);
  }

  console.log("\n🏁 ALL DONE");
}

/* =========================================================
   ENTRYPOINT
========================================================= */

run().catch(err => {
  console.error("❌ Runner crashed", err);
  process.exit(1);
});