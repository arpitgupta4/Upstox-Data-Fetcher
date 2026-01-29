import fs from "fs";
import path from "path";
import parquet from "parquetjs-lite";
import { schema } from "./schema.js";
import { mergeCandles } from "./mergeCandles.js";

export async function writeSymbolParquet({
  dataDir,
  timeframe,
  symbol,
  rows,
  source
}) {
  if (!rows.length) return;

  const dir = path.join(
    dataDir,
    `timeframe=${timeframe}`,
    `symbol=${symbol}`
  );
  fs.mkdirSync(dir, { recursive: true });

  const file = path.join(dir, "data.parquet");

  let existing = [];

  if (fs.existsSync(file)) {
    const reader = await parquet.ParquetReader.openFile(file);
    const cursor = reader.getCursor();
    let r;
    while ((r = await cursor.next())) {
      existing.push(r);
    }
    await reader.close();
  }

  const merged = mergeCandles(existing, rows, source);

  const tmp = file + ".tmp";
  const writer = await parquet.ParquetWriter.openFile(schema, tmp, {
    compression: "SNAPPY"
  });

  for (const r of merged) {
    await writer.appendRow(r);
  }

  await writer.close();

  if (fs.existsSync(file)) fs.unlinkSync(file);
  fs.renameSync(tmp, file);
}