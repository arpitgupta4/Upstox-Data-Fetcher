export function detectGaps(rows, timeframe) {
  if (rows.length < 2) return [];

  const expectedMinutes =
    timeframe === "15m" ? 15 :
    timeframe === "1h" ? 60 :
    null;

  if (!expectedMinutes) return [];

  const gaps = [];

  for (let i = 1; i < rows.length; i++) {
    const prev = new Date(rows[i - 1].timestamp);
    const curr = new Date(rows[i].timestamp);
    const diff = (curr - prev) / 60000;

    if (diff > expectedMinutes * 1.5) {
      gaps.push({
        from: rows[i - 1].timestamp,
        to: rows[i].timestamp,
        missing_minutes: diff - expectedMinutes
      });
    }
  }

  return gaps;
}