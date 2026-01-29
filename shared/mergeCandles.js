export function mergeCandles(existing, incoming, source) {
  const map = new Map();

  for (const r of existing) {
    map.set(r.timestamp, r);
  }

  for (const r of incoming) {
    const prev = map.get(r.timestamp);

    if (!prev) {
      map.set(r.timestamp, r);
      continue;
    }

    // Overwrite rules
    if (source === "historical") {
      map.set(r.timestamp, r);
    } else {
      if (prev.source !== "historical") {
        map.set(r.timestamp, r);
      }
    }
  }

  return Array.from(map.values()).sort(
    (a, b) => new Date(a.timestamp) - new Date(b.timestamp)
  );
}