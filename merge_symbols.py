import csv

equity_file = "EQUITY_L.csv"
symbols_file = "upstox_symbols.txt"
output_file = "symbol_isin.csv"

# Read symbols.txt
symbols = []
with open(symbols_file) as f:
    for line in f:
        s = line.strip().upper()
        if s:
            symbols.append(s)

# Load equity master and auto-detect columns
symbol_isin = {}

with open(equity_file, newline="", encoding="utf-8-sig") as f:
    reader = csv.reader(f)
    headers = next(reader)

    # Normalize headers
    headers = [h.strip().upper() for h in headers]

    symbol_idx = headers.index("SYMBOL")
    isin_idx = [i for i, h in enumerate(headers) if "ISIN" in h][0]

    for row in reader:
        symbol = row[symbol_idx].strip().upper()
        isin = row[isin_idx].strip()
        symbol_isin[symbol] = isin

# Write merged output
with open(output_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["SYMBOL", "ISIN"])

    for s in symbols:
        writer.writerow([s, symbol_isin.get(s, "")])

print("✅ Merged file saved:", output_file)