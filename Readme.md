📦 Setup & Installation
1️⃣ Prerequisites

Make sure you have these installed:

Node.js ≥ 18
Check:

node -v

npm (comes with Node)

npm -v
2️⃣ Clone the Repository
git clone <your-github-repo-url>
cd upstox-data-fetcher
3️⃣ Install Node Dependencies

Run this once after cloning:

npm install

This installs all required libraries:

axios → API requests

dotenv → environment variables

parquetjs-lite → Parquet storage

p-limit → concurrency control (if used elsewhere)

If you want to be explicit instead:

npm install axios dotenv parquetjs-lite p-limit
4️⃣ Create .env File (Required)

In the project root:

touch .env

Add your Upstox access token:

UPSTOX_ACCESS_TOKEN=your_upstox_access_token_here

⚠️ Do NOT commit .env to GitHub

5️⃣ Required Files

Ensure these files exist:

symbol_isin.csv   # symbol ↔ ISIN mapping

Format:

symbol,isin
3MINDIA,INE470A01017
RELIANCE,INE002A01018
6️⃣ Run Scripts
▶️ Fetch Intraday + Append Daily Candle
node intraday.js
▶️ Fetch Full Historical Data
node historical.js
7️⃣ Output Structure

Data is saved as Parquet files:

data/
├── timeframe=15m/
│   └── symbol=3MINDIA/data.parquet
├── timeframe=1d/
│   └── symbol=3MINDIA/data.parquet

15m → incremental, appends daily

1d → today’s candle overwrites if re-run after market close
