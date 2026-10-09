# KlineDataCollector

High-throughput, multi-exchange historical 1-minute kline (candlestick) & funding rate data collector and normalization engine for crypto derivatives.

---

## ⚡ Key Capabilities

- **10 Supported Exchanges**: Binance USD(M), Bybit Linear, OKX Swap, Bitget Futures, BingX Swap, KuCoin Futures, Gate.io Futures, MEXC Contract, Hyperliquid, and Aster DEX.
- **Configurable Time Horizons**: Global date boundaries (`collectFrom` & `collectTo`) defined in `crypto_alts_multi_exchange.json` with full support for CLI overrides (`--from`, `--to`).
- **Resilient Multi-Threaded Ingestion**: Exchange-specific rate limiters (token bucket), automatic backoff, and pagination algorithms.
- **Full Historical Funding Rates**: Download funding intervals (8h, 4h, 1h) with automated rate-unit normalization (decimal fractions vs percentages).
- **Strict Data Pipeline Normalization**: Atomic `.tmp` file writes, deduplication, timestamp sorting, sanity validation (bad bar checking), and automated generation of `coverage.csv` & `funding_coverage.csv`.

---

## 📁 Repository Structure

```text
KlineDataCollector/
├── crypto_alts_multi_exchange.json   # Coin specifications & time boundaries (collectFrom / collectTo)
├── config_loader.py                 # Central config and datetime parsing engine
├── pipeline_orchestrator.py         # Multi-exchange download orchestrator
├── download_binance.py              # Binance 1m klines (Vision archive + FAPI)
├── download_bybit.py                # Bybit 1m klines
├── download_okx.py                  # OKX 1m history candles
├── download_gateio.py               # Gate.io 1m candlesticks
├── download_bitget.py               # Bitget 1m history candles
├── download_kucoin.py               # KuCoin 1m klines
├── download_bingx.py                # BingX 1m klines
├── download_mexc.py                 # MEXC 1m contract klines
├── download_hyperliquid.py          # Hyperliquid 1m candle snapshots
├── download_asterdex.py             # Aster DEX 1m klines
├── download_funding.py              # Historical funding rate collector for all 10 exchanges
├── normalize_klines.py              # 1-min klines normalization & coverage generator
├── normalize_funding.py             # Funding rate normalization & coverage generator
├── KLINE_DOWNLOAD_GUIDE.md          # In-depth architectural & API guide
├── mappings/                        # Exchange symbol mappings (*_matched_symbols.json)
├── requirements.txt                 # Project dependencies
└── README.md
```

---

## ⚙️ Configuration (`crypto_alts_multi_exchange.json`)

Set desired date range at the top of `crypto_alts_multi_exchange.json`:

```json
{
  "collectFrom": "2026-07-25 00:00:00",
  "collectTo": "present",
  "exchanges": [
    {
      "exchange": "binance",
      "coins": ["AAVE", "ADA", "APT", "ARB", "AVAX", "DOGE", "SOL", "..."]
    },
    ...
  ]
}
```

* `collectFrom`: UTC date string (e.g. `"2024-01-01 00:00:00"` or `"2026-07-25 00:00:00"`).
* `collectTo`: `"present"` (current UTC timestamp) or a specific end date (e.g. `"2026-10-08 00:00:00"`).

---

## 🚀 Quickstart

### 1. Installation

```bash
pip install -r requirements.txt
```

### 2. Run All Kline Downloads via Orchestrator

```bash
# Run multi-exchange download queue with 2 concurrent processes:
python pipeline_orchestrator.py --max-concurrent 2

# Filter specific exchanges:
python pipeline_orchestrator.py --exchanges binance,bybit,okx

# Override collection window dynamically:
python pipeline_orchestrator.py --from "2026-08-01 00:00:00" --to "2026-10-01 00:00:00"
```

### 3. Individual Exchange Downloads

Each exchange downloader can be launched independently:

```bash
python download_binance.py
python download_bybit.py
python download_okx.py
python download_gateio.py
python download_bitget.py
python download_kucoin.py
python download_bingx.py
python download_mexc.py
python download_hyperliquid.py
python download_asterdex.py
```

### 4. Download Funding Rates

```bash
# Download funding rates for all 10 exchanges:
python download_funding.py --exchange all

# Or for a specific exchange:
python download_funding.py --exchange bybit
```

---

## 🧹 Normalization & Coverage Reports

### 1-min Klines Normalization

```bash
python normalize_klines.py --data-dir tf_1min_kline_data --workers 4
```

- Schema: `timestamp`, `date`, `exchange`, `symbol_base`, `symbol_original`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`.
- Generates `coverage.csv` containing bar counts, date bounds, zero-volume percentages, and median volumes.

### Funding Rates Normalization

```bash
python normalize_funding.py --raw-dir funding_rate_data --out-dir funding --workers 4
```

- Schema: `timestamp`, `date`, `exchange`, `symbol_base`, `symbol_original`, `funding_rate`, `funding_rate_pct`, `interval_hours`.
- Generates `funding/_meta/funding_coverage.csv` containing annualized rates, volatility, positive funding percentages, and interval frequencies.

---

## 📄 License

MIT License.
