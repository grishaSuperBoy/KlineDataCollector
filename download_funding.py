"""
download_funding.py
===================
High-throughput Historical Funding Rate Downloader for Crypto Altcoins.

Features:
- Dynamically respects 'collectFrom' and 'collectTo' from crypto_alts_multi_exchange.json
- CLI arguments --from and --to for manual override
- Multi-threaded token-bucket rate limiting per exchange API rules
- Strict schema: ['ts', 'funding_rate'] -> Snappy-compressed Parquet
- Supports all 10 major derivative exchanges
"""

import os
import sys
import time
import json
import argparse
import datetime
import threading
import requests
import urllib3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

import config_loader

urllib3.disable_warnings()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_BASE = os.path.join(BASE_DIR, "funding_rate_data")
MULTI_EXCHANGE_FILE = os.path.join(BASE_DIR, "crypto_alts_multi_exchange.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        thread_local.session = s
    return thread_local.session

class RateLimiter:
    """Thread-safe Token Bucket rate limiter."""
    def __init__(self, max_rate: float):
        self.max_rate = max_rate
        self.interval = 1.0 / max_rate
        self.lock = threading.Lock()
        self.last_time = time.time()
        self.tokens = max_rate

    def acquire(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_time
            self.last_time = now
            self.tokens = min(self.max_rate, self.tokens + elapsed * self.max_rate)
            if self.tokens < 1.0:
                sleep_time = (1.0 - self.tokens) / self.max_rate
                time.sleep(sleep_time)
                self.last_time = time.time()
                self.tokens = 0.0
            else:
                self.tokens -= 1.0

# Global active time window (configured dynamically in main / download_exchange_funding)
START_DT, END_DT, START_TS, END_TS = config_loader.get_collection_window()
START_SEC = int(START_TS / 1000)
END_SEC = int(END_TS / 1000)

def set_time_window(start_dt, end_dt, start_ts, end_ts):
    global START_DT, END_DT, START_TS, END_TS, START_SEC, END_SEC
    START_DT, END_DT, START_TS, END_TS = start_dt, end_dt, start_ts, end_ts
    START_SEC = int(start_ts / 1000)
    END_SEC = int(end_ts / 1000)

def save_funding_parquet(rows: list, out_path: str) -> int:
    """
    Saves funding rate records into Parquet.
    Strict schema: ['ts', 'funding_rate']
    - ts: int64 (Unix timestamp in milliseconds UTC)
    - funding_rate: float64
    """
    if not rows:
        return 0
    df = pd.DataFrame(rows, columns=['ts', 'funding_rate'])
    df['ts'] = pd.to_numeric(df['ts'], errors='coerce')
    df = df.dropna(subset=['ts'])
    df['ts'] = df['ts'].astype('int64')

    df['funding_rate'] = pd.to_numeric(df['funding_rate'], errors='coerce').astype('float64')
    df = df.dropna(subset=['funding_rate'])

    # Filter strictly within [START_TS, END_TS]
    df = df[(df['ts'] >= START_TS) & (df['ts'] <= END_TS)]
    df = df.drop_duplicates(subset=['ts']).sort_values('ts').reset_index(drop=True)

    if len(df) == 0:
        return 0

    tmp_file = out_path + ".tmp"
    df.to_parquet(tmp_file, engine='pyarrow', compression='snappy', index=False)
    os.replace(tmp_file, out_path)
    return len(df)

# =====================================================================
# EXCHANGE-SPECIFIC FUNDING RATE FETCHERS
# =====================================================================

def fetch_binance(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    limiter.acquire()
    url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&startTime={START_TS}&endTime={END_TS}&limit=1000"
    resp = session.get(url, timeout=10)
    if resp.status_code != 200:
        return []
    data = resp.json()
    if not isinstance(data, list):
        return []
    return [{'ts': int(item['fundingTime']), 'funding_rate': float(item['fundingRate'])} for item in data]

def fetch_bybit(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    cur_end = END_TS
    
    # Calculate approximate max pages needed
    for _ in range(12):
        limiter.acquire()
        url = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&endTime={cur_end}&limit=200"
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json().get('result', {}).get('list', [])
        if not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['fundingRateTimestamp']), 'funding_rate': float(item['fundingRate'])})
        oldest_ts = int(data[-1]['fundingRateTimestamp'])
        if oldest_ts <= START_TS:
            break
        cur_end = oldest_ts - 1
    return all_rows

def fetch_okx(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    cur_after = None
    
    for _ in range(15):
        limiter.acquire()
        url = f"https://www.okx.com/api/v5/public/funding-rate-history?instId={symbol}&limit=100"
        if cur_after:
            url += f"&after={cur_after}"
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json().get('data', [])
        if not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['fundingTime']), 'funding_rate': float(item['fundingRate'])})
        oldest_ts = int(data[-1]['fundingTime'])
        if oldest_ts <= START_TS:
            break
        cur_after = oldest_ts
    return all_rows

def fetch_bitget(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    for page in range(1, 15):
        limiter.acquire()
        url = f"https://api.bitget.com/api/v2/mix/market/history-fund-rate?symbol={symbol}&productType=USDT-FUTURES&pageSize=100&pageNo={page}"
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json().get('data', [])
        if not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['fundingTime']), 'funding_rate': float(item['fundingRate'])})
        oldest_ts = int(data[-1]['fundingTime'])
        if oldest_ts <= START_TS:
            break
    return all_rows

def fetch_bingx(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    limiter.acquire()
    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate?symbol={symbol}&startTime={START_TS}&endTime={END_TS}&limit=1000"
    resp = session.get(url, timeout=10)
    if resp.status_code != 200:
        return []
    data = resp.json().get('data', [])
    if not isinstance(data, list):
        return []
    return [{'ts': int(item['fundingTime']), 'funding_rate': float(item['fundingRate'])} for item in data]

def fetch_kucoin(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    offset = 0
    for _ in range(15):
        limiter.acquire()
        url = f"https://api-futures.kucoin.com/api/v1/funding-history?symbol={symbol}&from={START_TS}&to={END_TS}&offset={offset}&maxCount=100"
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json().get('data', {}).get('dataList', [])
        if not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['timepoint']), 'funding_rate': float(item['fundingRate'])})
        if len(data) < 100:
            break
        offset += len(data)
    return all_rows

def fetch_asterdex(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    cur_end = END_TS
    for _ in range(15):
        limiter.acquire()
        url = f"https://api.asterdex.com/api/v1/fundingRate/history?symbol={symbol}&endTime={cur_end}&limit=100"
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json().get('data', [])
        if not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['fundingTime']), 'funding_rate': float(item['fundingRate'])})
        oldest_ts = int(data[-1]['fundingTime'])
        if oldest_ts <= START_TS:
            break
        cur_end = oldest_ts - 1
    return all_rows

def fetch_mexc(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    for page in range(1, 15):
        limiter.acquire()
        url = f"https://contract.mexc.com/api/v1/contract/funding_rate/history?symbol={symbol}&page_num={page}&page_size=100"
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json().get('data', {}).get('resultList', [])
        if not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['settleTime']), 'funding_rate': float(item['fundingRate'])})
        oldest_ts = int(data[-1]['settleTime'])
        if oldest_ts <= START_TS:
            break
    return all_rows

def fetch_gateio(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    cur_to = END_SEC
    for _ in range(15):
        limiter.acquire()
        url = f"https://api.gateio.ws/api/v4/futures/usdt/funding_rate?contract={symbol}&to={cur_to}&limit=100"
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['t']) * 1000, 'funding_rate': float(item['r'])})
        oldest_sec = int(data[-1]['t'])
        if oldest_sec <= START_SEC:
            break
        cur_to = oldest_sec - 1
    return all_rows

def fetch_hyperliquid(symbol: str, limiter: RateLimiter) -> list:
    session = get_session()
    all_rows = []
    cur_start = START_TS
    now_ms = END_TS
    for _ in range(15):
        limiter.acquire()
        payload = {"type": "fundingHistory", "coin": symbol, "startTime": cur_start}
        resp = session.post("https://api.hyperliquid.xyz/info", json=payload, timeout=10)
        if resp.status_code != 200:
            break
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        for item in data:
            all_rows.append({'ts': int(item['time']), 'funding_rate': float(item['fundingRate'])})
        last_ts = int(data[-1]['time'])
        if last_ts >= now_ms or last_ts <= cur_start or len(data) < 500:
            break
        cur_start = last_ts + 1
    return all_rows

EXCHANGE_FETCHERS = {
    "binance": (fetch_binance, 15.0, 6),
    "bybit": (fetch_bybit, 12.0, 6),
    "okx": (fetch_okx, 10.0, 6),
    "bitget": (fetch_bitget, 12.0, 6),
    "bingx": (fetch_bingx, 10.0, 6),
    "kucoin": (fetch_kucoin, 10.0, 6),
    "asterdex": (fetch_asterdex, 3.0, 2),
    "mexc": (fetch_mexc, 10.0, 6),
    "gateio": (fetch_gateio, 10.0, 6),
    "hyperliquid": (fetch_hyperliquid, 12.0, 6),
}

# =====================================================================
# PIPELINE EXECUTION FOR ONE EXCHANGE
# =====================================================================

def download_exchange_funding(exchange_name: str, force: bool = False):
    if exchange_name not in EXCHANGE_FETCHERS:
        print(f"Unknown exchange: {exchange_name}")
        return

    fetch_fn, rate_limit, workers = EXCHANGE_FETCHERS[exchange_name]
    limiter = RateLimiter(max_rate=rate_limit)

    out_dir = os.path.join(OUT_BASE, exchange_name)
    os.makedirs(out_dir, exist_ok=True)

    # Load symbol mappings from config_loader
    symbols_map = config_loader.get_symbols_mapping(exchange_name, BASE_DIR)
    if not symbols_map:
        # Fallback to config coin names
        coins = config_loader.get_coins_for_exchange(exchange_name, MULTI_EXCHANGE_FILE)
        symbols_map = {c: f"{c}USDT" for c in coins}

    items = list(symbols_map.items())
    total = len(items)

    print("=" * 60, flush=True)
    print(f"Downloading FUNDING RATES for {exchange_name.upper()}: {total} coins", flush=True)
    print(f"Window: {START_DT} -> {END_DT}", flush=True)
    print(f"Target dir: {out_dir}", flush=True)
    print("=" * 60, flush=True)

    progress = 0
    success = 0
    total_points = 0
    t0 = time.time()
    prog_lock = threading.Lock()

    def process_coin(base: str, symbol: str):
        nonlocal progress, success, total_points
        safe_base = str(base).encode('ascii', 'ignore').decode('ascii') or "COIN"
        safe_sym = str(symbol).encode('ascii', 'ignore').decode('ascii') or "SYM"
        out_file = os.path.join(out_dir, f"{base}.parquet")

        if not force and os.path.exists(out_file) and os.path.getsize(out_file) > 100:
            with prog_lock:
                progress += 1
                success += 1
                if progress % 50 == 0 or progress == total:
                    print(f"[{progress}/{total}] {safe_base} ({safe_sym}): Cached", flush=True)
            return

        try:
            rows = fetch_fn(symbol, limiter)
            saved_count = save_funding_parquet(rows, out_file)
            with prog_lock:
                progress += 1
                if saved_count > 0:
                    success += 1
                    total_points += saved_count
                    print(f"[{progress}/{total}] {safe_base} ({safe_sym}): {saved_count} funding rates -> Parquet", flush=True)
                else:
                    print(f"[{progress}/{total}] {safe_base} ({safe_sym}): No funding data found", flush=True)
        except Exception as e:
            with prog_lock:
                progress += 1
                print(f"[{progress}/{total}] {safe_base} ({safe_sym}): ERROR {e}", flush=True)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_coin, b, s) for b, s in items]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    elapsed = time.time() - t0
    print("=" * 60, flush=True)
    print(f"Finished {exchange_name.upper()} in {elapsed:.1f}s!", flush=True)
    print(f"Saved: {success}/{total} coins | Total funding points: {total_points:,}", flush=True)
    print("=" * 60, flush=True)

def main():
    parser = argparse.ArgumentParser(description="Download Historical Funding Rates for Crypto Altcoins")
    parser.add_argument("--exchange", type=str, default="all", help="Exchange name or 'all'")
    parser.add_argument("--force", action="store_true", help="Force re-download even if parquet exists")
    parser.add_argument("--from", dest="from_date", type=str, default=None, help="Override start date (e.g. '2026-07-25 00:00:00')")
    parser.add_argument("--to", dest="to_date", type=str, default=None, help="Override end date (e.g. 'present' or '2026-10-08')")
    parser.add_argument("--config", type=str, default=None, help="Custom config json path")
    args = parser.parse_args()

    s_dt, e_dt, s_ms, e_ms = config_loader.get_collection_window(
        config_path=args.config,
        override_from=args.from_date,
        override_to=args.to_date
    )
    set_time_window(s_dt, e_dt, s_ms, e_ms)

    exchanges = list(EXCHANGE_FETCHERS.keys()) if args.exchange == "all" else [args.exchange]
    for ex in exchanges:
        download_exchange_funding(ex, force=args.force)

if __name__ == "__main__":
    main()
