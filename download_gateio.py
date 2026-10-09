import os
import time
import json
import threading
import datetime
import argparse
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3

import config_loader

urllib3.disable_warnings()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "tf_1min_kline_data", "gateio")
SUMMARY_FILE = os.path.join(OUT_DIR, "_download_summary.json")

START_DT, END_DT, START_TS, END_TS = config_loader.get_collection_window()
START_SEC = int(START_TS / 1000)
END_SEC = int(END_TS / 1000)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

class RateLimiter:
    """Thread-safe token bucket limiter targeting ~10 req/s."""
    def __init__(self, max_rate=10.0):
        self.max_rate = max_rate
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

rate_limiter = RateLimiter(max_rate=10.0)

progress_lock = threading.Lock()
progress_count = 0
total_coins = 0

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        thread_local.session = s
    return thread_local.session

def fetch_coin_klines(base_name: str, symbol: str):
    global progress_count
    safe_name = str(base_name).encode('ascii', 'ignore').decode('ascii') or "COIN"
    safe_sym = str(symbol).encode('ascii', 'ignore').decode('ascii') or "SYM"
    
    session = get_session()
    cur_to = END_SEC
    all_rows = []
    retries = 0
    
    while cur_to > START_SEC:
        rate_limiter.acquire()
        cur_from = max(START_SEC, cur_to - 2000 * 60)
        url = f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}&interval=1m&from={cur_from}&to={cur_to}"
        
        try:
            resp = session.get(url, timeout=12)
            if resp.status_code == 429:
                time.sleep(2)
                continue
                
            data = resp.json()
            if isinstance(data, dict) and data.get("label") == "INVALID_PARAM_VALUE":
                break
                
            if not isinstance(data, list) or not data:
                break
                
            retries = 0
            all_rows.extend(data)
            
            oldest_sec = int(data[0]['t'])
            if oldest_sec <= START_SEC:
                break
            if oldest_sec >= cur_to:
                break
            cur_to = oldest_sec - 60
        except Exception:
            retries += 1
            if retries > 3:
                break
            time.sleep(1)

    if not all_rows:
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {safe_name} ({safe_sym}): NO DATA FOUND", flush=True)
        return {"base": base_name, "symbol": symbol, "status": "no_data", "count": 0}

    try:
        rows = []
        for k in all_rows:
            rows.append({
                'ts': int(k['t']) * 1000,
                'open': float(k['o']),
                'high': float(k['h']),
                'low': float(k['l']),
                'close': float(k['c']),
                'volume': float(k['v'])
            })
            
        df = pd.DataFrame(rows)
        df['ts'] = pd.to_numeric(df['ts'], errors='coerce')
        df = df.dropna(subset=['ts'])
        df['ts'] = df['ts'].astype('int64')
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            
        df = df[(df['ts'] >= START_TS) & (df['ts'] <= END_TS)]
        df = df.drop_duplicates(subset=['ts']).sort_values('ts').reset_index(drop=True)
        
        cnt = len(df)
        out_file = os.path.join(OUT_DIR, f"{base_name}.parquet")
        tmp_file = out_file + ".tmp"
        df.to_parquet(tmp_file, engine='pyarrow', compression='snappy', index=False)
        os.replace(tmp_file, out_file)
        
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {safe_name} ({safe_sym}): {cnt:,} bars saved.", flush=True)
            
        return {"base": base_name, "symbol": symbol, "status": "success", "count": cnt}
    except Exception as e:
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {safe_name} ({safe_sym}): ERROR {e}", flush=True)
        return {"base": base_name, "symbol": symbol, "status": "error", "error": str(e), "count": 0}

def main():
    global START_DT, END_DT, START_TS, END_TS, START_SEC, END_SEC, total_coins, OUT_DIR

    parser = argparse.ArgumentParser(description="Download Gate.io 1m klines")
    parser.add_argument("--from", dest="from_date", type=str, default=None, help="Start date (UTC)")
    parser.add_argument("--to", dest="to_date", type=str, default=None, help="End date (UTC)")
    parser.add_argument("--out-dir", type=str, default=None, help="Custom output directory")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent workers")
    args = parser.parse_args()

    s_dt, e_dt, s_ms, e_ms = config_loader.get_collection_window(
        override_from=args.from_date,
        override_to=args.to_date
    )
    START_DT, END_DT, START_TS, END_TS = s_dt, e_dt, s_ms, e_ms
    START_SEC, END_SEC = int(START_TS / 1000), int(END_TS / 1000)

    if args.out_dir:
        OUT_DIR = args.out_dir
    os.makedirs(OUT_DIR, exist_ok=True)

    symbols_map = config_loader.get_symbols_mapping("gateio", BASE_DIR)
    if not symbols_map:
        coins = config_loader.get_coins_for_exchange("gateio")
        symbols_map = {c: f"{c}_USDT" for c in coins}

    total_coins = len(symbols_map)
    print("=" * 60, flush=True)
    print(f"Starting Gate.io 1m Kline Download: {total_coins} coins")
    print(f"Window: {START_DT} -> {END_DT}")
    print(f"Target directory: {OUT_DIR}")
    print("=" * 60, flush=True)

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(fetch_coin_klines, b, s): b for b, s in symbols_map.items()}
        for f in as_completed(futures):
            results.append(f.result())

    with open(SUMMARY_FILE, "w", encoding="utf-8") as fp:
        json.dump(results, fp, indent=2)

    print("Gate.io download completed.")

if __name__ == "__main__":
    main()
