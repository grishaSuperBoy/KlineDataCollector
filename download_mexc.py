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
OUT_DIR = os.path.join(BASE_DIR, "tf_1min_kline_data", "mexc")
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
    session = get_session()
    
    CHUNK_SEC = 1000 * 60
    cur_end = END_SEC
    all_time = []
    all_open = []
    all_high = []
    all_low = []
    all_close = []
    all_vol = []
    
    retries = 0
    empty_chunks = 0
    
    while cur_end > START_SEC:
        rate_limiter.acquire()
        cur_start = max(START_SEC, cur_end - CHUNK_SEC)
        url = f"https://contract.mexc.com/api/v1/contract/kline/{symbol}?interval=Min1&start={cur_start}&end={cur_end}"
        
        try:
            resp = session.get(url, timeout=12)
            if resp.status_code == 429:
                time.sleep(2)
                continue
                
            data = resp.json()
            if not data.get("success"):
                retries += 1
                if retries > 3:
                    break
                time.sleep(1)
                continue
                
            d = data.get("data", {})
            times = d.get("time", [])
            
            if not times:
                empty_chunks += 1
                if empty_chunks >= 5:
                    break
                cur_end = cur_start
                continue
                
            empty_chunks = 0
            retries = 0
            
            all_time.extend(times)
            all_open.extend(d.get("open", []))
            all_high.extend(d.get("high", []))
            all_low.extend(d.get("low", []))
            all_close.extend(d.get("close", []))
            all_vol.extend(d.get("vol", []))
            
            cur_end = cur_start
        except Exception:
            retries += 1
            if retries > 3:
                break
            time.sleep(1)

    if not all_time:
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {base_name} ({symbol}): NO DATA FOUND", flush=True)
        return {"base": base_name, "symbol": symbol, "status": "no_data", "count": 0}

    try:
        df = pd.DataFrame({
            'ts': [int(t) * 1000 for t in all_time],
            'open': all_open,
            'high': all_high,
            'low': all_low,
            'close': all_close,
            'volume': all_vol
        })
        
        df['ts'] = pd.to_numeric(df['ts'], errors='coerce')
        df = df.dropna(subset=['ts'])
        df['ts'] = df['ts'].astype('int64')
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            
        df = df[(df['ts'] >= START_TS) & (df['ts'] <= END_TS)]
        df = df.drop_duplicates(subset=['ts']).sort_values('ts').reset_index(drop=True)
        
        cnt = len(df)
        out_file = os.path.join(OUT_DIR, f"{base_name}_USDT_USDT.parquet")
        tmp_file = out_file + ".tmp"
        df.to_parquet(tmp_file, engine='pyarrow', compression='snappy', index=False)
        os.replace(tmp_file, out_file)
        
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {base_name} ({symbol}): {cnt:,} bars saved.", flush=True)
            
        return {"base": base_name, "symbol": symbol, "status": "success", "count": cnt}
    except Exception as e:
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {base_name} ({symbol}): ERROR {e}", flush=True)
        return {"base": base_name, "symbol": symbol, "status": "error", "error": str(e), "count": 0}

def main():
    global START_DT, END_DT, START_TS, END_TS, START_SEC, END_SEC, total_coins, OUT_DIR

    parser = argparse.ArgumentParser(description="Download MEXC 1m klines")
    parser.add_argument("--from", dest="from_date", type=str, default=None, help="Start date (UTC)")
    parser.add_argument("--to", dest="to_date", type=str, default=None, help="End date (UTC)")
    parser.add_argument("--out-dir", type=str, default=None, help="Custom output directory")
    parser.add_argument("--workers", type=int, default=5, help="Concurrent workers")
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

    symbols_map = config_loader.get_symbols_mapping("mexc", BASE_DIR)
    if not symbols_map:
        coins = config_loader.get_coins_for_exchange("mexc")
        symbols_map = {c: f"{c}_USDT" for c in coins}

    total_coins = len(symbols_map)
    print("=" * 60, flush=True)
    print(f"Starting MEXC 1m Kline Download: {total_coins} coins")
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

    print("MEXC download completed.")

if __name__ == "__main__":
    main()
