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
OUT_DIR = os.path.join(BASE_DIR, "tf_1min_kline_data", "bitget")
SUMMARY_FILE = os.path.join(OUT_DIR, "_download_summary.json")

START_DT, END_DT, START_TS, END_TS = config_loader.get_collection_window()

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
    
    cur_end = END_TS
    all_rows = []
    retries = 0
    
    while cur_end > START_TS:
        rate_limiter.acquire()
        url = f"https://api.bitget.com/api/v2/mix/market/history-candles?symbol={symbol}&productType=USDT-FUTURES&granularity=1m&endTime={cur_end}&limit=200"
        
        try:
            resp = session.get(url, timeout=12)
            if resp.status_code == 429:
                time.sleep(2)
                continue
                
            data = resp.json()
            code = data.get("code")
            
            if code == "00000":
                retries = 0
                klist = data.get("data", [])
                if not klist:
                    break
                all_rows.extend(klist)
                
                oldest_ts = int(klist[0][0])
                if oldest_ts <= START_TS:
                    break
                if cur_end is not None and oldest_ts >= cur_end:
                    break
                cur_end = oldest_ts - 60000
            elif code in ["429", "40014", "40017"]:
                time.sleep(1.5)
            else:
                retries += 1
                if retries > 3:
                    break
                time.sleep(0.5)
        except Exception:
            retries += 1
            if retries > 3:
                break
            time.sleep(1)

    if not all_rows:
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {base_name} ({symbol}): NO DATA FOUND", flush=True)
        return {"base": base_name, "symbol": symbol, "status": "no_data", "count": 0}

    try:
        df = pd.DataFrame(all_rows).iloc[:, :6]
        df.columns = ["ts", "open", "high", "low", "close", "volume"]
        
        df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
        df = df.dropna(subset=["ts"])
        df["ts"] = df["ts"].astype("int64")
        
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            
        df = df[(df["ts"] >= START_TS) & (df["ts"] <= END_TS)]
        df = df.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        
        cnt = len(df)
        out_file = os.path.join(OUT_DIR, f"{base_name}_USDT_USDT.parquet")
        tmp_file = out_file + ".tmp"
        df.to_parquet(tmp_file, engine="pyarrow", compression="snappy", index=False)
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
    global START_DT, END_DT, START_TS, END_TS, total_coins, OUT_DIR

    parser = argparse.ArgumentParser(description="Download Bitget 1m klines")
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

    if args.out_dir:
        OUT_DIR = args.out_dir
    os.makedirs(OUT_DIR, exist_ok=True)

    symbols_map = config_loader.get_symbols_mapping("bitget", BASE_DIR)
    if not symbols_map:
        coins = config_loader.get_coins_for_exchange("bitget")
        symbols_map = {c: f"{c}USDT" for c in coins}

    total_coins = len(symbols_map)
    print("=" * 60, flush=True)
    print(f"Starting Bitget 1m Kline Download: {total_coins} coins")
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

    print("Bitget download completed.")

if __name__ == "__main__":
    main()
