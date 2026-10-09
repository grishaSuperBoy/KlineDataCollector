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
OUT_DIR = os.path.join(BASE_DIR, "tf_1min_kline_data", "bybit")
SUMMARY_FILE = os.path.join(OUT_DIR, "_download_summary.json")

START_DT, END_DT, START_TS, END_TS = config_loader.get_collection_window()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

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
    
    cur_start = START_TS
    all_rows = []
    
    retries = 0
    while cur_start < END_TS:
        chunk_end = min(cur_start + 1000 * 60 * 1000, END_TS)
        url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=1&start={cur_start}&end={chunk_end}&limit=1000"
        
        try:
            resp = session.get(url, timeout=12)
            if resp.status_code == 429:
                time.sleep(2)
                continue
                
            data = resp.json()
            ret_code = data.get("retCode", -1)
            
            if ret_code == 0:
                retries = 0
                klist = data.get("result", {}).get("list", [])
                if not klist:
                    cur_start = chunk_end + 60000
                    continue
                    
                all_rows.extend(klist)
                
                max_in_chunk = max(int(k[0]) for k in klist)
                cur_start = max(max_in_chunk + 60000, chunk_end + 60000)
            else:
                retries += 1
                if retries > 3:
                    print(f"[{base_name}] Bybit API error {ret_code}: {data.get('retMsg')}")
                    break
                time.sleep(0.5)
        except Exception as e:
            retries += 1
            if retries > 3:
                print(f"[{base_name}] Network error: {e}")
                break
            time.sleep(1.0)
            
    if not all_rows:
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {base_name} ({symbol}): NO DATA FOUND", flush=True)
        return {"base": base_name, "symbol": symbol, "status": "no_data", "count": 0}
        
    try:
        df = pd.DataFrame(all_rows, columns=["ts", "open", "high", "low", "close", "volume", "turnover"])
        df = df[["ts", "open", "high", "low", "close", "volume"]]
        
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

    parser = argparse.ArgumentParser(description="Download Bybit 1m klines")
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

    symbols_map = config_loader.get_symbols_mapping("bybit", BASE_DIR)
    if not symbols_map:
        coins = config_loader.get_coins_for_exchange("bybit")
        symbols_map = {c: f"{c}USDT" for c in coins}

    total_coins = len(symbols_map)
    print("=" * 60, flush=True)
    print(f"Starting Bybit 1m Kline Download: {total_coins} coins")
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

    print("Bybit download completed.")

if __name__ == "__main__":
    main()
