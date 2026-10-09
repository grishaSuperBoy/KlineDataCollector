import os
import io
import time
import json
import zipfile
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
OUT_DIR = os.path.join(BASE_DIR, "tf_1min_kline_data", "binance")
SUMMARY_FILE = os.path.join(OUT_DIR, "_download_summary.json")

# Default window loaded from config_loader (crypto_alts_multi_exchange.json)
START_DT, END_DT, START_TS, END_TS = config_loader.get_collection_window()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

fapi_semaphore = threading.Semaphore(5)
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

def fetch_fapi_klines(symbol: str, start_ms: int, end_ms: int = None, limit: int = 1500):
    session = get_session()
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&startTime={start_ms}&limit={limit}"
    if end_ms:
        url += f"&endTime={end_ms}"
    with fapi_semaphore:
        try:
            resp = session.get(url, timeout=12)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
            elif resp.status_code == 429:
                time.sleep(2)
        except Exception:
            pass
    return []

def get_monthly_dates(start_dt: datetime.datetime, end_dt: datetime.datetime):
    yms = []
    cur = datetime.datetime(start_dt.year, start_dt.month, 1, tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    current_ym = f"{now.year}-{now.month:02d}"
    while cur <= end_dt:
        ym_str = f"{cur.year}-{cur.month:02d}"
        if ym_str != current_ym:
            yms.append(ym_str)
        if cur.month == 12:
            cur = datetime.datetime(cur.year + 1, 1, 1, tzinfo=datetime.timezone.utc)
        else:
            cur = datetime.datetime(cur.year, cur.month + 1, 1, tzinfo=datetime.timezone.utc)
    return yms

def download_coin(base_name: str, symbol: str):
    global progress_count
    session = get_session()
    all_dfs = []
    
    # 1. Vision Monthly Archives
    for ym in get_monthly_dates(START_DT, END_DT):
        url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/1m/{symbol}-1m-{ym}.zip"
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_name = z.namelist()[0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f, header=None)
                        if df.iloc[0, 0] == "open_time":
                            df = df.iloc[1:]
                        df = df.iloc[:, :6]
                        df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
                        all_dfs.append(df)
        except Exception:
            pass

    # 2. Vision Daily for recent days (up to 40 days back)
    days_to_check = []
    cur_d = max(START_DT, END_DT - datetime.timedelta(days=40))
    while cur_d <= END_DT:
        days_to_check.append(cur_d.strftime("%Y-%m-%d"))
        cur_d += datetime.timedelta(days=1)

    for ymd in days_to_check:
        url = f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{ymd}.zip"
        try:
            r = session.get(url, timeout=12)
            if r.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    csv_name = z.namelist()[0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f, header=None)
                        if df.iloc[0, 0] == "open_time":
                            df = df.iloc[1:]
                        df = df.iloc[:, :6]
                        df.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
                        all_dfs.append(df)
        except Exception:
            pass

    # 3. FAPI pagination for latest bars
    latest_ts = START_TS
    if all_dfs:
        try:
            tmp = pd.concat(all_dfs, ignore_index=True)
            tmp['ts'] = pd.to_numeric(tmp['ts'], errors='coerce')
            max_archived = tmp['ts'].max()
            if not pd.isna(max_archived):
                latest_ts = int(max_archived) + 60000
        except Exception:
            pass

    while latest_ts < END_TS:
        klines = fetch_fapi_klines(symbol, latest_ts, end_ms=END_TS, limit=1500)
        if not klines:
            break
        fdf = pd.DataFrame(klines).iloc[:, :6]
        fdf.columns = ['ts', 'open', 'high', 'low', 'close', 'volume']
        all_dfs.append(fdf)
        last_k_ts = int(klines[-1][0])
        if last_k_ts <= latest_ts or len(klines) < 1500:
            break
        latest_ts = last_k_ts + 60000

    if not all_dfs:
        with progress_lock:
            progress_count += 1
            print(f"[{progress_count}/{total_coins}] {base_name} ({symbol}): NO DATA FOUND", flush=True)
        return {"base": base_name, "symbol": symbol, "status": "no_data", "count": 0}

    try:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined['ts'] = pd.to_numeric(combined['ts'], errors='coerce')
        combined = combined.dropna(subset=['ts'])
        combined['ts'] = combined['ts'].astype('int64')
        combined = combined.drop_duplicates(subset=['ts']).sort_values('ts').reset_index(drop=True)

        # Filter strictly within [START_TS, END_TS]
        combined = combined[(combined['ts'] >= START_TS) & (combined['ts'] <= END_TS)]

        for col in ['open', 'high', 'low', 'close', 'volume']:
            combined[col] = pd.to_numeric(combined[col], errors='coerce').astype('float64')

        cnt = len(combined)
        out_file = os.path.join(OUT_DIR, f"{base_name}_USDT_USDT.parquet")
        tmp_file = out_file + ".tmp"
        combined.to_parquet(tmp_file, engine='pyarrow', compression='snappy', index=False)
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

    parser = argparse.ArgumentParser(description="Download Binance 1m klines")
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

    symbols_map = config_loader.get_symbols_mapping("binance", BASE_DIR)
    if not symbols_map:
        coins = config_loader.get_coins_for_exchange("binance")
        symbols_map = {c: f"{c}USDT" for c in coins}

    total_coins = len(symbols_map)
    print("=" * 60, flush=True)
    print(f"Starting Binance 1m Kline Download: {total_coins} coins")
    print(f"Window: {START_DT} -> {END_DT}")
    print(f"Target directory: {OUT_DIR}")
    print("=" * 60, flush=True)

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(download_coin, b, s): b for b, s in symbols_map.items()}
        for f in as_completed(futures):
            results.append(f.result())

    with open(SUMMARY_FILE, "w", encoding="utf-8") as fp:
        json.dump(results, fp, indent=2)

    print("Binance download completed.")

if __name__ == "__main__":
    main()
