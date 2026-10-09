"""
normalize_klines.py
===================
Нормализация 1-минутных kline (свечных) данных криптовалютных альткоинов.

Спецификация (ТЗ):
- Вход: DATA_DIR/<exchange>/<symbol>.parquet
- Выходные колонки в каждом parquet:
    timestamp          int64               (ms, UTC)
    date               datetime64[ns, UTC] (UTC timestamp)
    exchange           str                 ('binance'|'bybit'|'okx'|...)
    symbol_base        str                 ('BTC', 'PEPE', 'SOL', ...)
    symbol_original    str                 ('BTC/USDT:USDT', 'SOLUSDT', ...)
    open               float64
    high               float64
    low                float64
    close              float64
    volume             float64             (в монетах, исходно)
    quote_volume       float64             (volume * close, в USDT)

- Дедупликация: drop_duplicates(subset=['timestamp']).sort_values('timestamp')
- Без resample, без fillna, без дропа volume=0 строк.
- Атомарная перезапись с .tmp файлом.
- Генерация coverage.csv в корне DATA_DIR.
- Валидация свечей: bad_bars.log (high < max(open, close) или low > min(open, close)).
- Ошибки и повреждённые файлы: errors.log.
- Пропуск файлов, если < 1000 строк.
"""

import os
import sys
import glob
import time
import json
import argparse
import datetime
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR_DEFAULT = os.path.join(BASE_DIR, "tf_1min_kline_data")

# Нормализация имён папок бирж
EXCHANGE_FOLDER_MAP = {
    "binanceusdm": "binance",
    "kucoinfutures": "kucoin",
}

# Результирующий порядок колонок
NORMALIZED_COLUMNS = [
    "timestamp",
    "date",
    "exchange",
    "symbol_base",
    "symbol_original",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
]

def load_exchange_matched_symbols(base_dir: str) -> dict:
    """Загружает словари оригинальных тикеров для каждой биржи из корня или папки mappings/."""
    matched_maps = {}
    search_paths = [
        glob.glob(os.path.join(base_dir, "*_matched_symbols.json")),
        glob.glob(os.path.join(base_dir, "mappings", "*_matched_symbols.json")),
    ]
    for paths in search_paths:
        for f in paths:
            ex_name = os.path.basename(f).replace("_matched_symbols.json", "").lower()
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    d = json.load(fp)
                    if isinstance(d, dict) and ex_name not in matched_maps:
                        matched_maps[ex_name] = d
            except Exception:
                pass
    return matched_maps

def extract_symbols(folder_exchange: str, filename: str, matched_map: dict) -> tuple:
    """
    Определяет (exchange, symbol_base, symbol_original).
    Префиксы 1000, 10000 НЕ удаляются.
    """
    clean_exchange = EXCHANGE_FOLDER_MAP.get(folder_exchange.lower(), folder_exchange.lower())
    stem = os.path.splitext(filename)[0]

    # Извлекаем базовый тикер
    # Примеры имен: "0G_USDT_USDT", "0G", "1000PEPE_USDT_USDT", "1000BONK"
    symbol_base = stem.split("_")[0]

    # Определяем оригинальный символ биржи
    if clean_exchange in matched_map and symbol_base in matched_map[clean_exchange]:
        symbol_original = matched_map[clean_exchange][symbol_base]
    else:
        # Резервный формат: если в имени было _USDT_USDT, либо f"{symbol_base}USDT"
        if "_USDT_USDT" in stem:
            symbol_original = f"{symbol_base}/USDT:USDT"
        elif clean_exchange == "hyperliquid":
            symbol_original = symbol_base
        else:
            symbol_original = f"{symbol_base}USDT"

    return clean_exchange, symbol_base, symbol_original

def normalize_single_file(filepath: str, clean_exchange: str, symbol_base: str, symbol_original: str, dry_run: bool = False):
    """
    Обрабатывает один parquet файл:
    - Читает
    - Валидирует колонки и размер
    - Приводит типы и вычисляет date, quote_volume
    - Дедуплицирует по timestamp
    - Проверяет bad bars
    - Атомарно перезаписывает файл (если не dry_run)
    - Возвращает статистику для coverage.csv и отчётов
    """
    try:
        df = pd.read_parquet(filepath)
    except Exception as e:
        return {"status": "error", "error": f"read error: {e}", "filepath": filepath}

    # Маппинг колонки времени ts -> timestamp
    if "timestamp" not in df.columns:
        if "ts" in df.columns:
            df = df.rename(columns={"ts": "timestamp"})
        else:
            return {"status": "error", "error": "no 'timestamp' or 'ts' column", "filepath": filepath}

    # Проверка обязательных колонок
    required = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        return {"status": "error", "error": f"missing columns: {missing}", "filepath": filepath}

    # Проверка количества строк (< 1000 -> пропуск)
    if len(df) < 1000:
        return {"status": "skipped", "reason": f"less than 1000 rows ({len(df)})", "filepath": filepath, "n_bars": len(df)}

    # Приведение типов и дедупликация
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = df["timestamp"].astype("int64")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Проверка на NaN в OHLC
    nan_ohlc = df[["open", "high", "low", "close"]].isna().sum().sum()
    if nan_ohlc > 0:
        return {"status": "error", "error": f"NaN values in OHLC columns ({nan_ohlc} NaNs)", "filepath": filepath}

    # Дедупликация и сортировка строго по возрастанию timestamp
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    n_bars = len(df)
    if n_bars < 1000:
        return {"status": "skipped", "reason": f"less than 1000 rows after dedup ({n_bars})", "filepath": filepath, "n_bars": n_bars}

    # Формирование date (UTC)
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    # Мета-колонки
    df["exchange"] = clean_exchange
    df["symbol_base"] = symbol_base
    df["symbol_original"] = symbol_original

    # quote_volume = volume * close
    df["quote_volume"] = (df["volume"] * df["close"]).astype("float64")

    # Проверка quote_volume >= 0
    if (df["quote_volume"] < 0).any():
        df.loc[df["quote_volume"] < 0, "quote_volume"] = 0.0

    # Проверка аномальных свечей (bad bars)
    # high >= max(open, close) и low <= min(open, close)
    max_oc = df[["open", "close"]].max(axis=1)
    min_oc = df[["open", "close"]].min(axis=1)
    bad_high = df["high"] < (max_oc - 1e-9)
    bad_low = df["low"] > (min_oc + 1e-9)
    bad_mask = bad_high | bad_low
    bad_bars_count = int(bad_mask.sum())

    # Финальный порядок колонок
    df = df[NORMALIZED_COLUMNS]

    # Атомарная перезапись
    if not dry_run:
        tmp_path = filepath + ".tmp"
        df.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
        os.replace(tmp_path, filepath)

    # Расчёт метрик для coverage.csv
    first_date_val = df["date"].iloc[0]
    last_date_val = df["date"].iloc[-1]
    first_date_str = first_date_val.strftime("%Y-%m-%d %H:%M:%S")
    last_date_str = last_date_val.strftime("%Y-%m-%d %H:%M:%S")

    delta_minutes = (df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]) / 60000.0
    if delta_minutes > 0:
        coverage_pct = round((n_bars / delta_minutes) * 100.0, 2)
    else:
        coverage_pct = 100.0

    zero_vol_pct = round(float((df["volume"] == 0).sum() / n_bars * 100.0), 2)
    median_quote_vol = round(float(df["quote_volume"].median()), 4)
    median_volume = round(float(df["volume"].median()), 4)

    return {
        "status": "success",
        "filepath": filepath,
        "exchange": clean_exchange,
        "symbol_base": symbol_base,
        "symbol_original": symbol_original,
        "n_bars": n_bars,
        "first_date": first_date_str,
        "last_date": last_date_str,
        "coverage_pct": coverage_pct,
        "zero_vol_pct": zero_vol_pct,
        "median_quote_vol": median_quote_vol,
        "median_volume": median_volume,
        "bad_bars_count": bad_bars_count,
    }

def process_all_klines(data_dir: str, target_exchange: str = "all", workers: int = 2, dry_run: bool = False):
    t_start = time.time()
    matched_maps = load_exchange_matched_symbols(BASE_DIR)

    if not os.path.exists(data_dir):
        print(f"Directory {data_dir} does not exist.")
        return

    # Поиск директорий
    subdirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d)) and not d.startswith("_")]
    
    if target_exchange != "all":
        subdirs = [d for d in subdirs if d.lower() == target_exchange.lower() or EXCHANGE_FOLDER_MAP.get(d.lower()) == target_exchange.lower()]

    all_files = []
    for d in sorted(subdirs):
        folder_path = os.path.join(data_dir, d)
        files = glob.glob(os.path.join(folder_path, "*.parquet"))
        for f in files:
            clean_ex, sym_base, sym_orig = extract_symbols(d, os.path.basename(f), matched_maps)
            all_files.append((f, clean_ex, sym_base, sym_orig))

    total_files = len(all_files)
    print("=" * 70, flush=True)
    print("STARTING 1-MIN KLINE NORMALIZATION PIPELINE", flush=True)
    print(f"Data directory: {data_dir}", flush=True)
    print(f"Total files to process: {total_files}", flush=True)
    print(f"Workers: {workers} (safe CPU/RAM mode)", flush=True)
    print(f"Dry-run mode: {dry_run}", flush=True)
    print("=" * 70, flush=True)

    if total_files == 0:
        print("No Parquet files found to process.")
        return

    coverage_records = []
    errors_log_path = os.path.join(data_dir, "errors.log")
    bad_bars_log_path = os.path.join(data_dir, "bad_bars.log")

    if os.path.exists(errors_log_path):
        os.remove(errors_log_path)
    if os.path.exists(bad_bars_log_path):
        os.remove(bad_bars_log_path)

    n_success = 0
    n_skipped = 0
    n_errors = 0
    total_bars = 0
    unique_coins = set()
    unique_exchanges = set()
    total_bad_bars = 0

    err_file = open(errors_log_path, "a", encoding="utf-8")
    bad_file = open(bad_bars_log_path, "a", encoding="utf-8")

    processed_count = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(normalize_single_file, f, ex, base, orig, dry_run): (f, ex, base, orig)
            for f, ex, base, orig in all_files
        }

        for fut in as_completed(futures):
            res = fut.result()
            processed_count += 1

            if res["status"] == "success":
                n_success += 1
                total_bars += res["n_bars"]
                unique_coins.add(res["symbol_base"])
                unique_exchanges.add(res["exchange"])

                coverage_records.append({
                    "exchange": res["exchange"],
                    "symbol_original": res["symbol_original"],
                    "symbol_base": res["symbol_base"],
                    "n_bars": res["n_bars"],
                    "first_date": res["first_date"],
                    "last_date": res["last_date"],
                    "coverage_pct": res["coverage_pct"],
                    "zero_vol_pct": res["zero_vol_pct"],
                    "median_quote_vol": res["median_quote_vol"],
                    "median_volume": res["median_volume"],
                })

                if res["bad_bars_count"] > 0:
                    total_bad_bars += res["bad_bars_count"]
                    bad_file.write(f"{res['exchange']}/{res['symbol_base']}: {res['bad_bars_count']} bad bars\n")
                    bad_file.flush()

                print(f"[{processed_count}/{total_files}] [OK] {res['exchange']}/{res['symbol_base']}: {res['n_bars']:,} bars, {res['first_date'][:10]} -> {res['last_date'][:10]}", flush=True)

            elif res["status"] == "skipped":
                n_skipped += 1
                print(f"[{processed_count}/{total_files}] [SKIP] {res['filepath']} ({res['reason']})", flush=True)

            elif res["status"] == "error":
                n_errors += 1
                err_file.write(f"[ERR] {res['filepath']}: {res['error']}\n")
                err_file.flush()
                print(f"[{processed_count}/{total_files}] [ERR] {res['filepath']}: {res['error']}", flush=True)

    err_file.close()
    bad_file.close()

    coverage_path = os.path.join(data_dir, "coverage.csv")
    if coverage_records:
        cov_df = pd.DataFrame(coverage_records)
        cov_df = cov_df.sort_values(["exchange", "symbol_base"]).reset_index(drop=True)
        cov_df.to_csv(coverage_path, index=False, encoding="utf-8")
        print(f"\n[INFO] Saved coverage.csv: {coverage_path} ({len(cov_df)} records)", flush=True)

    elapsed = time.time() - t_start

    print("\n" + "=" * 70, flush=True)
    print("FINAL REPORT: 1-MIN KLINE NORMALIZATION", flush=True)
    print("=" * 70, flush=True)
    print(f"Processed files:                {processed_count}")
    print(f"Success:                        {n_success}")
    print(f"Skipped (<1000 rows):           {n_skipped}")
    print(f"Errors:                         {n_errors} (see {errors_log_path})")
    print(f"Total rows (bars):              {total_bars:,}")
    print(f"Unique coins:                   {len(unique_coins)}")
    print(f"Unique exchanges:               {len(unique_exchanges)}")
    print(f"Bad bars count:                 {total_bad_bars} (see {bad_bars_log_path})")
    print(f"Elapsed time:                   {elapsed:.1f} sec ({elapsed/60:.2f} min)")
    print("=" * 70, flush=True)

def main():
    parser = argparse.ArgumentParser(description="Normalizer for 1-minute crypto kline Parquet data")
    parser.add_argument("--data-dir", type=str, default=DATA_DIR_DEFAULT, help="Path to kline data folder")
    parser.add_argument("--exchange", type=str, default="all", help="Target exchange or 'all'")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker threads (default 2)")
    parser.add_argument("--dry-run", action="store_true", help="Perform checks and coverage without file overwriting")
    args = parser.parse_args()

    process_all_klines(data_dir=args.data_dir, target_exchange=args.exchange, workers=args.workers, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
