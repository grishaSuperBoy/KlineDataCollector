"""
normalize_funding.py
====================
Нормализация исторических данных Funding Rates криптовалютных деривативов.

Спецификация (ТЗ):
- Вход: RAW_DIR/<exchange>/<symbol>.parquet (funding_rate_data)
- Выход: OUT_DIR/<exchange>/<symbol_base>.parquet (funding)
         OUT_DIR/_meta/funding_coverage.csv
         OUT_DIR/_meta/funding_errors.log
         OUT_DIR/_meta/funding_anomalies.log

Колонки в каждом parquet:
    timestamp          int64               (Unix ms, UTC)
    date               datetime64[ns, UTC] (UTC timestamp)
    exchange           str                 ('binance'|'bybit'|'okx'|...)
    symbol_base        str                 ('0G', 'SOL', '1000PEPE', ...)
    symbol_original    str                 ('0G-USDT-SWAP', 'SOLUSDT', ...)
    funding_rate       float64             (в долях: 0.0001 = 0.01%)
    funding_rate_pct   float64             (в процентах: 0.01 = 0.01%)
    interval_hours     int                 (8, 4 или 1)

Алгоритм:
1. Читать parquet. Пропустить если < 10 строк.
2. exchange, symbol_base, symbol_original из пути и *_matched_symbols.json.
3. pd.to_datetime(timestamp, unit='ms', utc=True) -> date.
4. Нормализовать funding_rate в доли:
   Если median(abs(funding_rate)) > 0.005 -> делить на 100. Иначе в долях.
5. funding_rate_pct = funding_rate * 100.
6. interval_hours: diffs = df['date'].diff().dt.total_seconds() / 3600 -> мода.
7. Дедупликация: drop_duplicates(subset=['timestamp']).sort_values('timestamp').
8. Без fillna, без resample.
9. Валидация: аномалии вне [-0.05, 0.05] -> funding_anomalies.log.
10. Атомарная запись во временный файл .tmp перед replace.
11. Генерация funding_coverage.csv со всеми метриками.
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

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR_DEFAULT = os.path.join(BASE_DIR, "funding_rate_data")
OUT_DIR_DEFAULT = os.path.join(BASE_DIR, "funding")
META_DIR_DEFAULT = os.path.join(OUT_DIR_DEFAULT, "_meta")

EXCHANGE_NAME_MAP = {
    "binanceusdm": "binance",
    "kucoinfutures": "kucoin",
}

NORMALIZED_COLUMNS = [
    "timestamp",
    "date",
    "exchange",
    "symbol_base",
    "symbol_original",
    "funding_rate",
    "funding_rate_pct",
    "interval_hours",
]

def load_exchange_matched_symbols(base_dir: str) -> dict:
    """Загружает маппинги тикеров из файлов *_matched_symbols.json."""
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

def resolve_symbol_meta(folder_exchange: str, filename: str, matched_map: dict) -> tuple:
    """Определяет (exchange, symbol_base, symbol_original)."""
    clean_exchange = EXCHANGE_NAME_MAP.get(folder_exchange.lower(), folder_exchange.lower())
    stem = os.path.splitext(filename)[0]

    # Базовый тикер (например 'SOL', '0G', '1000PEPE')
    symbol_base = stem.split("_")[0]

    if clean_exchange in matched_map and symbol_base in matched_map[clean_exchange]:
        symbol_original = matched_map[clean_exchange][symbol_base]
    elif clean_exchange in matched_map and stem in matched_map[clean_exchange]:
        symbol_original = matched_map[clean_exchange][stem]
        symbol_base = stem
    else:
        if clean_exchange == "okx":
            symbol_original = f"{symbol_base}-USDT-SWAP"
        elif clean_exchange == "gateio":
            symbol_original = f"{symbol_base}_USDT"
        elif clean_exchange == "hyperliquid":
            symbol_original = symbol_base
        elif clean_exchange == "kucoin":
            symbol_original = f"{symbol_base}USDTM"
        else:
            symbol_original = f"{symbol_base}USDT"

    return clean_exchange, symbol_base, symbol_original

def normalize_single_funding_file(
    raw_path: str,
    clean_exchange: str,
    symbol_base: str,
    symbol_original: str,
    out_dir: str,
    dry_run: bool = False
) -> dict:
    """
    Обрабатывает один parquet файл:
    - Проверяет >= 10 строк
    - Нормализует типы, timestamp -> date
    - Приводит funding_rate в доли (0.0001 = 0.01%)
    - Автоматически вычисляет interval_hours через моду разностей
    - Дедуплицирует по timestamp
    - Атомарно сохраняет в out_dir/<exchange>/<symbol_base>.parquet
    - Возвращает метрики для funding_coverage.csv
    """
    try:
        df = pd.read_parquet(raw_path)
    except Exception as e:
        return {"status": "error", "error": f"read error: {e}", "filepath": raw_path}

    if "timestamp" not in df.columns:
        if "ts" in df.columns:
            df = df.rename(columns={"ts": "timestamp"})
        else:
            return {"status": "error", "error": "no 'timestamp' or 'ts' column", "filepath": raw_path}

    if "funding_rate" not in df.columns:
        if "fundingRate" in df.columns:
            df = df.rename(columns={"fundingRate": "funding_rate"})
        elif "rate" in df.columns:
            df = df.rename(columns={"rate": "funding_rate"})
        else:
            return {"status": "error", "error": "no 'funding_rate' column", "filepath": raw_path}

    # Пропуск если < 10 строк
    if len(df) < 10:
        return {"status": "skipped", "reason": f"less than 10 rows ({len(df)})", "filepath": raw_path, "n_events": len(df)}

    # Приведение типов
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = df["timestamp"].astype("int64")

    df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce").astype("float64")
    df = df.dropna(subset=["funding_rate"])

    if len(df) < 10:
        return {"status": "skipped", "reason": f"less than 10 valid rows ({len(df)})", "filepath": raw_path, "n_events": len(df)}

    # Дедупликация и сортировка
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    n_events = len(df)

    if n_events < 10:
        return {"status": "skipped", "reason": f"less than 10 rows after dedup ({n_events})", "filepath": raw_path, "n_events": n_events}

    # Генерация date (UTC)
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    # Нормализация в доли
    med_rate = float(df["funding_rate"].abs().median())
    if med_rate > 0.005:
        df["funding_rate"] = df["funding_rate"] / 100.0

    df["funding_rate_pct"] = df["funding_rate"] * 100.0

    # Автоматическое определение interval_hours
    diff_hours = df["date"].diff().dt.total_seconds() / 3600.0
    valid_diffs = diff_hours.dropna()
    
    if len(valid_diffs) > 0:
        rounded_diffs = valid_diffs.round().astype(int)
        common_candidates = rounded_diffs[rounded_diffs.isin([1, 2, 4, 8, 12, 24])]
        if not common_candidates.empty:
            interval_hours = int(common_candidates.mode().iloc[0])
        else:
            mode_val = int(rounded_diffs.mode().iloc[0])
            interval_hours = mode_val if mode_val > 0 else 8
    else:
        interval_hours = 8

    # Присваиваем метаданные
    df["exchange"] = clean_exchange
    df["symbol_base"] = symbol_base
    df["symbol_original"] = symbol_original
    df["interval_hours"] = interval_hours

    # Финальный порядок колонок
    df = df[NORMALIZED_COLUMNS]

    # Проверка аномалий
    anomalies_mask = df["funding_rate"].abs() > 0.05
    n_anomalies = int(anomalies_mask.sum())
    max_anomaly_val = float(df["funding_rate"].abs().max()) if n_anomalies > 0 else 0.0

    # Сохранение в чистую структуру
    target_folder = os.path.join(out_dir, clean_exchange)
    target_file = os.path.join(target_folder, f"{symbol_base}.parquet")

    if not dry_run:
        os.makedirs(target_folder, exist_ok=True)
        tmp_target = target_file + ".tmp"
        df.to_parquet(tmp_target, engine="pyarrow", compression="snappy", index=False)
        os.replace(tmp_target, target_file)

    # Метрики покрытия
    first_date_val = df["date"].iloc[0]
    last_date_val = df["date"].iloc[-1]
    first_date_str = first_date_val.strftime("%Y-%m-%d %H:%M:%S")
    last_date_str = last_date_val.strftime("%Y-%m-%d %H:%M:%S")

    mean_rate = float(df["funding_rate"].mean())
    median_rate = float(df["funding_rate"].median())
    std_rate = float(df["funding_rate"].std()) if len(df) > 1 else 0.0
    pct_positive = round(float((df["funding_rate"] > 0).sum() / n_events * 100.0), 2)

    periods_per_year = (24.0 / interval_hours) * 365.0
    mean_annualized_pct = round(mean_rate * periods_per_year * 100.0, 2)
    max_abs_rate = round(float(df["funding_rate"].abs().max()), 6)

    return {
        "status": "success",
        "filepath": raw_path,
        "target_file": target_file,
        "exchange": clean_exchange,
        "symbol_original": symbol_original,
        "symbol_base": symbol_base,
        "n_events": n_events,
        "first_date": first_date_str,
        "last_date": last_date_str,
        "interval_hours": interval_hours,
        "mean_rate": round(mean_rate, 7),
        "median_rate": round(median_rate, 7),
        "std_rate": round(std_rate, 7),
        "pct_positive": pct_positive,
        "mean_annualized_pct": mean_annualized_pct,
        "max_abs_rate": max_abs_rate,
        "n_anomalies": n_anomalies,
        "max_anomaly_val": max_anomaly_val,
    }

def process_all_funding(
    raw_dir: str = RAW_DIR_DEFAULT,
    out_dir: str = OUT_DIR_DEFAULT,
    target_exchange: str = "all",
    workers: int = 4,
    dry_run: bool = False
):
    t_start = time.time()
    matched_maps = load_exchange_matched_symbols(BASE_DIR)

    if not os.path.exists(raw_dir):
        print(f"Raw directory does not exist: {raw_dir}")
        return

    meta_dir = os.path.join(out_dir, "_meta")
    os.makedirs(meta_dir, exist_ok=True)

    subdirs = [d for d in os.listdir(raw_dir) if os.path.isdir(os.path.join(raw_dir, d)) and not d.startswith("_")]
    if target_exchange != "all":
        subdirs = [d for d in subdirs if d.lower() == target_exchange.lower() or EXCHANGE_NAME_MAP.get(d.lower()) == target_exchange.lower()]

    all_files = []
    for d in sorted(subdirs):
        folder_path = os.path.join(raw_dir, d)
        files = glob.glob(os.path.join(folder_path, "*.parquet"))
        for f in files:
            clean_ex, sym_base, sym_orig = resolve_symbol_meta(d, os.path.basename(f), matched_maps)
            all_files.append((f, clean_ex, sym_base, sym_orig))

    total_files = len(all_files)
    print("=" * 70, flush=True)
    print("STARTING FUNDING RATE NORMALIZATION PIPELINE", flush=True)
    print(f"Source raw directory:  {raw_dir}", flush=True)
    print(f"Target directory:      {out_dir}", flush=True)
    print(f"Total files:           {total_files}", flush=True)
    print(f"Workers:               {workers}", flush=True)
    print(f"Dry-run:               {dry_run}", flush=True)
    print("=" * 70, flush=True)

    if total_files == 0:
        print("No Parquet files found in raw directory.")
        return

    coverage_records = []
    errors_log_path = os.path.join(meta_dir, "funding_errors.log")
    anomalies_log_path = os.path.join(meta_dir, "funding_anomalies.log")

    if os.path.exists(errors_log_path):
        os.remove(errors_log_path)
    if os.path.exists(anomalies_log_path):
        os.remove(anomalies_log_path)

    err_file = open(errors_log_path, "a", encoding="utf-8")
    anom_file = open(anomalies_log_path, "a", encoding="utf-8")

    n_success = 0
    n_skipped = 0
    n_errors = 0
    total_events = 0
    unique_coins = set()
    unique_exchanges = set()
    total_anomalies = 0

    processed_count = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(normalize_single_funding_file, f, ex, base, orig, out_dir, dry_run): (f, ex, base, orig)
            for f, ex, base, orig in all_files
        }

        for fut in as_completed(futures):
            res = fut.result()
            processed_count += 1

            if res["status"] == "success":
                n_success += 1
                total_events += res["n_events"]
                unique_coins.add(res["symbol_base"])
                unique_exchanges.add(res["exchange"])

                coverage_records.append({
                    "exchange": res["exchange"],
                    "symbol_original": res["symbol_original"],
                    "symbol_base": res["symbol_base"],
                    "n_events": res["n_events"],
                    "first_date": res["first_date"],
                    "last_date": res["last_date"],
                    "interval_hours": res["interval_hours"],
                    "mean_rate": res["mean_rate"],
                    "median_rate": res["median_rate"],
                    "std_rate": res["std_rate"],
                    "pct_positive": res["pct_positive"],
                    "mean_annualized_pct": res["mean_annualized_pct"],
                    "max_abs_rate": res["max_abs_rate"],
                })

                if res["n_anomalies"] > 0:
                    total_anomalies += res["n_anomalies"]
                    anom_file.write(
                        f"[{res['exchange']}/{res['symbol_base']}] {res['n_anomalies']} anomalies (max: {res['max_anomaly_val']:.6f})\n"
                    )
                    anom_file.flush()

                if processed_count % 50 == 0 or processed_count == total_files:
                    print(
                        f"[{processed_count}/{total_files}] [OK] {res['exchange']}/{res['symbol_base']}: "
                        f"{res['n_events']} events ({res['interval_hours']}h), {res['first_date'][:10]} -> {res['last_date'][:10]}",
                        flush=True
                    )

            elif res["status"] == "skipped":
                n_skipped += 1

            elif res["status"] == "error":
                n_errors += 1
                err_file.write(f"[ERR] {res['filepath']}: {res['error']}\n")
                err_file.flush()
                print(f"[{processed_count}/{total_files}] [ERR] {res['filepath']}: {res['error']}", flush=True)

    err_file.close()
    anom_file.close()

    coverage_path = os.path.join(meta_dir, "funding_coverage.csv")
    if coverage_records:
        cov_df = pd.DataFrame(coverage_records)
        cov_df = cov_df.sort_values(["exchange", "symbol_base"]).reset_index(drop=True)
        cov_df.to_csv(coverage_path, index=False, encoding="utf-8")
        print(f"\n[INFO] Saved funding_coverage.csv: {coverage_path} ({len(cov_df)} records)", flush=True)

    elapsed = time.time() - t_start

    print("\n" + "=" * 70, flush=True)
    print("FINAL REPORT: FUNDING RATE NORMALIZATION", flush=True)
    print("=" * 70, flush=True)
    print(f"Processed raw files:            {processed_count}")
    print(f"Success:                        {n_success}")
    print(f"Skipped (<10 rows):             {n_skipped}")
    print(f"Errors:                         {n_errors} (see {errors_log_path})")
    print(f"Total funding events:           {total_events:,}")
    print(f"Unique coins:                   {len(unique_coins)}")
    print(f"Unique exchanges:               {len(unique_exchanges)}")
    print(f"Detected anomalies:             {total_anomalies} (see {anomalies_log_path})")
    print(f"Elapsed time:                   {elapsed:.1f} sec ({elapsed/60:.2f} min)")
    print("=" * 70, flush=True)

def main():
    parser = argparse.ArgumentParser(description="Normalizer for Crypto Derivatives Funding Rate Data")
    parser.add_argument("--raw-dir", type=str, default=RAW_DIR_DEFAULT, help="Path to raw funding rate parquet folder")
    parser.add_argument("--out-dir", type=str, default=OUT_DIR_DEFAULT, help="Target clean output directory")
    parser.add_argument("--exchange", type=str, default="all", help="Target exchange or 'all'")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads (default 4)")
    parser.add_argument("--dry-run", action="store_true", help="Perform checks and coverage without writing files")
    args = parser.parse_args()

    process_all_funding(
        raw_dir=args.raw_dir,
        out_dir=args.out_dir,
        target_exchange=args.exchange,
        workers=args.workers,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()
