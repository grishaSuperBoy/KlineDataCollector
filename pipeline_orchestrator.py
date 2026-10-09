"""
pipeline_orchestrator.py
========================
Dynamic Multi-Exchange Download Pipeline Orchestrator.

Manages parallel execution of exchange downloaders with configurable concurrency,
real-time status logging, and automatic error handling.
"""

import os
import sys
import time
import json
import argparse
import datetime
import subprocess

import config_loader

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_BASE = os.path.join(BASE_DIR, "tf_1min_kline_data")
LOG_DIR = os.path.join(OUT_BASE, "_orchestrator_logs")
QUEUE_FILE = os.path.join(OUT_BASE, "pipeline_queue.json")
STATUS_FILE = os.path.join(OUT_BASE, "pipeline_status.json")
ORCHESTRATOR_LOG = os.path.join(LOG_DIR, "orchestrator.log")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_BASE, exist_ok=True)

ALL_EXCHANGE_TASKS = [
    {"name": "binance", "script": "download_binance.py"},
    {"name": "bybit", "script": "download_bybit.py"},
    {"name": "okx", "script": "download_okx.py"},
    {"name": "bitget", "script": "download_bitget.py"},
    {"name": "bingx", "script": "download_bingx.py"},
    {"name": "kucoin", "script": "download_kucoin.py"},
    {"name": "gateio", "script": "download_gateio.py"},
    {"name": "mexc", "script": "download_mexc.py"},
    {"name": "hyperliquid", "script": "download_hyperliquid.py"},
    {"name": "asterdex", "script": "download_asterdex.py"},
]

def log(msg: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(ORCHESTRATOR_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def run_orchestrator(
    exchanges=None,
    max_concurrent: int = 2,
    from_date: str = None,
    to_date: str = None,
    with_funding: bool = False
):
    start_dt, end_dt, start_ms, end_ms = config_loader.get_collection_window(
        override_from=from_date,
        override_to=to_date
    )

    log("=" * 65)
    log("KlineDataCollector Download Pipeline Orchestrator")
    log(f"Window: {start_dt} -> {end_dt}")
    log(f"Max concurrency: {max_concurrent}")
    log("=" * 65)

    if exchanges and exchanges != ["all"]:
        tasks_queue = [t for t in ALL_EXCHANGE_TASKS if t["name"] in exchanges]
    else:
        tasks_queue = list(ALL_EXCHANGE_TASKS)

    active_tasks = []
    completed_tasks = []
    failed_tasks = []

    def update_status():
        st = {
            "updated_at": datetime.datetime.now().isoformat(),
            "time_window": {"from": str(start_dt), "to": str(end_dt)},
            "active_tasks": [
                {
                    "name": t["name"],
                    "pid": t["popen"].pid if t.get("popen") else None,
                    "elapsed_sec": round(time.time() - t["start_time"], 1),
                }
                for t in active_tasks
            ],
            "completed": completed_tasks,
            "failed": failed_tasks,
            "queue": [t["name"] for t in tasks_queue],
        }
        try:
            with open(STATUS_FILE, "w", encoding="utf-8") as f:
                json.dump(st, f, indent=2)
        except Exception:
            pass

    # Process loop
    while tasks_queue or active_tasks:
        # Check active tasks
        still_active = []
        for task in active_tasks:
            proc = task.get("popen")
            ret = proc.poll() if proc else None

            if ret is None:
                still_active.append(task)
            elif ret == 0:
                elapsed = time.time() - task["start_time"]
                log(f"[SUCCESS] {task['name'].upper()} finished in {elapsed:.1f}s")
                completed_tasks.append({
                    "name": task["name"],
                    "script": task["script"],
                    "status": "success",
                    "elapsed_sec": round(elapsed, 1),
                    "completed_at": datetime.datetime.now().isoformat(),
                })
            else:
                elapsed = time.time() - task["start_time"]
                log(f"[FAILED] {task['name'].upper()} exited with code {ret} after {elapsed:.1f}s")
                failed_tasks.append({
                    "name": task["name"],
                    "script": task["script"],
                    "status": f"exit code {ret}",
                    "elapsed_sec": round(elapsed, 1),
                })
        active_tasks = still_active

        # Launch new tasks up to max_concurrent
        while len(active_tasks) < max_concurrent and tasks_queue:
            task = tasks_queue.pop(0)
            script_path = os.path.join(BASE_DIR, task["script"])
            log_file = os.path.join(LOG_DIR, f"{task['name']}.log")

            cmd = [sys.executable, script_path]
            if from_date:
                cmd.extend(["--from", from_date])
            if to_date:
                cmd.extend(["--to", to_date])

            log(f"[LAUNCH] Starting {task['name'].upper()} ({task['script']}) -> Log: {task['name']}.log")
            out_fp = open(log_file, "a", encoding="utf-8")
            proc = subprocess.Popen(cmd, stdout=out_fp, stderr=subprocess.STREQUR_MERGE if hasattr(subprocess, 'STREQUR_MERGE') else subprocess.STDOUT)

            task["popen"] = proc
            task["start_time"] = time.time()
            active_tasks.append(task)

        update_status()
        time.sleep(2)

    log("=" * 65)
    log(f"All Kline Download Tasks Completed! (Success: {len(completed_tasks)}, Failed: {len(failed_tasks)})")
    log("=" * 65)

    if with_funding:
        log("Starting Funding Rates Download for all exchanges...")
        cmd = [sys.executable, os.path.join(BASE_DIR, "download_funding.py"), "--exchange", "all"]
        if from_date:
            cmd.extend(["--from", from_date])
        if to_date:
            cmd.extend(["--to", to_date])
        subprocess.run(cmd)
        log("Funding rate download finished.")

def main():
    parser = argparse.ArgumentParser(description="Multi-Exchange Kline Download Pipeline Orchestrator")
    parser.add_argument("--exchanges", type=str, default="all", help="Comma-separated list of exchanges or 'all'")
    parser.add_argument("--max-concurrent", type=int, default=2, help="Max simultaneous downloader processes (default: 2)")
    parser.add_argument("--from", dest="from_date", type=str, default=None, help="Start date override (UTC)")
    parser.add_argument("--to", dest="to_date", type=str, default=None, help="End date override (UTC)")
    parser.add_argument("--with-funding", action="store_true", help="Also trigger download_funding.py after klines")
    args = parser.parse_args()

    exchanges = [e.strip().lower() for e in args.exchanges.split(",")] if args.exchanges != "all" else ["all"]
    run_orchestrator(
        exchanges=exchanges,
        max_concurrent=args.max_concurrent,
        from_date=args.from_date,
        to_date=args.to_date,
        with_funding=args.with_funding
    )

if __name__ == "__main__":
    main()
