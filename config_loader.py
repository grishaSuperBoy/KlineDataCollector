"""
config_loader.py
================
Centralized configuration & date range parser for KlineDataCollector.

Reads 'collectFrom' and 'collectTo' dynamically from crypto_alts_multi_exchange.json,
or allows CLI flag overrides (--from, --to).
"""

import os
import json
import datetime
from typing import Dict, List, Any, Optional, Tuple

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_DIR, "crypto_alts_multi_exchange.json")


def parse_date(val: Any) -> datetime.datetime:
    """
    Parse a string, number, or keyword into a timezone-aware UTC datetime.
    Supports:
      - 'present', 'now', 'latest', None -> current UTC time
      - 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD', ISO-8601
      - Integer/float unix timestamps (seconds or milliseconds)
    """
    if val is None or val == "":
        return datetime.datetime.now(datetime.timezone.utc)

    if isinstance(val, (int, float)):
        # Milliseconds if timestamp > 1e11 (e.g. 1700000000000)
        if val > 1e11:
            return datetime.datetime.fromtimestamp(val / 1000.0, tz=datetime.timezone.utc)
        return datetime.datetime.fromtimestamp(val, tz=datetime.timezone.utc)

    if isinstance(val, str):
        cleaned = val.strip().lower()
        if cleaned in ("present", "now", "latest"):
            return datetime.datetime.now(datetime.timezone.utc)

        # Standard formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(val.strip(), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt
            except ValueError:
                pass

        try:
            dt = datetime.datetime.fromisoformat(val.strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt
        except Exception:
            pass

    raise ValueError(f"Unable to parse date string into UTC datetime: '{val}'")


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load crypto_alts_multi_exchange.json configuration file."""
    path = config_path or DEFAULT_CONFIG_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def get_collection_window(
    config_path: Optional[str] = None,
    override_from: Optional[str] = None,
    override_to: Optional[str] = None
) -> Tuple[datetime.datetime, datetime.datetime, int, int]:
    """
    Returns (start_dt, end_dt, start_ts_ms, end_ts_ms).
    
    Priority:
    1. CLI overrides (override_from, override_to)
    2. Fields 'collectFrom' and 'collectTo' in crypto_alts_multi_exchange.json
    3. Fallback: 2.5 months ago to present
    """
    path = config_path or DEFAULT_CONFIG_PATH
    data = load_config(path) if os.path.exists(path) else {}

    raw_from = override_from
    raw_to = override_to

    if raw_from is None and isinstance(data, dict):
        raw_from = data.get("collectFrom")
    if raw_to is None and isinstance(data, dict):
        raw_to = data.get("collectTo")

    # Sensible defaults if not defined
    if not raw_from:
        raw_from = "2026-07-25 00:00:00"
    if not raw_to:
        raw_to = "present"

    start_dt = parse_date(raw_from)
    end_dt = parse_date(raw_to)

    start_ts_ms = int(start_dt.timestamp() * 1000)
    end_ts_ms = int(end_dt.timestamp() * 1000)

    return start_dt, end_dt, start_ts_ms, end_ts_ms


def get_coins_for_exchange(exchange: str, config_path: Optional[str] = None) -> List[str]:
    """Get coin list for a specific exchange from config."""
    data = load_config(config_path)
    exchanges = data.get("exchanges", data) if isinstance(data, dict) else data

    if isinstance(exchanges, list):
        for item in exchanges:
            if isinstance(item, dict) and item.get("exchange") == exchange:
                return item.get("coins", [])
    return []


def get_symbols_mapping(exchange: str, project_dir: Optional[str] = None) -> Dict[str, str]:
    """
    Finds and loads the symbol mapping dictionary {base_coin: market_symbol}
    for the specified exchange from local matched symbols files.
    """
    root = project_dir or PROJECT_DIR
    candidates = [
        os.path.join(root, f"{exchange}_matched_symbols.json"),
        os.path.join(root, "mappings", f"{exchange}_matched_symbols.json"),
    ]
    for p in candidates:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
    return {}
