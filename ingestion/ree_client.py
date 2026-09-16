"""Client for the Red Electrica de Espana (REE) REData API.

REData is a public JSON API (no API key required) exposing electricity demand,
generation and market price data for Spain. Docs: https://www.ree.es/es/apidatos

Each indicator is fetched for a single day, stored as raw JSON under data/raw/
and loaded into the DuckDB raw layer. Loads are idempotent per (indicator, day),
so re-running a day (an Airflow backfill, for example) never duplicates rows.
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date, timedelta
from pathlib import Path

import requests

BASE_URL = "https://apidatos.ree.es/es/datos"

# indicator name -> (API path, only supported time_trunc).
# demanda/evolucion rejects time_trunc=hour with a misleading 400; hourly
# demand comes from demanda-tiempo-real instead.
INDICATORS: dict[str, tuple[str, str]] = {
    "demand_realtime": ("demanda/demanda-tiempo-real", "hour"),
    "demand_daily": ("demanda/evolucion", "day"),
    "generation_mix": ("generacion/estructura-generacion", "day"),
    "market_prices": ("mercados/precios-mercados-tiempo-real", "hour"),
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse" / "energy.duckdb"

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw_ree_indicators (
    indicator      VARCHAR,
    series_type    VARCHAR,
    series_title   VARCHAR,
    datetime_local VARCHAR,
    value          DOUBLE,
    percentage     DOUBLE,
    extraction_day VARCHAR,
    loaded_at      TIMESTAMP DEFAULT current_timestamp
)
"""

logger = logging.getLogger(__name__)


def fetch_indicator(indicator: str, start: date, end: date) -> dict:
    """Fetch one indicator for [start, end]. The API accepts up to ~1 month."""
    path, time_trunc = INDICATORS[indicator]
    params = {
        "start_date": f"{start}T00:00",
        "end_date": f"{end}T23:59",
        "time_trunc": time_trunc,
    }
    response = requests.get(
        f"{BASE_URL}/{path}",
        params=params,
        headers={"Accept": "application/json"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def save_raw(indicator: str, start: date, end: date, payload: dict) -> Path:
    name = f"{start}.json" if start == end else f"{start}_{end}.json"
    target = RAW_DIR / indicator / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def load_to_duckdb(indicator: str, start: date, end: date, payload: dict) -> int:
    """Flatten the REData 'included' series into one row per data point.

    extraction_day is derived from each point's own datetime, so a range load
    partitions correctly and re-loading any overlapping range stays idempotent.
    """
    import duckdb

    rows = []
    for series in payload.get("included", []):
        attrs = series["attributes"]
        for point in attrs.get("values", []):
            rows.append(
                (
                    indicator,
                    series.get("type"),
                    attrs.get("title"),
                    point.get("datetime"),
                    point.get("value"),
                    point.get("percentage"),
                    str(point.get("datetime", ""))[:10],
                )
            )

    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        con.execute(RAW_TABLE_DDL)
        con.execute(
            "DELETE FROM raw_ree_indicators WHERE indicator = ? AND extraction_day BETWEEN ? AND ?",
            [indicator, str(start), str(end)],
        )
        if rows:
            con.executemany(
                """
                INSERT INTO raw_ree_indicators
                    (indicator, series_type, series_title, datetime_local,
                     value, percentage, extraction_day)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    return len(rows)


def ingest_range(indicator: str, start: date, end: date) -> int:
    payload = fetch_indicator(indicator, start, end)
    raw_path = save_raw(indicator, start, end, payload)
    n_rows = load_to_duckdb(indicator, start, end, payload)
    logger.info("%s (%s..%s): %d rows -> %s", indicator, start, end, n_rows, raw_path)
    return n_rows


def ingest(indicator: str, day: date) -> int:
    """Single-day ingest, used by the daily Airflow DAG."""
    return ingest_range(indicator, day, day)


def month_chunks(start: date, end: date):
    """Split [start, end] into calendar-month-sized chunks the API accepts."""
    chunk_start = start
    while chunk_start <= end:
        next_month = (chunk_start.replace(day=1) + timedelta(days=32)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest REE REData indicators into DuckDB")
    parser.add_argument("--indicator", choices=[*INDICATORS, "all"], default="all")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="Single day to ingest (YYYY-MM-DD); defaults to yesterday if no range given",
    )
    parser.add_argument("--start", type=date.fromisoformat, help="Range start (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, help="Range end (YYYY-MM-DD)")
    args = parser.parse_args()

    if (args.start is None) != (args.end is None):
        parser.error("--start and --end must be given together")

    indicators = list(INDICATORS) if args.indicator == "all" else [args.indicator]
    if args.start:
        for chunk_start, chunk_end in month_chunks(args.start, args.end):
            for indicator in indicators:
                ingest_range(indicator, chunk_start, chunk_end)
    else:
        day = args.date or date.today() - timedelta(days=1)
        for indicator in indicators:
            ingest(indicator, day)


if __name__ == "__main__":
    main()
