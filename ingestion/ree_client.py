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


def fetch_indicator(indicator: str, day: date) -> dict:
    path, time_trunc = INDICATORS[indicator]
    params = {
        "start_date": f"{day}T00:00",
        "end_date": f"{day}T23:59",
        "time_trunc": time_trunc,
    }
    response = requests.get(
        f"{BASE_URL}/{path}",
        params=params,
        headers={"Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def save_raw(indicator: str, day: date, payload: dict) -> Path:
    target = RAW_DIR / indicator / f"{day}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def load_to_duckdb(indicator: str, day: date, payload: dict) -> int:
    """Flatten the REData 'included' series into one row per data point."""
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
                    str(day),
                )
            )

    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE_PATH)) as con:
        con.execute(RAW_TABLE_DDL)
        con.execute(
            "DELETE FROM raw_ree_indicators WHERE indicator = ? AND extraction_day = ?",
            [indicator, str(day)],
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


def ingest(indicator: str, day: date) -> int:
    payload = fetch_indicator(indicator, day)
    raw_path = save_raw(indicator, day, payload)
    n_rows = load_to_duckdb(indicator, day, payload)
    logger.info("%s (%s): %d rows -> %s", indicator, day, n_rows, raw_path)
    return n_rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Ingest REE REData indicators into DuckDB")
    parser.add_argument("--indicator", choices=[*INDICATORS, "all"], default="all")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="Day to ingest (YYYY-MM-DD), defaults to yesterday",
    )
    args = parser.parse_args()

    indicators = list(INDICATORS) if args.indicator == "all" else [args.indicator]
    for indicator in indicators:
        ingest(indicator, args.date)


if __name__ == "__main__":
    main()
