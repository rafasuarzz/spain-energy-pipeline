# spain-energy-pipeline

End-to-end data engineering pipeline for the Spanish electricity market, built on public data
from **Red Eléctrica de España (REE)** via the [REData API](https://www.ree.es/es/apidatos)
(no API key required).

## Architecture

```
REE REData API ──> Airflow (Docker) ──> raw JSON (data/raw/)
                                   └──> DuckDB raw layer (data/warehouse/energy.duckdb)
                                              │
                              Great Expectations (raw data validation)
                                              │
                                   dbt (staging ──> marts)
                                              │
                        ┌─────────────────────┴─────────────────────┐
                   Metabase (local BI, Docker)              Streamlit (public dashboard)
```

| Layer | Tool | Notes |
|---|---|---|
| Ingestion & orchestration | Apache Airflow 3 (Docker Compose) | Daily DAG, one mapped task per indicator |
| Storage | DuckDB | Single-file warehouse, fully reproducible |
| Transformation | dbt (dbt-duckdb) | staging → marts |
| Data quality | Great Expectations + dbt tests | GE validates raw inputs, dbt tests validate models |
| Visualization | Metabase (local) + Streamlit (deployed) | |

## Data sources (REData indicators)

| Indicator | Endpoint | Granularity |
|---|---|---|
| `demand_realtime` | `demanda/demanda-tiempo-real` | hour |
| `demand_daily` | `demanda/evolucion` | day |
| `generation_mix` | `generacion/estructura-generacion` | day |
| `market_prices` | `mercados/precios-mercados-tiempo-real` | hour |

Note: `demanda/evolucion` rejects `time_trunc=hour` with a misleading 400 ("data not available");
hourly demand comes from `demanda-tiempo-real` instead.

## Quickstart (local, no Docker)

```bash
python -m venv .venv
.venv/Scripts/activate      # Windows
pip install -r requirements.txt
python -m ingestion.ree_client --indicator all
```

This fetches yesterday's data for every indicator, stores the raw JSON under `data/raw/`
and loads it into `data/warehouse/energy.duckdb` (table `raw_ree_indicators`).

## Full stack (Docker)

```bash
docker compose up
```

- Airflow UI: http://localhost:8080
- Metabase: http://localhost:3000

## Project status

- [x] REE API client + raw layer in DuckDB
- [x] Repo scaffolding (Airflow DAG, dbt project, docker-compose)
- [ ] Airflow stack verified end-to-end in Docker
- [ ] dbt staging + marts models
- [ ] Great Expectations suites + Data Docs
- [ ] Metabase DuckDB driver + dashboards
- [ ] Streamlit app deployed
- [ ] CI (GitHub Actions): lint + dbt build
