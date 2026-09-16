"""Public dashboard for the Spanish electricity market (placeholder).

Run locally with: streamlit run dashboard/streamlit_app.py
"""
from pathlib import Path

import duckdb
import streamlit as st

WAREHOUSE = Path(__file__).resolve().parents[1] / "data" / "warehouse" / "energy.duckdb"

st.set_page_config(page_title="Spain Energy Dashboard", page_icon="⚡", layout="wide")
st.title("⚡ Spanish Electricity Market")

if not WAREHOUSE.exists():
    st.warning("Warehouse not found. Run the ingestion first: `python -m ingestion.ree_client`")
    st.stop()

con = duckdb.connect(str(WAREHOUSE), read_only=True)
demand = con.execute(
    """
    select cast(datetime_local as timestamp) as hour, series_title as series, value as mw
    from raw_ree_indicators
    where indicator = 'demand_realtime'
    order by hour
    """
).df()

st.subheader("Hourly demand (raw layer)")
if demand.empty:
    st.info("No demand data yet.")
else:
    st.line_chart(demand.pivot_table(index="hour", columns="series", values="mw"))
