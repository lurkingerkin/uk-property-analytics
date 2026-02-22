from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
import folium
from folium.plugins import HeatMap

DB_PATH = Path("db/housing.db")
TILES_TABLE = "heatmap_tiles"
PUBLISHED_TILES = Path("data/published/heatmap_tiles.parquet")

st.set_page_config(page_title="UK Property Heatmap", layout="wide")

st.title("UK Property Heatmap")
st.caption("Heatmap built from Land Registry Price Paid Data (PPD) aggregated into grid tiles.")


@st.cache_data(show_spinner=False)
def load_tiles() -> pd.DataFrame:
    if PUBLISHED_TILES.exists():
        df = pd.read_parquet(PUBLISHED_TILES)
    else:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            df = pd.read_sql_query(
                f"SELECT lat_bin, lon_bin, sales_count, avg_price, median_price FROM {TILES_TABLE}",
                conn,
            )
        finally:
            conn.close()

    # Basic cleaning
    for col in ["lat_bin", "lon_bin", "sales_count", "avg_price", "median_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["lat_bin", "lon_bin"]).copy()
    return df


def make_map(df: pd.DataFrame, metric: str, min_sales: int) -> folium.Map:
    df2 = df[df["sales_count"] >= min_sales].copy()

    if df2.empty:
        center = (52.4, -2.2)  # Midlands-ish
        m = folium.Map(location=center, zoom_start=7, tiles="CartoDB positron")
        folium.Marker(location=center, tooltip="No data after filters").add_to(m)
        return m

    center = (float(df2["lat_bin"].mean()), float(df2["lon_bin"].mean()))
    m = folium.Map(location=center, zoom_start=7, tiles="CartoDB positron")

    # Build heatmap weights
    if metric == "sales_count":
        weights = df2["sales_count"].astype(float)
    else:
        weights = df2[metric].astype(float).clip(lower=0)

    heat_data = df2[["lat_bin", "lon_bin"]].assign(w=weights).dropna().values.tolist()

    HeatMap(
        heat_data,
        radius=12,
        blur=16,
        max_zoom=10,
    ).add_to(m)

    return m


# Sidebar controls
with st.sidebar:
    st.header("Controls")

    tiles = load_tiles()

    metric_label = st.selectbox(
        "Metric",
        options=[
            ("Median price", "median_price"),
            ("Average price", "avg_price"),
            ("Sales volume", "sales_count"),
        ],
        format_func=lambda x: x[0],
    )
    metric = metric_label[1]

    min_sales = st.slider(
        "Minimum sales per tile",
        min_value=1,
        max_value=50,
        value=5,
        step=1,
    )

    st.divider()
    st.write("**Tiles loaded:**", f"{len(tiles):,}")
    st.write("**After filter:**", f"{len(tiles[tiles['sales_count'] >= min_sales]):,}")


m = make_map(tiles, metric=metric, min_sales=min_sales)

# Render Folium map
st.components.v1.html(m._repr_html_(), height=720, scrolling=False)


with st.expander("Preview heatmap tile data"):
    st.dataframe(
        tiles.sort_values("sales_count", ascending=False).head(200),
        width="stretch",
    )