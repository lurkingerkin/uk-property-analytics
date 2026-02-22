from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("db/housing.db")

# Use region table if it exists, else fall back to UK joined table
JOINED_REGION = "ppd_sales_geo_region"
JOINED_UK = "ppd_sales_geo"
TILES_TABLE = "heatmap_tiles"

# ~1km-ish grid. 0.01 degrees lat ~= 1.11km. Good enough for a heatmap MVP.
GRID_DEGREES = 0.01

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None

def table_rowcount(conn: sqlite3.Connection, name: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]

def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        # Prefer the region table, but only if it exists AND has rows
        source = JOINED_UK
        if table_exists(conn, JOINED_REGION):
            region_rows = table_rowcount(conn, JOINED_REGION)
            print(f"{JOINED_REGION} rows: {region_rows:,}")
            if region_rows > 0:
                source = JOINED_REGION

        uk_rows = table_rowcount(conn, JOINED_UK) if table_exists(conn, JOINED_UK) else 0
        print(f"{JOINED_UK} rows: {uk_rows:,}")
        print(f"Using source table: {source}")

        conn.execute(f"DROP TABLE IF EXISTS {TILES_TABLE}")

        # median in SQLite: use percentile_cont if available? (often not)
        # so use median approximation via window functions:
        conn.execute(f"""
            CREATE TABLE {TILES_TABLE} AS
            WITH binned AS (
                SELECT
                    CAST(FLOOR(lat / {GRID_DEGREES}) * {GRID_DEGREES} AS REAL) AS lat_bin,
                    CAST(FLOOR(lon / {GRID_DEGREES}) * {GRID_DEGREES} AS REAL) AS lon_bin,
                    price
                FROM {source}
                WHERE lat IS NOT NULL AND lon IS NOT NULL AND price IS NOT NULL
            ),
            ranked AS (
                SELECT
                    lat_bin,
                    lon_bin,
                    price,
                    ROW_NUMBER() OVER (PARTITION BY lat_bin, lon_bin ORDER BY price) AS rn,
                    COUNT(*) OVER (PARTITION BY lat_bin, lon_bin) AS cnt
                FROM binned
            )
            SELECT
                lat_bin,
                lon_bin,
                cnt AS sales_count,
                AVG(price) AS avg_price,
                -- median: average of middle values (handles odd/even)
                AVG(
                    CASE
                        WHEN rn IN ((cnt + 1) / 2, (cnt + 2) / 2) THEN price
                    END
                ) AS median_price
            FROM ranked
            GROUP BY lat_bin, lon_bin, cnt
            HAVING cnt >= 5
        """)

        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TILES_TABLE}_latlon ON {TILES_TABLE}(lat_bin, lon_bin)")
        conn.commit()

        n = conn.execute(f"SELECT COUNT(*) FROM {TILES_TABLE}").fetchone()[0]
        if n == 0:
            # Helpful diagnostics
            cur = conn.cursor()
            src_rows = table_rowcount(conn, source)
            print(f"No tiles created. {source} rows: {src_rows:,}")
            if src_rows > 0:
                latlon = cur.execute(
                    f"SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lon) FROM {source}"
                ).fetchone()
                print(f"{source} lat/lon range: {latlon}")
        print(f"Built {TILES_TABLE} with {n:,} tiles.")
        print(f"DB: {DB_PATH}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()