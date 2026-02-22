from __future__ import annotations

import csv
import glob
import sqlite3
from pathlib import Path

RAW_DIR = Path("data/raw/onspd")
DB_PATH = Path("db/housing.db")

TABLE_ALL = "postcode_geo"
TABLE_REGION = "postcode_geo_region"

# Worcestershire + bordering counties, filtered by Local Authority (LAD) name.
LAD_NAMES_REGION = {
    # Worcestershire
    "Worcester", "Wychavon", "Malvern Hills", "Bromsgrove", "Redditch", "Wyre Forest",
    # Herefordshire (unitary)
    "Herefordshire, County of",
    # Gloucestershire
    "Cheltenham", "Cotswold", "Forest of Dean", "Gloucester", "Stroud", "Tewkesbury",
    # Warwickshire
    "North Warwickshire", "Nuneaton and Bedworth", "Rugby", "Stratford-on-Avon", "Warwick",
    # Staffordshire
    "Cannock Chase", "East Staffordshire", "Lichfield", "Newcastle-under-Lyme",
    "South Staffordshire", "Stafford", "Staffordshire Moorlands", "Tamworth",
    # Shropshire
    "Shropshire", "Telford and Wrekin",
    # West Midlands (met county)
    "Birmingham", "Coventry", "Dudley", "Sandwell", "Solihull", "Walsall", "Wolverhampton",
}


def normalise_postcode(pc: str) -> str:
    return pc.strip().upper().replace(" ", "")


def find_csv_files() -> list[Path]:
    patterns = [str(RAW_DIR / "*.csv"), str(RAW_DIR / "**" / "*.csv")]
    files: list[Path] = []
    for p in patterns:
        files.extend(Path(x) for x in glob.glob(p, recursive=True))
    return sorted(files)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
               CREATE TABLE IF NOT EXISTS {TABLE_ALL} (
                   postcode TEXT PRIMARY KEY,
                   lat REAL,
                   lon REAL,
                   ladcd TEXT,
                   ladnm TEXT
               )
           """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_ladnm ON {TABLE_ALL}(ladnm)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_latlon ON {TABLE_ALL}(lat, lon)")
    conn.commit()


def upsert_batch(conn: sqlite3.Connection,
                 batch: list[tuple[str, float | None, float | None, str | None, str | None]]) -> int:
    cur = conn.cursor()
    cur.executemany(
        f"""
               INSERT INTO {TABLE_ALL} (postcode, lat, lon, ladcd, ladnm)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(postcode) DO UPDATE SET
                 lat=excluded.lat,
                 lon=excluded.lon,
                 ladcd=excluded.ladcd,
                 ladnm=excluded.ladnm
               """,
        batch,
    )
    conn.commit()
    return cur.rowcount


def ingest_csv(conn: sqlite3.Connection, path: Path) -> None:
    print(f"Ingesting {path} ...")
    batch: list[tuple[str, float | None, float | None, str | None, str | None]] = []
    BATCH_SIZE = 50_000
    total = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):
            pc_raw = row.get("pcds") or row.get("pcd") or ""
            if not pc_raw:
                continue
            pc = normalise_postcode(pc_raw)

            lat_s = (row.get("lat") or "").strip()
            lon_s = (row.get("long") or row.get("lon") or "").strip()
            lat = float(lat_s) if lat_s else None
            lon = float(lon_s) if lon_s else None

            ladcd = (row.get("ladcd") or "").strip() or None
            ladnm = (row.get("ladnm") or "").strip() or None

            batch.append((pc, lat, lon, ladcd, ladnm))

            if len(batch) >= BATCH_SIZE:
                total += upsert_batch(conn, batch)
                print(f"  processed ~{i:,} rows...")
                batch.clear()

        if batch:
            total += upsert_batch(conn, batch)

    print(f"Done ingesting. Upserted approx {total:,} rows.")


def build_region(conn: sqlite3.Connection) -> None:
    print("Building region subset table ...")
    conn.execute(f"DROP TABLE IF EXISTS {TABLE_REGION}")
    placeholders = ",".join(["?"] * len(LAD_NAMES_REGION))
    conn.execute(
        f"""
               CREATE TABLE {TABLE_REGION} AS
               SELECT *
               FROM {TABLE_ALL}
               WHERE ladnm IN ({placeholders})
                 AND lat IS NOT NULL
                 AND lon IS NOT NULL
               """,
        tuple(sorted(LAD_NAMES_REGION)),
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_REGION}_ladnm ON {TABLE_REGION}(ladnm)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_REGION}_latlon ON {TABLE_REGION}(lat, lon)")
    conn.commit()
    print("Region table built.")


def main() -> None:
    if not RAW_DIR.exists():
        raise SystemExit(f"Missing folder: {RAW_DIR}. Create it and place ONSPD CSV inside.")

    csvs = find_csv_files()
    if not csvs:
        raise SystemExit(f"No CSV found under {RAW_DIR}. Expected something like ONSPD_*_UK.csv")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        ensure_schema(conn)

        chosen = None
        for p in csvs:
            if p.name.upper().endswith("_UK.CSV"):
                chosen = p
                break
        if chosen is None:
            chosen = csvs[0]

        ingest_csv(conn, chosen)
        build_region(conn)

        cur = conn.cursor()
        all_count = cur.execute(f"SELECT COUNT(*) FROM {TABLE_ALL}").fetchone()[0]
        reg_count = cur.execute(f"SELECT COUNT(*) FROM {TABLE_REGION}").fetchone()[0]
        print(f"postcode_geo rows: {all_count:,}")
        print(f"postcode_geo_region rows: {reg_count:,}")
        print(f"SQLite DB written to: {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()