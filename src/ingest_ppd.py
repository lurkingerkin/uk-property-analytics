from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

PPD_CSV = Path("data/raw/ppd/pp-2025.csv")
PPD_CSV_ALT = Path("Data/raw/ppd/pp-2025.csv")  # in case the folder was created with a capital D
DB_PATH = Path("db/housing.db")

TABLE_PPD = "ppd_sales"
TABLE_JOINED_UK = "ppd_sales_geo"
TABLE_JOINED_REGION = "ppd_sales_geo_region"


def normalise_postcode(pc: str) -> str:
    return pc.strip().upper().replace(" ", "")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_PPD} (
            transaction_id TEXT PRIMARY KEY,
            price INTEGER NOT NULL,
            transfer_date TEXT NOT NULL,
            postcode TEXT NOT NULL,
            property_type TEXT,
            new_build TEXT,
            tenure TEXT,
            paon TEXT,
            saon TEXT,
            street TEXT,
            locality TEXT,
            town TEXT,
            district TEXT,
            county TEXT,
            ppd_category_type TEXT,
            record_status TEXT
        )
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PPD}_postcode ON {TABLE_PPD}(postcode)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_PPD}_date ON {TABLE_PPD}(transfer_date)")
    conn.commit()


def detect_header(first_row: list[str]) -> bool:
    """
    PPD downloads are usually headerless. If first row contains non-date/non-numeric fields,
    treat as header.
    """
    if not first_row:
        return False
    # Typical first row values: {GUID},{price},{YYYY-MM-DD},{postcode},...
    # If first column looks like 'transaction unique identifier' etc, that's a header.
    joined = ",".join(first_row).lower()
    return ("transaction" in joined and "identifier" in joined) or ("price" in joined and "postcode" in joined)


def ingest_ppd(conn: sqlite3.Connection) -> None:
    ppd_path = PPD_CSV
    if not ppd_path.exists() and PPD_CSV_ALT.exists():
        ppd_path = PPD_CSV_ALT

    if not ppd_path.exists():
        # Helpful debug output
        candidates = list(Path("data/raw/ppd").glob("*.csv")) if Path("data/raw/ppd").exists() else []
        candidates_alt = list(Path("Data/raw/ppd").glob("*.csv")) if Path("Data/raw/ppd").exists() else []
        raise SystemExit(
            f"Missing file: {PPD_CSV}.\n"
            f"Looked also for: {PPD_CSV_ALT}.\n"
            f"Found in data/raw/ppd: {[p.name for p in candidates]}.\n"
            f"Found in Data/raw/ppd: {[p.name for p in candidates_alt]}."
        )

    print(f"Ingesting PPD from: {ppd_path}")

    BATCH_SIZE = 50_000
    batch = []
    total_rows_read = 0
    total_rows_written = 0

    insert_sql = f"""
        INSERT INTO {TABLE_PPD} (
            transaction_id, price, transfer_date, postcode, property_type, new_build, tenure,
            paon, saon, street, locality, town, district, county,
            ppd_category_type, record_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(transaction_id) DO UPDATE SET
            price=excluded.price,
            transfer_date=excluded.transfer_date,
            postcode=excluded.postcode,
            property_type=excluded.property_type,
            new_build=excluded.new_build,
            tenure=excluded.tenure,
            paon=excluded.paon,
            saon=excluded.saon,
            street=excluded.street,
            locality=excluded.locality,
            town=excluded.town,
            district=excluded.district,
            county=excluded.county,
            ppd_category_type=excluded.ppd_category_type,
            record_status=excluded.record_status
    """

    with ppd_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)

        # Peek at first row to see if it's a header
        try:
            first_row = next(reader)
        except StopIteration:
            raise SystemExit(f"{ppd_path} is empty")

        has_header = detect_header(first_row)
        if has_header:
            print("Detected header row in PPD CSV; skipping it.")
        else:
            # Process the first row as data
            reader = iter([first_row] + list(reader))

        for row in reader:
            total_rows_read += 1

            # PPD format (typically 16 cols):
            #  0 transaction_id
            #  1 price
            #  2 transfer_date (YYYY-MM-DD)
            #  3 postcode
            #  4 property_type (D/S/T/F/O)
            #  5 old/new (Y/N)
            #  6 tenure (F/L)
            #  7 PAON
            #  8 SAON
            #  9 street
            # 10 locality
            # 11 town_city
            # 12 district
            # 13 county
            # 14 ppd_category_type (A/B)
            # 15 record_status (A/D)
            if not row or len(row) < 7:
                continue

            txid = (row[0] or "").strip()
            if not txid:
                continue

            try:
                price = int((row[1] or "").strip())
            except Exception:
                continue

            transfer_date = (row[2] or "").strip()
            postcode = normalise_postcode(row[3] or "")
            if not postcode:
                continue

            property_type = (row[4] or "").strip() or None
            new_build = (row[5] or "").strip() or None
            tenure = (row[6] or "").strip() or None

            # Optional address-ish fields (may or may not exist in your download)
            paon = (row[7] or "").strip() if len(row) > 7 else ""
            saon = (row[8] or "").strip() if len(row) > 8 else ""
            street = (row[9] or "").strip() if len(row) > 9 else ""
            locality = (row[10] or "").strip() if len(row) > 10 else ""
            town = (row[11] or "").strip() if len(row) > 11 else ""
            district = (row[12] or "").strip() if len(row) > 12 else ""
            county = (row[13] or "").strip() if len(row) > 13 else ""
            ppd_category_type = (row[14] or "").strip() if len(row) > 14 else ""
            record_status = (row[15] or "").strip() if len(row) > 15 else ""

            batch.append((
                txid, price, transfer_date, postcode, property_type, new_build, tenure,
                paon or None, saon or None, street or None, locality or None, town or None,
                district or None, county or None, ppd_category_type or None, record_status or None
            ))

            if len(batch) >= BATCH_SIZE:
                conn.executemany(insert_sql, batch)
                conn.commit()
                total_rows_written += len(batch)
                print(f"  processed ~{total_rows_read:,} rows (written ~{total_rows_written:,})")
                batch.clear()

        if batch:
            conn.executemany(insert_sql, batch)
            conn.commit()
            total_rows_written += len(batch)

    print(f"Done. Read ~{total_rows_read:,} rows; wrote ~{total_rows_written:,} rows into {TABLE_PPD}.")


def build_joined_tables(conn: sqlite3.Connection) -> None:
    print("Building joined table (PPD + postcode_geo)...")

    conn.execute(f"DROP TABLE IF EXISTS {TABLE_JOINED_UK}")
    conn.execute(f"""
        CREATE TABLE {TABLE_JOINED_UK} AS
        SELECT
            p.transaction_id,
            p.price,
            p.transfer_date,
            p.postcode,
            p.property_type,
            p.new_build,
            p.tenure,
            g.lat,
            g.lon,
            g.ladnm
        FROM {TABLE_PPD} p
        JOIN postcode_geo g
          ON p.postcode = g.postcode
        WHERE g.lat IS NOT NULL AND g.lon IS NOT NULL
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_JOINED_UK}_latlon ON {TABLE_JOINED_UK}(lat, lon)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_JOINED_UK}_date ON {TABLE_JOINED_UK}(transfer_date)")
    conn.commit()

    # If you have region table, also build a smaller joined version
    exists = conn.execute("""
        SELECT 1 FROM sqlite_master WHERE type='table' AND name='postcode_geo_region'
    """).fetchone()

    if exists:
        print("Building joined table for region (PPD + postcode_geo_region)...")
        conn.execute(f"DROP TABLE IF EXISTS {TABLE_JOINED_REGION}")
        conn.execute(f"""
            CREATE TABLE {TABLE_JOINED_REGION} AS
            SELECT
                p.transaction_id,
                p.price,
                p.transfer_date,
                p.postcode,
                p.property_type,
                p.new_build,
                p.tenure,
                g.lat,
                g.lon,
                g.ladnm
            FROM {TABLE_PPD} p
            JOIN postcode_geo_region g
              ON p.postcode = g.postcode
            WHERE g.lat IS NOT NULL AND g.lon IS NOT NULL
        """)
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_JOINED_REGION}_latlon ON {TABLE_JOINED_REGION}(lat, lon)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_JOINED_REGION}_date ON {TABLE_JOINED_REGION}(transfer_date)")
        conn.commit()


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")

        ensure_schema(conn)
        ingest_ppd(conn)

        # sanity check: postcode_geo must exist
        has_geo = conn.execute("""
            SELECT 1 FROM sqlite_master WHERE type='table' AND name='postcode_geo'
        """).fetchone()
        if not has_geo:
            raise SystemExit("Missing table postcode_geo. Run python src/ingest_onspd.py first.")

        build_joined_tables(conn)

        ppd_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_PPD}").fetchone()[0]
        joined_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_JOINED_UK}").fetchone()[0]
        print(f"{TABLE_PPD} rows: {ppd_count:,}")
        print(f"{TABLE_JOINED_UK} rows: {joined_count:,}")

        # Optional: show a sample
        sample = conn.execute(f"""
            SELECT transfer_date, price, postcode, lat, lon
            FROM {TABLE_JOINED_UK}
            ORDER BY transfer_date DESC
            LIMIT 5
        """).fetchall()
        print("Sample joined rows:", sample)

        print(f"SQLite DB: {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()