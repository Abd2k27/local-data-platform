"""Ingest NYC TLC yellow-taxi trips and the taxi-zone lookup into the `raw` schema.

Loads are idempotent: re-running a month replaces that month's rows only.

Usage (outside Airflow, against the dockerised warehouse on localhost:5433):
    python -m ingestion.nyc_taxi --year 2024 --month 1
"""

from __future__ import annotations

import argparse
import io
import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests

log = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
ZONES_URL = f"{BASE_URL}/misc/taxi_zone_lookup.csv"
BATCH_SIZE = 250_000

# Raw columns we keep, with their Postgres type. The source schema drifts over the
# years (e.g. `cbd_congestion_fee` appears in 2025), so missing columns become NULL.
TRIP_COLUMNS: dict[str, str] = {
    "vendorid": "integer",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double precision",
    "trip_distance": "double precision",
    "ratecodeid": "double precision",
    "store_and_fwd_flag": "text",
    "pulocationid": "integer",
    "dolocationid": "integer",
    "payment_type": "integer",
    "fare_amount": "double precision",
    "extra": "double precision",
    "mta_tax": "double precision",
    "tip_amount": "double precision",
    "tolls_amount": "double precision",
    "improvement_surcharge": "double precision",
    "total_amount": "double precision",
    "congestion_surcharge": "double precision",
    "airport_fee": "double precision",
    "cbd_congestion_fee": "double precision",
}
META_COLUMNS: dict[str, str] = {
    "source_file": "text not null",
    "row_num": "bigint not null",
    "loaded_at": "timestamptz not null default now()",
}


def trip_file_name(year: int, month: int) -> str:
    if not (1 <= month <= 12):
        raise ValueError(f"month must be in 1..12, got {month}")
    return f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def trip_url(year: int, month: int) -> str:
    return f"{BASE_URL}/trip-data/{trip_file_name(year, month)}"


def warehouse_dsn() -> str:
    env = os.environ
    return (
        f"host={env.get('WAREHOUSE_HOST', 'localhost')} "
        f"port={env.get('WAREHOUSE_PORT', '5433')} "
        f"user={env.get('WAREHOUSE_USER', 'warehouse')} "
        f"password={env.get('WAREHOUSE_PASSWORD', 'warehouse')} "
        f"dbname={env.get('WAREHOUSE_DB', 'warehouse')}"
    )


def data_dir() -> Path:
    path = Path(os.environ.get("DATA_DIR", "data")) / "raw"
    path.mkdir(parents=True, exist_ok=True)
    return path


def download(url: str, dest: Path) -> Path:
    """Stream `url` to `dest`, skipping the download if the file already exists."""
    if dest.exists() and dest.stat().st_size > 0:
        log.info("Already downloaded: %s", dest)
        return dest
    tmp = dest.with_suffix(dest.suffix + ".part")
    log.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    tmp.rename(dest)
    return dest


def download_trips(year: int, month: int) -> Path:
    return download(trip_url(year, month), data_dir() / trip_file_name(year, month))


def normalize_batch(df: pd.DataFrame, source_file: str, first_row: int) -> pd.DataFrame:
    """Lower-case columns, align them to TRIP_COLUMNS and add lineage columns."""
    df = df.rename(columns=str.lower)
    out = df.reindex(columns=list(TRIP_COLUMNS))
    for col in ("vendorid", "pulocationid", "dolocationid", "payment_type"):
        out[col] = out[col].astype("Int64")
    out["source_file"] = source_file
    out["row_num"] = range(first_row, first_row + len(out))
    return out


def _copy_df(cur, table: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ", ".join(df.columns)
    with cur.copy(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)") as copy:
        while chunk := buf.read(1 << 20):
            copy.write(chunk)


def ensure_raw_tables(cur) -> None:
    cols = ",\n  ".join(f"{c} {t}" for c, t in {**TRIP_COLUMNS, **META_COLUMNS}.items())
    cur.execute("create schema if not exists raw")
    cur.execute(f"create table if not exists raw.yellow_trips (\n  {cols}\n)")
    cur.execute(
        "create index if not exists yellow_trips_source_file_idx on raw.yellow_trips (source_file)"
    )
    cur.execute(
        """
        create table if not exists raw.taxi_zones (
          locationid integer primary key,
          borough text,
          zone text,
          service_zone text,
          loaded_at timestamptz not null default now()
        )
        """
    )


def load_trips(path: Path) -> int:
    """Replace the rows of `path`'s month in raw.yellow_trips. Returns the row count."""
    import psycopg

    source_file = path.name
    parquet = pq.ParquetFile(path)
    total = 0
    with psycopg.connect(warehouse_dsn()) as conn, conn.cursor() as cur:
        ensure_raw_tables(cur)
        cur.execute("delete from raw.yellow_trips where source_file = %s", (source_file,))
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            df = normalize_batch(batch.to_pandas(), source_file, total)
            _copy_df(cur, "raw.yellow_trips", df)
            total += len(df)
            log.info("%s: %s rows loaded", source_file, f"{total:,}")
    return total


def load_zones() -> int:
    import psycopg

    path = download(ZONES_URL, data_dir() / "taxi_zone_lookup.csv")
    # keep_default_na=False: zone 265 has borough "N/A" ("Outside of NYC"), a real value.
    df = pd.read_csv(path, keep_default_na=False, na_values=[""]).rename(columns=str.lower)[
        ["locationid", "borough", "zone", "service_zone"]
    ]
    with psycopg.connect(warehouse_dsn()) as conn, conn.cursor() as cur:
        ensure_raw_tables(cur)
        cur.execute("truncate raw.taxi_zones")
        _copy_df(cur, "raw.taxi_zones", df)
    log.info("taxi_zones: %s rows loaded", len(df))
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_zones()
    load_trips(download_trips(args.year, args.month))


if __name__ == "__main__":
    main()
