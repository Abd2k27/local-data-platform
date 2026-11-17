import pandas as pd
import pytest

from ingestion import nyc_taxi


def test_trip_url_is_zero_padded():
    assert nyc_taxi.trip_url(2024, 3).endswith("/trip-data/yellow_tripdata_2024-03.parquet")


@pytest.mark.parametrize("month", [0, 13])
def test_invalid_month_rejected(month):
    with pytest.raises(ValueError):
        nyc_taxi.trip_file_name(2024, month)


def test_normalize_batch_aligns_schema_and_adds_lineage():
    raw = pd.DataFrame(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 11:00"]),
            "PULocationID": [132, 161],
            "Airport_fee": [1.75, None],
            "unexpected_column": ["x", "y"],
        }
    )
    out = nyc_taxi.normalize_batch(raw, "yellow_tripdata_2024-01.parquet", first_row=500)

    assert list(out.columns) == [*nyc_taxi.TRIP_COLUMNS, "source_file", "row_num"]
    assert out["row_num"].tolist() == [500, 501]
    assert out["source_file"].unique().tolist() == ["yellow_tripdata_2024-01.parquet"]
    assert out["pulocationid"].tolist() == [132, 161]
    # Columns absent from the source file (schema drift) are NULL, not an error.
    assert out["cbd_congestion_fee"].isna().all()


def test_dsn_reads_environment(monkeypatch):
    monkeypatch.setenv("WAREHOUSE_HOST", "postgres")
    monkeypatch.setenv("WAREHOUSE_PORT", "5432")
    dsn = nyc_taxi.warehouse_dsn()
    assert "host=postgres" in dsn and "port=5432" in dsn
