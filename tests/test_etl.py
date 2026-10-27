"""Unit tests for the Polars transformation step."""

import logging
from datetime import datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from src.common.transform import get_source_schema, transform

PRIMARY_KEY = ("SCHEDULE_CODE",)
LOGGER = logging.getLogger("etl.test")

SAMPLE_ROW = {
    "SCHEDULE_CODE": "SC001",
    "SCHEDULE_DATE": datetime(2026, 9, 8, 10, 0),
    "CABANG": "MERAK",
    "DERMAGA": "DERMAGA_MERAK",
    "JENIS_PELAYANAN": "EKONOMI",
    "NAMA_KAPAL_REALISASI": "KAPAL SATU",
    "OPERATOR_REALISASI": "PT PELNI",
    "REALISASI_TIBA_ALUR": datetime(2026, 9, 8, 11, 0),
    "REALISASI_SANDAR": datetime(2026, 9, 8, 11, 15),
    "REALISASI_MULAI_PELAYANAN": datetime(2026, 9, 8, 11, 30),
    "REALISASI_SELESAI_PELAYANAN": datetime(2026, 9, 8, 12, 30),
    "REALISASI_TUTUP_RAMPDOOR": datetime(2026, 9, 8, 12, 40),
    "REALISASI_KEBERANGKATAN": datetime(2026, 9, 8, 13, 0),
}


def make_raw(rows):
    return pl.DataFrame(rows, schema=get_source_schema(), strict=False)


def test_source_schema_defines_the_target_contract():
    schema = get_source_schema()
    assert len(schema) == 13
    assert schema["SCHEDULE_CODE"] == pl.String
    assert schema["SCHEDULE_DATE"] == pl.Datetime("us")
    assert schema["REALISASI_KEBERANGKATAN"] == pl.Datetime("us")


def test_transform_trims_strings_and_matches_schema():
    padded = dict(SAMPLE_ROW, CABANG="  MERAK  ", OPERATOR_REALISASI=" PT PELNI ")
    result = transform(make_raw([padded]), get_source_schema(), PRIMARY_KEY, LOGGER)

    assert result.schema == get_source_schema()
    expected = make_raw([SAMPLE_ROW])
    assert_frame_equal(result, expected)


def test_transform_preserves_nulls():
    row = dict(SAMPLE_ROW, CABANG=None, SCHEDULE_DATE=None)
    result = transform(make_raw([row]), get_source_schema(), PRIMARY_KEY, LOGGER)

    assert result.height == 1
    assert result["CABANG"][0] is None
    assert result["SCHEDULE_DATE"][0] is None


def test_transform_missing_column_raises():
    raw_df = make_raw([SAMPLE_ROW]).drop("DERMAGA")
    with pytest.raises(ValueError, match="Missing required source columns.*DERMAGA"):
        transform(raw_df, get_source_schema(), PRIMARY_KEY, LOGGER)


def test_transform_deduplicates_on_primary_key_keeping_last(caplog):
    first = dict(SAMPLE_ROW, CABANG="OLD")
    second = dict(SAMPLE_ROW, CABANG="NEW")
    other = dict(SAMPLE_ROW, SCHEDULE_CODE="SC002", CABANG="BAKAUHENI")

    with caplog.at_level(logging.WARNING, logger="etl.test"):
        result = transform(
            make_raw([first, second, other]), get_source_schema(), PRIMARY_KEY, LOGGER
        )

    assert result.height == 2
    assert result["SCHEDULE_CODE"].to_list() == ["SC001", "SC002"]
    assert result["CABANG"].to_list() == ["NEW", "BAKAUHENI"]
    assert "duplicate primary key" in caplog.text


def test_transform_drops_null_primary_key_rows(caplog):
    null_key = dict(SAMPLE_ROW, SCHEDULE_CODE=None)

    with caplog.at_level(logging.WARNING, logger="etl.test"):
        result = transform(
            make_raw([SAMPLE_ROW, null_key]), get_source_schema(), PRIMARY_KEY, LOGGER
        )

    assert result.height == 1
    assert result["SCHEDULE_CODE"].to_list() == ["SC001"]
    assert "null primary key" in caplog.text


def test_transform_handles_empty_dataset():
    result = transform(make_raw([]), get_source_schema(), PRIMARY_KEY, LOGGER)

    assert result.height == 0
    assert result.schema == get_source_schema()


def test_transform_rejects_bad_source_types():
    schema = get_source_schema()
    raw_df = pl.DataFrame(
        {
            "SCHEDULE_CODE": ["SC001"],
            "SCHEDULE_DATE": ["not-a-date"],
            "CABANG": ["MERAK"],
            "DERMAGA": ["D1"],
            "JENIS_PELAYANAN": ["EKONOMI"],
            "NAMA_KAPAL_REALISASI": ["KAPAL"],
            "OPERATOR_REALISASI": ["OP"],
            "REALISASI_TIBA_ALUR": [None],
            "REALISASI_SANDAR": [None],
            "REALISASI_MULAI_PELAYANAN": [None],
            "REALISASI_SELESAI_PELAYANAN": [None],
            "REALISASI_TUTUP_RAMPDOOR": [None],
            "REALISASI_KEBERANGKATAN": [None],
        },
        schema={**schema, "SCHEDULE_DATE": pl.String},
        strict=False,
    )
    with pytest.raises((pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError)):
        transform(raw_df, schema, PRIMARY_KEY, LOGGER)
