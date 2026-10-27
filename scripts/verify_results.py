"""Verify ETL results and UTC+7 (Asia/Jakarta) timestamps in talend-lab-db."""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from dotenv import load_dotenv

load_dotenv()

WIB = ZoneInfo("Asia/Jakarta")

conn = psycopg2.connect(
    host=os.getenv("DB_HOST_POCC", "127.0.0.1"),
    port=int(os.getenv("DB_PORT_POCC", "5433")),
    dbname=os.getenv("DB_NAME_POCC", "talend_lab"),
    user=os.getenv("DB_USERNAME_POCC", "talend_user"),
    password=os.getenv("DB_PASSWORD_POCC", "TalendLab2026_TestOnly"),
)
cur = conn.cursor()

cur.execute("SHOW TIMEZONE;")
db_tz = cur.fetchone()[0]

cur.execute("SELECT LOCALTIMESTAMP(0), (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::timestamp(0);")
db_local_ts, db_wib_ts = cur.fetchone()
py_wib_ts = datetime.now(WIB).replace(tzinfo=None, microsecond=0)

cur.execute("SELECT COUNT(*) FROM stg_pocc_fleet_realization_dev;")
stg_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM mart_pocc_fleet_realization_dev;")
mart_count = cur.fetchone()[0]

cur.execute(
    "SELECT COUNT(*) FROM (SELECT schedule_code FROM mart_pocc_fleet_realization_dev GROUP BY schedule_code HAVING COUNT(*) > 1) d;"
)
dup_count = cur.fetchone()[0]

print("=== TIMEZONE & TIMESTAMP VERIFICATION (UTC+7 / Asia/Jakarta) ===")
print(f"PostgreSQL Default TimeZone : {db_tz}")
print(f"PostgreSQL LOCALTIMESTAMP   : {db_local_ts}")
print(f"PostgreSQL WIB Timestamp    : {db_wib_ts}")
print(f"Python Host WIB Timestamp   : {py_wib_ts}")

print("\n=== TABLE COUNTS ===")
print(f"Staging table count : {stg_count} (Expected: 0)")
print(f"Mart table count    : {mart_count} (Expected: 8)")
print(f"Duplicate PK count  : {dup_count} (Expected: 0)")

cur.execute("""
SELECT
    schedule_code,
    cabang,
    nama_kapal_realisasi,
    schedule_date,
    realisasi_tiba_alur,
    realisasi_sandar,
    realisasi_mulai_pelayanan,
    realisasi_selesai_pelayanan,
    realisasi_tutup_rampdoor,
    realisasi_keberangkatan,
    created_on,
    updated_on
FROM mart_pocc_fleet_realization_dev
ORDER BY realisasi_tiba_alur DESC;
""")
rows = cur.fetchall()
print("\n=== RECORDS & TIMESTAMPS IN MART TABLE (All in WIB / UTC+7) ===")
for r in rows:
    (
        code,
        cabang,
        kapal,
        sch_dt,
        tiba,
        sandar,
        mulai,
        selesai,
        ramp,
        berangkat,
        created,
        updated,
    ) = r
    print(f"\n[{code}] {cabang} - {kapal}")
    print(f"  SCHEDULE_DATE               : {sch_dt}")
    print(f"  REALISASI_TIBA_ALUR         : {tiba}")
    print(f"  REALISASI_SANDAR            : {sandar}")
    print(f"  REALISASI_MULAI_PELAYANAN   : {mulai}")
    print(f"  REALISASI_SELESAI_PELAYANAN : {selesai}")
    print(f"  REALISASI_TUTUP_RAMPDOOR    : {ramp}")
    print(f"  REALISASI_KEBERANGKATAN     : {berangkat}")
    print(f"  CREATED_ON (WIB)            : {created.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  UPDATED_ON (WIB)            : {updated.strftime('%Y-%m-%d %H:%M:%S')}")

conn.close()
