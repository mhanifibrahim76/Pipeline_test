"""Setup schemas, tables, timezone (Asia/Jakarta UTC+7), and dummy data in talend-lab-db (PostgreSQL)."""

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST_BRONZE", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT_BRONZE", "5433"))
DB_NAME = os.getenv("DB_NAME_BRONZE", "talend_lab")
DB_USER = os.getenv("DB_USERNAME_BRONZE", "talend_user")
DB_PASS = os.getenv("DB_PASSWORD_BRONZE", "TalendLab2026_TestOnly")

WIB = ZoneInfo("Asia/Jakarta")


def main():
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        options="-c timezone=Asia/Jakarta",
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("1. Configuring database & role timezone to Asia/Jakarta (UTC+7)...")
    cur.execute(f"ALTER DATABASE {DB_NAME} SET timezone TO 'Asia/Jakarta';")
    cur.execute(f"ALTER ROLE {DB_USER} SET timezone TO 'Asia/Jakarta';")
    cur.execute("SET TIME ZONE 'Asia/Jakarta';")
    cur.execute("SHOW TIMEZONE;")
    print("   Active PostgreSQL TimeZone:", cur.fetchone()[0])

    conn.autocommit = False

    print("2. Setting up schema and Oracle-compatibility views/functions...")
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS dwhbronzeuser;

    -- DUAL view for Oracle compatibility (SELECT 1 FROM DUAL)
    CREATE OR REPLACE VIEW DUAL AS SELECT 1 AS DUMMY;

    -- TRUNC(timestamp) functions for Oracle compatibility
    CREATE OR REPLACE FUNCTION trunc(ts timestamp) RETURNS timestamp LANGUAGE sql IMMUTABLE AS $$
        SELECT date_trunc('day', ts)::timestamp;
    $$;

    CREATE OR REPLACE FUNCTION trunc(ts timestamptz) RETURNS timestamptz LANGUAGE sql IMMUTABLE AS $$
        SELECT date_trunc('day', ts);
    $$;
    """)
    conn.commit()

    print("3. Recreating source tables & dynamic SYSDATE view in schema dwhbronzeuser...")
    cur.execute("""
    DO $$
    DECLARE
        r RECORD;
    BEGIN
        SELECT relkind INTO r FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE n.nspname = 'dwhbronzeuser' AND c.relname = 'brz_ferizy_t_trx_schedule';
        IF FOUND THEN
            IF r.relkind = 'v' THEN
                EXECUTE 'DROP VIEW dwhbronzeuser.brz_ferizy_t_trx_schedule CASCADE';
            ELSE
                EXECUTE 'DROP TABLE dwhbronzeuser.brz_ferizy_t_trx_schedule CASCADE';
            END IF;
        END IF;
    END $$;

    DROP TABLE IF EXISTS dwhbronzeuser._brz_ferizy_t_trx_schedule CASCADE;
    DROP TABLE IF EXISTS dwhbronzeuser.brz_ferizy_t_mtr_ship CASCADE;
    DROP TABLE IF EXISTS dwhbronzeuser.brz_ferizy_t_mtr_port CASCADE;
    DROP TABLE IF EXISTS dwhbronzeuser.brz_ferizy_t_mtr_dock CASCADE;
    DROP TABLE IF EXISTS dwhbronzeuser.brz_ferizy_t_mtr_ship_company CASCADE;
    DROP TABLE IF EXISTS dwhbronzeuser.brz_ferizy_t_mtr_ship_class CASCADE;

    CREATE TABLE dwhbronzeuser.brz_ferizy_t_mtr_port (
        id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );

    CREATE TABLE dwhbronzeuser.brz_ferizy_t_mtr_dock (
        id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );

    CREATE TABLE dwhbronzeuser.brz_ferizy_t_mtr_ship_company (
        id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );

    CREATE TABLE dwhbronzeuser.brz_ferizy_t_mtr_ship_class (
        id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );

    CREATE TABLE dwhbronzeuser.brz_ferizy_t_mtr_ship (
        id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        ship_company_id INT REFERENCES dwhbronzeuser.brz_ferizy_t_mtr_ship_company(id),
        ship_class INT REFERENCES dwhbronzeuser.brz_ferizy_t_mtr_ship_class(id)
    );

    CREATE TABLE dwhbronzeuser._brz_ferizy_t_trx_schedule (
        schedule_code VARCHAR(100) PRIMARY KEY,
        schedule_date TIMESTAMP NOT NULL,
        ship_id INT REFERENCES dwhbronzeuser.brz_ferizy_t_mtr_ship(id),
        port_id INT REFERENCES dwhbronzeuser.brz_ferizy_t_mtr_port(id),
        dock_id INT REFERENCES dwhbronzeuser.brz_ferizy_t_mtr_dock(id),
        ploting_date TIMESTAMP,
        docking_date TIMESTAMP,
        open_boarding_date TIMESTAMP,
        close_boarding_date TIMESTAMP,
        close_ramp_door_date TIMESTAMP,
        sail_date TIMESTAMP
    );

    -- Updatable view exposing dynamic SYSDATE in Asia/Jakarta (UTC+7) at query time
    CREATE OR REPLACE VIEW dwhbronzeuser.brz_ferizy_t_trx_schedule AS
    SELECT
        schedule_code,
        schedule_date,
        ship_id,
        port_id,
        dock_id,
        ploting_date,
        docking_date,
        open_boarding_date,
        close_boarding_date,
        close_ramp_door_date,
        sail_date,
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::timestamp AS sysdate
    FROM dwhbronzeuser._brz_ferizy_t_trx_schedule;
    """)
    conn.commit()

    print("4. Recreating target and staging tables in public schema...")
    cur.execute("""
    DROP TABLE IF EXISTS stg_pocc_fleet_realization_dev CASCADE;
    CREATE TABLE stg_pocc_fleet_realization_dev (
        schedule_code VARCHAR(100) PRIMARY KEY,
        schedule_date TIMESTAMP,
        cabang VARCHAR(100),
        dermaga VARCHAR(100),
        jenis_pelayanan VARCHAR(100),
        nama_kapal_realisasi VARCHAR(100),
        operator_realisasi VARCHAR(100),
        realisasi_tiba_alur TIMESTAMP,
        realisasi_sandar TIMESTAMP,
        realisasi_mulai_pelayanan TIMESTAMP,
        realisasi_selesai_pelayanan TIMESTAMP,
        realisasi_tutup_rampdoor TIMESTAMP,
        realisasi_keberangkatan TIMESTAMP
    );

    DROP TABLE IF EXISTS mart_pocc_fleet_realization_dev CASCADE;
    CREATE TABLE mart_pocc_fleet_realization_dev (
        schedule_code VARCHAR(100) PRIMARY KEY,
        schedule_date TIMESTAMP,
        cabang VARCHAR(100),
        dermaga VARCHAR(100),
        jenis_pelayanan VARCHAR(100),
        nama_kapal_realisasi VARCHAR(100),
        operator_realisasi VARCHAR(100),
        realisasi_tiba_alur TIMESTAMP,
        realisasi_sandar TIMESTAMP,
        realisasi_mulai_pelayanan TIMESTAMP,
        realisasi_selesai_pelayanan TIMESTAMP,
        realisasi_tutup_rampdoor TIMESTAMP,
        realisasi_keberangkatan TIMESTAMP,
        created_on TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'),
        updated_on TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')
    );
    """)
    conn.commit()

    print("5. Inserting master data...")
    cur.execute("""
    INSERT INTO dwhbronzeuser.brz_ferizy_t_mtr_port (id, name) VALUES
    (1, 'MERAK'),
    (2, 'BAKAUHENI'),
    (3, 'KETAPANG'),
    (4, 'GILIMANUK'),
    (5, 'SURABAYA'),
    (6, 'PADANGBAI');

    INSERT INTO dwhbronzeuser.brz_ferizy_t_mtr_dock (id, name) VALUES
    (1, 'Dermaga Eksekutif Sosoro'),
    (2, 'Dermaga Eksekutif Anjungan'),
    (3, 'Dermaga 1 Reguler'),
    (4, 'Dermaga 2 Reguler'),
    (5, 'Dermaga 3'),
    (6, 'Dermaga MB 1');

    INSERT INTO dwhbronzeuser.brz_ferizy_t_mtr_ship_company (id, name) VALUES
    (1, 'PT ASDP Indonesia Ferry (Persero)'),
    (2, 'PT Dharma Lautan Utama'),
    (3, 'PT Jembatan Nusantara'),
    (4, 'PT Munic Line');

    INSERT INTO dwhbronzeuser.brz_ferizy_t_mtr_ship_class (id, name) VALUES
    (1, 'EKSEKUTIF'),
    (2, 'REGULER');

    INSERT INTO dwhbronzeuser.brz_ferizy_t_mtr_ship (id, name, ship_company_id, ship_class) VALUES
    (101, 'KMP Portlink', 1, 1),
    (102, 'KMP Batumandi', 1, 2),
    (103, 'KMP Sebuku', 1, 1),
    (104, 'KMP Legundi', 1, 2),
    (105, 'KMP Dharma Rucitra VII', 2, 1),
    (106, 'KMP Dharma Ferry IX', 2, 2),
    (107, 'KMP Jembatan Musi II', 3, 2),
    (108, 'KMP Munic VII', 4, 2);
    """)
    conn.commit()

    print("6. Inserting dummy transactional schedule data in Asia/Jakarta (UTC+7)...")
    now_wib = datetime.now(WIB).replace(tzinfo=None, microsecond=0)
    print(f"   Reference WIB time (UTC+7): {now_wib}")

    # Create 8 valid schedules (within 30-day window, valid ports)
    # Note: ploting_date must be <= SYSDATE (now_wib) to match BETWEEN SYSDATE - 30 AND SYSDATE
    schedules = [
        # (code, hours_ago_scheduled, ship_id, port_id, dock_id)
        ("SCH-MRK-20261026-01", 4, 101, 1, 1),      # Merak Eksekutif, today (4h ago)
        ("SCH-BKH-20261026-02", 6, 102, 2, 3),      # Bakauheni Reguler, today (6h ago)
        ("SCH-MRK-20261025-03", 28, 103, 1, 1),     # Merak Eksekutif, yesterday
        ("SCH-KTP-20261023-04", 72, 105, 3, 5),     # Ketapang, 3 days ago
        ("SCH-GLM-20261018-05", 192, 106, 4, 6),    # Gilimanuk, 8 days ago
        ("SCH-MRK-20261013-06", 312, 104, 1, 4),    # Merak Reguler, 13 days ago
        ("SCH-BKH-20261008-07", 432, 107, 2, 4),    # Bakauheni, 18 days ago
        ("SCH-KTP-20261001-08", 600, 108, 3, 5),    # Ketapang, 25 days ago
    ]

    for code, hours_ago, ship_id, port_id, dock_id in schedules:
        sch_time = now_wib - timedelta(hours=hours_ago)
        ploting = sch_time + timedelta(minutes=30)
        docking = sch_time + timedelta(minutes=45)
        open_b = sch_time + timedelta(minutes=60)
        close_b = sch_time + timedelta(minutes=110)
        close_r = sch_time + timedelta(minutes=115)
        sail = sch_time + timedelta(minutes=125)

        cur.execute("""
        INSERT INTO dwhbronzeuser._brz_ferizy_t_trx_schedule (
            schedule_code, schedule_date, ship_id, port_id, dock_id,
            ploting_date, docking_date, open_boarding_date, close_boarding_date,
            close_ramp_door_date, sail_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            code, sch_time, ship_id, port_id, dock_id,
            ploting, docking, open_b, close_b, close_r, sail
        ))

    # Add 1 schedule older than 30 days (should be excluded)
    old_time = now_wib - timedelta(days=45)
    cur.execute("""
    INSERT INTO dwhbronzeuser._brz_ferizy_t_trx_schedule (
        schedule_code, schedule_date, ship_id, port_id, dock_id,
        ploting_date, docking_date, open_boarding_date, close_boarding_date,
        close_ramp_door_date, sail_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        "SCH-OLD-EXCLUDED-01", old_time, 101, 1, 1,
        old_time, old_time, old_time, old_time, old_time, old_time
    ))

    # Add 1 schedule at excluded port (Surabaya)
    sby_time = now_wib - timedelta(days=2)
    cur.execute("""
    INSERT INTO dwhbronzeuser._brz_ferizy_t_trx_schedule (
        schedule_code, schedule_date, ship_id, port_id, dock_id,
        ploting_date, docking_date, open_boarding_date, close_boarding_date,
        close_ramp_door_date, sail_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        "SCH-SBY-EXCLUDED-02", sby_time, 101, 5, 1,
        sby_time, sby_time, sby_time, sby_time, sby_time, sby_time
    ))

    conn.commit()

    cur.execute("SELECT COUNT(*), MIN(sysdate) FROM dwhbronzeuser.brz_ferizy_t_trx_schedule;")
    total_trx, db_sysdate = cur.fetchone()
    print(f"Database setup complete! Total schedule records: {total_trx} (8 valid, 2 excluded).")
    print(f"Database dynamic SYSDATE (UTC+7): {db_sysdate}")

    conn.close()


if __name__ == "__main__":
    main()
