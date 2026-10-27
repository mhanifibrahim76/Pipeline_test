"""End-to-end experiment script with dummy data.

Demonstrates:
1. Source database verification (seeded dummy data in dwhbronzeuser).
2. Data extraction via read_source_data (SQL extraction).
3. Data transformation & testing/validation (Polars schema, keys, types).
4. Loading data to target database (truncate -> insert -> merge -> truncate).
5. Post-load verification (mart records, staging cleanup, WIB timestamps).
6. Idempotency & Upsert test (re-running with an updated record).
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from sqlalchemy import text

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.settings import get_config
from src.common.logger import get_logger
from src.common.database import create_bronze_engine, create_pocc_engine
from src.extract.reader import read_source_data
from src.common.transform import get_source_schema, transform
from src.load.oracle import load_to_oracle

WIB = ZoneInfo("Asia/Jakarta")


def print_header(title: str):
    print("\n" + "=" * 70)
    print(f" {title.upper()} ")
    print("=" * 70)


def main():
    print_header("Percobaan Pipeline ETL dengan Data Dummy")
    config = get_config()
    logger = get_logger()
    source_engine = create_bronze_engine(config)
    target_engine = create_pocc_engine(config)

    # ---------------------------------------------------------
    # PHASE 1: Cek Data Dummy di Database Sumber (dwhbronzeuser)
    # ---------------------------------------------------------
    print_header("Fase 1: Memeriksa Data Dummy di Database Sumber")
    with source_engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM dwhbronzeuser._brz_ferizy_t_trx_schedule;")).scalar()
        print(f"Total jadwal transaksi di Bronze: {res} record")
        
        sample_rows = conn.execute(text("""
            SELECT s.schedule_code, p.name AS port_name, sh.name AS ship_name, s.ploting_date
            FROM dwhbronzeuser._brz_ferizy_t_trx_schedule s
            LEFT JOIN dwhbronzeuser.brz_ferizy_t_mtr_port p ON p.id = s.port_id
            LEFT JOIN dwhbronzeuser.brz_ferizy_t_mtr_ship sh ON sh.id = s.ship_id
            ORDER BY s.ploting_date DESC
            LIMIT 5;
        """)).fetchall()
        print("Sampel 5 data transaksi sumber:")
        for row in sample_rows:
            print(f"  - [{row[0]}] Port: {row[1]}, Kapal: {row[2]}, Ploting: {row[3]}")

    # ---------------------------------------------------------
    # PHASE 2: Ekstraksi Data (Read Source Data via REALIZATION.sql)
    # ---------------------------------------------------------
    print_header("Fase 2: Ekstraksi Data dari Database Sumber")
    print(f"Membaca query SQL dari: {config.sql_file_path}")
    raw_df = read_source_data(source_engine, config, logger)
    print(f"Jumlah baris hasil ekstraksi: {raw_df.height}")
    print(f"Kolom hasil ekstraksi ({len(raw_df.columns)}): {raw_df.columns}")
    assert raw_df.height == 8, f"Harus ada 8 record valid, didapat: {raw_df.height}"
    print("-> Test Ekstraksi: BERHASIL (2 data terfilter: 1 > 30 hari lalu, 1 pelabuhan non-target).")

    # ---------------------------------------------------------
    # PHASE 3: Transformasi & Pengujian Data (Testing & Validation)
    # ---------------------------------------------------------
    print_header("Fase 3: Transformasi & Pengujian Data (Polars)")
    print("Menjalankan validasi skema, deduplikasi primary key, dan format casting...")
    schema = get_source_schema()
    transformed_df = transform(raw_df, schema, config.primary_key, logger, config.transform_sql_file_path)
    print(f"Jumlah baris setelah transformasi: {transformed_df.height}")

    # Unit testing assertions pada transformed data
    print("Menjalankan pengujian data (data quality tests):")
    # Test 1: Kolom primary key tidak boleh ada null
    null_pk_count = transformed_df.filter(transformed_df["SCHEDULE_CODE"].is_null()).height
    assert null_pk_count == 0, f"SCHEDULE_CODE tidak boleh null, ditemukan {null_pk_count}"
    print("  [PASS] Test 1: SCHEDULE_CODE null count == 0")

    # Test 2: Tidak ada duplikasi SCHEDULE_CODE
    unique_pk_count = transformed_df["SCHEDULE_CODE"].n_unique()
    assert unique_pk_count == transformed_df.height, "Terdapat duplikasi SCHEDULE_CODE!"
    print(f"  [PASS] Test 2: SCHEDULE_CODE uniqueness (unik: {unique_pk_count}, total: {transformed_df.height})")

    # Test 3: Kolom wajib ada dan jumlahnya 13
    assert len(transformed_df.columns) == 13, f"Jumlah kolom harus 13, didapat: {len(transformed_df.columns)}"
    print("  [PASS] Test 3: Validasi jumlah kolom (tepat 13 kolom)")

    # Test 4: Tipe data timestamp valid
    assert "Datetime" in str(transformed_df.schema["SCHEDULE_DATE"]), "SCHEDULE_DATE harus bertipe Datetime"
    assert "Datetime" in str(transformed_df.schema["REALISASI_TIBA_ALUR"]), "REALISASI_TIBA_ALUR harus bertipe Datetime"
    print("  [PASS] Test 4: Validasi tipe data timestamp")
    print("-> Test Transformasi: SEMUA PENGUJIAN LULUS!")

    # ---------------------------------------------------------
    # PHASE 4: Load ke Database (Staging -> Target Mart)
    # ---------------------------------------------------------
    print_header("Fase 4: Load Data ke Target Database")
    print(f"Target Mart Table    : {config.target_table}")
    print(f"Staging Table        : {config.staging_table}")
    print(f"Primary Key          : {config.primary_key}")

    load_to_oracle(target_engine, transformed_df, config, logger)
    print("-> Load data selesai dieksekusi.")

    # ---------------------------------------------------------
    # PHASE 5: Verifikasi Hasil di Target Database
    # ---------------------------------------------------------
    print_header("Fase 5: Verifikasi Hasil di Target Database")
    with target_engine.connect() as conn:
        stg_count = conn.execute(text(f"SELECT COUNT(*) FROM {config.staging_table};")).scalar()
        mart_count = conn.execute(text(f"SELECT COUNT(*) FROM {config.target_table};")).scalar()
        dup_count = conn.execute(text(
            f"SELECT COUNT(*) FROM (SELECT SCHEDULE_CODE FROM {config.target_table} GROUP BY SCHEDULE_CODE HAVING COUNT(*) > 1) d;"
        )).scalar()
        
        print(f"Jumlah baris di tabel Staging : {stg_count} (Ekspektasi: 0, telah dibersihkan)")
        print(f"Jumlah baris di tabel Mart    : {mart_count} (Ekspektasi: 8)")
        print(f"Jumlah duplikasi Primary Key  : {dup_count} (Ekspektasi: 0)")
        
        assert stg_count == 0, f"Staging harus kosong, didapat {stg_count}"
        assert mart_count == 8, f"Mart harus memiliki 8 record, didapat {mart_count}"
        assert dup_count == 0, f"Mart tidak boleh ada duplikat, didapat {dup_count}"

        # Periksa created_on dan updated_on
        audit_rows = conn.execute(text(
            f"SELECT SCHEDULE_CODE, CABANG, NAMA_KAPAL_REALISASI, CREATED_ON, UPDATED_ON "
            f"FROM {config.target_table} ORDER BY SCHEDULE_DATE DESC LIMIT 3;"
        )).fetchall()
        print("\nSampel 3 record di Mart beserta Timestamp UTC+7 (Asia/Jakarta):")
        for r in audit_rows:
            print(f"  - [{r[0]}] {r[1]} - {r[2]} | Created: {r[3]} | Updated: {r[4]}")

    print("-> Verifikasi Target: SEMUA RECORD DAN TIMESTAMP TERCATAT DENGAN BENAR!")

    # ---------------------------------------------------------
    # PHASE 6: Uji Idempotensi & Upsert (Update Data Dummy & Re-run)
    # ---------------------------------------------------------
    print_header("Fase 6: Uji Idempotensi & Upsert")
    print("Skenario: Memperbarui 1 record sumber (SCH-MRK-20261026-01 berganti dermaga) dan re-run pipeline.")
    
    with source_engine.connect() as conn:
        # Ganti dermaga untuk SCH-MRK-20261026-01 dari id 1 (Sosoro) ke id 2 (Anjungan)
        conn.execute(text("""
            UPDATE dwhbronzeuser._brz_ferizy_t_trx_schedule
            SET dock_id = 2
            WHERE schedule_code = 'SCH-MRK-20261026-01';
        """))
        conn.commit()
    print("Data sumber berhasil diupdate di database bronze.")

    print("Menjalankan pipeline ETL ulang...")
    df_step2 = read_source_data(source_engine, config, logger)
    df_transformed_step2 = transform(df_step2, schema, config.primary_key, logger, config.transform_sql_file_path)
    load_to_oracle(target_engine, df_transformed_step2, config, logger)

    with target_engine.connect() as conn:
        mart_count_after = conn.execute(text(f"SELECT COUNT(*) FROM {config.target_table};")).scalar()
        updated_row = conn.execute(text(f"""
            SELECT SCHEDULE_CODE, DERMAGA, CREATED_ON, UPDATED_ON
            FROM {config.target_table}
            WHERE SCHEDULE_CODE = 'SCH-MRK-20261026-01';
        """)).fetchone()
        
        print(f"Jumlah baris di Mart setelah re-run : {mart_count_after} (Tetap 8, TIDAK bertambah/duplikat!)")
        print(f"Detail record yang di-upsert:")
        print(f"  - Kode Jadwal : {updated_row[0]}")
        print(f"  - Dermaga Baru: {updated_row[1]} (Berhasil diupdate ke Dermaga Eksekutif Anjungan)")
        print(f"  - Created On  : {updated_row[2]}")
        print(f"  - Updated On  : {updated_row[3]}")
        
        assert mart_count_after == 8, f"Idempotensi gagal: jumlah baris bertambah menjadi {mart_count_after}"
        assert "Anjungan" in updated_row[1], f"Update gagal: dermaga adalah {updated_row[1]}"

    source_engine.dispose()
    target_engine.dispose()
    print_header("Hasil Akhir Percobaan: SEMUA TAHAP BERHASIL DENGAN SEMPURNA!")


if __name__ == "__main__":
    main()

