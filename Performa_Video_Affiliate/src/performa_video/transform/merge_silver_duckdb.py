import re
from pathlib import Path
import duckdb
import pandas as pd


def _transpile_bq_to_duckdb(sql_content: str) -> str:
    """Helper untuk merubah sintaks BigQuery SQL agar kompatibel dengan DuckDB di Memory."""
    # 1. Hapus prefix 'database-sigma.' & tanda backtick (`)
    sql_executable = (
        sql_content.replace("`database-sigma.", "")
        .replace("database-sigma.", "")
        .replace("`", "")
    )

    # 2. Sisipkan kata 'INTO' pada klausa MERGE
    sql_executable = re.sub(
        r"\bMERGE\s+(?!INTO\b)", "MERGE INTO ", sql_executable, flags=re.IGNORECASE
    )

    # 3. Ubah EXCEPT(...) BigQuery menjadi EXCLUDE(...) DuckDB
    sql_executable = re.sub(
        r"\bEXCEPT\s*\(", "EXCLUDE (", sql_executable, flags=re.IGNORECASE
    )

    # 4. Ubah BigQuery raw string r'...' menjadi standard string '...' DuckDB
    sql_executable = re.sub(r"\br(['\"][^'\"]*['\"])", r"\1", sql_executable)

    # 5. Hapus TO_HEX() karena SHA256() di DuckDB sudah mengembalikan HEX String
    sql_executable = re.sub(
        r"TO_HEX\s*\(\s*(SHA256\([^)]+\))\s*\)",
        r"\1",
        sql_executable,
        flags=re.IGNORECASE,
    )

    # 6. Ubah ARRAY_TO_STRING BigQuery menjadi array_to_string DuckDB
    sql_executable = re.sub(
        r"\bARRAY_TO_STRING\b",
        "array_to_string",
        sql_executable,
        flags=re.IGNORECASE,
    )

    # 7. Ubah INSERT ROW BigQuery menjadi INSERT BY NAME DuckDB (jika ada)
    sql_executable = re.sub(
        r"\bWHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s+ROW\b",
        "WHEN NOT MATCHED THEN INSERT BY NAME",
        sql_executable,
        flags=re.IGNORECASE,
    )

    # 8. Ubah fungsi & tipe data khas BigQuery
    sql_executable = re.sub(
        r"\bSAFE_CAST\b", "TRY_CAST", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bINT64\b", "BIGINT", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bFLOAT64\b", "DOUBLE", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bNUMERIC\b", "DECIMAL", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"FORMAT_DATE\(\s*'%F'\s*,\s*([^)]+)\)",
        r"STRFTIME(\1, '%Y-%m-%d')",
        sql_executable,
        flags=re.IGNORECASE,
    )

    return sql_executable


def test_merge_to_silver_duckdb(df_bronze: pd.DataFrame):
    """Menjalankan simulasi MERGE Bronze -> Silver di DuckDB In-Memory."""
    # 1. Init Connection
    con = duckdb.connect(":memory:")
    con.sql("INSTALL bigquery FROM community; LOAD bigquery;")

    # 2. Setup Bronze
    con.sql("CREATE SCHEMA IF NOT EXISTS BRONZE_DB;")
    con.sql("CREATE TABLE BRONZE_DB.bronze_video AS SELECT * FROM df_bronze")
    print("[BRONZE] Load to BRONZE_DB.bronze_video DONE")
    print("Show Sampel Data bronze_video:")
    con.sql("SELECT * FROM BRONZE_DB.bronze_video LIMIT 3").show()

    # 3. Setup Silver Schema (Kolom disesuaikan ke Snake Case lengkap)
    con.sql("CREATE SCHEMA IF NOT EXISTS SILVER_DB;")

    con.sql("""
        CREATE TABLE IF NOT EXISTS SILVER_DB.silver_tt_video (
            tanggal DATE,
            toko VARCHAR,
            nama_kreator VARCHAR,
            id_kreator VARCHAR,
            informasi_video VARCHAR,
            id_video VARCHAR,
            waktu TIMESTAMP,
            produk VARCHAR,
            vv BIGINT,
            likes BIGINT,
            komentar BIGINT,
            dibagikan BIGINT,
            pengikut_baru BIGINT,
            klik_video_ke_live BIGINT,
            produk_dilihat BIGINT,
            klik_produk BIGINT,
            pembeli_unik BIGINT,
            pesanan_sku_teratribusi BIGINT,
            pesanan_sku_dari_video BIGINT,
            pesanan_sku_tidak_langsung_dari_video BIGINT,
            produk_yang_terjual_melalui_video BIGINT,
            produk_yang_terjual_dari_video BIGINT,
            produk_yang_terjual_dari_video_secara_tidak_langsung BIGINT,
            gmv_dari_video DECIMAL,
            gmv_video DECIMAL,
            gmv_tidak_langsung_dari_video DECIMAL,
            gpm DECIMAL,
            ctr_video DOUBLE,
            ratio_video_ke_live DOUBLE,
            pct_tonton_selesai DOUBLE,
            ctor_pesanan_sku DOUBLE,
            diagnosis VARCHAR,
            snapshot_ts VARCHAR,
            snapshot_date DATE,
            run_id VARCHAR,
            row_hash_raw VARCHAR,
            row_hash_clean VARCHAR
        );
    """)



    # 4. Read & Transpile SQL
    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_video ...")
    root_dir = Path(__file__).resolve().parents[3]  # Path ke root etl-data-produk/
    sql_path = root_dir / "sql" / "silver_merge_tt_video.sql"

    sql_content = sql_path.read_text(encoding="utf-8")
    sql_executable = _transpile_bq_to_duckdb(sql_content)

    # 5. Execute MERGE Query
    try:
        con.sql(sql_executable)
        print("✅ MERGE SQL Execution Success!")
    except Exception as e:
        print(f"❌ Error saat eksekusi SQL: {e}")

    print("[SILVER] MERGE DONE")
    print("Show Sampel Data silver_tt_video:")
    con.sql("SELECT * FROM SILVER_DB.silver_tt_video LIMIT 3").show()
    con.close()