# src/performa_video/pipelines/run_daily_etl.py

import io
import os
from datetime import date

from dotenv import load_dotenv
from google.oauth2 import service_account

# Load variables from .env into environment
load_dotenv()

from src.performa_video.ingestion.fetch_performa_video_gsheet import (
    fetch_tiktok_video,
    SHEET_REGISTRY,
)
from src.performa_video.transform.clean_bronze import build_bronze_video
from src.performa_video.utils.bq_client import get_bq_client
from src.performa_video.utils.gsheet_client import get_gspread_client
from src.performa_video.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
)

# Import modul testing duckdb terpisah
from src.performa_video.transform.merge_silver_duckdb import test_merge_to_silver_duckdb

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/performa_video_affiliate.json"

def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def run_daily_etl():
    print("== Start ETL Performa Video Affiliate ==")

    # 1) Client
    gc = get_gspread_client()
    bq_client = get_bq_client()
    creds = _get_credentials()
    minio_client, minio_bucket = get_minio_client()

    # 2) Date key: partition pakai YYYYMMDD, nama file pakai YYYYMMDDHH
    #    (jam agar 2 run di hari yang sama menghasilkan file terpisah, tanpa overwrite).
    today_obj = date.today()
    today_key = today_obj.strftime("%Y%m%d")
    run_key = today_obj.strftime("%Y%m%d%H")

    # 3) Per-sheet watermark check
    # sheet_registry hanya dibutuhkan utk FAILSAFE migrasi format lama (sheet_name/global -> creds).
    sheet_registry = {name: os.getenv(env_key) for name, env_key in SHEET_REGISTRY.items()}
    watermark_map, watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, sheet_registry=sheet_registry
    )

    # 4) Ingest dari GSheet
    df_raw = fetch_tiktok_video(gc)
    print(f"[INGEST] Rows raw video from GSheet: {len(df_raw)}")

    # 5) Bronze: cleaning + snapshot + hash + per-sheet incremental filter
    df_bronze, sheet_max_dates = build_bronze_video(
        df_raw, sheet_watermarks=watermark_map
    )
    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")

    if df_bronze.empty:
        print("[MINIO] No new data to process. Data is up-to-date.")
        print("== ETL Performa Video Affiliate DONE ==")
        return

    # 6) Parquet conversion & Load to MinIO
    folder_path = f"performa_video/affiliate/date={today_key}/"
    file_path = f"{folder_path}affiliate_{run_key}.parquet"

    # Folder partition marker
    minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

    # Convert & Upload Parquet
    parquet_bytes = df_bronze.to_parquet(index=False, engine="pyarrow")
    minio_client.put_object(
        minio_bucket,
        file_path,
        io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    print(f"[MINIO] Load to {file_path} DONE")

    # 7) Update per-sheet watermark (selalu tulis format baru)
    update_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, watermark_records, sheet_max_dates,
        sheet_registry=sheet_registry,
    )

    # 8) Testing Load to Bronze & Silver via DuckDB (In-Memory)
    test_merge_to_silver_duckdb(df_bronze)

    print("== ETL Performa Video Affiliate DONE ==")


# Kalau kamu mau bisa juga di-run langsung:
if __name__ == "__main__":
    run_daily_etl()
