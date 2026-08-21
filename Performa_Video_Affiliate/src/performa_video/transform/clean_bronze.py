# src/performa_video/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone

import pandas as pd

from src.performa_video.utils.transform_utils import (
    # clean_numeric_columns,
    parse_mixed_dates,
    to_snake_case,
)
from src.performa_video.utils.minio_client import filter_by_sheet_watermark


# NUMERIC_COLS = [
#     "VV",
#     "Likes",
#     "Komentar",
#     "Dibagikan",
#     "Pengikut baru",
#     "Klik Video ke LIVE",
#     "Produk Dilihat",
#     "Klik Produk",
#     "Pembeli unik",
#     "Pesanan SKU teratribusi",
#     "Produk yang terjual dari video",
#     "GMV dari video (Rp)",
#     "GPM (Rp)",
# ]

def _canon(x):
    import pandas as pd

    x = "" if pd.isna(x) else str(x).strip()
    return x.upper()


def build_bronze_video(
    tiktok_video_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per sheet (creds-keyed) berdasarkan watermark (sheet_watermarks).
    Output: (df siap di-load ke BRONZE_DB.bronze_live, sheet_max_dates)
    """
    # numeric cleaning
    # tiktok_video_clean1 = clean_numeric_columns(
    #     tiktok_video_raw, NUMERIC_COLS, fillna_value=0
    # )
    tiktok_video_clean1 = tiktok_video_raw.copy()
    # parse tanggal
    tiktok_video_clean1["Tanggal"] = parse_mixed_dates(
        tiktok_video_clean1["Tanggal"], return_date=False
    )
    tiktok_video_clean1["Waktu"] = parse_mixed_dates(
        tiktok_video_clean1["Waktu"], return_date=False
    )

    # copy & snake_case
    df = tiktok_video_clean1.copy()
    df.columns = df.columns.map(to_snake_case)

    # buang baris tanpa id
    df = df[df["tanggal"].astype(str).str.strip() != ""]

    # Replace empty strings dengan None di kolom berjenis object/string
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].replace("", None)

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["tanggal", "toko", "nama_kreator", "id_video", "vv", "produk_dilihat"]

    def build_hash(row):
        # Menerapkan _canon ke setiap cell, digabung '||', lalu di-SHA256
        raw_str = "||".join([_canon(val) for val in row])
        return hashlib.sha256(raw_str.encode()).hexdigest()

    df["row_hash_raw"] = df[cols_for_hash].apply(build_hash, axis=1)

    # Filter incremental per sheet (creds-keyed) berdasarkan watermark
    if "creds" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates
