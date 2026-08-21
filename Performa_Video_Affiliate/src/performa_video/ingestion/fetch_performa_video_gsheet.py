# src/performa_video/ingestion/fetch_performa_video_gsheet.py
import os

import gspread
import pandas as pd


SHEET_REGISTRY = {
    "tmb": "SH_KEY_TMB",
}


def fetch_tiktok_video(gc: gspread.Client) -> pd.DataFrame:
    """
    Ambil performa video dari GSheet yang terdaftar di SHEET_REGISTRY,
    tag tiap sheet dengan kolom 'creds' & 'sheet_name', lalu concat jadi satu
    DataFrame raw (belum dibersihkan).
    """
    frames = []
    for sheet_name, env_key in SHEET_REGISTRY.items():
        sh = gc.open_by_key(os.getenv(env_key))
        ws = sh.worksheet("Performa Video Affiliate")
        values = ws.get_all_values()
        df_sheet = pd.DataFrame(values[3:], columns=values[2])
        df_sheet = df_sheet.loc[:, ~df_sheet.columns.duplicated()]
        blank_cols = [
            c for c in df_sheet.columns
            if not (isinstance(c, str) and c.strip())
        ]
        if blank_cols:
            print(f"[INGEST] Dropping blank-named columns: {len(blank_cols)}")
            df_sheet = df_sheet.drop(columns=blank_cols)
        df_sheet["creds"] = os.getenv(env_key)
        df_sheet["sheet_name"] = sheet_name
        frames.append(df_sheet)
        print(f"[INGEST] {sheet_name}: {len(df_sheet)} rows")

    tiktok_video = pd.concat(frames, ignore_index=True)
    return tiktok_video
