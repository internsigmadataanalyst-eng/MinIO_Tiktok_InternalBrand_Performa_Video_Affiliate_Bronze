# src/performa_video/utils/transform_utils.py
import numpy as np
import pandas as pd

EXCEL_EPOCH = pd.Timestamp("1899-12-30")


def to_snake_case(column_name: str) -> str:
    return (
        column_name.lower()
        .strip()
        .replace(" ", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "")
        .replace("/", "")
        .replace("-", "")
    )


def clean_numeric_columns(df: pd.DataFrame, cols, fillna_value=0) -> pd.DataFrame:
    df = df.copy()

    for col in cols:
        if col not in df.columns:
            print(f"Kolom '{col}' tidak ditemukan di DataFrame. Lewati Nggih.")
            continue

        df[col] = df[col].astype(str)
        df[col] = df[col].replace("-", np.nan)
        df[col] = df[col].str.replace(r"[^\d,\.]", "", regex=True)
        df[col] = df[col].str.replace(".", "", regex=False)
        df[col] = df[col].str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(fillna_value)

        if (df[col] % 1 == 0).all():
            df[col] = df[col].astype(int)

    return df

def parse_mixed_dates(series: pd.Series, return_date=True) -> pd.Series:
    s = series.astype("string").str.strip()  # ADDED: lebih aman dari astype(str)

    s = s.replace({
        "": np.nan,
        "-": np.nan,
        "nan": np.nan,
        "None": np.nan,
        "NaT": np.nan,  # ADDED
    })

    # hapus whitespace tersembunyi Google Sheet
    s = s.str.replace("\u00a0", " ", regex=False).str.strip()

    # Normalisasi pemisah
    s_norm = s.str.replace(r"[-\.]", "/", regex=True)

    # yyyy/mm/dd
    mask_ymd = s_norm.str.match(
        r"^\s*\d{4}/\d{1,2}/\d{1,2}\s*$",
        na=False
    )

    ymd = pd.to_datetime(
        s_norm.where(mask_ymd),
        format="%Y/%m/%d",
        errors="coerce"
    )

    mask_ymd_datetime = s.str.match(
        r"^\d{4}/\d{1,2}/\d{1,2}\s+\d{1,2}:\d{2}:\d{2}$",
        na=False
    )

    ymd_datetime = pd.to_datetime(
        s.where(mask_ymd_datetime),
        format="%Y/%m/%d %H:%M:%S",
        errors="coerce"
    )

    # dd/mm/yyyy
    mask_dmy4 = s_norm.str.match(
        r"^\s*\d{1,2}/\d{1,2}/\d{4}\s*$",
        na=False
    )

    dmy4 = pd.to_datetime(
        s_norm.where(mask_dmy4),
        format="%d/%m/%Y",
        errors="coerce"
    )

    # dd/mm/yy
    mask_dmy2 = s_norm.str.match(
        r"^\s*\d{1,2}/\d{1,2}/\d{2}\s*$",
        na=False
    )

    dmy2 = pd.to_datetime(
        s_norm.where(mask_dmy2),
        format="%d/%m/%y",
        errors="coerce"
    )

    # yyyy-mm-dd
    mask_iso = s.str.match(
        r"^\d{4}-\d{1,2}-\d{1,2}$",
        na=False
    )

    iso = pd.to_datetime(
        s.where(mask_iso),
        format="%Y-%m-%d",
        errors="coerce"
    )

    # yyyy-mm-dd hh:mm:ss
    mask_datetime = s.str.match(
        r"^\d{4}-\d{1,2}-\d{1,2}\s+\d{2}:\d{2}:\d{2}$",
        na=False
    )

    datetime_str = pd.to_datetime(
        s.where(mask_datetime),
        errors="coerce"
    )

    # Excel serial number
    mask_serial = s.str.match(
        r"^\d{3,6}$",
        na=False
    )

    serial_vals = pd.to_numeric(
        s.where(mask_serial),
        errors="coerce"
    )

    serial = pd.Series(
        pd.NaT,
        index=s.index,
        dtype="datetime64[ns]"
    )

    # safe serial conversion
    try:
        serial.loc[mask_serial] = (
            EXCEL_EPOCH
            + pd.to_timedelta(
                serial_vals.loc[mask_serial],
                unit="D"
            )
        )
    except Exception:
        serial = pd.Series(
            pd.NaT,
            index=s.index,
            dtype="datetime64[ns]"
        )


    # filter tanggal abnormal sebelum combine_first
    date_candidates = [
        ymd,
        ymd_datetime,
        dmy4,
        dmy2,
        iso,
        datetime_str,
        serial,
    ]

    date_candidates = [
        x.where(
            x.dt.year.between(1900, 2100)
        )
        for x in date_candidates
    ]

    (
        ymd,
        ymd_datetime,
        dmy4,
        dmy2,
        iso,
        datetime_str,
        serial
    ) = date_candidates

    # Combine hasil parsing
    parsed = (
        ymd
        .combine_first(ymd_datetime)
        .combine_first(dmy4)
        .combine_first(dmy2)
        .combine_first(iso)
        .combine_first(datetime_str)
        .combine_first(serial)
    )

    # Log gagal parsing
    failed = parsed.isna() & s.notna()

    if failed.any():
        print(
            f"[DATE PARSER] Failed parsing: {failed.sum()} rows"
        )
        print(
            s[failed]
            .drop_duplicates()
            .head(20)
            .tolist()
        )

    # Return
    if return_date:
        return parsed.dt.date

    return parsed