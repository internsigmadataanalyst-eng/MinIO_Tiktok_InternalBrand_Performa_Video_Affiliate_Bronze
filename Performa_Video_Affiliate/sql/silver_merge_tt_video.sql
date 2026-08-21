MERGE INTO `database-sigma.Testing.silver_tt_video_aff_internal` T
USING (
  -- ambil snapshot terbaru per (toko, id_kreator, id_video, tanggal)
  WITH latest_raw AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT b.*,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(TRIM(b.toko)),
                       UPPER(TRIM(COALESCE(b.id_kreator,''))),
                       UPPER(TRIM(COALESCE(b.id_video,''))),
                       DATE(b.tanggal)
          ORDER BY b.snapshot_ts DESC, b.run_id DESC
        ) rn
      FROM `database-sigma.Testing.bronze_video_aff_internal` b
    )
    WHERE rn = 1
  ),

  base AS (
    SELECT
      DATE(tanggal)                   AS tanggal,
      UPPER(TRIM(toko))               AS toko,
      UPPER(TRIM(nama_kreator))       AS nama_kreator,
      UPPER(TRIM(id_kreator))         AS id_kreator,
      UPPER(TRIM(informasi_video))    AS informasi_video,
      UPPER(TRIM(id_video))           AS id_video,
      waktu,                          -- sudah DATETIME di bronze
      UPPER(TRIM(produk))             AS produk,

      -- metrik integer -> INT64 / uang -> NUMERIC
      SAFE_CAST(vv AS INT64)                                         AS vv,
      SAFE_CAST(likes AS INT64)                                      AS likes,
      SAFE_CAST(komentar AS INT64)                                   AS komentar,
      SAFE_CAST(dibagikan AS INT64)                                  AS dibagikan,
      SAFE_CAST(pengikut_baru AS INT64)                              AS pengikut_baru,
      SAFE_CAST(klik_video_ke_live AS INT64)                         AS klik_video_ke_live,
      SAFE_CAST(produk_dilihat AS INT64)                             AS produk_dilihat,
      SAFE_CAST(klik_produk AS INT64)                                AS klik_produk,
      SAFE_CAST(pembeli_unik AS INT64)                               AS pembeli_unik,
      SAFE_CAST(pesanan_sku_teratribusi AS INT64)                    AS pesanan_sku_teratribusi,
      SAFE_CAST(pesanan_sku_dari_video AS INT64)                     AS pesanan_sku_dari_video,
      SAFE_CAST(pesanan_sku_tidak_langsung_dari_video AS INT64)      AS pesanan_sku_tidak_langsung_dari_video,
      SAFE_CAST(produk_yang_terjual_melalui_video AS INT64)          AS produk_yang_terjual_melalui_video,
      SAFE_CAST(produk_yang_terjual_dari_video AS INT64)             AS produk_yang_terjual_dari_video,
      SAFE_CAST(produk_yang_terjual_dari_video_secara_tidak_langsung AS INT64)
                                                                      AS produk_yang_terjual_dari_video_secara_tidak_langsung,

      SAFE_CAST(gmv_dari_video_rp AS NUMERIC)                        AS gmv_dari_video,
      SAFE_CAST(gmv_video_rp AS NUMERIC)                             AS gmv_video,
      SAFE_CAST(gmv_tidak_langsung_dari_video_rp AS NUMERIC)         AS gmv_tidak_langsung_dari_video,
      SAFE_CAST(gpm_rp AS NUMERIC)                                   AS gpm,

      -- persentase string "12.3%" -> 0.123
      SAFE_CAST(REGEXP_REPLACE(rasio_klik_tayang_video, r'[%\s]', '') AS FLOAT64)/100
                                                                      AS ctr_video,
      SAFE_CAST(REGEXP_REPLACE(rasio_video_ke_live, r'[%\s]', '') AS FLOAT64)/100
                                                                      AS ratio_video_ke_live,
      SAFE_CAST(REGEXP_REPLACE(persentase_video_yang_ditonton_hingga_selesai, r'[%\s]', '') AS FLOAT64)/100
                                                                      AS pct_tonton_selesai,
      SAFE_CAST(REGEXP_REPLACE(ctor_pesanan_sku, r'[%\s]', '') AS FLOAT64)/100
                                                                      AS ctor_pesanan_sku,

      diagnosis,

      snapshot_ts, snapshot_date, run_id, row_hash_raw
    FROM latest_raw
  ),

  with_hash AS (
    SELECT
      b.*,
      TO_HEX(
        SHA256(
          ARRAY_TO_STRING([
            FORMAT_DATE('%F', b.tanggal),
            b.toko, COALESCE(b.id_kreator,''), COALESCE(b.id_video,''),
            COALESCE(b.nama_kreator,''), COALESCE(b.informasi_video,''), COALESCE(b.produk,''),
            CAST(b.waktu AS STRING),
            CAST(b.vv AS STRING), CAST(b.likes AS STRING), CAST(b.komentar AS STRING), CAST(b.dibagikan AS STRING),
            CAST(b.pengikut_baru AS STRING), CAST(b.klik_video_ke_live AS STRING),
            CAST(b.produk_dilihat AS STRING), CAST(b.klik_produk AS STRING),
            CAST(b.pembeli_unik AS STRING),
            CAST(b.pesanan_sku_teratribusi AS STRING),
            CAST(b.pesanan_sku_dari_video AS STRING),
            CAST(b.pesanan_sku_tidak_langsung_dari_video AS STRING),
            CAST(b.produk_yang_terjual_melalui_video AS STRING),
            CAST(b.produk_yang_terjual_dari_video AS STRING),
            CAST(b.produk_yang_terjual_dari_video_secara_tidak_langsung AS STRING),
            CAST(b.gmv_dari_video AS STRING),
            CAST(b.gmv_video AS STRING),
            CAST(b.gmv_tidak_langsung_dari_video AS STRING),
            CAST(b.gpm AS STRING),
            CAST(b.ctr_video AS STRING),
            CAST(b.ratio_video_ke_live AS STRING),
            CAST(b.pct_tonton_selesai AS STRING),
            CAST(b.ctor_pesanan_sku AS STRING),
            COALESCE(b.diagnosis,'')
          ], '||', '')
        )
      ) AS row_hash_clean
    FROM base b
  )
  SELECT * FROM with_hash
) S
ON  T.tanggal    = S.tanggal
AND T.toko       = S.toko
AND COALESCE(T.id_kreator,'') = COALESCE(S.id_kreator,'')
AND COALESCE(T.id_video  ,'') = COALESCE(S.id_video  ,'')
WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    nama_kreator = S.nama_kreator,
    informasi_video = S.informasi_video,
    waktu = S.waktu,
    produk = S.produk,
    vv = S.vv, likes = S.likes, komentar = S.komentar, dibagikan = S.dibagikan,
    pengikut_baru = S.pengikut_baru, klik_video_ke_live = S.klik_video_ke_live,
    produk_dilihat = S.produk_dilihat, klik_produk = S.klik_produk,
    pembeli_unik = S.pembeli_unik,
    pesanan_sku_teratribusi = S.pesanan_sku_teratribusi,
    pesanan_sku_dari_video = S.pesanan_sku_dari_video,
    pesanan_sku_tidak_langsung_dari_video = S.pesanan_sku_tidak_langsung_dari_video,
    produk_yang_terjual_melalui_video = S.produk_yang_terjual_melalui_video,
    produk_yang_terjual_dari_video = S.produk_yang_terjual_dari_video,
    produk_yang_terjual_dari_video_secara_tidak_langsung = S.produk_yang_terjual_dari_video_secara_tidak_langsung,
    gmv_dari_video = S.gmv_dari_video,
    gmv_video = S.gmv_video,
    gmv_tidak_langsung_dari_video = S.gmv_tidak_langsung_dari_video,
    gpm = S.gpm,
    ctr_video = S.ctr_video,
    ratio_video_ke_live = S.ratio_video_ke_live,
    pct_tonton_selesai = S.pct_tonton_selesai,
    ctor_pesanan_sku = S.ctor_pesanan_sku,
    diagnosis = S.diagnosis,
    snapshot_ts = S.snapshot_ts, snapshot_date = S.snapshot_date, run_id = S.run_id,
    row_hash_raw = S.row_hash_raw, row_hash_clean = S.row_hash_clean
WHEN NOT MATCHED THEN
  INSERT (
    tanggal, toko, nama_kreator, id_kreator, informasi_video, id_video, waktu, produk,
    vv, likes, komentar, dibagikan, pengikut_baru, klik_video_ke_live,
    produk_dilihat, klik_produk, pembeli_unik,
    pesanan_sku_teratribusi, pesanan_sku_dari_video, pesanan_sku_tidak_langsung_dari_video,
    produk_yang_terjual_melalui_video, produk_yang_terjual_dari_video,
    produk_yang_terjual_dari_video_secara_tidak_langsung,
    gmv_dari_video, gmv_video, gmv_tidak_langsung_dari_video,
    gpm,
    ctr_video, ratio_video_ke_live, pct_tonton_selesai, ctor_pesanan_sku,
    diagnosis,
    snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
  )
  VALUES (
    S.tanggal, S.toko, S.nama_kreator, S.id_kreator, S.informasi_video, S.id_video, S.waktu, S.produk,
    S.vv, S.likes, S.komentar, S.dibagikan, S.pengikut_baru, S.klik_video_ke_live,
    S.produk_dilihat, S.klik_produk, S.pembeli_unik,
    S.pesanan_sku_teratribusi, S.pesanan_sku_dari_video, S.pesanan_sku_tidak_langsung_dari_video,
    S.produk_yang_terjual_melalui_video, S.produk_yang_terjual_dari_video,
    S.produk_yang_terjual_dari_video_secara_tidak_langsung,
    S.gmv_dari_video, S.gmv_video, S.gmv_tidak_langsung_dari_video,
    S.gpm,
    S.ctr_video, S.ratio_video_ke_live, S.pct_tonton_selesai, S.ctor_pesanan_sku,
    S.diagnosis,
    S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
  );