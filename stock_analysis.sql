
WITH
  /* ============================================================
    1. MASTER DATA JOIN
    Distributor × Product (filter distri brand sesuai mapping)
  ============================================================ */
  distri_product AS (
      SELECT
        d.region,
        d.distributor_company,
        d.distributor_code,
        d.distributor,
        d.brand AS distributor_brand,
        COALESCE(SAFE_CAST(d.lead_time_week AS FLOAT64), 0) AS lead_time_week,
        p.brand,
        p.sku,
        p.product_name,
        p.price_for_distri,
        p.price_for_store,
        p.category,
        p.assortment,
        p.moq,
        p.inner_pcs
      FROM gt_schema.master_distributor d
      CROSS JOIN gt_schema.master_product p
      WHERE d.status = 'Active'
        AND (
          (REGEXP_CONTAINS(UPPER(d.brand), r'G2G') AND UPPER(p.brand) = 'G2G') OR
          (REGEXP_CONTAINS(UPPER(d.brand), r'SKT') AND UPPER(p.brand) = 'SKINTIFIC') OR
          (REGEXP_CONTAINS(UPPER(d.brand), r'TPH') AND UPPER(p.brand) = 'TIMEPHORIA')
        )
      GROUP BY ALL
  ),






  /* ============================================================
    2. SELL THROUGH DATA (L3M) -- BUAT SKINTIFIC--
  ============================================================ */
      -- Aggregate ST per bulan
  monthly_summary AS (
      SELECT
        CASE --- Request penyesuaian historikal sales distri lama ke baru
          WHEN UPPER(sti.distributor_name) = 'PT OMEGA SUKSES ABADI' THEN 'PT TRIKARSA RAYA MANDIRI'
          WHEN UPPER(sti.distributor_name) = 'PT OMEGA SURYA ANUGRAH' THEN 'PT KARYAINDO PUTRA KENCANA'
          ELSE UPPER(sti.distributor_name) 
        END AS distributor_name,
        UPPER(sti.product_id) AS item_id,
        sti.brand AS brand_of,
        DATE_TRUNC(sti.calendar_date, MONTH) AS month,
        SUM(sti.quantity) AS monthly_st
      FROM `pbi_gt_dataset.fact_sell_through_all` sti
      WHERE sti.calendar_date BETWEEN '2025-07-01' AND '2025-09-30' -- L3M Loncat 1 bulan kebelakang lagi karena belom closing
      GROUP BY
        distributor_name,
        item_id,
        brand_of,
        month
  ),




  -- Total L3M ST
  final_summary AS (
      SELECT
        ms.distributor_name,
        ms.item_id,
        ms.brand_of,
        COUNT(DISTINCT CASE WHEN ms.monthly_st IS NOT NULL AND ms.monthly_st <> 0 THEN ms.month END) AS st_active_months,
        SUM(ms.monthly_st) AS total_l3m_st_qty
      FROM monthly_summary ms
      GROUP BY ms.distributor_name, ms.item_id, ms.brand_of
  ),




  -- AVG L3M by active month
  l3m_st AS (
      SELECT
        distributor_name,
        item_id,
        brand_of,
        COALESCE(total_l3m_st_qty, 0) AS total_l3m_st_qty,
        COALESCE(st_active_months, 0) AS st_active_months,
        COALESCE(total_l3m_st_qty * 1.0 / NULLIF(st_active_months, 0), 0) AS avg_am_l3m_st_qty
      FROM final_summary
  ),






  /* ============================================================
    3. LAST MONTH ST (Priority: July → June → May 2025) -- BUAT G2G--
  ============================================================ */
  max_l3m_st AS (
      SELECT
        distributor_name,
        item_id,
        brand_of,
        MAX(monthly_st) AS max_l3m_st_qty -- Despite of the name, ini MAX L3M
      FROM monthly_summary
      GROUP BY distributor_name, item_id, brand_of
  ),




  max_l6m_ish_st AS(
      SELECT
        distributor_name,
        item_id,
        brand_of,
        MAX(monthly_st) AS max_l6m_st_qty
      FROM (
          SELECT
            UPPER(sti.distributor_name) AS distributor_name,
            UPPER(sti.product_id)       AS item_id,
            sti.brand                   AS brand_of,
            DATE_TRUNC(sti.calendar_date, MONTH) AS month,
            SUM(sti.quantity)           AS monthly_st
          FROM `pbi_gt_dataset.fact_sell_through_all` sti
          WHERE sti.calendar_date BETWEEN '2025-04-01' AND '2025-06-30'
          GROUP BY distributor_name, item_id, brand_of, month
      )
      GROUP BY distributor_name, item_id, brand_of
  ),




  last_month_st AS (
      SELECT
        COALESCE(l3.distributor_name, l6.distributor_name) AS distributor_name,
        COALESCE(l3.item_id, l6.item_id)                   AS item_id,
        COALESCE(l3.brand_of, l6.brand_of)                 AS brand_of,
        CASE
          WHEN l3.max_l3m_st_qty IS NULL OR l3.max_l3m_st_qty = 0
              THEN GREATEST(l6.max_l6m_st_qty, 0)
          ELSE GREATEST(l3.max_l3m_st_qty, 0)
        END AS last_month_st_qty --Max L3M loncat 3 bulan (6 bulan kebbelakang), despite of the name
      FROM max_l3m_st l3
      FULL OUTER JOIN max_L6M_ish_st l6
        ON l3.distributor_name = l6.distributor_name
      AND l3.item_id = l6.item_id
      AND l3.brand_of = l6.brand_of
  ),






  /* ============================================================
    4. STOCK DATA -- Dari Master Stock Data
  ============================================================ */
  stock_data AS (
      SELECT
        sa.distributor,
        sa.prod_id,
        SUM(CASE WHEN tagging = 'CURRENT STOCK' THEN sa.stock ELSE 0 END) AS current_stock_qty,
        SUM(CASE WHEN tagging = 'IN TRANSIT' THEN sa.stock ELSE 0 END) AS in_transit_stock_qty,
        COALESCE(SUM(sa.stock), 0) AS total_stock
      FROM gt_schema.gt_raw_data_stock sa
      GROUP BY sa.distributor, sa.prod_id
  ),






  /* ============================================================
    5. MAIN DATASET (Join Master + ST + Stock)
  ============================================================ */
  main_data AS (
      SELECT
        dp.region,
        dp.distributor_company,
        dp.distributor_code,
        dp.distributor,
        dp.distributor_brand,
        dp.brand,
        dp.sku,
        dp.product_name,
        dp.category,
        dp.assortment,
        dp.moq,
        dp.inner_pcs,
        COALESCE(os.lifecycle_status, 'UNAVAILABLE') AS lifecycle_status,
        os.supply_control_status_gt,
        dp.price_for_distri,
        dp.price_for_store,
        COALESCE(sist.total_l3m_st_qty, 0) AS total_l3m_st_qty,
        COALESCE(sist.st_active_months, 0) AS st_active_months,
        COALESCE(sist.avg_am_l3m_st_qty, 0) AS avg_am_l3m_st_qty,
        COALESCE(sist.avg_am_l3m_st_qty, 0) * dp.price_for_store AS avg_am_l3m_st_value,
        COALESCE(lms.last_month_st_qty, 0) AS last_month_st_qty,
        COALESCE(lms.last_month_st_qty * dp.price_for_store, 0) AS last_month_st_value,
        COALESCE(sd.current_stock_qty, 0) AS current_stock_qty,
        COALESCE(sd.in_transit_stock_qty, 0) AS in_transit_stock_qty,
        COALESCE(sd.total_stock, 0) AS total_stock,
        COALESCE(sd.current_stock_qty, 0) * dp.price_for_distri AS current_stock_value,
        COALESCE(sd.in_transit_stock_qty, 0) * dp.price_for_distri AS in_transit_stock_value,
        COALESCE(sd.total_stock, 0) * dp.price_for_distri AS total_stock_value,
        pw.rank,
        pw.woi_standard + dp.lead_time_week AS woi_standard -- WOI SKU + Lead Time Distributor
      FROM distri_product dp
      LEFT JOIN gt_schema.master_offline_stock os
        ON TRIM(UPPER(dp.sku)) = TRIM(UPPER(os.sku))
      LEFT JOIN l3m_st sist
        ON TRIM(UPPER(dp.distributor)) = TRIM(UPPER(sist.distributor_name))
      AND TRIM(UPPER(dp.sku)) = TRIM(UPPER(sist.item_id))
      LEFT JOIN stock_data sd
        ON UPPER(dp.distributor) = UPPER(sd.distributor)
      AND UPPER(dp.sku) = UPPER(sd.prod_id)
      LEFT JOIN gt_schema.master_product_woi_v pw
        ON UPPER(dp.sku) = UPPER(pw.sku)
      LEFT JOIN last_month_st lms
        ON TRIM(UPPER(dp.distributor)) = TRIM(UPPER(lms.distributor_name))
      AND TRIM(UPPER(dp.sku)) = TRIM(UPPER(lms.item_id))
  ),






  /* ============================================================
    6. AVERAGE DATA (SELL THROUGH) L3M&LM
  ============================================================ */
    avg_data AS (
      SELECT *,
        SAFE_DIVIDE(COALESCE(avg_am_l3m_st_qty, 0), 30) * 7 AS avg_weekly_st_am_l3m_qty,
        SAFE_DIVIDE(COALESCE(last_month_st_qty, 0), 30) *7 AS avg_weekly_st_lm_qty
      FROM main_data
    ),




    avg_data_final AS (
      SELECT *,
        avg_weekly_st_am_l3m_qty * price_for_store AS avg_weekly_st_am_l3m_val,
        avg_data.avg_weekly_st_lm_qty * price_for_store AS avg_weekly_st_lm_val,
        CASE
          WHEN avg_weekly_st_am_l3m_qty = 0 THEN 0
          ELSE SAFE_DIVIDE(total_stock, NULLIF(avg_weekly_st_am_l3m_qty, 0))
        END AS current_woi_by_am_l3m,
        CASE
          WHEN avg_weekly_st_lm_qty = 0 THEN 0
          ELSE SAFE_DIVIDE(total_stock, NULLIF(avg_weekly_st_lm_qty, 0))
        END AS current_woi_by_lm
      FROM avg_data
    ),






  /* ============================================================
    7. TARGET DATA SI & PO MTD (SELL IN)
  ============================================================ */
  target_data AS (
      SELECT
        distributor,
        skintific_target,
        g2g_target,
        timephoria_target
      FROM gt_schema.fact_gt_target_v2_t
      WHERE UPPER(type_subject) = 'SELL IN'
        AND DATE_TRUNC(CAST(calendar_date AS DATE), MONTH) =
              /*dynamic_date*/
        DATE_TRUNC(CURRENT_DATE(), MONTH) -- Current month
              /*backdate*/
        -- DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) -- Last month
  ),




  -- Mapping target per brand
  target_data_j AS (
      SELECT
        df.*,
        CASE
          WHEN UPPER(df.brand) = 'SKINTIFIC'  THEN td.skintific_target
          WHEN UPPER(df.brand) = 'G2G'        THEN td.g2g_target
          WHEN UPPER(df.brand) = 'TIMEPHORIA' THEN td.timephoria_target
          ELSE NULL
        END AS target_si
      FROM avg_data_final df
      LEFT JOIN target_data td
        ON TRIM(UPPER(td.distributor)) = TRIM(UPPER(df.distributor))
  ),


  -- PO MTD tiap Branch Distributor
  po_mtd AS (
      SELECT
        po.distributor_name,
        po.brand,
        SUM(COALESCE(po.order_qty, 0) * COALESCE(pr.price_for_distri, 0)) AS order_val
      FROM dms.gt_po_tracking_mtd_mv po
      LEFT JOIN gt_schema.master_product pr
        ON po.sku = pr.sku
      GROUP BY po.distributor_name, po.brand
  ),


  -- Target SI - PO MTD
  target_remaining AS (
      SELECT
        td.*,
        GREATEST(td.target_si - COALESCE(po.order_val, 0), 0) AS target_remaining
      FROM target_data_j td
      LEFT JOIN po_mtd po
        ON td.distributor = po.distributor_name
      AND td.brand = po.brand
  ),



  /* ============================================================
    8. REDUCTION OF CURRENT STOCK WITH SALES FROM THE STOCK DATE TO MTD
  ============================================================ */



  -- 1) Tanggal stok terakhir per distributor
  last_stock_date AS (
    SELECT
      TRIM(UPPER(sa.distributor)) AS distributor,
      MAX(PARSE_DATE('%Y-%m-%d', sa.date)) AS last_stock_date
    FROM gt_schema.gt_raw_data_stock sa
    WHERE sa.date IS NOT NULL
      AND UPPER(sa.tagging) = 'CURRENT STOCK'
    GROUP BY TRIM(UPPER(sa.distributor))
  ),


  -- 2) Tanggal stok terakhir nasional (semua distributor) - (buat dipake kalo ada stock yang ga bertanggal)
  national_last_stock_date AS (
    SELECT
      MAX(PARSE_DATE('%Y-%m-%d', sa.date)) AS national_last_stock_date
    FROM gt_schema.gt_raw_data_stock sa
    WHERE sa.date IS NOT NULL
      AND UPPER(sa.tagging) = 'CURRENT STOCK'
  ),


-- 3) Agregasi Sell-Through - G2G untuk pengurangan
st_since_last_stock AS (
  SELECT
    TRIM(UPPER(t.distributor_name)) AS distributor,
    TRIM(UPPER(t.product_id)) AS sku,
    SUM(t.quantity) AS st_since_stock_date
  FROM `pbi_gt_dataset.fact_sell_through_all` t
  JOIN last_stock_date s
    ON TRIM(UPPER(t.distributor_name)) = s.distributor
  WHERE
    t.calendar_date > s.last_stock_date
    AND t.calendar_date <= CURRENT_DATE("Asia/Jakarta")
    AND t.brand = 'G2G'
  GROUP BY
    distributor,
    sku
),

-- 4) Gabungkan hasil agregasi ke data utama
g2g_stock_adjust AS (
  SELECT
    tr.* EXCEPT (current_stock_qty, current_stock_value),
    tr.current_stock_qty - COALESCE(ss.st_since_stock_date, 0) AS current_stock_qty,
    (tr.current_stock_qty - COALESCE(ss.st_since_stock_date, 0)) * tr.price_for_distri AS current_stock_value,
    tr.current_stock_qty AS ori_current_stock_qty,
    tr.current_stock_value AS ori_current_stock_value,
    COALESCE(ss.st_since_stock_date, 0) AS st_since_stock_date,
    COALESCE(lsd.last_stock_date, nlsd.national_last_stock_date) AS used_stock_date
  FROM target_remaining tr
  LEFT JOIN st_since_last_stock ss
    ON TRIM(UPPER(tr.distributor)) = ss.distributor
    AND TRIM(UPPER(tr.sku)) = ss.sku
  LEFT JOIN last_stock_date lsd
    ON TRIM(UPPER(tr.distributor)) = lsd.distributor
  LEFT JOIN national_last_stock_date nlsd ON TRUE
),


  /* ============================================================
    9. BUFFER PLAN AWAL (QTY) -- biar SKU healty--
  ============================================================ */
  buffer_data AS (
      SELECT
        tdj.*,


        -- Versi AM L3M
        COALESCE(CASE
          WHEN TRIM(UPPER(supply_control_status_gt)) = 'DISCONTINUED'
              OR TRIM(UPPER(supply_control_status_gt)) = 'STOP PO'
              OR TRIM(UPPER(supply_control_status_gt)) = 'OOS'
              OR current_woi_by_am_l3m >= woi_standard
              OR woi_standard IS NULL OR woi_standard = 0
              OR avg_weekly_st_am_l3m_qty = 0
            THEN 0
          WHEN avg_weekly_st_am_l3m_qty * woi_standard - GREATEST(total_stock,0) > 0
            THEN CEIL((avg_weekly_st_am_l3m_qty * woi_standard) - GREATEST(total_stock,0))
        END, 0) AS buffer_plan_by_am_l3m_qty,


        -- Versi LM
        COALESCE(CASE
          WHEN TRIM(UPPER(supply_control_status_gt)) = 'DISCONTINUED'
              OR TRIM(UPPER(supply_control_status_gt)) = 'STOP PO'
              OR TRIM(UPPER(supply_control_status_gt)) = 'OOS'
              OR current_woi_by_lm >= woi_standard
              OR woi_standard IS NULL OR woi_standard = 0
              OR avg_weekly_st_lm_qty = 0
            THEN 0
          WHEN avg_weekly_st_lm_qty * woi_standard - GREATEST(total_stock,0) > 0
            THEN CEIL((avg_weekly_st_lm_qty * woi_standard) - GREATEST(total_stock,0))
        END, 0) AS buffer_plan_by_lm_qty
      FROM g2g_stock_adjust tdj
  ),




  /* ============================================================
    10. ADJUSTMENT BUFFER REMAINING DAYS UNTIL CLOSING OF THE MONTH
  ============================================================ */


  -- 1) Kalkulasi proyeksi & kebutuhan tambahan
  buffer_gap_calc AS (
    SELECT
      b.*,


      -- fallback: kalau last_stock_date null → pakai nasional
      COALESCE(lsd.last_stock_date, n.national_last_stock_date) AS last_stock_date,


      -- sisa hari
      GREATEST(
        DATE_DIFF(
          LAST_DAY(COALESCE(lsd.last_stock_date, n.national_last_stock_date)),
          COALESCE(lsd.last_stock_date, n.national_last_stock_date),
          DAY
        ), 0
      ) AS days_remaining,


      -- avg harian (asumsi 30 hari)
      SAFE_DIVIDE(b.last_month_st_qty, 30) AS avg_daily_st_lm_qty,


      -- perkiraan konsumsi s/d akhir bulan
      CEIL(
        SAFE_DIVIDE(b.last_month_st_qty, 30) *
        GREATEST(
          DATE_DIFF(
            LAST_DAY(COALESCE(lsd.last_stock_date, n.national_last_stock_date)),
            COALESCE(lsd.last_stock_date, n.national_last_stock_date),
            DAY
          ), 0
        )
      ) AS expected_consumption_to_month_end,


      -- stok proyeksi akhir bulan
      (COALESCE(s.total_stock, 0) -
      CEIL(
        SAFE_DIVIDE(b.last_month_st_qty, 30) *
        GREATEST(
          DATE_DIFF(
            LAST_DAY(COALESCE(lsd.last_stock_date, n.national_last_stock_date)),
            COALESCE(lsd.last_stock_date, n.national_last_stock_date),
            DAY
          ), 0
        )
      )
      ) AS projected_stock_end,


      -- kebutuhan woi
      COALESCE(b.avg_weekly_st_lm_qty, 0) * COALESCE(b.woi_standard, 0) AS required_stock


    FROM buffer_data b
    LEFT JOIN stock_data s
      ON TRIM(UPPER(b.distributor)) = TRIM(UPPER(s.distributor))
    AND TRIM(UPPER(b.sku)) = TRIM(UPPER(s.prod_id))
    LEFT JOIN last_stock_date lsd
      ON TRIM(UPPER(b.distributor)) = TRIM(UPPER(lsd.distributor))
    CROSS JOIN national_last_stock_date n
  ),


  -- 2) Final buffer LM
  buffer_gap_adj AS (
    SELECT
      bg.* EXCEPT(
        buffer_plan_by_am_l3m_qty,
        buffer_plan_by_lm_qty
        -- expected_consumption_to_month_end
        -- projected_stock_end,
        -- required_stock,
        -- avg_daily_st_lm_qty,
        -- days_remaining
      ),


      bg.buffer_plan_by_am_l3m_qty,


      CASE
        WHEN TRIM(UPPER(bg.supply_control_status_gt)) IN ('DISCONTINUED','STOP PO','OOS') THEN 0
        WHEN bg.woi_standard IS NULL OR bg.woi_standard = 0 THEN bg.buffer_plan_by_lm_qty
        WHEN COALESCE(bg.avg_weekly_st_lm_qty, 0) = 0 THEN bg.buffer_plan_by_lm_qty
        ELSE
          GREATEST(bg.required_stock - bg.projected_stock_end, 0)
      END AS buffer_plan_by_lm_qty


    FROM buffer_gap_calc bg
  ),








  /* ============================================================
    11. ADJUSTMENT BUFFER INGREDIENT
  ============================================================ */
  -- Buffer val awal
  buffer_val AS (
      SELECT
        *,
        buffer_plan_by_am_l3m_qty * price_for_distri AS buffer_plan_by_am_l3m_val,
        buffer_plan_by_lm_qty     * price_for_distri AS buffer_plan_by_lm_val
      FROM buffer_gap_adj
  ),


  -- Buffer val by Branch Distri
  buffer_sum AS (
      SELECT
        distributor,
        brand,
        SUM(buffer_plan_by_am_l3m_val) AS total_buffer_am_val,
        SUM(buffer_plan_by_lm_val)     AS total_buffer_lm_val,
        MAX(target_remaining)          AS target_remaining
      FROM buffer_val
      GROUP BY distributor, brand
  ),


  -- Current WOI by Branch Distri
  dist_woi AS (
      SELECT
        distributor,
        brand,
        SAFE_DIVIDE(SUM(total_stock), SUM(avg_weekly_st_am_l3m_qty)) AS dist_woi_am_l3m,
        SAFE_DIVIDE(SUM(total_stock), SUM(avg_weekly_st_lm_qty))     AS dist_woi_lm
      FROM buffer_val
      GROUP BY distributor, brand
  ),






  /* ============================================================
    12. ADJUSTMENT BUFFER PLAN WITH TARGET SI (VALUE, NOT FINAL with Headroom)
  ============================================================ */
  headroom AS (
      SELECT
        h.*,

        -- Headroom maksimal tambahan VALUE per SKU
        -- Khusus buat SKU Must Have & Best Selling
        -- CASE
        --   WHEN h.assortment IN ('Must Have SKU','Best Selling SKU')
        --   THEN GREATEST(
        --         ((h.woi_standard + 2 - h.current_woi_by_lm) 
        --         * h.avg_weekly_st_am_l3m_qty * h.price_for_distri)
        --         - COALESCE(h.buffer_plan_by_am_l3m_val, 0),
        --         0
        --       )
        --   ELSE 0
        -- END AS headroom_am,
        -- CASE
        --   WHEN h.assortment IN ('Must Have SKU','Best Selling SKU')
        --   THEN GREATEST(
        --         ((h.woi_standard + 2 - h.current_woi_by_lm) 
        --         * h.avg_weekly_st_lm_qty * h.price_for_distri)
        --         - COALESCE(h.buffer_plan_by_lm_val, 0),
        --         0
        --       )
        --   ELSE 0
        -- END AS headroom_lm


        -- Headroom buat seluruh SKU
        GREATEST(
          ((h.woi_standard + 2 - h.current_woi_by_lm) 
          * h.avg_weekly_st_am_l3m_qty * h.price_for_distri)
          - COALESCE(h.buffer_plan_by_am_l3m_val, 0),
          0
        ) AS headroom_am,

        GREATEST(
          ((h.woi_standard + 2 - h.current_woi_by_lm) 
          * h.avg_weekly_st_lm_qty * h.price_for_distri)
          - COALESCE(h.buffer_plan_by_lm_val, 0),
          0
        ) AS headroom_lm
      FROM buffer_val h
  ),




  headroom_sum AS (
      SELECT
        distributor,
        brand,
        SUM(headroom_am) AS total_headroom_am,
        SUM(headroom_lm) AS total_headroom_lm,
        MAX(target_remaining) AS target_remaining,
        SUM(buffer_plan_by_am_l3m_val) AS total_buffer_am_val,
        SUM(buffer_plan_by_lm_val)     AS total_buffer_lm_val
      FROM headroom
      GROUP BY distributor, brand
  ),




  buffer_adjusted AS (
      SELECT
        h.*,


        /* =======================
          Versi AM L3M
        ======================= */
        CASE
          WHEN dw.dist_woi_am_l3m >= 24 THEN 0
          WHEN hs.total_headroom_am < (hs.target_remaining - hs.total_buffer_am_val)
            THEN h.buffer_plan_by_am_l3m_val + h.headroom_am
          ELSE h.buffer_plan_by_am_l3m_val +
              LEAST(
                COALESCE(
                  SAFE_DIVIDE(
                    GREATEST(
                      h.avg_weekly_st_am_l3m_qty * (hs.target_remaining - hs.total_buffer_am_val),0),
                      SUM(CASE WHEN h.headroom_am > 0 THEN h.avg_weekly_st_am_l3m_qty ELSE 0 END)
                        OVER(PARTITION BY h.distributor, h.brand)
                  ), 0
                ),
                h.headroom_am
              )
        END AS buffer_plan_by_am_l3m_val_aloc,




        /* =======================
          Versi LM
        ======================= */
        CASE
          WHEN dw.dist_woi_lm >= 24 THEN 0
          WHEN hs.total_headroom_lm < (hs.target_remaining - hs.total_buffer_lm_val)
            THEN h.buffer_plan_by_lm_val + h.headroom_lm
          ELSE h.buffer_plan_by_lm_val +
              LEAST(
                COALESCE(
                  SAFE_DIVIDE(
                    GREATEST(
                    h.avg_weekly_st_lm_qty * (hs.target_remaining - hs.total_buffer_lm_val),0),
                    SUM(CASE WHEN h.headroom_lm > 0 THEN h.avg_weekly_st_lm_qty ELSE 0 END)
                      OVER(PARTITION BY h.distributor, h.brand)
                  ), 0
                ),
                h.headroom_lm
              )
        END AS buffer_plan_by_lm_val_aloc
      FROM headroom h
      JOIN headroom_sum hs
        ON h.distributor = hs.distributor
      AND h.brand = hs.brand
      JOIN dist_woi dw
        ON h.distributor = dw.distributor
      AND h.brand = dw.brand
  ),




  buffer_qty_adj AS (
      SELECT
        ba.*,
        CASE
          WHEN ba.inner_pcs IS NULL OR ba.inner_pcs = 0 OR ba.inner_pcs > 50
            THEN 1
          ELSE ba.inner_pcs
        END AS inner_pcs_adj,


        COALESCE(
          CEIL(
            (ba.buffer_plan_by_am_l3m_val_aloc / NULLIF(ba.price_for_distri,0))
            / COALESCE(NULLIF(
                CASE
                  WHEN ba.inner_pcs IS NULL OR ba.inner_pcs = 0 OR ba.inner_pcs > 50
                    THEN 1
                  ELSE ba.inner_pcs
                END
            ,0),1)
          ) * GREATEST(
                CASE
                  WHEN ba.inner_pcs IS NULL OR ba.inner_pcs = 0 OR ba.inner_pcs > 50
                    THEN 1
                  ELSE ba.inner_pcs
                END, 1
          ),0
        ) AS buffer_plan_by_am_l3m_qty_adj,


        COALESCE(
          CEIL(
            (ba.buffer_plan_by_lm_val_aloc / NULLIF(ba.price_for_distri,0))
            / COALESCE(NULLIF(
                CASE
                  WHEN ba.inner_pcs IS NULL OR ba.inner_pcs = 0 OR ba.inner_pcs > 50
                    THEN 1
                  ELSE ba.inner_pcs
                END
            ,0),1)
          ) * GREATEST(
                CASE
                  WHEN ba.inner_pcs IS NULL OR ba.inner_pcs = 0 OR ba.inner_pcs > 50
                    THEN 1
                  ELSE ba.inner_pcs
                END, 1
          ),0
        ) AS buffer_plan_by_lm_qty_adj
      FROM buffer_adjusted ba
  ),






  /* ============================================================
    13. NPD ALLOCATION (overwrite buffer_plan_by_lm_qty_adj) & Buffer Value
  ============================================================ */
  contrib AS (
    SELECT
      b.region,
      b.distributor,
      SUM(COALESCE(b.last_month_st_qty,0)) AS contrib_qty
    FROM buffer_qty_adj b
    WHERE
  REGEXP_CONTAINS(UPPER(b.brand), r'\bG2G\b') OR
  UPPER(b.brand) LIKE '%G2G%'
    GROUP BY b.region, b.distributor
  ),


  contrib_region AS (
    SELECT
      region,
      SUM(contrib_qty) AS total_contrib
    FROM contrib
    GROUP BY region
  ),




  allocation_base AS (
    SELECT
      a.region,
      a.sku,
      a.allocation,
      c.distributor,
      c.contrib_qty,
      cr.total_contrib,
      SAFE_DIVIDE(c.contrib_qty, cr.total_contrib) AS contrib_ratio,
      ROUND(a.allocation * SAFE_DIVIDE(c.contrib_qty, cr.total_contrib)) AS allocation_per_dist
    FROM `gt_schema.npd_allocation` a
    JOIN contrib c
      ON a.region = c.region
    JOIN contrib_region cr
      ON a.region = cr.region
    WHERE DATE_TRUNC(a.calendar_date, MONTH) = DATE '2025-10-01'
  ),




  po_npd_mtd AS (
    SELECT
      distributor_name,
      brand,
      sku,
      SUM(
        CASE
            -- Condition 1: Include ONLY Oct 28-31 quantity for the specific SKUs
            WHEN
                sku IN ('G2G-2111', 'G2G-2112', 'G2G-2113', 'G2G-2114')
                AND order_date BETWEEN DATE('2025-10-28') AND DATE('2025-10-31')
            THEN COALESCE(order_qty, 0)

            -- Condition 2: Include ALL quantity for ALL other SKUs
            WHEN
                sku NOT IN ('G2G-2111', 'G2G-2112', 'G2G-2113', 'G2G-2114')
            THEN COALESCE(order_qty, 0)

            -- Everything else (which is the specific SKUs outside Oct 28-31) gets zeroed out
            ELSE 0
        END
      ) AS order_qty
    FROM dms.gt_po_tracking_mtd_mv
    GROUP BY distributor_name, brand, sku
  ),




  po_npd_mtd_region AS (
    SELECT
      region,
      brand,
      sku,
      SUM(
        CASE
            -- Condition 1: Include ONLY Oct 28-31 quantity for the specific SKUs
            WHEN
                sku IN ('G2G-2111', 'G2G-2112', 'G2G-2113', 'G2G-2114')
                AND order_date BETWEEN DATE('2025-10-28') AND DATE('2025-10-31')
            THEN COALESCE(order_qty, 0)

            -- Condition 2: Include ALL quantity for ALL other SKUs
            WHEN
                sku NOT IN ('G2G-2111', 'G2G-2112', 'G2G-2113', 'G2G-2114')
            THEN COALESCE(order_qty, 0)

            -- Everything else (which is the specific SKUs outside Oct 28-31) gets zeroed out
            ELSE 0
        END
      ) AS order_qty_region
    FROM dms.gt_po_tracking_mtd_mv
    GROUP BY region, brand, sku
  ),    




  allocation_remaining AS (
    SELECT
      ab.region,
      ab.sku,
      ab.distributor,
      GREATEST(ab.allocation_per_dist - COALESCE(po.order_qty, 0), 0) AS remaining_allocation_qty,
      GREATEST(ab.allocation - COALESCE(pr.order_qty_region,0), 0) AS remaining_allocation_qty_region      
    FROM allocation_base ab
    LEFT JOIN po_npd_mtd po
      ON ab.distributor = po.distributor_name
    AND ab.sku = po.sku
    LEFT JOIN po_npd_mtd_region pr
      ON ab.region = pr.region
    AND ab.sku = pr.sku
  ),




  npd_allocation_adj AS (
    SELECT
      b.* EXCEPT (buffer_plan_by_am_l3m_qty_adj, buffer_plan_by_lm_qty_adj),
      CASE
        WHEN TRIM(UPPER(mos.supply_control_status_gt)) IN ('DISCONTINUED', 'STOP PO', 'OOS')
            THEN 0
        ELSE COALESCE(ab.remaining_allocation_qty, b.buffer_plan_by_lm_qty_adj)
      END AS buffer_plan_by_lm_qty_adj,
      b.buffer_plan_by_am_l3m_qty_adj,
      ab.remaining_allocation_qty_region
    FROM buffer_qty_adj b
    LEFT JOIN allocation_remaining ab
      ON b.region = ab.region
    AND b.sku = ab.sku
    AND b.distributor = ab.distributor
    LEFT JOIN `gt_schema.master_offline_stock` mos
      ON b.sku = mos.sku
    ),




  -- Perhitungan Value
  buffer_adj_val AS (
      SELECT
        npa.*,
        -- Buffer Value Adjustment
        npa.buffer_plan_by_am_l3m_qty_adj * npa.price_for_distri AS buffer_plan_by_am_l3m_val_adj,
        npa.buffer_plan_by_lm_qty_adj     * npa.price_for_distri AS buffer_plan_by_lm_val_adj,
        -- WOI setelah buffer plan (pakai L3M AM)
        COALESCE(
          SAFE_DIVIDE(
            npa.total_stock + npa.buffer_plan_by_am_l3m_qty_adj,
            NULLIF(npa.avg_weekly_st_am_l3m_qty, 0)
          ),
          0
        ) AS woi_after_buffer_plan_by_am_l3m,
        -- WOI setelah buffer plan (pakai Last Month)
        COALESCE(
          SAFE_DIVIDE(
            npa.total_stock + npa.buffer_plan_by_lm_qty_adj,
            NULLIF(npa.avg_weekly_st_lm_qty, 0)
          ),
          0
        ) AS woi_after_buffer_plan_by_lm,
        COALESCE(
          SAFE_DIVIDE(
            npa.total_stock + npa.buffer_plan_by_lm_qty_adj - npa.expected_consumption_to_month_end,
            NULLIF(npa.avg_weekly_st_lm_qty, 0)
          ),
          0
        ) AS woi_end_of_month_by_lm
      FROM npd_allocation_adj npa
  ),






  /* ============================================================
    13. ST POTENTIAL
  ============================================================ */
  -- ST Potential Qty
  st_potential_data AS (
      SELECT *,
        CASE
          WHEN SAFE_DIVIDE((buffer_plan_by_am_l3m_qty_adj + total_stock), NULLIF(avg_weekly_st_am_l3m_qty, 0)) > woi_standard
            THEN ROUND(
              (SAFE_DIVIDE((buffer_plan_by_am_l3m_qty_adj + total_stock), NULLIF(avg_weekly_st_am_l3m_qty, 0)) - woi_standard)
              * avg_weekly_st_am_l3m_qty
            )
          ELSE 0
        END AS st_potential_by_am_l3m_qty,
        CASE
          WHEN SAFE_DIVIDE((buffer_plan_by_lm_qty_adj + total_stock), NULLIF(avg_weekly_st_lm_qty, 0)) > woi_standard
            THEN ROUND(
              (SAFE_DIVIDE((buffer_plan_by_lm_qty_adj + total_stock), NULLIF(avg_weekly_st_lm_qty, 0)) - woi_standard)
              * avg_weekly_st_lm_qty
            )
          ELSE 0
        END AS st_potential_by_lm_qty
      FROM buffer_adj_val
  ),




  -- ST Potential Value & WOI after + buffer
  st_potential_data_val AS (
      SELECT *,
        st_potential_by_am_l3m_qty * price_for_store AS st_potential_by_am_l3m_val,
        st_potential_by_lm_qty     * price_for_store AS st_potential_by_lm_val,
        COALESCE(SAFE_DIVIDE((buffer_plan_by_am_l3m_qty_adj + total_stock), NULLIF(avg_weekly_st_am_l3m_qty,0)),0) AS woi_after_buffer_am_l3m,
        COALESCE(SAFE_DIVIDE((buffer_plan_by_lm_qty_adj + total_stock), NULLIF(avg_weekly_st_lm_qty,0)),0) AS woi_after_buffer_lm
      FROM st_potential_data
  )






  /* ============================================================
    FINAL OUTPUT
  ============================================================ */
      SELECT
        region,
        distributor_company,
        distributor_code,
        distributor,
        distributor_brand,
        brand,
        sku,
        product_name,
        category,
        assortment,
        woi_standard,
        moq,
        rank,
        supply_control_status_gt,
        price_for_distri,
        price_for_store,
        avg_am_l3m_st_qty,
        avg_am_l3m_st_value,
        last_month_st_qty,
        last_month_st_value,
        avg_weekly_st_am_l3m_qty,
        avg_weekly_st_am_l3m_val,
        avg_weekly_st_lm_qty,
        avg_weekly_st_lm_val,
        ori_current_stock_qty,
        ori_current_stock_value,
        st_since_stock_date,
        used_stock_date,
        current_stock_qty,
        current_stock_value,
        in_transit_stock_qty,
        in_transit_stock_value,
        total_stock,
        total_stock_value,
        current_woi_by_am_l3m,
        current_woi_by_lm,
        buffer_plan_by_am_l3m_qty_adj,
        buffer_plan_by_am_l3m_val_adj,
        buffer_plan_by_lm_qty_adj,
        buffer_plan_by_lm_val_adj,
        woi_after_buffer_plan_by_am_l3m,
        woi_after_buffer_plan_by_lm,
        expected_consumption_to_month_end,
        woi_end_of_month_by_lm,
        remaining_allocation_qty_region,
        st_potential_by_am_l3m_qty,
        st_potential_by_am_l3m_val,
        st_potential_by_lm_qty,
        st_potential_by_lm_val
      FROM st_potential_data_val
      ORDER BY
        region,
        distributor_company,
        distributor,
        sku