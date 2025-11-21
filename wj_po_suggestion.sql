
WITH
  /* ============================================================
    1. MASTER DATA JOIN
    Distributor × Product (filter distri brand sesuai mappin
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
        UPPER(sti.distributor_name) AS distributor_name,
        UPPER(sti.product_id) AS item_id,
        sti.brand AS brand_of,
        DATE_TRUNC(sti.calendar_date, MONTH) AS month,
        SUM(sti.quantity) AS monthly_st
      FROM `pbi_gt_dataset.fact_sell_through_all` sti
      WHERE sti.calendar_date BETWEEN '2025-06-01' AND '2025-08-31'
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
        MAX(monthly_st) AS max_l3m_st_qty
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
          WHERE sti.calendar_date BETWEEN '2025-03-01' AND '2025-05-31'
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
              THEN l6.max_l6m_st_qty
          ELSE l3.max_l3m_st_qty
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
        dp.moq,
        dp.inner_pcs,
        os.lifecycle_status,
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
        dp.lead_time_week
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

assortment_ranked AS (
  SELECT
    m.*,
    CASE
      WHEN rnk <= 16 THEN 'Must Have SKU'
      WHEN rnk <= 16 + 30 THEN 'Best Selling SKU'
      WHEN rnk <= 16 + 30 + 66 THEN 'Popular SKU'
      ELSE 'Others SKU'
    END AS assortment
  FROM (
    SELECT 
      m.*,
      ROW_NUMBER() OVER (
        PARTITION BY UPPER(m.distributor)
        ORDER BY m.avg_am_l3m_st_qty DESC
      ) AS rnk
    FROM main_data m
  ) m
),

woi_standard AS (
  SELECT
    a.*,
    CASE
      WHEN a.assortment = 'Must Have SKU' THEN 8 + COALESCE(a.lead_time_week,0)
      WHEN a.assortment = 'Best Selling SKU' THEN 6 + COALESCE(a.lead_time_week,0)
      WHEN a.assortment = 'Popular SKU' THEN 4 + COALESCE(a.lead_time_week,0)
      ELSE 4 + COALESCE(a.lead_time_week,0)
    END AS woi_standard
  FROM assortment_ranked a
),




/* ============================================================
  6. AVERAGE DATA (SELL THROUGH) L3M&LM
============================================================ */
avg_data AS (
  SELECT *,
    SAFE_DIVIDE(COALESCE(avg_am_l3m_st_qty, 0), 30) * 7 AS avg_weekly_st_am_l3m_qty,
    SAFE_DIVIDE(COALESCE(last_month_st_qty, 0), 30) *7 AS avg_weekly_st_lm_qty
  FROM woi_standard
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
  8. BUFFER PLAN AWAL (QTY) -- biar SKU healty--
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
    FROM avg_data_final tdj
),






/* ============================================================
  9. ADJUSTMENT BUFFER REMAINING DAYS UNTIL CLOSING OF THE MONTH
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


-- 2) Tanggal stok terakhir nasional (semua distributor)
national_last_stock_date AS (
  SELECT
    MAX(PARSE_DATE('%Y-%m-%d', sa.date)) AS national_last_stock_date
  FROM gt_schema.gt_raw_data_stock sa
  WHERE sa.date IS NOT NULL
    AND UPPER(sa.tagging) = 'CURRENT STOCK'
),


  -- 3) Kalkulasi proyeksi & kebutuhan tambahan
 -- 3) Kalkulasi proyeksi & kebutuhan tambahan (LM + L3M)
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


    /* ====================== LM ====================== */

    -- avg harian LM (asumsi 30 hari)
    SAFE_DIVIDE(b.last_month_st_qty, 30) AS avg_daily_st_lm_qty,

    -- perkiraan konsumsi LM s/d akhir bulan
    CEIL(
      SAFE_DIVIDE(b.last_month_st_qty, 30) *
      GREATEST(
        DATE_DIFF(
          LAST_DAY(COALESCE(lsd.last_stock_date, n.national_last_stock_date)),
          COALESCE(lsd.last_stock_date, n.national_last_stock_date),
          DAY
        ), 0
      )
    ) AS expected_consumption_to_month_end_lm,

    -- stok proyeksi akhir bulan LM
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
    ) AS projected_stock_end_lm,

    -- kebutuhan woi LM
    COALESCE(b.avg_weekly_st_lm_qty, 0) * COALESCE(b.woi_standard, 0) AS required_stock_lm,


    /* ====================== L3M ====================== */

    -- avg harian L3M (asumsi 30 hari)
    SAFE_DIVIDE(b.avg_am_l3m_st_qty, 30) AS avg_daily_st_l3m_qty,

    -- perkiraan konsumsi L3M s/d akhir bulan
    CEIL(
      SAFE_DIVIDE(b.avg_am_l3m_st_qty, 30) *
      GREATEST(
        DATE_DIFF(
          LAST_DAY(COALESCE(lsd.last_stock_date, n.national_last_stock_date)),
          COALESCE(lsd.last_stock_date, n.national_last_stock_date),
          DAY
        ), 0
      )
    ) AS expected_consumption_to_month_end_l3m,

    -- stok proyeksi akhir bulan L3M
    (COALESCE(s.total_stock, 0) -
      CEIL(
        SAFE_DIVIDE(b.avg_am_l3m_st_qty, 30) *
        GREATEST(
          DATE_DIFF(
            LAST_DAY(COALESCE(lsd.last_stock_date, n.national_last_stock_date)),
            COALESCE(lsd.last_stock_date, n.national_last_stock_date),
            DAY
          ), 0
        )
      )
    ) AS projected_stock_end_l3m,

    -- kebutuhan woi L3M
    COALESCE(b.avg_weekly_st_am_l3m_qty, 0) * COALESCE(b.woi_standard, 0) AS required_stock_l3m


  FROM buffer_data b
  LEFT JOIN stock_data s
    ON TRIM(UPPER(b.distributor)) = TRIM(UPPER(s.distributor))
   AND TRIM(UPPER(b.sku)) = TRIM(UPPER(s.prod_id))
  LEFT JOIN last_stock_date lsd
    ON TRIM(UPPER(b.distributor)) = TRIM(UPPER(lsd.distributor))
  CROSS JOIN national_last_stock_date n
),

-- 4) Final buffer (LM + L3M)
buffer_gap_adj AS (
  SELECT
    bg.* EXCEPT(
      buffer_plan_by_am_l3m_qty,
      buffer_plan_by_lm_qty
    ),

    -- buffer AM L3M baru
    CASE
      WHEN TRIM(UPPER(bg.supply_control_status_gt)) IN ('DISCONTINUED','STOP PO','OOS') THEN 0
      WHEN bg.woi_standard IS NULL OR bg.woi_standard = 0 THEN bg.buffer_plan_by_am_l3m_qty
      WHEN COALESCE(bg.avg_weekly_st_am_l3m_qty, 0) = 0 THEN bg.buffer_plan_by_am_l3m_qty
      ELSE
        GREATEST(bg.required_stock_l3m - bg.projected_stock_end_l3m, 0)
    END AS buffer_plan_by_am_l3m_qty,

    -- buffer LM baru
    CASE
      WHEN TRIM(UPPER(bg.supply_control_status_gt)) IN ('DISCONTINUED','STOP PO','OOS') THEN 0
      WHEN bg.woi_standard IS NULL OR bg.woi_standard = 0 THEN bg.buffer_plan_by_lm_qty
      WHEN COALESCE(bg.avg_weekly_st_lm_qty, 0) = 0 THEN bg.buffer_plan_by_lm_qty
      ELSE
        GREATEST(bg.required_stock_lm - bg.projected_stock_end_lm, 0)
    END AS buffer_plan_by_lm_qty

  FROM buffer_gap_calc bg
),



/* ============================================================
   10. TAMBAH KOLOM inner_pcs_adj
============================================================ */
buffer_with_inner AS (
  SELECT
    b.*,
    CASE
      WHEN b.inner_pcs IS NULL OR b.inner_pcs = 0 OR b.inner_pcs > 50 THEN 1
      ELSE b.inner_pcs
    END AS inner_pcs_adj
  FROM buffer_gap_adj b
),


-- buffer_qty_adj AS (
--   SELECT
--     bi.*,

--     -- AM L3M
--     COALESCE(
--       CEIL(
--         (bi.buffer_plan_by_am_l3m_qty / NULLIF(bi.inner_pcs_adj, 0))
--         * GREATEST(bi.inner_pcs_adj, 1)
--       ), 0
--     ) AS buffer_plan_by_am_l3m_qty_adj,

--     -- LM
--     COALESCE(
--       CEIL(
--         (bi.buffer_plan_by_lm_qty / NULLIF(bi.inner_pcs_adj, 0))
--         * GREATEST(bi.inner_pcs_adj, 1)
--       ), 0
--     ) AS buffer_plan_by_lm_qty_adj

--   FROM buffer_with_inner bi
-- ),


--COUNTER CTE KELIPATAN INNER_PCS_QTY
buffer_qty_adj AS (
  SELECT
    bi.* EXCEPT(buffer_plan_by_am_l3m_qty, buffer_plan_by_lm_qty),

    CEIL(buffer_plan_by_am_l3m_qty) AS buffer_plan_by_am_l3m_qty_adj,
    CEIL(buffer_plan_by_lm_qty)     AS buffer_plan_by_lm_qty_adj

  FROM buffer_with_inner bi
),




/* ============================================================
  11. NPD ALLOCATION (overwrite buffer_plan_by_lm_qty_adj) & Buffer Value
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
  WHERE DATE_TRUNC(a.calendar_date, MONTH) = DATE '2025-09-01'
),




po_npd_mtd AS (
  SELECT
    distributor_name,
    brand,
    sku,
    SUM(COALESCE(order_qty, 0)) AS order_qty
  FROM dms.gt_po_tracking_mtd_mv
  GROUP BY distributor_name, brand, sku
),




po_npd_mtd_region AS (
  SELECT
    region,
    brand,
    sku,
    SUM(COALESCE(order_qty, 0)) AS order_qty_region
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
      CASE WHEN npa.buffer_plan_by_lm_qty_adj = 0 THEN 0
      ELSE
        COALESCE(
          SAFE_DIVIDE(
            npa.total_stock + npa.buffer_plan_by_am_l3m_qty_adj - npa.expected_consumption_to_month_end_l3m,
            NULLIF(npa.avg_weekly_st_am_l3m_qty, 0)
          ),
          0
        )END AS woi_end_of_month_by_l3m

    FROM npd_allocation_adj npa
),






/* ============================================================
  12. ST POTENTIAL
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
      avg_weekly_st_am_l3m_qty,
      avg_weekly_st_am_l3m_val,
      current_stock_qty,
      current_stock_value,
      in_transit_stock_qty,
      in_transit_stock_value,
      total_stock,
      total_stock_value,
      current_woi_by_am_l3m,
      buffer_plan_by_am_l3m_qty_adj,
      buffer_plan_by_am_l3m_val_adj,
      woi_after_buffer_plan_by_am_l3m,
      expected_consumption_to_month_end_l3m,
      woi_end_of_month_by_l3m,
      remaining_allocation_qty_region
    FROM st_potential_data_val
    WHERE 
      (UPPER(region) LIKE '%WEST JAVA%'
      OR UPPER(region) LIKE '%JAKARTA%')
      AND UPPER(brand) = 'G2G'
    ORDER BY
      region,
      distributor_company,
      distributor,
      sku;