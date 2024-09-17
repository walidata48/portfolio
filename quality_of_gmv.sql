WITH
phb_per_sku_data AS (
  SELECT
      DATE(ordered_at) AS day,
      sku_code,
      COUNT(DISTINCT pharmacy_id) AS phb
  FROM purchase_order_item_staging
  WHERE DATE(ordered_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND net_amount > 0
  GROUP BY day, sku_code
  ORDER BY phb DESC
),

avg_phb_per_sku AS (
  SELECT
      DISTINCT day,
      ROUND(AVG(phb), 2) AS phb_per_sku
  FROM phb_per_sku_data
  GROUP BY day
),

sku_per_phb_data AS (
  SELECT
      DATE(ordered_at) AS day,
      pharmacy_id,
      COUNT(DISTINCT sku_code) AS sku
  FROM purchase_order_item_staging
  WHERE DATE(ordered_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND net_amount > 0
  GROUP BY day, pharmacy_id
  ORDER BY sku DESC
),

avg_sku_per_phb AS (
  SELECT
      day,
      ROUND(AVG(sku), 2) AS sku_per_phb
  FROM sku_per_phb_data
  GROUP BY day
),

gmv_pharmacy AS (
  SELECT
      DATE(ordered_at) AS day,
      pharmacy_id,
      pharmacy_name,
      SUM(net_amount) AS gmv,
      COUNT(DISTINCT sku_code) AS sku
  FROM purchase_order_item_staging
  WHERE DATE(ordered_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND net_amount > 0
  GROUP BY day, pharmacy_id, pharmacy_name
),

rank_gmv_pharmacy AS (
  SELECT
      day,
      pharmacy_id,
      pharmacy_name,
      gmv,
      ROW_NUMBER() OVER (PARTITION BY day ORDER BY gmv DESC) AS rank_phb
  FROM gmv_pharmacy
),

top_pharmacy AS (
  SELECT
      day,
      ROUND(SUM(gmv)) AS top10phb_gmv
  FROM rank_gmv_pharmacy
  WHERE rank_phb BETWEEN 1 AND 10
  GROUP BY day
),

gmv_sku AS (
  SELECT
      DATE(ordered_at) AS day,
      sku_code,
      product_name,
      SUM(net_amount) AS gmv,
      COUNT(DISTINCT pharmacy_id) AS phb
  FROM purchase_order_item_staging
  WHERE DATE(ordered_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND net_amount > 0
  GROUP BY day, sku_code, product_name
),

rank_gmv_sku AS (
  SELECT
      day,
      sku_code,
      gmv,
      ROW_NUMBER() OVER (PARTITION BY day ORDER BY gmv DESC) AS rank_sku
  FROM gmv_sku
),

top_sku AS (
  SELECT
      day,
      ROUND(SUM(gmv)) AS top10sku_gmv
  FROM rank_gmv_sku
  WHERE rank_sku BETWEEN 1 AND 10
  GROUP BY day
),

pharmacy_detail_staging AS (
  SELECT
      pharmacy_id,
      pharmacy_name,
      region,
      territory
  FROM pharmacy_detail_staging

),

final_trx AS (
  SELECT
      poi.ordered_at,
      poi.pharmacy_id,
      poi.sku_code,
      poi.quantity,
      poi.net_amount,
      pd.territory
  FROM purchase_order_item_staging poi
  LEFT JOIN pharmacy_detail_staging pd USING (pharmacy_id)
  WHERE net_amount > 0
),

gross_margin_table AS (
  SELECT
      DATE(poi.ordered_at) AS day,
      SUM(poi.net_amount) AS gmv,
      SUM(mc.final_cogs * poi.quantity) AS cogs,
      ROUND((SUM(poi.net_amount) - SUM(mc.final_cogs * poi.quantity)) / SUM(poi.net_amount) * 100, 2) AS gross_margin
  FROM mart_cogs_order_item mc
  JOIN purchase_order_item_staging poi USING (purchase_order_id, sku_code)
  WHERE net_amount > 0
  GROUP BY day
  ORDER BY day DESC
),

gm_table AS (
  SELECT
      day,
      gross_margin
  FROM gross_margin_table
),

all_data AS (
  SELECT
      DATE(ordered_at) AS day,
      FORMAT_DATE('%A', DATE(ordered_at)) AS day_name,
      ROUND(SUM(net_amount)) AS gmv,
      COUNT(DISTINCT pharmacy_id) AS phb,
      COUNT(DISTINCT sku_code) AS sku,
      
  FROM purchase_order_item_staging
  WHERE DATE(ordered_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND net_amount > 0
  GROUP BY day, day_name
),

final AS (
  SELECT
      ad.day,
      ad.day_name,
      ad.gmv,
      ad.phb,
      ad.sku,
      asku.sku_per_phb,
      aphb.phb_per_sku,
      gm.gross_margin,
      tp.top10phb_gmv,
      tp.top10sku_gmv
  FROM all_data ad
  LEFT JOIN avg_sku_per_phb asku USING (day)
  LEFT JOIN avg_phb_per_sku aphb USING (day)
  LEFT JOIN top_pharmacy tp USING (day)
  LEFT JOIN top_sku ts USING (day)
  LEFT JOIN gm_table gm USING (day)
),

final_gmv_data AS (
  SELECT
      day,
      day_name,
      gmv,
      phb,
      sku,
      sku_per_phb,
      phb_per_sku,
      gross_margin,
      top10phb_gmv,
      top10sku_gmv,
      ROUND((top10phb_gmv / gmv) * 100) AS top10phb_percent,
      ROUND((top10sku_gmv / gmv) * 100) AS top10sku_percent
  FROM final
  ORDER BY day DESC
),

engage AS (
  SELECT 
      FORMAT_DATE('%A', DATE(date_loaded_at_jkt)) AS day_name, 
      DATE(date_loaded_at_jkt) AS day , 
      COUNT(event_name) total_engagement
  FROM engagement_mart
  WHERE DATE(date_loaded_at_jkt) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
  GROUP BY day_name, day
)

SELECT 
    fg.day,
    fg.day_name,
    fg.gmv,
    fg.phb,
    fg.sku,
    fg.sku_per_phb,
    fg.phb_per_sku,
    fg.gross_margin,
    fg.fg.top10phb_percent,
    fg.top10sku_percent,
    en.total_engagement
FROM final_gmv_data fg
LEFT JOIN engage en USING(day)
ORDER BY day DESC

