WITH
sla_table AS (
    SELECT 
        pharmacy_id,
        abbreviation AS sla
    FROM logistics_pharmacy_area_staging
    WHERE market_id = 'id'

), 
pharmacy_detail AS (
    SELECT
        pharmacy_id,
        territory,
        city
    FROM pharmacy_detail_staging
), 
purchase_order_table AS (
    SELECT 
        DATE(ordered_at) AS ordered_at,
        po_number AS po_number,
        CASE 
            WHEN purchase_order_status IN ("pending","processing") THEN 1 ELSE 0 
        END AS processing,
        CASE 
            WHEN purchase_order_status = "accepted" AND fulfillment_status = "dispatched" AND delivered_at IS NULL THEN 1 ELSE 0 
        END AS ontheway,
        CASE 
            WHEN purchase_order_status IN ("delivered", "completed", "fulfilled") THEN 1 ELSE 0 
        END AS is_delivered,
        STRING_AGG(DISTINCT cancellation_reason, " | " ORDER BY cancellation_reason) AS cancellation_reason,
        return_type AS return_type, --because in 1 PO there are >1 product with each reason 
        STRING_AGG(DISTINCT return_reason, " | " ORDER BY return_reason) AS return_reason,
        distributor_id AS distributor_id,
        pharmacy_id AS pharmacy_id,
        SUM(CASE 
            WHEN previous_discount_rate > discount_rate THEN 1 ELSE 0 END) AS discount_change,
        SUM(
            CASE 
                WHEN previous_selling_price < selling_price THEN 1 ELSE 0 END
        ) AS price_change,
        SUM(
            CASE 
                WHEN previous_quantity IS NOT NULL AND previous_quantity != quantity THEN 1 ELSE 0 
            END
        ) AS quantity_change,
        SUM(is_out_of_stock) AS outofstock,
        DATE(cancelled_at) AS cancelled_at,
        SUM(net_amount) AS net_gmv,
        is_on_time AS is_on_time,
        CASE 
            WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 
        END AS is_received,
        DATETIME(TIMESTAMP(JSON_EXTRACT_SCALAR(purchase_order_status_history, '$.dispatched.date')),'Asia/Jakarta') AS status_dispatched_at,
        expected_dispatch_at AS expected_dispatch_at,
        expected_delivery_at,
        delivered_at,
        logistics_delivery_partner_name
    FROM purchase_order_item_staging
    WHERE distributor_id IN (SELECT * FROM distributor_seed)
        AND (net_amount > 0 OR is_out_of_stock = 1)
    GROUP BY ordered_at, po_number, processing, ontheway, is_delivered,
            return_type, distributor_id, pharmacy_id, cancelled_at,
            is_on_time, is_received, status_dispatched_at,
            expected_dispatch_at, expected_delivery_at,
            delivered_at, logistics_delivery_partner_name
),
otif_table AS (
    SELECT
        po_number,
        ordered_at,
        processing,
        ontheway,
        is_delivered,
        cancellation_reason,
        return_type,
        return_reason,
        distributor_id,
        pharmacy_id,
        quantity_change,
        outofstock,
        cancelled_at,
        net_gmv,
        is_on_time,
        is_received,
        sla,
        CASE 
            WHEN outofstock > 0 OR quantity_change > 0 THEN "OOS"
            WHEN price_change > 0 OR discount_change > 0 THEN "Price"
            WHEN REGEXP_CONTAINS(LOWER(return_reason), 'damage') OR REGEXP_CONTAINS(LOWER(cancellation_reason), 'damage') THEN "Damaged Goods"
            WHEN REGEXP_CONTAINS(LOWER(return_reason), 'near') OR REGEXP_CONTAINS(LOWER(cancellation_reason), 'near') THEN "Near ED"
            WHEN return_type IS NOT NULL OR cancelled_at IS NOT NULL THEN "Other Reason"
        END AS classification,
        status_dispatched_at,
        expected_dispatch_at,
        CASE 
            WHEN status_dispatched_at <= expected_dispatch_at THEN 1 ELSE 0 
        END AS allocation,
        expected_delivery_at,
        delivered_at,
        logistics_delivery_partner_name
    FROM purchase_order_table
    LEFT JOIN sla_table USING (pharmacy_id)
),
final_table AS (
    SELECT 
        ordered_at,
        territory,
        city,
        po_number,
        processing,
        ontheway,
        return_type,
        is_delivered,
        is_on_time,
        distributor_id,
        pharmacy_id,
        is_received,
        sla,
        classification,
        quantity_change,
        outofstock,
        status_dispatched_at,
        expected_dispatch_at,
        allocation, 
        CASE 
            WHEN IFNULL(classification, return_type) IS NOT NULL THEN 0 ELSE 1
        END AS is_in_full,
        expected_delivery_at,
        delivered_at,
        logistics_delivery_partner_name,
        net_gmv
    FROM otif_table
    LEFT JOIN pharmacy_detail USING (pharmacy_id)
)

SELECT * FROM final_table


