with products as( 
SELECT *
FROM {{ ref('stg_products') }}
),

sales as( 
SELECT *
FROM {{ ref('stg_sales') }}
),

stores as(
SELECT * 
FROM {{ ref('stg_stores') }}
),

final as(
SELECT 

    store_location,
    product_category,
    COUNT(units) as units_sold,
    ROUND(SUM(units * product_price),2) as total_revenue,
    ROUND(SUM(units * product_cost),2) as total_cost,
    ROUND(total_revenue-total_cost,2) as total_profit

FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
LEFT JOIN stores ON store_id = fk_sales_store_id
GROUP BY product_category, store_location
ORDER BY store_location ASC, total_profit DESC
)

SELECT * FROM final