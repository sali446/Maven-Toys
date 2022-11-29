with sales as( 
SELECT *
FROM {{ ref('stg_sales') }}
),

stores as( 
SELECT *
FROM {{ ref('stg_stores') }}
),

products as( 
SELECT *
FROM {{ ref('stg_products') }}
),

store_sales as(
SELECT

    fk_sales_store_id,
    ROUND(SUM(product_price*units - product_cost*units),2) as total_profit,
    ROUND(SUM(product_price*units),2) as total_revenue,
    ROUND(SUM(product_cost*units),2) as total_cost,
    ROUND(SUM(units),2) as units


FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
GROUP BY fk_sales_store_id
ORDER BY fk_sales_store_id
)

SELECT * FROM store_sales