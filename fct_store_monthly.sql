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

sales_dated as(
SELECT

    fk_sales_store_id,
    date_part('year', sale_date) as year,
    date_part('month', sale_date) as month,
    ROUND(SUM(product_price*units - product_cost*units),2) as total_profits,
    ROUND(SUM(product_price*units),2) as total_revenue,
    ROUND(SUM(product_cost*units),2) as total_cost,
    SUM(units) as units_sold

FROM sales
LEFT JOIN products on product_id = fk_sales_product_id
GROUP BY fk_sales_store_id, year, month
ORDER BY fk_sales_store_id ASC, year ASC, month ASC
)

SELECT * FROM sales_dated

