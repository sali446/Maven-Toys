with sales as( 
SELECT *
FROM {{ ref('stg_sales') }}
),

products as( 
SELECT *
FROM {{ ref('stg_products') }}
),

stores as(
SELECT * 
FROM {{ ref('stg_stores') }}
),

inventory as(
SELECT * 
FROM {{ ref('stg_inventory') }}
),

monthly_sales as(

SELECT

    store_name,
    product_name,
    product_id,
    store_id,
    SUM(units) as avg_sales

FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
LEFT JOIN stores ON store_id = fk_sales_store_id
GROUP BY store_name, product_name, date_part('month', sale_date), product_id, store_id
),

final as(

SELECT

    store_name,
    product_name,
    product_id,
    ROUND(AVG(avg_sales),0) as avg_monthly_sales,
    coalesce(stock_on_hand,0) as stock,
    stock-avg_monthly_sales as stock_deficit
    
    
FROM monthly_sales
LEFT JOIN inventory ON store_id = fk_inventory_store_id AND product_id = fk_inventory_product_id
GROUP BY store_name, product_name, stock_on_hand, product_id
ORDER BY store_name ASC, product_id ASC
)

SELECT * FROM final