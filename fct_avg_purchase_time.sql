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

purchase_time as(

SELECT

    product_name,
    sale_date,
    fk_sales_store_id as store_id,
    product_id,
    DATEDIFF(day, LAG(sale_date,1,'2017-01-01') OVER(PARTITION BY product_name,store_id ORDER BY sale_date ASC), sale_date) as sell_time
    
FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
ORDER BY store_id, product_id, sale_date ASC
),

final as(

SELECT

    product_name,
    store_id,
    product_id,
    AVG(sell_time) as sell_time

FROM purchase_time
GROUP BY product_name, store_id, product_id
ORDER BY store_id ASC, product_id ASC
)

SELECT * FROM final