with stock as(
SELECT * 
FROM {{ ref('fct_avg_purchase_time') }}
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

final as(

SELECT

    store_name,
    ROUND(SUM(product_cost*stock_on_hand),2) as inventory_money,
    ROUND(AVG(stock_on_hand/sell_time),0) as turnover_time

FROM stock
LEFT JOIN products a ON a.product_id = stock.product_id
LEFT JOIN stores b ON b.store_id = stock.store_id
LEFT JOIN inventory ON fk_inventory_store_id = stock.store_id
GROUP BY store_name
ORDER BY inventory_money DESC
)

SELECT * FROM final