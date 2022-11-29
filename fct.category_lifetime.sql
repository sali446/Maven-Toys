with products as( 
SELECT *
FROM {{ ref('stg_products') }}
),

sales as( 
SELECT *
FROM {{ ref('stg_sales') }}
),

final as(
SELECT 

    product_category,
    COUNT(units) as units_sold,
    SUM(units * product_price) as total_revenue,
    SUM(units * product_cost) as total_cost,
    ROUND(total_revenue-total_cost,2) as total_profit

FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
GROUP BY product_category
)

SELECT * FROM final