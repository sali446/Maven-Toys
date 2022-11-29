with products as( 
SELECT *
FROM {{ ref('stg_products') }}
),

sales as( 
SELECT *
FROM {{ ref('stg_sales') }}
),

summary as(
SELECT 

    date_part('year', sale_date) as year,
    product_category,
    COUNT(units) as units_sold,
    ROUND(SUM(units * product_price),2) as total_revenue,
    ROUND(SUM(units * product_cost),2) as total_cost,
    ROUND(total_revenue-total_cost,2) as total_profit

FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
GROUP BY product_category, year
ORDER BY total_profit DESC
),

final as(

SELECT * FROM summary
WHERE year = 2017
UNION ALL
SELECT * FROM summary
WHERE year = 2018
ORDER BY year ASC, total_profit DESC
)

SELECT * FROM final