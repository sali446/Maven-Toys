with yearly as(
SELECT * 
FROM {{ ref('fct_products_annual') }}
),

yoy as(
SELECT

    product_name,
    year,
    total_revenue,
    ROUND(total_revenue - LAG(total_revenue) OVER (PARTITION BY product_name ORDER BY year ASC),2) as year_over_year

FROM yearly
ORDER BY product_name ASC, year ASC
)

SELECT * FROM yoy