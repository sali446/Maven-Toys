with yearly as(
SELECT * 
FROM {{ ref('fct_store_annual') }}
),

stores as(
SELECT *
FROM {{ ref('stg_stores') }}
),

yoy as(
SELECT

    fk_sales_store_id,
    store_name,
    year,
    total_revenue,
    ROUND(total_revenue - LAG(total_revenue) OVER (PARTITION BY fk_sales_store_id ORDER BY year ASC),2) as year_over_year

FROM yearly
LEFT JOIN stores ON fk_sales_store_id = store_id
ORDER BY fk_sales_store_id ASC, year ASC
)

SELECT * FROM yoy