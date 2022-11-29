with store_sales as(
SELECT * 
FROM {{ ref('fct_store_lifetime') }}
),

stores as (
SELECT *
FROM {{ ref('stg_stores') }}
),

city_sales as(
SELECT

    store_city as city,
    ROUND(SUM(total_profit),2) as total_profit,
    ROUND(SUM(total_revenue),2) as total_revenue,
    ROUND(SUM(total_cost),2) as total_cost,
    ROUND(SUM(units),2) as units_sold

FROM store_sales
LEFT JOIN stores on fk_sales_store_id = store_id
GROUP BY store_city
)

SELECT * from city_sales