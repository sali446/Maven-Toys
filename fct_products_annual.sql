with sales as(
SELECT *
FROM {{ ref('stg_sales') }}
),

products as(
SELECT *
FROM {{ ref('stg_products') }}
),

product_sales as(
SELECT

    product_name,
    date_part('year',sale_date) as year,
    ROUND(SUM(product_price*units-product_cost*units),2) as total_profits,
    ROUND(SUM(product_price*units),2) as total_revenue,
    ROUND(SUM(product_cost*units),2) as total_cost,
    SUM(units) as units_sold

FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
GROUP BY product_name, year
ORDER BY product_name ASC, year ASC
)

SELECT * FROM product_sales