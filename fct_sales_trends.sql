with sales as( 
SELECT *
FROM {{ ref('stg_sales') }}
),

products as( 
SELECT *
FROM {{ ref('stg_products') }}
),

trends as(

SELECT 

    date_part('year', sale_date) as year,
    date_part('month', sale_date) as month,
    product_name,
    product_category,
    SUM(units) as units_sold

FROM sales
LEFT JOIN products ON product_id = fk_sales_product_id
GROUP BY product_name, month, year, product_category 
ORDER BY year ASC, product_name ASC, units_sold DESC
)

SELECT * FROM trends