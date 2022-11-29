SELECT 

product_id,
product_name,
product_category as product_category,
REPLACE(product_cost, '$','')::float as product_cost,
REPLACE(product_price, '$','')::float as product_price

FROM toys.records.products