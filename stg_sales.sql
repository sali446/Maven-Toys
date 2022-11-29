SELECT

sale_id,
product_id as fk_sales_product_id,
store_id as fk_sales_store_id,
units,
date as sale_date

FROM toys.records.sales