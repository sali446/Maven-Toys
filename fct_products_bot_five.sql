with bot_five as(
SELECT *
FROM {{ ref('fct_products_lifetime') }}
)

SELECT * FROM bot_five ORDER BY total_revenue ASC LIMIT 5