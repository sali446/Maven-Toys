with top_five as(
SELECT *
FROM {{ ref('fct_products_lifetime') }}
)

SELECT * FROM top_five LIMIT 5