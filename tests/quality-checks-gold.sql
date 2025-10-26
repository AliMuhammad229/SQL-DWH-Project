--Check whether it relates right or not - FK Integrity (Dimensions)
SELECT * FROM Gold.fact_sales AS f
LEFT JOIN Gold.dim_customer As c
On f.customer_key = c.customer_key
LEFT JOIN Gold.dim_products AS p
On f.product_key = p.product_key
WHERE f.product_key IS NULL;
