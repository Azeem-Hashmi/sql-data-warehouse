-- =========================
-- Sales change over time
-- =========================

SELECT 
-- YEAR (order_date) as sales_year,
-- MONTH (order_date) as sales_month,
DATETRUNC(month, order_date) AS month_of_year,
SUM(sales_amount) AS total_sales,
COUNT(customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date is not null
group by DATETRUNC(month, order_date)
order by DATETRUNC(month, order_date)


-- =========================
-- Cumulative Analysis (adding previous sale value into onward result) (if our sales is spread on multiple months or years)
-- =========================

-- Calculate the total sales per month
-- and running total of sales over time

SELECT 
sale_year,
total_sales,
SUM(total_sales) OVER (order by sale_year asc) AS running_sales,
AVG(avg_price) OVER (order by sale_year asc) AS moving_avg_price
FROM (
SELECT 
DATETRUNC(YEAR, order_date) AS sale_year,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date is not null
group by DATETRUNC(YEAR, order_date)
) t


-- =========================
-- Performance Analysis
-- =========================

-- Analyze the yearly performance of products by comparing their sales
-- to both the average sales performance of the product and the previous year sales

-- year-over-year sales analysis

WITH yearly_product_sales AS 
(
	SELECT 
	YEAR(s.order_date) AS year_of_sale,
	p.product_name,
	SUM(s.sales_amount) AS current_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	WHERE s.order_date is not null
	GROUP BY YEAR(s.order_date), p.product_name
)
SELECT 
year_of_sale,
product_name,
current_sales,
AVG(current_sales) OVER (partition by product_name) AS avg_sales,
current_sales - AVG(current_sales) OVER (partition by product_name) AS diff_of_avg_sales,
CASE WHEN current_sales - AVG(current_sales) OVER (partition by product_name) > 0 THEN 'Above Average'
	WHEN current_sales - AVG(current_sales) OVER (partition by product_name) < 0 THEN 'Below Average'
	ELSE 'On Average'
END AS progress_over_year,
LAG(current_sales) OVER (partition by product_name ORDER BY year_of_sale) AS previous_year_sales,
current_sales - LAG(current_sales) OVER (partition by product_name ORDER BY year_of_sale) AS diff_with_py_sales,
CASE WHEN current_sales > LAG(current_sales) OVER (partition by product_name ORDER BY year_of_sale) THEN 'Increase'
	WHEN current_sales < LAG(current_sales) OVER (partition by product_name ORDER BY year_of_sale) THEN 'Decrease'
	ELSE 'Constant'
END AS sales_performance
FROM yearly_product_sales
ORDER BY product_name, year_of_sale


-- =========================
-- Proportional Analysis - Part to Whole
-- =========================

-- Which categories contribute the most to overall sales

WITH category_total_sales AS (
	SELECT 
	category,
	SUM(sales_amount) AS total_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
	group by category
)
SELECT 
category,
total_sales,
SUM(total_sales) OVER () AS overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER () )*100 ,2), '%') AS contribution_percent
FROM category_total_sales 
ORDER BY total_sales desc


-- =========================
-- Data Segmentation - group the data based on specific range
-- =========================

-- Segment products into cost ranges and count how manu products fall into each segment

WITH product_segments AS (
SELECT 
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
	WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE 'Above 100'
END AS cost_ranges
FROM gold.dim_products
)
SELECT 
cost_ranges,
count(cost_ranges) AS total_products
FROM product_segments
GROUP BY cost_ranges
ORDER BY total_products DESC

/*
Group customers into three segments
	- VIP: at least 12 months of history and spending more than €5,000.
	- Regular: at least 12 months of history but spending €5,000 or less.
	- New: lifespan less than 12 months.
and find the total number of customers by each group
*/


WITH customer_info AS (
	SELECT 
	c.customer_key,
	SUM(s.sales_amount) AS total_spending,
	MIN(s.order_date) AS first_order_date,
	MAX(s.order_date) AS last_order_date,
	DATEDIFF(MONTH, MIN(s.order_date), MAX(s.order_date)) AS customer_lifespan
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
	GROUP BY c.customer_key
)

SELECT 
customer_segment,
count(customer_key) AS customer_count
FROM (
SELECT 
	customer_key,
	total_spending,
	customer_lifespan,
	CASE WHEN total_spending > 5000 AND customer_lifespan > = 12 THEN 'VIP Customer'
		WHEN total_spending <= 5000 AND customer_lifespan >= 12 THEN 'Regular Customer'
		WHEN total_spending > 5000 AND customer_lifespan < 12 THEN 'Aspiring VIP Customer'
		ELSE 'New Customer'
		END AS customer_segment
	FROM customer_info
) t
GROUP BY customer_segment
ORDER BY 2 DESC




