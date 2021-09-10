/* Q1 Determine the top 3 products in Dec 2019 in terms of total sales */

SELECT p.product_name, sum(s.total_sale),
RANK() OVER (ORDER BY sum(s.total_sale) DESC) AS RANK
FROM product p, date_time d, sales s
WHERE p.product_id = s.product_id
AND d.date_id = s.date_id
AND d.time_year = 2019
AND d.time_month = 12
GROUP BY p.product_name, d.time_year, d.time_month
FETCH FIRST 3 ROWS ONLY;

/*Q2 Determine which customer produced highest sales in the whole year?*/

SELECT c.customer_name, sum(s.total_sale)
FROM customer c, date_time d, sales s
WHERE c.customer_id = s.customer_id
AND d.date_id = s.date_id
AND d.time_year = 2019
GROUP BY c.customer_name, d.time_year
ORDER BY sum(s.total_sale) DESC
FETCH FIRST ROW ONLY;

/*Q3 How many sales transactions were there for the product that generated maximum sales
revenue in 2019? 
Also identify the:
a) product quantity sold and
b) supplier name*/

SELECT p.product_name, supplier.supplier_name, count(s.product_id), sum(s.quantity_sold),
RANK() OVER (ORDER BY sum(s.total_sale) DESC) AS RANK
FROM product p, supplier, sales s, date_time d
WHERE p.product_id = s.product_id
AND supplier.supplier_id = s.supplier_id
AND time_year = 2019
GROUP BY p.product_name, supplier.supplier_name ,d.time_year
FETCH FIRST ROW ONLY;

/*Q4 Present the quarterly sales analysis for all warehouses using drill down query concepts*/

SELECT w.warehouse_name, 
SUM(CASE WHEN TO_CHAR(d.t_date,'Q') = 1 THEN s.total_sale END) Q1_2019,
SUM(CASE WHEN TO_CHAR(d.t_date,'Q') = 2 THEN s.total_sale END) Q2_2019,
SUM(CASE WHEN TO_CHAR(d.t_date,'Q') = 3 THEN s.total_sale END) Q3_2019,
SUM(CASE WHEN TO_CHAR(d.t_date,'Q') = 4 THEN s.total_sale END) Q4_2019
FROM warehouse w, date_time d, sales s
WHERE w.warehouse_id = s.warehouse_id
AND d.date_id = s.date_id
AND d.time_year = 2019
GROUP BY w.warehouse_name;

/*Q5 Create a materialised view named “Warehouse_Analysis_mv” that presents the productwise sales analysis for each warehouse*/

CREATE MATERIALIZED VIEW Warehouse_Analysis_mv
BUILD IMMEDIATE
ENABLE QUERY REWRITE
AS SELECT w.warehouse_id, p.product_id, SUM(s.total_sale)
FROM warehouse w, product p, sales s
WHERE w.warehouse_id = s.warehouse_id
AND p.product_id = s.product_id
GROUP BY ROLLUP(w.warehouse_id, p.product_id);