DROP TABLE product CASCADE CONSTRAINTS;
DROP TABLE warehouse CASCADE CONSTRAINTS;
DROP TABLE supplier CASCADE CONSTRAINTS;
DROP TABLE customer CASCADE CONSTRAINTS;
DROP TABLE date_time CASCADE CONSTRAINTS;
DROP TABLE sales CASCADE CONSTRAINTS;

CREATE TABLE product(
    product_id VARCHAR(6) NOT NULL,
    product_name VARCHAR(28) NOT NULL,
    sale_price NUMBER(5,2) NOT NULL,
    PRIMARY KEY(product_id)
);

CREATE TABLE warehouse(
    warehouse_id VARCHAR(4) NOT NULL,
    warehouse_name VARCHAR(20) NOT NULL,
    PRIMARY KEY(warehouse_id)
);

CREATE TABLE supplier(
    supplier_id VARCHAR(5) NOT NULL,
    supplier_name VARCHAR(30) NOT NULL,
    PRIMARY KEY(supplier_id)
);


CREATE TABLE customer(
    customer_id VARCHAR(4) NOT NULL,
    customer_name VARCHAR(28) NOT NULL,
    PRIMARY KEY(customer_id)
);


CREATE TABLE date_time(
    date_id NUMBER NOT NULL,
    t_date DATE NOT NULL,
    time_day NUMBER NOT NULL,
    time_month NUMBER NOT NULL,
    time_year NUMBER NOT NULL,
    PRIMARY KEY(date_id)
);


CREATE TABLE sales(
    product_id CONSTRAINT fk_product_id REFERENCES product(product_id),
    date_id CONSTRAINT fk_date_id REFERENCES date_time(date_id),
    customer_id CONSTRAINT fk_customer_id REFERENCES customer(customer_id),
    warehouse_id CONSTRAINT fk_warehouse_id REFERENCES warehouse(warehouse_id),
    quantity_sold NUMBER(3,0) NOT NULL,
    supplier_id CONSTRAINT fk_supplier_id REFERENCES supplier(supplier_id),
    total_sale NUMBER NOT NULL
);

