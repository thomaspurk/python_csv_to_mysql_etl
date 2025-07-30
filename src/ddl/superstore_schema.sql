-- Drop the "superstore" database if it exists
DROP DATABASE IF EXISTS superstore;

-- Create the "superstore" database
CREATE DATABASE superstore;

-- customers table
CREATE TABLE superstore.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50)
);

-- orders table
CREATE TABLE superstore.orders (
    order_id VARCHAR(20) PRIMARY KEY,
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    customer_id VARCHAR(20)
);

-- products table
CREATE TABLE superstore.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255),
    category_name VARCHAR(100),
    sub_category_name VARCHAR(100)
);

-- order_details table
CREATE TABLE superstore.order_details (
    row_id INT PRIMARY KEY,
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    sales DECIMAL(10, 2),
    quantity INT,
    discount DECIMAL(4, 2),
    profit DECIMAL(10, 4)
);
