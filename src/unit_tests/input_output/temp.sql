
-- Drop the database if it exists
DROP DATABASE IF EXISTS abcde_test;

CREATE DATABASE abcde_test;

CREATE TABLE abcde_test.test_ab (
    a INT PRIMARY KEY,
    b VARCHAR(4)
);

CREATE TABLE abcde_test.test_cde (
    c VARCHAR(4),
    d VARCHAR(50),
    e INT PRIMARY KEY
);
