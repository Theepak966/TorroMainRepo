-- Oracle Database Initialization Script
-- This script creates sample schemas and tables for testing

-- Create a test user/schema
CREATE USER test_user IDENTIFIED BY test_password;
GRANT CONNECT, RESOURCE, DBA TO test_user;
GRANT UNLIMITED TABLESPACE TO test_user;

-- Connect as test_user
ALTER SESSION SET CURRENT_SCHEMA = test_user;

-- Create sample tables
CREATE TABLE employees (
    employee_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    email VARCHAR2(100),
    phone_number VARCHAR2(20),
    hire_date DATE,
    job_id VARCHAR2(10),
    salary NUMBER(8,2),
    department_id NUMBER
);

CREATE TABLE departments (
    department_id NUMBER PRIMARY KEY,
    department_name VARCHAR2(100),
    location VARCHAR2(100),
    manager_id NUMBER
);

CREATE TABLE products (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR2(100),
    category VARCHAR2(50),
    price NUMBER(10,2),
    stock_quantity NUMBER,
    created_date DATE DEFAULT SYSDATE
);

CREATE TABLE orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    order_date DATE DEFAULT SYSDATE,
    total_amount NUMBER(10,2),
    status VARCHAR2(20)
);

-- Insert sample data
INSERT INTO employees VALUES (1, 'John', 'Doe', 'john.doe@example.com', '555-0101', SYSDATE, 'IT_PROG', 75000, 10);
INSERT INTO employees VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', '555-0102', SYSDATE, 'IT_PROG', 80000, 10);
INSERT INTO employees VALUES (3, 'Bob', 'Johnson', 'bob.johnson@example.com', '555-0103', SYSDATE, 'SALES', 60000, 20);
INSERT INTO employees VALUES (4, 'Alice', 'Williams', 'alice.williams@example.com', '555-0104', SYSDATE, 'HR', 65000, 30);

INSERT INTO departments VALUES (10, 'IT', 'Building A', 1);
INSERT INTO departments VALUES (20, 'Sales', 'Building B', 3);
INSERT INTO departments VALUES (30, 'HR', 'Building C', 4);

INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 999.99, 50, SYSDATE);
INSERT INTO products VALUES (2, 'Mouse', 'Electronics', 29.99, 200, SYSDATE);
INSERT INTO products VALUES (3, 'Keyboard', 'Electronics', 79.99, 150, SYSDATE);
INSERT INTO products VALUES (4, 'Monitor', 'Electronics', 299.99, 75, SYSDATE);

INSERT INTO orders VALUES (1, 1001, SYSDATE, 1029.98, 'COMPLETED');
INSERT INTO orders VALUES (2, 1002, SYSDATE, 379.98, 'PENDING');
INSERT INTO orders VALUES (3, 1003, SYSDATE, 1299.97, 'COMPLETED');

COMMIT;

-- Create another schema for testing schema filtering
CREATE USER hr_user IDENTIFIED BY hr_password;
GRANT CONNECT, RESOURCE TO hr_user;
GRANT UNLIMITED TABLESPACE TO hr_user;

ALTER SESSION SET CURRENT_SCHEMA = hr_user;

CREATE TABLE hr_employees (
    emp_id NUMBER PRIMARY KEY,
    emp_name VARCHAR2(100),
    emp_dept VARCHAR2(50),
    emp_salary NUMBER(8,2)
);

INSERT INTO hr_employees VALUES (1, 'HR Employee 1', 'HR', 55000);
INSERT INTO hr_employees VALUES (2, 'HR Employee 2', 'HR', 60000);

COMMIT;

-- Display created objects
SELECT 'Sample schemas and tables created successfully!' AS status FROM DUAL;

