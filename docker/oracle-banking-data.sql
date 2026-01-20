-- Oracle Banking Data Setup Script
-- Creates banking-related tables and data for testing discovery

-- Connect as test_user
ALTER SESSION SET CURRENT_SCHEMA = test_user;

-- Drop existing tables if they exist (for clean setup)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE transactions CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE accounts CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE loans CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE credit_cards CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

-- Create Customers table
CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    email VARCHAR2(100) UNIQUE,
    phone_number VARCHAR2(20),
    date_of_birth DATE,
    address_line1 VARCHAR2(100),
    address_line2 VARCHAR2(100),
    city VARCHAR2(50),
    state VARCHAR2(50),
    zip_code VARCHAR2(10),
    country VARCHAR2(50) DEFAULT 'USA',
    customer_type VARCHAR2(20) DEFAULT 'INDIVIDUAL',
    created_date DATE DEFAULT SYSDATE,
    status VARCHAR2(20) DEFAULT 'ACTIVE'
);

-- Create Accounts table
CREATE TABLE accounts (
    account_id NUMBER PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    account_number VARCHAR2(20) UNIQUE NOT NULL,
    account_type VARCHAR2(20) NOT NULL, -- CHECKING, SAVINGS, BUSINESS
    balance NUMBER(15,2) DEFAULT 0.00,
    currency VARCHAR2(3) DEFAULT 'USD',
    interest_rate NUMBER(5,4) DEFAULT 0.0000,
    opened_date DATE DEFAULT SYSDATE,
    closed_date DATE,
    status VARCHAR2(20) DEFAULT 'ACTIVE',
    CONSTRAINT fk_accounts_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Create Transactions table
CREATE TABLE transactions (
    transaction_id NUMBER PRIMARY KEY,
    account_id NUMBER NOT NULL,
    transaction_type VARCHAR2(20) NOT NULL, -- DEPOSIT, WITHDRAWAL, TRANSFER, PAYMENT
    amount NUMBER(15,2) NOT NULL,
    transaction_date DATE DEFAULT SYSDATE,
    description VARCHAR2(200),
    reference_number VARCHAR2(50),
    status VARCHAR2(20) DEFAULT 'COMPLETED',
    balance_after NUMBER(15,2),
    CONSTRAINT fk_transactions_account FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

-- Create Loans table
CREATE TABLE loans (
    loan_id NUMBER PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    account_id NUMBER,
    loan_number VARCHAR2(20) UNIQUE NOT NULL,
    loan_type VARCHAR2(30) NOT NULL, -- PERSONAL, MORTGAGE, AUTO, BUSINESS
    principal_amount NUMBER(15,2) NOT NULL,
    interest_rate NUMBER(5,4) NOT NULL,
    term_months NUMBER NOT NULL,
    monthly_payment NUMBER(10,2) NOT NULL,
    outstanding_balance NUMBER(15,2) NOT NULL,
    issued_date DATE DEFAULT SYSDATE,
    maturity_date DATE,
    status VARCHAR2(20) DEFAULT 'ACTIVE',
    CONSTRAINT fk_loans_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_loans_account FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

-- Create Credit Cards table
CREATE TABLE credit_cards (
    card_id NUMBER PRIMARY KEY,
    customer_id NUMBER NOT NULL,
    account_id NUMBER,
    card_number VARCHAR2(16) UNIQUE NOT NULL,
    card_type VARCHAR2(20) NOT NULL, -- VISA, MASTERCARD, AMEX
    credit_limit NUMBER(10,2) NOT NULL,
    available_credit NUMBER(10,2) NOT NULL,
    current_balance NUMBER(10,2) DEFAULT 0.00,
    issued_date DATE DEFAULT SYSDATE,
    expiry_date DATE NOT NULL,
    status VARCHAR2(20) DEFAULT 'ACTIVE',
    CONSTRAINT fk_credit_cards_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_credit_cards_account FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

-- Insert sample customers
INSERT INTO customers VALUES (1001, 'John', 'Smith', 'john.smith@email.com', '555-1001', DATE '1985-03-15', '123 Main St', 'Apt 4B', 'New York', 'NY', '10001', 'USA', 'INDIVIDUAL', SYSDATE, 'ACTIVE');
INSERT INTO customers VALUES (1002, 'Sarah', 'Johnson', 'sarah.johnson@email.com', '555-1002', DATE '1990-07-22', '456 Oak Ave', NULL, 'Los Angeles', 'CA', '90001', 'USA', 'INDIVIDUAL', SYSDATE, 'ACTIVE');
INSERT INTO customers VALUES (1003, 'Michael', 'Williams', 'michael.williams@email.com', '555-1003', DATE '1978-11-08', '789 Pine Rd', NULL, 'Chicago', 'IL', '60601', 'USA', 'INDIVIDUAL', SYSDATE, 'ACTIVE');
INSERT INTO customers VALUES (1004, 'Emily', 'Brown', 'emily.brown@email.com', '555-1004', DATE '1992-05-30', '321 Elm St', 'Suite 200', 'Houston', 'TX', '77001', 'USA', 'INDIVIDUAL', SYSDATE, 'ACTIVE');
INSERT INTO customers VALUES (1005, 'David', 'Jones', 'david.jones@email.com', '555-1005', DATE '1988-09-12', '654 Maple Dr', NULL, 'Phoenix', 'AZ', '85001', 'USA', 'BUSINESS', SYSDATE, 'ACTIVE');

-- Insert sample accounts
INSERT INTO accounts VALUES (2001, 1001, 'ACC-001-1001', 'CHECKING', 5000.00, 'USD', 0.0000, DATE '2020-01-15', NULL, 'ACTIVE');
INSERT INTO accounts VALUES (2002, 1001, 'ACC-002-1001', 'SAVINGS', 25000.00, 'USD', 0.0150, DATE '2020-01-15', NULL, 'ACTIVE');
INSERT INTO accounts VALUES (2003, 1002, 'ACC-001-1002', 'CHECKING', 3200.00, 'USD', 0.0000, DATE '2021-03-20', NULL, 'ACTIVE');
INSERT INTO accounts VALUES (2004, 1002, 'ACC-002-1002', 'SAVINGS', 15000.00, 'USD', 0.0125, DATE '2021-03-20', NULL, 'ACTIVE');
INSERT INTO accounts VALUES (2005, 1003, 'ACC-001-1003', 'CHECKING', 8500.00, 'USD', 0.0000, DATE '2019-06-10', NULL, 'ACTIVE');
INSERT INTO accounts VALUES (2006, 1004, 'ACC-001-1004', 'CHECKING', 1200.00, 'USD', 0.0000, DATE '2022-02-14', NULL, 'ACTIVE');
INSERT INTO accounts VALUES (2007, 1005, 'ACC-001-1005', 'BUSINESS', 50000.00, 'USD', 0.0000, DATE '2018-11-05', NULL, 'ACTIVE');

-- Insert sample transactions
INSERT INTO transactions VALUES (3001, 2001, 'DEPOSIT', 5000.00, DATE '2024-01-15', 'Initial deposit', 'REF-001', 'COMPLETED', 5000.00);
INSERT INTO transactions VALUES (3002, 2001, 'WITHDRAWAL', -500.00, DATE '2024-01-20', 'ATM withdrawal', 'REF-002', 'COMPLETED', 4500.00);
INSERT INTO transactions VALUES (3003, 2001, 'TRANSFER', -1000.00, DATE '2024-01-25', 'Transfer to savings', 'REF-003', 'COMPLETED', 3500.00);
INSERT INTO transactions VALUES (3004, 2002, 'TRANSFER', 1000.00, DATE '2024-01-25', 'Transfer from checking', 'REF-003', 'COMPLETED', 26000.00);
INSERT INTO transactions VALUES (3005, 2003, 'DEPOSIT', 3200.00, DATE '2024-02-01', 'Salary deposit', 'REF-004', 'COMPLETED', 3200.00);
INSERT INTO transactions VALUES (3006, 2003, 'PAYMENT', -150.00, DATE '2024-02-05', 'Utility bill payment', 'REF-005', 'COMPLETED', 3050.00);
INSERT INTO transactions VALUES (3007, 2005, 'DEPOSIT', 8500.00, DATE '2024-02-10', 'Initial deposit', 'REF-006', 'COMPLETED', 8500.00);
INSERT INTO transactions VALUES (3008, 2007, 'DEPOSIT', 50000.00, DATE '2024-02-15', 'Business account opening', 'REF-007', 'COMPLETED', 50000.00);

-- Insert sample loans
INSERT INTO loans VALUES (4001, 1001, 2002, 'LOAN-001-1001', 'MORTGAGE', 300000.00, 0.0350, 360, 1347.00, 285000.00, DATE '2020-06-01', DATE '2050-06-01', 'ACTIVE');
INSERT INTO loans VALUES (4002, 1002, 2004, 'LOAN-001-1002', 'AUTO', 25000.00, 0.0450, 60, 465.00, 20000.00, DATE '2023-01-15', DATE '2028-01-15', 'ACTIVE');
INSERT INTO loans VALUES (4003, 1003, 2005, 'LOAN-001-1003', 'PERSONAL', 15000.00, 0.0750, 36, 465.00, 12000.00, DATE '2023-08-20', DATE '2026-08-20', 'ACTIVE');
INSERT INTO loans VALUES (4004, 1005, 2007, 'LOAN-001-1005', 'BUSINESS', 100000.00, 0.0550, 120, 1087.00, 95000.00, DATE '2022-03-10', DATE '2032-03-10', 'ACTIVE');

-- Insert sample credit cards
INSERT INTO credit_cards VALUES (5001, 1001, 2001, '4532123456789012', 'VISA', 10000.00, 7500.00, 2500.00, DATE '2023-01-01', DATE '2027-01-01', 'ACTIVE');
INSERT INTO credit_cards VALUES (5002, 1002, 2003, '5555123456789012', 'MASTERCARD', 8000.00, 6800.00, 1200.00, DATE '2023-05-15', DATE '2027-05-15', 'ACTIVE');
INSERT INTO credit_cards VALUES (5003, 1003, 2005, '378912345678901', 'AMEX', 15000.00, 12000.00, 3000.00, DATE '2022-11-20', DATE '2026-11-20', 'ACTIVE');
INSERT INTO credit_cards VALUES (5004, 1004, 2006, '4532987654321098', 'VISA', 5000.00, 4500.00, 500.00, DATE '2024-01-10', DATE '2028-01-10', 'ACTIVE');

COMMIT;

-- Create views for reporting
CREATE OR REPLACE VIEW customer_account_summary AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(a.account_id) AS total_accounts,
    SUM(a.balance) AS total_balance,
    MAX(a.opened_date) AS latest_account_date
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
WHERE c.status = 'ACTIVE' AND (a.status = 'ACTIVE' OR a.status IS NULL)
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

CREATE OR REPLACE VIEW transaction_summary AS
SELECT 
    a.account_id,
    a.account_number,
    a.account_type,
    COUNT(t.transaction_id) AS transaction_count,
    SUM(CASE WHEN t.transaction_type = 'DEPOSIT' THEN t.amount ELSE 0 END) AS total_deposits,
    SUM(CASE WHEN t.transaction_type = 'WITHDRAWAL' THEN t.amount ELSE 0 END) AS total_withdrawals,
    SUM(CASE WHEN t.transaction_type = 'TRANSFER' THEN ABS(t.amount) ELSE 0 END) AS total_transfers,
    MAX(t.transaction_date) AS last_transaction_date
FROM accounts a
LEFT JOIN transactions t ON a.account_id = t.account_id
WHERE a.status = 'ACTIVE'
GROUP BY a.account_id, a.account_number, a.account_type;

CREATE OR REPLACE VIEW loan_summary AS
SELECT 
    l.loan_id,
    l.loan_number,
    c.first_name || ' ' || c.last_name AS customer_name,
    l.loan_type,
    l.principal_amount,
    l.outstanding_balance,
    l.monthly_payment,
    l.status,
    ROUND((l.principal_amount - l.outstanding_balance) / l.principal_amount * 100, 2) AS percent_paid
FROM loans l
JOIN customers c ON l.customer_id = c.customer_id
WHERE l.status = 'ACTIVE';

-- Create a stored procedure for account balance update
CREATE OR REPLACE PROCEDURE update_account_balance(
    p_account_id IN NUMBER,
    p_transaction_amount IN NUMBER,
    p_new_balance OUT NUMBER
) AS
BEGIN
    UPDATE accounts
    SET balance = balance + p_transaction_amount
    WHERE account_id = p_account_id;
    
    SELECT balance INTO p_new_balance
    FROM accounts
    WHERE account_id = p_account_id;
    
    COMMIT;
END;
/

-- Create a function to calculate total customer assets
CREATE OR REPLACE FUNCTION get_customer_total_assets(
    p_customer_id IN NUMBER
) RETURN NUMBER AS
    v_total_assets NUMBER := 0;
BEGIN
    SELECT NVL(SUM(balance), 0) INTO v_total_assets
    FROM accounts
    WHERE customer_id = p_customer_id AND status = 'ACTIVE';
    
    RETURN v_total_assets;
END;
/

-- Display summary
SELECT 'Banking data created successfully!' AS status FROM DUAL;
SELECT 'Customers: ' || COUNT(*) AS summary FROM customers;
SELECT 'Accounts: ' || COUNT(*) AS summary FROM accounts;
SELECT 'Transactions: ' || COUNT(*) AS summary FROM transactions;
SELECT 'Loans: ' || COUNT(*) AS summary FROM loans;
SELECT 'Credit Cards: ' || COUNT(*) AS summary FROM credit_cards;
SELECT 'Views: 3' AS summary FROM DUAL;
SELECT 'Procedures: 1' AS summary FROM DUAL;
SELECT 'Functions: 1' AS summary FROM DUAL;



