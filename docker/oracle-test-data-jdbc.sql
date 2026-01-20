-- Create test schema for JDBC connection with 100 rows and various data sizes
-- This will test PII detection and column metadata

-- Create a comprehensive test table with various data types and PII
CREATE TABLE BANK_CUSTOMERS_JDBC (
    CUSTOMER_ID NUMBER PRIMARY KEY,
    FIRST_NAME VARCHAR2(100),
    LAST_NAME VARCHAR2(100),
    EMAIL VARCHAR2(255),
    PHONE_NUMBER VARCHAR2(20),
    SSN VARCHAR2(11),
    CREDIT_CARD_NUMBER VARCHAR2(19),
    DATE_OF_BIRTH DATE,
    ADDRESS_LINE1 VARCHAR2(200),
    ADDRESS_LINE2 VARCHAR2(200),
    CITY VARCHAR2(100),
    STATE VARCHAR2(50),
    ZIP_CODE VARCHAR2(10),
    COUNTRY VARCHAR2(50),
    ACCOUNT_BALANCE NUMBER(18,2),
    IP_ADDRESS VARCHAR2(45),
    NOTES CLOB,
    CREATED_DATE TIMESTAMP,
    UPDATED_DATE TIMESTAMP
);

-- Insert 100 rows with varying data sizes
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO BANK_CUSTOMERS_JDBC VALUES (
            i,
            CASE MOD(i, 10)
                WHEN 0 THEN 'John' || LPAD(i, 3, '0')
                WHEN 1 THEN 'Jane' || LPAD(i, 3, '0')
                WHEN 2 THEN 'Robert' || LPAD(i, 3, '0')
                WHEN 3 THEN 'Mary' || LPAD(i, 3, '0')
                WHEN 4 THEN 'Michael' || LPAD(i, 3, '0')
                WHEN 5 THEN 'Sarah' || LPAD(i, 3, '0')
                WHEN 6 THEN 'David' || LPAD(i, 3, '0')
                WHEN 7 THEN 'Emily' || LPAD(i, 3, '0')
                WHEN 8 THEN 'James' || LPAD(i, 3, '0')
                ELSE 'Patricia' || LPAD(i, 3, '0')
            END,
            CASE MOD(i, 8)
                WHEN 0 THEN 'Smith' || LPAD(i, 3, '0')
                WHEN 1 THEN 'Johnson' || LPAD(i, 3, '0')
                WHEN 2 THEN 'Williams' || LPAD(i, 3, '0')
                WHEN 3 THEN 'Brown' || LPAD(i, 3, '0')
                WHEN 4 THEN 'Jones' || LPAD(i, 3, '0')
                WHEN 5 THEN 'Garcia' || LPAD(i, 3, '0')
                WHEN 6 THEN 'Miller' || LPAD(i, 3, '0')
                ELSE 'Davis' || LPAD(i, 3, '0')
            END,
            'customer' || LPAD(i, 3, '0') || '@bank' || MOD(i, 5) || '.com',
            CASE MOD(i, 3)
                WHEN 0 THEN '+1-555-' || LPAD(i, 4, '0')
                WHEN 1 THEN '(555) ' || LPAD(i, 3, '0') || '-' || LPAD(i, 4, '0')
                ELSE '555-' || LPAD(i, 3, '0') || '-' || LPAD(i, 4, '0')
            END,
            LPAD(MOD(i, 999), 3, '0') || '-' || LPAD(MOD(i*2, 99), 2, '0') || '-' || LPAD(MOD(i*3, 9999), 4, '0'),
            CASE MOD(i, 4)
                WHEN 0 THEN '4532-' || LPAD(MOD(i, 9999), 4, '0') || '-' || LPAD(MOD(i*2, 9999), 4, '0') || '-' || LPAD(MOD(i*3, 9999), 4, '0')
                WHEN 1 THEN '5123-' || LPAD(MOD(i, 9999), 4, '0') || '-' || LPAD(MOD(i*2, 9999), 4, '0') || '-' || LPAD(MOD(i*3, 9999), 4, '0')
                WHEN 2 THEN '6011-' || LPAD(MOD(i, 9999), 4, '0') || '-' || LPAD(MOD(i*2, 9999), 4, '0') || '-' || LPAD(MOD(i*3, 9999), 4, '0')
                ELSE '3782-' || LPAD(MOD(i, 9999), 4, '0') || '-' || LPAD(MOD(i*2, 9999), 4, '0') || '-' || LPAD(MOD(i*3, 9999), 4, '0')
            END,
            TO_DATE('1980-01-01', 'YYYY-MM-DD') + MOD(i, 15000),
            LPAD(i, 3, '0') || ' Main Street',
            CASE MOD(i, 3)
                WHEN 0 THEN 'Apt ' || LPAD(i, 3, '0')
                WHEN 1 THEN 'Suite ' || LPAD(i, 2, '0')
                ELSE NULL
            END,
            CASE MOD(i, 10)
                WHEN 0 THEN 'New York'
                WHEN 1 THEN 'Los Angeles'
                WHEN 2 THEN 'Chicago'
                WHEN 3 THEN 'Houston'
                WHEN 4 THEN 'Phoenix'
                WHEN 5 THEN 'Philadelphia'
                WHEN 6 THEN 'San Antonio'
                WHEN 7 THEN 'San Diego'
                WHEN 8 THEN 'Dallas'
                ELSE 'San Jose'
            END,
            CASE MOD(i, 5)
                WHEN 0 THEN 'NY'
                WHEN 1 THEN 'CA'
                WHEN 2 THEN 'TX'
                WHEN 3 THEN 'FL'
                ELSE 'IL'
            END,
            LPAD(MOD(i, 99999), 5, '0'),
            'United States',
            ROUND(DBMS_RANDOM.VALUE(1000, 1000000), 2),
            '192.168.' || MOD(i, 255) || '.' || MOD(i*2, 255),
            CASE MOD(i, 4)
                WHEN 0 THEN 'Customer notes for account ' || i || '. This is a longer text field to test CLOB data type with varying sizes. ' || RPAD('X', 100 + MOD(i, 500), 'X')
                WHEN 1 THEN 'Account details: ' || i || ' - Premium customer with special handling requirements.'
                WHEN 2 THEN 'Notes: ' || i || RPAD(' - ', 200 + MOD(i, 300), 'A')
                ELSE 'Customer ' || i || ' information and notes.'
            END,
            SYSTIMESTAMP - MOD(i, 365),
            SYSTIMESTAMP - MOD(i, 30)
        );
    END LOOP;
    COMMIT;
END;
/

-- Create a view for testing
CREATE OR REPLACE VIEW CUSTOMER_SUMMARY_JDBC AS
SELECT 
    CUSTOMER_ID,
    FIRST_NAME || ' ' || LAST_NAME AS FULL_NAME,
    EMAIL,
    PHONE_NUMBER,
    CITY || ', ' || STATE AS LOCATION,
    ACCOUNT_BALANCE,
    CREATED_DATE
FROM BANK_CUSTOMERS_JDBC
WHERE ACCOUNT_BALANCE > 5000;

-- Create a procedure for testing
CREATE OR REPLACE PROCEDURE GET_CUSTOMER_BALANCE_JDBC(
    p_customer_id IN NUMBER,
    p_balance OUT NUMBER
) AS
BEGIN
    SELECT ACCOUNT_BALANCE INTO p_balance
    FROM BANK_CUSTOMERS_JDBC
    WHERE CUSTOMER_ID = p_customer_id;
END;
/

-- Create a function for testing
CREATE OR REPLACE FUNCTION CALCULATE_TOTAL_ASSETS_JDBC RETURN NUMBER AS
    v_total NUMBER := 0;
BEGIN
    SELECT SUM(ACCOUNT_BALANCE) INTO v_total
    FROM BANK_CUSTOMERS_JDBC;
    RETURN v_total;
END;
/

-- Create a trigger for testing
CREATE OR REPLACE TRIGGER UPDATE_CUSTOMER_TIMESTAMP_JDBC
BEFORE UPDATE ON BANK_CUSTOMERS_JDBC
FOR EACH ROW
BEGIN
    :NEW.UPDATED_DATE := SYSTIMESTAMP;
END;
/

COMMIT;
