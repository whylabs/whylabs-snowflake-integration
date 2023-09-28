use role sysadmin;

create or replace database whylogs_demo;

use whylogs_demo;

create warehouse if not exists whylogs_warehouse
    warehouse_size = 'XSMALL'
    warehouse_type = 'STANDARD'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;

use warehouse whylogs_warehouse;

-- drop table if exists employees;

create table if not exists employees (
    name STRING,
    salary INT,
    job STRING,
    state STRING,
    email STRING,
    phone STRING,
    dob STRING,
    ssn STRING,
    hire_date TIMESTAMP_NTZ,
    years_xp INT
);

-- Initially populated by ./sql/snowflake-inserts.sql

-- Duplicate the table however many times
INSERT INTO employees (name, salary, job, state, email, phone, dob, ssn, hire_date, years_xp)
SELECT name, salary, job, state, email, phone, dob, ssn, hire_date, years_xp FROM employees;


select count(1) from employees;