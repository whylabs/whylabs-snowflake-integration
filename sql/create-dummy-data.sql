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

create table if not exists demo_table (
    id INT PRIMARY KEY,
    name STRING,
    age INT,
    department STRING
);

-- Initially populated by ./sql/snowflake-inserts.sql

-- Duplicate the table however many times
INSERT INTO demo_table (id, name, age, department)
    SELECT id, name, age, department
    FROM demo_table;