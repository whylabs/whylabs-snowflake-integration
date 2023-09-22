create table if not exists demo_table (
    id INT PRIMARY KEY,
    name STRING,
    age INT,
    department STRING
);


-- Duplicate th rows
INSERT INTO demo_table (id, name, age, department)
SELECT id, name, age, department
FROM demo_table;