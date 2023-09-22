use schema WHYLOGS_DEMO.PUBLIC;

create or replace function whylogs(id int, name string, department string, age int)
returns table (profile_view varchar, total_processed int, random_number int)
language python
runtime_version = '3.10'
packages = ('whylogs', 'pandas')
handler = 'whylogs_udf.handler'
imports = ('@funcs/whylogs_udf.py')
;

create or replace function whylogs_chunk(id int, name string, department string, age int)
returns table (profile_view varchar, total_processed int, random_number int)
language python
runtime_version = '3.10'
packages = ('whylogs', 'pandas')
handler = 'whylogs_chunk_udf.handler'
imports = ('@funcs/whylogs_chunk_udf.py')
;

create or replace function whylogs_array(data array)
returns table (profile_view varchar, total_processed int, random_number int)
language python
runtime_version = '3.10'
packages = ('whylogs', 'pandas')
handler = 'whylogs_array_udf.handler'
imports = ('@funcs/whylogs_array_udf.py')
;

create or replace function whylogs_object(data object)
returns table (profile_view varchar, total_processed int, random_number int)
language python
runtime_version = '3.10'
packages = ('whylogs', 'pandas')
handler = 'whylogs_object_udf.handler'
imports = ('@funcs/whylogs_object_udf.py')
;

select count(*) from demo_table;

-- Just to see the data
SELECT id, NAME, department, FLOOR(ABS(UNIFORM(0, 3, RANDOM()))) as rand, max(age) OVER (partition by department) as max_age FROM demo_table limit 10;


-- Run the regular UDF
select department, profile_view, total_processed from (select * from demo_table limit 4000) , table(whylogs(id, name, department, age) over (partition by 1));



-- Run the regular UDF with hacky query chunking
select department, profile_view, total_processed 
from 
    (select id, name, department, age, FLOOR(ABS(UNIFORM(0, 30, RANDOM()))) as rand from demo_table)
    , 
    table(whylogs(id, name, department, age) over (partition by department, rand))
;


-- Run the chunked UDF on a subset of data
select department, profile_view, total_processed from (select * from demo_table limit 4000) , table(whylogs_chunk(id, name, department, age) over (partition by 1));

-- Run the chunked UDF on a entire table
select department, profile_view, total_processed from demo_table , table(whylogs_chunk(id, name, department, age) over (partition by 1));

-- Run the array UDF
select department, profile_view, total_processed 
from 
    (select id, name, department, age, FLOOR(ABS(UNIFORM(0, 30, RANDOM()))) as rand from demo_table limit 40)
    , 
    table(whylogs_array([id, name, department, age]) over (partition by department))
;


-- Run the object UDF
-- Probably pretty good as a generic thing that people can just reference from our public s3. They'll have to make their own if they
-- want to avoid the overhead of dataframe parsing though, and they'll have to generate this query if they have a ton
-- of columns because of the naming, unless I can find a way to make the col name flow through TODO.
select department, profile_view, total_processed 
from 
    (select id, name, department, age, FLOOR(ABS(UNIFORM(0, 30, RANDOM()))) as rand from demo_table)
    , 
    table(whylogs_object( { 'id': id, 'name':name, 'department':department, 'age' :age } ) over (partition by department, rand))
;
