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
select department, profile_view
from 
    (select id, name, department, age, FLOOR(ABS(UNIFORM(0, 30, RANDOM()))) as rand from demo_table)
    , 
    table(whylogs_object( { 'id': id, 'name':name, 'department':department, 'age' :age } ) over (partition by department, rand))
;

-- Without the random partitioning
select department, profile_view
from 
    (select id, name, department, age from demo_table limit 10000)
    , 
    table(whylogs_object( { 'id': id, 'name':name, 'department':department, 'age' :age } ) over (partition by department))
;


-- Upload data to whylabs after profiling
with profiles as (
    select department, profile_view
    from 
        (select id, name, department, age from demo_table limit 10000)
        , 
        table(whylogs_object({ 'id': id, 'name':name, 'department': department, 'age': age} ) over (partition by department))
)

select profiles.profile_view, result from 
    profiles
    ,
    table(whylabs_upload(profile_view) over (partition by profile_view))
;