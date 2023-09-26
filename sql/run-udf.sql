select * from demo_table where month(dataset_timestamp) > 8 limit 10;

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
select department, profile_view
from 
    (select department, object_construct(*) as data, FLOOR(ABS(UNIFORM(0, 29, RANDOM()))) as rand from demo_table)
    ,
    table(whylogs_object(data) over (partition by department, rand))
;


select department, object_construct(*), date_trunc('DAY', dataset_timestamp) as data from demo_table limit 10;


-- Without the random partitioning


select department, to_timestamp_ntz(dataset_timestamp, 3) as dt, profile_view
from 
    (select department, object_insert(object_construct(*), 'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, dataset_timestamp), TRUE) as data from demo_table limit 10)
    ,
    table(whylogs_object(data) over (partition by department))
order by dt
;


-- Upload data to whylabs after profiling
with 
    profiles as (
        select department, profile_view
        from 
            (select department, object_construct(*) as data from demo_table)
            ,
            table(whylogs_object(data) over (partition by department)))

select profiles.profile_view, result from 
    profiles
    ,
    table(whylabs_upload(profile_view) over (partition by profile_view))
;
