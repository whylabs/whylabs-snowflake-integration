select count(1) from employees; 

-- Run the regular UDF
select hire_date, profile_view, total_processed from (select * from employees limit 4000) , table(whylogs(id, name, hire_date, age) over (partition by 1));


-- Run the regular UDF with hacky query chunking
select hire_date, profile_view, total_processed 
from 
    (select id, name, hire_date, age, FLOOR(ABS(UNIFORM(0, 30, RANDOM()))) as rand from employees)
    , 
    table(whylogs(id, name, hire_date, age) over (partition by hire_date, rand))
;


-- Run the chunked UDF on a subset of data
select hire_date, profile_view, total_processed from (select * from employees limit 4000) , table(whylogs_chunk(id, name, hire_date, age) over (partition by 1));

-- Run the chunked UDF on a entire table
select hire_date, profile_view, total_processed from employees , table(whylogs_chunk(id, name, hire_date, age) over (partition by 1));

-- Run the array UDF
select hire_date, profile_view, total_processed 
from 
    (select id, name, hire_date, age, FLOOR(ABS(UNIFORM(0, 30, RANDOM()))) as rand from employees limit 40)
    , 
    table(whylogs_array([id, name, hire_date, age]) over (partition by hire_date))
;

-- Run the object UDF
select hire_date, profile_view
from 
    (select hire_date, object_construct(*) as data, FLOOR(ABS(UNIFORM(0, 29, RANDOM()))) as rand from employees)
    ,
    table(whylogs_object(data) over (partition by hire_date, rand))
;


select hire_date, object_construct(*), date_trunc('DAY', hire_date) as day from employees order by day desc limit 10;
select hire_date, object_construct(*), date_trunc('DAY', hire_date) as day from employees where day='2023-09-26 00:00:00.000'::timestamp  limit 10;

select count(1) from employees where date_trunc('DAY', hire_date)='2023-09-26 00:00:00.000'::timestamp;
select count(1) from employees;

select object_construct(*) from employees limit 10;


select profile_view, segment_partition, segment
from 
    (select state, hire_date, object_insert(object_construct(*), 'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date), TRUE) as data from employees)
    ,
    table(whylogs(data) over (partition by state))
;


-- Profile data with whylogs
select profile_view, segment_partition, segment
from 
    (select state, hire_date, object_insert(object_construct(*), 'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date), TRUE) as data from employees)
    ,
    table(whylogs(data) over (partition by state))
;


-- Profile segmented data with whylogs
select profile_view, segment_partition, segment, rows_processed, debug_info 
from 
    (
        select 
            date_trunc('DAY', hire_date) as day,
            state,
            object_insert(object_construct(*), 'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)) as data
        from employees 
        where day>='2023-09-20 00:00:00.000'::timestamp
        limit 3
    )
    ,
    table(whylogs(data) over (partition by day, state))
;


-- Upload segmented data to whylabs after profiling for a single day
with 
    profiles as (
        select day, profile_view, segment_partition, segment, rows_processed, debug_info
        from 
            (
                select 
                    date_trunc('DAY', hire_date) as day,
                    state,
                    object_insert(object_construct(*), 'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)) as data 
                from employees
                where 
                    day >= '2023-09-10 00:00:00.000'::timestamp
                    and
                    day <= '2023-09-19 00:00:00.000'::timestamp
            )
            ,
            table(whylogs(data) over (partition by day, state))
)
select upload_result 
from 
    profiles
    ,
    table(whylabs_upload(profile_view, segment_partition, segment) over (partition by day))
;


