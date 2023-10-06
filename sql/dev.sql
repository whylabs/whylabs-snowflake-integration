use database WHYLOGS_INTEGRATION_DB;

create or replace function whylogs_dev(data object)
    returns table (
        profile_view varchar,
        dataset_timestamp int,
        segment_partition varchar,
        segment varchar,
        rows_processed int,
        debug_info varchar
    )
    language python
    runtime_version = '3.10'
    external_access_integrations = (whylabs_integration)
    secrets = ('data_grouper_freq' = data_grouper_freq, 'segment_columns' = segment_columns)
    packages = ('snowflake-snowpark-python', 'whylogs', 'pandas')
    handler = 'whylogs_udf.handler'
    imports = ('@dev/whylogs_udf.py')
    ;


create or replace function whylabs_upload_dev(profile_view varchar, segment_partition varchar, segment varchar)
    returns table (upload_result varchar)
    language python
    runtime_version = '3.10'
    external_access_integrations = (whylabs_upload_integration)
    secrets = ('whylabs_api_key' = whylabs_api_key, 'whylabs_org_id' = whylabs_org_id, 'whylabs_dataset_id' = whylabs_dataset_id)
    packages = ('snowflake-snowpark-python', 'requests', 'whylogs', 'whylabs-client')
    handler = 'whylabs_upload_udf.handler'
    imports = ('@dev/whylabs_upload_udf.py')
;


select profile_view, segment_partition, segment, rows_processed, debug_info 
from 
    (
        select 
            date_trunc('DAY', hire_date) as day,
            state,
            object_insert(object_construct(*), 'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)) as data,
            FLOOR(ABS(UNIFORM(0, 9, RANDOM()))) as rand
        from employees
        where --day='2023-09-20 00:00:00.000'::timestamp
            date_trunc('DAY', hire_date) >='2023-09-20 00:00:00.000'::timestamp
            and
            date_trunc('DAY', hire_date) <= '2023-09-22 00:00:00.000'::timestamp
    )
    ,
    table(whylogs_dev(data) over (partition by day, state, rand))
;


with 
    profiles as (
        select day, state, profile_view, segment_partition, segment, rows_processed, debug_info
        from 
            (
                select 
                    date_trunc('DAY', hire_date) as day,
                    state,
                    object_insert(object_construct(*), 'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)) as data 
                from employees
                where day = '2023-10-03 00:00:00.000'::timestamp
            )
            ,
            table(whylogs_dev(data) over (partition by day, state))
)
select upload_result 
from 
    profiles
    ,
    table(whylabs_upload_dev(profile_view, segment_partition, segment) over (partition by day, state))
;


