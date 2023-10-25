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
    packages = ('snowflake-snowpark-python', 'whylogs', 'pandas')
    handler = 'whylogs_udf.handler'
    imports = ('@dev/whylogs_udf.py')
;


create or replace function whylabs_upload_dev(data object)
    returns table (
        dataset_id varchar,
        result varchar,
        error varchar,
        dataset_timestamp int,
        segment varchar
    )
    language python
    runtime_version = '3.10'
    external_access_integrations = (whylabs_upload_integration)
    secrets = ('whylabs_api_key' = whylabs_api_key, 'whylabs_org_id' = whylabs_org_id)
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
            object_insert(
                object_insert(
                    object_construct(*), 
                    'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)
                ),
                'SEGMENT_COLUMNS', 'STATE'
            ) as data,
            FLOOR(ABS(UNIFORM(0, 9, RANDOM()))) as rand
        from employees
        where 
            -- day='2023-09-20 00:00:00.000'::timestamp
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
                    object_insert(
                        object_insert(
                            object_construct(*), 
                            'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)
                        ),
                        'SEGMENT_COLUMNS', 'STATE'
                    ) as data,
                    FLOOR(ABS(UNIFORM(0, 9, RANDOM()))) as rand
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
    table(whylabs_upload_dev(
        object_construct(
            'WHYLABS_DATASET_ID', 'model-90',
            'PROFILE_VIEW', profile_view,
            'SEGMENT_PARTITION', segment_partition,
            'SEGMENT', segment)
    ) over (partition by day, state))
;

 -- Query a day's worth of data 
SELECT 
    count(1)
FROM employees
WHERE date_trunc('DAY', hire_date)= '2023-10-03 00:00:00.000'::timestamp and state = 'TX'
;

WITH 
    raw_data AS ( -- Query a day's worth of data 
        SELECT 
            date_trunc('DAY', hire_date) AS day,
            state,
            object_insert(
                object_insert(
                    object_construct(*), 
                    'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)
                ),
                'SEGMENT_COLUMNS', 'STATE'
            ) AS data,
            FLOOR(ABS(UNIFORM(0, 9, RANDOM()))) as rand
        FROM employees
        WHERE day = '2023-10-03 00:00:00.000'::timestamp
    )
-- Use whylogs to profile data
SELECT 
    day, 
    state, 
    profile_view, 
    segment_partition, 
    segment, 
    rows_processed, 
    debug_info
FROM raw_data, TABLE(whylogs_dev(data) OVER (PARTITION BY day, state))
;


-- Query and upload data
WITH 
    raw_data AS ( -- Query a day's worth of data 
        SELECT 
            date_trunc('DAY', hire_date) AS day,
            state,
            object_insert(
                object_insert(
                    object_construct(*), 
                    'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)
                ),
                'SEGMENT_COLUMNS', 'STATE' -- Specify the columns to segment on as an all-caps CSV 
            ) AS data,
            FLOOR(ABS(UNIFORM(0, 9, RANDOM()))) as rand
        FROM employees
        WHERE day <= '2023-10-03 00:00:00.000'::timestamp
            and day > '2023-10-01 00:00:00.000'::timestamp
    ),
    profiled_data AS (  -- Use whylogs to profile data
        SELECT 
            day, 
            state, 
            profile_view, 
            segment_partition, 
            segment, 
            rows_processed, 
            debug_info
        FROM raw_data,
        TABLE(whylogs_dev(data) OVER (PARTITION BY day, state))
    ),
    upload_data AS ( -- Format the input for the whylabs upload function
        SELECT 
            day,
            state,
            object_construct(
                'WHYLABS_DATASET_ID', 'model-96', -- Specify tGhe model id we're uploading to
                'PROFILE_VIEW', profile_view,
                'SEGMENT_PARTITION', segment_partition,
                'SEGMENT', segment
            ) AS upload_object
        FROM profiled_data
    )
SELECT 
    dataset_id, result, error, dataset_timestamp, segment
FROM 
    upload_data,
    TABLE(whylabs_upload_dev(upload_object) OVER (PARTITION BY day, state));
;



-- Same, but no segmentation
WITH 
    raw_data AS ( -- Query a day's worth of data 
        SELECT 
            date_trunc('DAY', hire_date) AS day,
            state,
            object_insert(
                object_construct(*), 
                'DATASET_TIMESTAMP', date_part(EPOCH_MILLISECONDS, hire_date)
            ) AS data
        FROM employees
        WHERE day <= '2023-10-03 00:00:00.000'::timestamp
            and day > '2023-10-01 00:00:00.000'::timestamp
    ),
    profiled_data AS (  -- Use whylogs to profile data
        SELECT 
            day, 
            state, 
            profile_view, 
            segment_partition, 
            segment, 
            rows_processed, 
            debug_info
        FROM raw_data,
        TABLE(whylogs_dev(data) OVER (PARTITION BY day))
    ),
    upload_data AS ( -- Format the input for the whylabs upload function
        SELECT 
            day,
            state,
            object_construct(
                'WHYLABS_DATASET_ID', 'model-96', -- Specify the model id we're uploading to
                'PROFILE_VIEW', profile_view
            ) AS upload_object
        FROM profiled_data
    )
SELECT 
    dataset_id, result, error, to_timestamp(dataset_timestamp::varchar), segment
FROM 
    upload_data,
    TABLE(whylabs_upload_dev(upload_object) OVER (PARTITION BY day));
;