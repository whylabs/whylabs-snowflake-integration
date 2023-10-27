-- Example of generating profiles
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
            ) AS data
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
FROM raw_data, TABLE(whylogs(data) OVER (PARTITION BY day, state))
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
FROM raw_data, TABLE(whylogs(data) OVER (PARTITION BY day, state))
;

-- Example of generating and uploading profiles to WhyLabs
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
                'SEGMENT_COLUMNS', 'STATE'  -- Specify the columns to segment on as an all-caps CSV
            ) AS data,
            FLOOR(ABS(UNIFORM(0, 9, RANDOM()))) as rand
        FROM employees
        WHERE day = '2023-10-03 00:00:00.000'::timestamp
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
        TABLE(whylogs(data) OVER (PARTITION BY day, state))
    ),
    upload_data AS ( -- Format the input for the whylabs upload function
        SELECT
            day,
            state,
            object_construct(
                'WHYLABS_DATASET_ID', 'model-97', -- Specify the model id we're uploading to
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
    TABLE(whylabs_upload(upload_object) OVER (PARTITION BY day, state));
;



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
        TABLE(whylogs(data) OVER (PARTITION BY day))
    ),
    upload_data AS ( -- Format the input for the whylabs upload function
        SELECT
            day,
            state,
            object_construct(
                'WHYLABS_DATASET_ID', 'model-97', -- Specify the model id we're uploading to
                'PROFILE_VIEW', profile_view
            ) AS upload_object
        FROM profiled_data
    )
SELECT
    dataset_id, result, error, to_timestamp(dataset_timestamp::varchar), segment
FROM
    upload_data,
    TABLE(whylabs_upload(upload_object) OVER (PARTITION BY day));
;
