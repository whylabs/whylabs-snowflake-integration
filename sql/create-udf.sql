create or replace function whylogs(data object)
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
    imports = ('@whylabs_udf_stage/v1/latest/whylogs_udf.py')
    ;


create or replace function whylabs_upload(profile_view varchar, segment_partition varchar, segment varchar)
    returns table (upload_result varchar)
    language python
    runtime_version = '3.10'
    external_access_integrations = (whylabs_upload_integration)
    secrets = ('whylabs_api_key' = whylabs_api_key, 'whylabs_org_id' = whylabs_org_id, 'whylabs_dataset_id' = whylabs_dataset_id)
    packages = ('snowflake-snowpark-python', 'requests', 'whylogs', 'whylabs-client')
    handler = 'whylabs_upload_udf.handler'
    imports = ('@whylabs_udf_stage/v1/latest/whylabs_upload_udf.py')
;
