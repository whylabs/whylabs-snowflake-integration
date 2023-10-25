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
    packages = ('snowflake-snowpark-python', 'whylogs', 'pandas')
    handler = 'whylogs_udf.handler'
    imports = ('@whylabs_udf_stage/v1/latest/whylogs_udf.py')
;


create or replace function whylabs_upload(data object)
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
    imports = ('@whylabs_udf_stage/v1/latest/whylabs_upload_udf.py')
;


