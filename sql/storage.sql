
CREATE OR REPLACE STORAGE INTEGRATION whylabs_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::207285235248:role/public-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://guest-session-testing-public/udfs/')
;
--   STORAGE_BLOCKED_LOCATIONS = ();


CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = whylabs_s3_integration
  URL = 's3://guest-session-testing-public/udfs/'
;


create or replace function whylogs_public(data object)
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
    imports = ('@my_s3_stage/whylogs_udf.py')
    ;