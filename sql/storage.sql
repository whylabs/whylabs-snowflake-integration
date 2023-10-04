
CREATE OR REPLACE STORAGE INTEGRATION whylabs_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::207285235248:role/public-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://whylabs-snowflake-udfs/udfs/')
;


CREATE OR REPLACE STAGE whylabs_udf_stage
  STORAGE_INTEGRATION = whylabs_s3_integration
  URL = 's3://whylabs-snowflake-udfs/udfs/'
;
