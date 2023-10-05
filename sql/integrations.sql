CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION whylabs_integration
  ALLOWED_NETWORK_RULES = (whylabs_profiling_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (segment_columns, data_grouper_freq)
  ENABLED = true;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION whylabs_upload_integration
  ALLOWED_NETWORK_RULES = (whylabs_api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (whylabs_api_key, whylabs_org_id, whylabs_dataset_id)
  ENABLED = true;
