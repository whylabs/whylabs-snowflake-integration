CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION whylabs_upload_integration
  ALLOWED_NETWORK_RULES = (whylabs_api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (whylabs_api_key, whylabs_org_id)
  ENABLED = true;
