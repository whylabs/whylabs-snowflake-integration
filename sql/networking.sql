
CREATE OR REPLACE NETWORK RULE whylabs_api_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.whylabsapp.com', 'log.whylabsapp.com', 'songbird-20201223060057342600000001.s3.us-west-2.amazonaws.com');