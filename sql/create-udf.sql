use schema WHYLOGS_DEMO.PUBLIC;

create stage if not exists funcs; 

-- This can't be done in a single command because of a bug in the snowflake python connector
put file://./udfs/*.py @funcs/ auto_compress=false overwrite=true;

create or replace function whylogs(id int, name string, department string, age int)
    returns table (profile_view varchar, total_processed int, random_number int)
    language python
    runtime_version = '3.10'
    packages = ('whylogs', 'pandas')
    handler = 'whylogs_udf.handler'
    imports = ('@funcs/whylogs_udf.py')
    ;

create or replace function whylogs_chunk(id int, name string, department string, age int)
    returns table (profile_view varchar, total_processed int, random_number int)
    language python
    runtime_version = '3.10'
    packages = ('whylogs', 'pandas')
    handler = 'whylogs_chunk_udf.handler'
    imports = ('@funcs/whylogs_chunk_udf.py')
    ;

create or replace function whylogs_array(data array)
    returns table (profile_view varchar, total_processed int, random_number int)
    language python
    runtime_version = '3.10'
    packages = ('whylogs', 'pandas')
    handler = 'whylogs_array_udf.handler'
    imports = ('@funcs/whylogs_array_udf.py')
    ;

create or replace function whylogs_object(data object)
    returns table (profile_view varchar)
    language python
    runtime_version = '3.10'
    packages = ('whylogs', 'pandas')
    handler = 'whylogs_object_udf.handler'
    imports = ('@funcs/whylogs_object_udf.py')
    ;

create or replace function whylabs_upload(profile_view varchar)
    returns table (profile_view varchar, result varchar)
    language python
    runtime_version = '3.10'
    external_access_integrations = (whylabs_integration)
    secrets = ('whylabs_api_key' = whylabs_api_key )
    packages = ('snowflake-snowpark-python','requests', 'whylogs', 'whylabs-client')
    handler = 'whylabs_upload_udf.handler'
    imports = ('@funcs/whylabs_upload_udf.py')
    ;

