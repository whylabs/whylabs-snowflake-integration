# End to End Tutorial

## Optional, create some data
If you already have data then go ahead and skip this. If you want a quick way to test on fake data then you can run 

```bash
snowsql -f ./sql/create-dummy-data.sql
python ./generate-data.py
snowsql -f ./sql/snowflake-inserts.sql
```

## Enable conda repos

Follow the [Snowflake instructions](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages#getting-started) for enabling the use of third party packages. You'll be using whylogs as a Python UDTF from the private Snowflake conda repo and it's disabled by default until you complete their form.

## Add network permissions
Add a network rule to allow traffic to the WhyLabs platform. This is required in order to upload the generated profile data to WhyLabs for monitoring. No raw data is uploaded, only profiles.

```bash
snowsql ./sql/networking.sql
```

or 

```sql
CREATE OR REPLACE NETWORK RULE whylabs_api_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.whylabsapp.com', 'log.whylabsapp.com', 'songbird-20201223060057342600000001.s3.us-west-2.amazonaws.com');
```

You may not need the aws domain here if you have a newer org. Newer orgs use a dedicated endpoint `log.whylabsapp.com`. If you want your older org to use this then let us know. If you don't know which one your org uses then just try removing the aws domain and see if it fails.

## Create API key secret
Create a secret that contains your WhyLabs API key. You can get this key from your [account settings](https://hub.whylabsapp.com/settings/access-tokens).


> ⚠️ WhyLabs recently changed its api key format to include the org id at the end of the api key, like `:org-12345`. The version of whylogs in Snowflake is a little behind Pypi and it's missing an update that allows it to use this new format. If you have an api key with the org id at the end then you might have to remove the org id from the api key. So, you would go from `xxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:org-12345` to  `xxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`.

```bash
# Update this file with your api key
snowsql ./sql/secrets.sql
```

or

```sql
CREATE OR REPLACE SECRET whylabs_api_key
  TYPE = GENERIC_STRING
  SECRET_STRING = 'API_KEY';  -- Update your key
```

## Create Integration Permissions
Create an External Access Integration that allows the UDTFs to use the network rule and access the secret.

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION whylabs_integration
  ALLOWED_NETWORK_RULES = (whylabs_api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (whylabs_api_key)
  ENABLED = true;
```

## Create the `whylogs_object` UDTF 

Create a Python file with the following contents (from `./udtfs/whylogs_object_udf.py`). This version of the UDTF is designed to take in a SQL `OBJECT`` type which shows up in python as a Pandas data frame with a single series of dicts, where each dict represents a row of inputs.

This method is the simplest since you don't have to specify the names of the rows in the UDTF (because they come along with the data) but takes more memory and has t be mapped into the right format before whylogs can process it. Check out `./udtfs/whylogs_udtf.py` for a more performant, hardcoded variant.

```python
import whylogs as why
import pandas as pd
import base64


class handler:
    def end_partition(self, df):
        """
        Args:
            df: When the sql type is OBJECT, the df has a single series with a bunch of dicts
        """
        # Convert the dataframe into a normally structured dataframe
        df_norm = pd.DataFrame(list(df[0]))

        # Log the dataframe with whylogs and create a profile
        profile_view = why.log(df_norm).profile().view()

        # Serialize the profile and encode it as base64
        ser = profile_view.serialize()
        base64_encoded = base64.b64encode(ser).decode('utf-8')

        # Return the results as a table
        return pd.DataFrame({"profile_view": [base64_encoded]})


handler.end_partition._sf_vectorized_input = pd.DataFrame 
```

Upload this python udtf fle to a stage in your account. We're using the `func` stage here.

```sql
create stage if not exists funcs; 

-- You might have to execute this as a standalone statement in snowsql. PUT isn't upported in various drivers.
put file://./udfs/*.py @funcs/ auto_compress=false overwrite=true;
```

Create a `whylogs_object` UDTF that uses the python source code.

```sql
create or replace function whylogs_object(data object)
    returns table (profile_view varchar)
    language python
    runtime_version = '3.10'
    packages = ('whylogs', 'pandas')
    handler = 'whylogs_object_udf.handler'
    imports = ('@funcs/whylogs_object_udf.py')
    ;
```

## Create the `whylogs_upload` UDTF 

Now we have a `whylogs_object()` function available in our SQL queries. We need a second `whylabs_upload()` function for sending those results to WhyLabs.

Use the `./sql/whylabs_upload_udf.py` and make sure it's uploaded to your `funcs` stage.

```sql
-- You might have to execute this as a standalone statement in snowsql. PUT isn't upported in various drivers.
put file://./udfs/*.py @funcs/ auto_compress=false overwrite=true;
```

Create the `whylabs_upload()` function.

```sql
create or replace function whylabs_upload(profile_view varchar)
    returns table (profile_view varchar, result varchar)
    language python
    runtime_version = '3.10'
    external_access_integrations = (whylabs_integration)
    secrets = ('whylabs_api_key' = whylabs_api_key )
    packages = ('snowflake-snowpark-python', 'whylogs', 'whylabs-client')
    handler = 'whylabs_upload_udf.handler'
    imports = ('@funcs/whylabs_upload_udf.py')
    ;
```

## Run `whylogs_object` on a query to generate profiles

Now you'll be able to run the UDTF on a query. The recommended way of doing this is in a partition query because this let's whylogs leverage the vectorized Snowflake API and profile data in bulk, instead of row by row. The following example will return a table of serialied profiles that are ready to be uploaded.

```sql
select department, profile_view, total_processed 
from 
    (select id, name, department, age from demo_table)
    , 
    table(whylogs_object({ 'id': id, 'name':name, 'department':department, 'age' :age }) over (partition by department))
;
```

This will return a table that contains the base64 encoded whylogs profiles, ready to be uploaded to WhyLabs.

## Run `whylogs_upload` on the profile data

You can extend the query from the previous example to also upload the profiles after it finishes.

```sql
with profiles as (
    select department, profile_view
    from 
        (select id, name, department, age from demo_table limit 10000)
        , 
        table(whylogs_object({ 'id': id, 'name':name, 'department': department, 'age': age} ) over (partition by department))
)

select profiles.profile_view, result from 
    profiles
    ,
    table(whylabs_upload(profile_view) over (partition by department))  -- Partitioning here also determines concurrency
;
```

This will run the upload on every unique department in this example, which will have one profile each. How you partition will determine how many profilfes are put into a single partion and how much concurrency you have in the upload process.

## View profiles in WhyLabs

After this you should be able to view your uploaded profiles on WhyLabs by navigating to your model and going to the Profiles tab.


# Caveats

## Partition Size

A lot of the performance is determined by the way that your query partitions data. If you end up running out of memory in the UDTF then it probably means that the partition had too much data, so you have to make the partition more granular. If you don't have anything else that you can meaningfully partition on then you could do something like this as a workaround.

```sql
select department, profile_view
from 
    (select id, name, department, age, FLOOR(ABS(UNIFORM(0, 30, RANDOM()))) as rand from demo_table)
    , 
    table(whylogs_object( { 'id': id, 'name':name, 'department':department, 'age' :age } ) over (partition by department, rand))
;
```

This just assigns a random number to each row and uses that to create partitions, in addition to the department column to create additional even partitions.

## Manual Column Name Passing

We don't know of a great way to pass the column names without having to duplicate them in the query yet. If you know a better way then reach out. This is obviously not ideal for very wide tables and it probably means those queries need to be auto generated.