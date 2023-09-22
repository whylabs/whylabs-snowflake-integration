This repo shows some examples for integrating whylogs into Snowflake.

# Custom UDTF

- UDTF Source: https://github.com/whylabs/snowflake-whylogs-example/blob/master/udfs/whylogs_udf.py
- UDTF Upload: https://github.com/whylabs/snowflake-whylogs-example/blob/master/sql/put-udf.sql#L1-L9
- UDTF Create: https://github.com/whylabs/snowflake-whylogs-example/blob/master/sql/create-udf.sql#L3-L10
- Usage: https://github.com/whylabs/snowflake-whylogs-example/blob/master/sql/create-udf.sql#L46

This is the simplest. You just create a python file that uses whylogs, create the UDF with the right column types, and pass them into the UDTF. You can decide on the output format as well. This example just shows the serialized whylogs profiles.

You can see those files for other variants of the UDTF that take different inputs.
