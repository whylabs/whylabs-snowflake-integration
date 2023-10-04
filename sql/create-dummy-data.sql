use role sysadmin;

create or replace database whylogs_demo;

use whylogs_demo;

create warehouse if not exists whylogs_warehouse
    warehouse_size = 'XSMALL'
    warehouse_type = 'STANDARD'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;

use warehouse whylogs_warehouse;

create or replace function FAKE(locale varchar,provider varchar,parameters variant)
returns variant
language python
volatile
runtime_version = '3.8'
packages = ('faker','simplejson')
handler = 'fake'
as
$$
import simplejson as json
from faker import Faker
def fake(locale,provider,parameters):
  if type(parameters).__name__=='sqlNullWrapper':
    parameters = {}
  fake = Faker(locale=locale)
  return json.loads(json.dumps(fake.format(formatter=provider,**parameters), default=str))
$$;

drop table if exists employees;

create table if not exists employees (
    name STRING,
    salary INT,
    job STRING,
    state STRING,
    email STRING,
    phone STRING,
    dob STRING,
    ssn STRING,
    hire_date TIMESTAMP_NTZ,
    years_xp INT
);

-- Will take about a minute
INSERT INTO employees (name, salary, job, state, email, phone, dob, ssn, hire_date, years_xp)
SELECT FAKE_NAME, FAKE_SALARY, FAKE_JOB, FAKE_STATE, FAKE_EMAIL, FAKE_PHONE, FAKE_DOB, FAKE_SSN, FAKE_HIRE_DATE, FAKE_YEARS_XP
FROM (
        select 
            FAKE('en_US','name',null)::varchar as FAKE_NAME,
            FAKE('en_US','pyint', {'min_value': 40000, 'max_value': 400000})::varchar as FAKE_SALARY,
            FAKE('en_US','job',null)::varchar as FAKE_JOB,
            FAKE('en_US','state_abbr', {'include_territories': 'False', 'include_freely_associated_states': 'False'})::varchar as FAKE_STATE,
            FAKE('en_US','email',null)::varchar as FAKE_EMAIL,
            FAKE('en_US','phone_number',null)::varchar as FAKE_PHONE,
            FAKE('en_US','date_of_birth',null)::varchar as FAKE_DOB,
            FAKE('en_US','ssn',null)::varchar as FAKE_SSN,
            FAKE('en_US','date_time_between', {'start_date': '-90d'})::varchar as FAKE_HIRE_DATE,
            FAKE('en_US','pyint', {'min_value': 0, 'max_value': 20})::varchar as FAKE_YEARS_XP
        from 
            table(generator(rowcount => 1000))
)
;

-- Count at 0
select count(1) from employees;

-- Duplicate a bunch of times
CREATE PROCEDURE break_out_of_loop()
RETURNS INTEGER
LANGUAGE SQL
AS
$$
    DECLARE
        counter INTEGER;
    BEGIN
        counter := 0;
        LOOP
            counter := counter + 1;
            IF (counter > 15) THEN
                BREAK;
            END IF;

            INSERT INTO employees (name, salary, job, state, email, phone, dob, ssn, hire_date, years_xp)
            SELECT name, salary, job, state, email, phone, dob, ssn, hire_date, years_xp FROM employees;

        END LOOP;
        RETURN counter;
    END;
$$
;
CALL break_out_of_loop();

select count(1) from employees;