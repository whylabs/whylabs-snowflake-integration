import random
from functools import partial
from faker import Faker  # type: ignore
from typing import Any


def conditional(value_getter, override_condition, override_getter, chance=-1.1) -> Any:
    if override_condition and random.random() < chance:
        return override_getter()

    return value_getter()


fake = Faker()

# Number of fake records you wanna create
num_records = 16_000

# Your SQL table name
table_name = "employees"

# Initialize empty list for your SQL rows
sql_rows = []

job_list = ["Software Engineer", "Marketing Manager", "Nurse", "Data Analyst", "Electrician", "Journalist", "Chef", "Architect"]


start_epoch = 1690395957
now_ms_epoch = 1695752819


def generate_random_epoch_between(start: int, end: int) -> int:
    """Generate a random epoch between start and end"""
    return random.randint(start, end)


# Generate fake data
for _ in range(num_records):
    fake_name = fake.name().replace("'", "''")
    fake_role: str = random.choice(job_list)

    fake_state = fake.state_abbr(include_territories=False, include_freely_associated_states=False)
    hire_date_epoch = generate_random_epoch_between(start_epoch, now_ms_epoch)

    fake_salary = conditional(
        value_getter=partial(fake.random_int, min=40_000, max=120_000),
        override_condition=fake_state == "CA" or fake_role == "Software Engineer",
        override_getter=partial(fake.random_int, min=100_000, max=200_000),  # California branch makes more
        chance=0.5,
    )

    fake_email = fake.email().replace("'", "''")
    fake_phone = fake.phone_number().replace("'", "''")
    fake_dob = fake.date_of_birth().strftime("%Y-%m-%d")
    fake_ssn = fake.ssn()
    years_xp = conditional(
        value_getter=partial(fake.random_int, min=0, max=20),
        override_condition=fake_state == "NY",
        override_getter=partial(fake.random_int, min=0, max=8),  # Lots of turnover in the NY office
        chance=0.1,
    )

    row_item_list = [
        f"'{fake_name}'",
        str(fake_salary),
        f"'{fake_role}'",
        f"'{fake_state}'",
        f"'{fake_email}'",
        f"'{fake_phone}'",
        f"'{fake_dob}'",
        f"'{fake_ssn}'",
        f"TO_TIMESTAMP_NTZ({hire_date_epoch})",
        str(years_xp),
    ]

    row = ",".join(row_item_list)

    sql_rows.append(f"({row})\n")

# Create a single SQL Insert Statement for bulk insert
statements = [
    "use whylogs_demo;",
    "use warehouse whylogs_warehouse;",
    "INSERT INTO employees (name, salary, job, state, email, phone, dob, ssn, hire_date, years_xp) VALUES",
    f"{', '.join(sql_rows)};",
]

print("\n".join(statements))
