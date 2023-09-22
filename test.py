import pandas as pd
import base64
import random
import whylogs as why


df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"], "age": [10, 20, 30, 40, 50], "department": ["Accounting", "Research", "Sales", "Operations", "Marketing"]})

batch_size = 1
for i in range(0, len(df), batch_size):
    chunk = df.iloc[i:i+batch_size]
    print(chunk)
    id_col = chunk[0]
    name_col = chunk[1]
    department_col = chunk[2]
    age_col = chunk[3]
    chunk = pd.DataFrame({"id": id_col, "name": name_col, "age": age_col, "department": department_col})
    profile_view = why.log(df).profile().view()
    ser = profile_view.serialize()
    base64_encoded = base64.b64encode(ser).decode('utf-8')
    random_number = random.randint(0, 100)
    print(pd.DataFrame({"profile_view": [base64_encoded], 'total_processed': [len(df)], 'random_number': [random_number]}))