import whylogs as why  # type: ignore
import pandas as pd  # type: ignore
import base64
import random


class handler:
    def end_partition(self, df):
        batch_size = 10_000_000
        for i in range(0, len(df), batch_size):
            chunk = df.iloc[i:i+batch_size]
            id_col = chunk[0]
            name_col = chunk[1]
            department_col = chunk[2]
            age_col = chunk[3]
            cur_df = pd.DataFrame({"id": id_col, "name": name_col, "age": age_col, "department": department_col})
            profile_view = why.log(cur_df).profile().view()
            ser = profile_view.serialize()
            base64_encoded = base64.b64encode(ser).decode('utf-8')
            random_number = random.randint(0, 100)
            yield pd.DataFrame({"profile_view": [base64_encoded], 'total_processed': [len(chunk)], 'random_number': [random_number]})


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
handler.end_partition._sf_max_batch_size = 1_000_000  # type: ignore

