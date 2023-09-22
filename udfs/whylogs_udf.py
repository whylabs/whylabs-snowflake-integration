import whylogs as why  # type: ignore
import pandas as pd  # type: ignore
import base64
import random


class handler:
    def end_partition(self, df):
        id_col = df[0]
        name_col = df[1]
        department_col = df[2]
        age_col = df[3]
        cur_df = pd.DataFrame({"id": id_col, "name": name_col, "age": age_col, "department": department_col})
        profile_view = why.log(cur_df).profile().view()
        ser = profile_view.serialize()
        base64_encoded = base64.b64encode(ser).decode('utf-8')
        random_number = random.randint(0, 100)
        return pd.DataFrame({"profile_view": [base64_encoded], 'total_processed': [len(df)], 'random_number': [random_number]})


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
# handler.end_partition._sf_max_batch_size = 1_00

