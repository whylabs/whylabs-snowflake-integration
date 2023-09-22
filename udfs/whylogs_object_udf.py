import whylogs as why  # type: ignore
import pandas as pd  # type: ignore
import base64
import random


class handler:
    def end_partition(self, df):
        """
        Args:
            df: Apparently, when the sql type is OBJECT, the df has a single series with a bunch of dicts
        """
        df_norm = pd.DataFrame(list(df[0]))
        profile_view = why.log(df_norm).profile().view()
        ser = profile_view.serialize()
        base64_encoded = base64.b64encode(ser).decode('utf-8')
        random_number = random.randint(0, 100)
        return pd.DataFrame({"profile_view": [base64_encoded], 'total_processed': [len(df)], 'random_number': [random_number]})


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
# handler.end_partition._sf_max_batch_size = 1_00  # Doesn't seem to work
