import whylogs as why  # type: ignore
import pandas as pd  # type: ignore
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


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
