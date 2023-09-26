import whylogs as why  # type: ignore
from whylogs.core.view import DatasetProfileView  # type: ignore
import pandas as pd  # type: ignore
import base64

# WARN if the return type is variant then snowflake has a lot of issues with json serialization. Can't even return an int
# in place of a variant.

class handler:
    def end_partition(self, df):
        """
        Args:
            df: When the sql type is OBJECT, the df has a single series with a bunch of dicts
        """
        # Convert the dataframe into a normally structured dataframe
        df_norm = pd.DataFrame(list(df[0]))
        df_norm['DATASET_TIMESTAMP'] = pd.to_datetime(df_norm['DATASET_TIMESTAMP'], unit='ms')
        grouped = df_norm.set_index('DATASET_TIMESTAMP').groupby(pd.Grouper(freq='D'))

        for date_group, dataframe in grouped:

            if len(dataframe) == 0:
                continue

            ms_epoch = (date_group.timestamp() * 1000)

            # Log the dataframe with whylogs and create a profile
            # TODO we have no way of setting the dataset timestamp in log() in the current version
            result_set = why.log(df_norm)
            ms_epoch_datetime = pd.to_datetime(ms_epoch, unit='ms', utc=True).to_pydatetime()
            result_set.set_dataset_timestamp(ms_epoch_datetime)

            view: DatasetProfileView = result_set.profile().view()
            base64_encoded = base64.b64encode(view.serialize()).decode('utf-8')
            yield pd.DataFrame({"profile_view": [base64_encoded], "dataset_timestamp": [ms_epoch]})


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
