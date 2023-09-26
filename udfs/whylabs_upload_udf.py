from whylogs.core.dataset_profile import DatasetProfileView  # type: ignore
from whylogs.api.writer.whylabs import WhyLabsWriter  # type: ignore
import pandas as pd  # type: ignore
import os
import _snowflake  # type: ignore
import base64
import multiprocessing


# Monkey patch the multiprocessing.cpu_count() function to return 1 because
# Snowfake security blocks it
def _patch_mutliprocess_cpu_count():
    return 1


multiprocessing.cpu_count = _patch_mutliprocess_cpu_count


class handler:
    def end_partition(self, df):
        """
        Args:
            df: A dataframe tht contains the serialized profiles to be uploaded, generated from
                the whylabs_*_udf.py functions.
        """

        token = _snowflake.get_generic_secret_string('whylabs_api_key')
        os.environ["WHYLABS_DEFAULT_ORG_ID"] = 'org-JpsdM6'
        os.environ["WHYLABS_DEFAULT_DATASET_ID"] = 'model-71'
        os.environ["WHYLABS_API_KEY"] = token

        writer = WhyLabsWriter()
        profile_views_col = df[0]

        results = []
        for serialized_view in profile_views_col:
            try:
                decoded_profile = base64.b64decode(serialized_view)
                view = DatasetProfileView.deserialize(decoded_profile)
                writer.write(file=view)
                results.append("OK")
            except Exception as e:
                results.append(str(e))

        return pd.DataFrame({"profile_view": profile_views_col, "results": results})


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
