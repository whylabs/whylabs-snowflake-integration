from whylogs.core.dataset_profile import DatasetProfileView  # type: ignore
import traceback
from whylogs.api.writer.whylabs import WhyLabsWriter  # type: ignore
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
import pandas as pd  # type: ignore
import pickle
import _snowflake  # type: ignore
import base64
import multiprocessing


# Monkey patch the multiprocessing.cpu_count() function to return 1 because
# Snowfake security blocks it. It's used by the python swagger client to optimize requests.
def _patch_mutliprocess_cpu_count() -> int:
    return 1


multiprocessing.cpu_count = _patch_mutliprocess_cpu_count


class handler:
    def __init__(self) -> None:
        self.writer = WhyLabsWriter(
            org_id=_snowflake.get_generic_secret_string("whylabs_org_id"),
            dataset_id=_snowflake.get_generic_secret_string("whylabs_dataset_id"),
            api_key=_snowflake.get_generic_secret_string("whylabs_api_key"),
        )

    def end_partition(self, df):
        """
        Profile data with whylogs.

        The input to this UDF should be three columns (order matters). The columns in the object influence the results.

        - PROFILE_VIEW (str): Required. The serialized profile view to upload to WhyLabs.
        - SEGMENT_PARTITION (str): Required if the profiles are segmented. Can be null otherwise.
        - SEGMENT (str): Required if the profiles are segmented. Can be null otherwise.

        The output of the profiling whylogs UDF `whylogs()` will return the right columns for this UDF, so you can
        just pass the output of the profiling query to this to upload everything.

        You also need to provide various configuration as secrets.

        - whylabs_org_id (str): The organization id for your WhyLabs account.
        - whylabs_dataset_id (str): The dataset id for your WhyLabs account.
        - whylabs_api_key (str): The api key for your WhyLabs account.

        Args:
            df: A dataframe containing all of the data for profiling.
        """

        profile_views_col = df['PROFILE_VIEW']
        try:
            segment_partitions_col = df['SEGMENT_PARTITION']
            segments_col = df['SEGMENT']
        except Exception:
            segment_partitions_col = None
            segments_col = None

        if segment_partitions_col is not None and segments_col is not None:
            # Upload segments
            for serialized_view, partition, segment in zip(profile_views_col, segment_partitions_col, segments_col):
                try:
                    decoded_profile = base64.b64decode(serialized_view)
                    view = DatasetProfileView.deserialize(decoded_profile)

                    decoded_segment = base64.b64decode(segment)
                    segment = pickle.loads(decoded_segment)
                    seg_view = SegmentedDatasetProfileView(
                        profile_view=view,
                        segment=segment,
                        partition=segment_on_column(column_name=partition)[partition],
                    )
                    self.writer.write(seg_view)
                    yield pd.DataFrame({"upload_result": ["OK"]})
                except Exception as e:
                    stack = traceback.format_exc()
                    yield pd.DataFrame({"upload_result": [f"Error uploading: {e} {stack}.\n {serialized_view}"]})

        else:
            results = []
            for serialized_view in profile_views_col:
                try:
                    decoded_profile = base64.b64decode(serialized_view)
                    view = DatasetProfileView.deserialize(decoded_profile)
                    self.writer.write(file=view)
                    results.append("OK")
                    yield pd.DataFrame({"upload_result": ["OK"]})
                except Exception as e:
                    yield pd.DataFrame({"upload_result": [f"Error uploading: {e} {stack}.\n {serialized_view}"]})


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
