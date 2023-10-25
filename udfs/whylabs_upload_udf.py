import traceback
from typing import Dict
from whylogs.api.writer.whylabs import WhyLabsWriter
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
import pandas as pd
import _snowflake
import multiprocessing

from .udf_utils import deserialize_profile_view, deserialize_segment
from .config import get_dataset_id_config


# Monkey patch the multiprocessing.cpu_count() function to return 1 because
# Snowfake security blocks it. It's used by the python swagger client to optimize requests.
def _patch_mutliprocess_cpu_count() -> int:
    return 1


multiprocessing.cpu_count = _patch_mutliprocess_cpu_count


class handler:
    def __init__(self) -> None:
        # self.writer: Optional[WhyLabsWriter] = None
        self.writers: Dict[str, WhyLabsWriter] = {}
        self.org_id = _snowflake.get_generic_secret_string("whylabs_org_id")
        self.api_key = _snowflake.get_generic_secret_string("whylabs_api_key")

    def _get_or_create_writer(self, dataset_id: str) -> WhyLabsWriter:
        if dataset_id not in self.writers:
            self.writers[dataset_id] = WhyLabsWriter(org_id=self.org_id, dataset_id=dataset_id, api_key=self.api_key)

        return self.writers[dataset_id]

    def end_partition(self, df):
        """
        Profile data with whylogs.

        The input to this UDF should be three columns. The columns in the object influence the results.

        - WHYLABS_DATASET_ID (str): Required. The dataset id for your WhyLabs account.
        - PROFILE_VIEW (str): Required. The serialized profile view to upload to WhyLabs.
        - SEGMENT_PARTITION (str): Required if the profiles are segmented. Can be null otherwise.
        - SEGMENT (str): Required if the profiles are segmented. Can be null otherwise.

        The output of the profiling whylogs UDF `whylogs()` will return the right columns for this UDF, so you can
        just pass the output of the profiling query to this to upload everything.

        You also need to provide various configuration as secrets.

        - whylabs_org_id (str): The organization id for your WhyLabs account.
        - whylabs_api_key (str): The api key for your WhyLabs account.

        Args:
            df: A dataframe containing all of the data for profiling.
        """
        df = pd.DataFrame(list(df["DATA"]))

        dataset_id_col = df["WHYLABS_DATASET_ID"]
        profile_views_col = df["PROFILE_VIEW"]
        try:
            segment_partitions_col = df["SEGMENT_PARTITION"]
            segments_col = df["SEGMENT"]
        except Exception:
            segment_partitions_col = None
            segments_col = None

        if segment_partitions_col is not None and segments_col is not None:
            # Upload segments
            for dataset_id, serialized_view, partition, segment in zip(
                dataset_id_col, profile_views_col, segment_partitions_col, segments_col
            ):
                writer = self._get_or_create_writer(dataset_id)

                try:
                    # Should never have errors here, but just in case
                    view = deserialize_profile_view(serialized_view)
                    segment = deserialize_segment(segment)
                    seg_view = SegmentedDatasetProfileView(
                        profile_view=view,
                        segment=segment,
                        partition=segment_on_column(column_name=partition)[partition],
                    )
                except Exception as e:
                    stack = traceback.format_exc()
                    yield pd.DataFrame(
                        {
                            "dataset_id": [dataset_id],
                            "result": [None],
                            "error": [f"Error uploading: {e} {stack}.\n {serialized_view}"],
                            "dataset_timestamp": -1,
                            "segment": ["Error"],
                        }
                    )

                try:
                    writer.write(seg_view)
                    ms_epoch = view.dataset_timestamp.timestamp() * 1000
                    yield pd.DataFrame(
                        {
                            "dataset_id": [dataset_id],
                            "result": ["Ok"],
                            "error": [None],
                            "dataset_timestamp": [ms_epoch],
                            "segment": [str(segment.key)],
                        }
                    )
                except Exception as e:
                    stack = traceback.format_exc()
                    yield pd.DataFrame(
                        {
                            "dataset_id": [dataset_id],
                            "result": [None],
                            "error": [f"Error uploading: {e} {stack}.\n {serialized_view}"],
                            "dataset_timestamp": [str(view.dataset_timestamp)],
                            "segment": [str(segment.key)],
                        }
                    )

        else:
            for dataset_id, serialized_view in zip(dataset_id_col, profile_views_col):
                writer = self._get_or_create_writer(dataset_id)
                try:
                    view = deserialize_profile_view(serialized_view)
                    writer.write(file=view)
                    ms_epoch = view.dataset_timestamp.timestamp() * 1000
                    yield pd.DataFrame(
                        {
                            "dataset_id": [dataset_id],
                            "result": ["Ok"],
                            "error": [None],
                            "dataset_timestamp": [ms_epoch],
                            "segment": [None],
                        }
                    )
                except Exception as e:
                    yield pd.DataFrame(
                        {
                            "dataset_id": [dataset_id],
                            "result": [None],
                            "error": [f"Error uploading: {e} {stack}.\n {serialized_view}"],
                            "dataset_timestamp": [str(view.dataset_timestamp)],
                            "segment": [None],
                        }
                    )


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
