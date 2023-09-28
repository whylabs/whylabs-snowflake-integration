import whylogs as why  # type: ignore
from whylogs.core.view import DatasetProfileView  # type: ignore
from whylogs.core.segmentation_partition import ColumnMapperFunction, SegmentationPartition  # type: ignore
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from whylogs.core.schema import DatasetSchema

import time
import pandas as pd  # type: ignore
import _snowflake  # type: ignore
import base64
import pickle
from typing import List, Optional, Tuple, Any, Dict, Union


def get_segment_columns_config() -> Optional[List[str]]:
    try:
        csv = _snowflake.get_generic_secret_string("segment_columns")
        return csv.split(",")
    except Exception:
        return None


def get_freq_config() -> str:
    try:
        return _snowflake.get_generic_secret_string("data_grouper_freq")
    except Exception:
        return "D"


def timeit(fn) -> Tuple[Any, float]:
    start = time.perf_counter()
    result = fn()
    end = time.perf_counter()
    return result, end - start


def format_debug_info(perf_times: Dict[str, Union[int, str, float]]) -> str:
    return "\n".join([f"{k}: {round(v, 3) if type(v) == float else v}" for k, v in perf_times.items()])


date_col = "DATASET_TIMESTAMP"


class handler:
    def end_partition(self, df: pd.DataFrame):
        """
        Profile data with whylogs.

        The input to this UDF should be an object on the SQL side. The object needs to have a timestamp column as well.

        - DATASET_TIMESTAMP (int): The millisecond epoch time for that row. If this column is present then the udf will
            use it to group data into profiles. You can get this from a timestamp column in your query with
            `date_part(EPOCH_MILLISECONDS, dataset_timestamp)`.

        You can provide various configuration as secrets.

        - data_grouper_freq (str): The pandas grouper frequency to group data into profiles. Defaults to 'D' for daily.
            You should make this match the type of model you have in WhyLabs.
        - segment_columns (str): A comma separated list of columns to use to segment data into profiles. Segments aren't
            enabled by default.

        Args:
            df: A dataframe containing all of the data for profiling.
        """
        freq = get_freq_config()
        segment_columns = get_segment_columns_config()
        debug_info: Dict[str, Union[str, int, float]] = {}

        # TODO This used to make me get the first series with df[0] and then randomly changed to df['DATA']
        df_norm, debug_info["norm_time"] = timeit(lambda: pd.DataFrame(list(df["DATA"])))

        df_norm[date_col], debug_info["date_conversion_time"] = timeit(lambda: pd.to_datetime(df_norm[date_col], unit="ms"))
        grouped, debug_info["grouping_time"] = timeit(lambda: df_norm.set_index(date_col).groupby(pd.Grouper(freq=freq)))
        debug_info["group_count"] = len(grouped)

        if segment_columns is not None:
            segmentation_partition = SegmentationPartition(
                name=",".join(segment_columns), mapper=ColumnMapperFunction(col_names=segment_columns)
            )

            if df_norm[date_col].isna().values.any():
                # TODO document this in the readme
                raise ValueError("Segmentation columns cannot contain null timestamps. Filter null timestamps out of the query.")

            multi_column_segments = {segmentation_partition.name: segmentation_partition}
            dataset_schema = DatasetSchema(segments=multi_column_segments)

            for date_group, dataframe in grouped:
                if len(dataframe) == 0:
                    continue

                ms_epoch = date_group.timestamp() * 1000
                ms_epoch_datetime = pd.to_datetime(ms_epoch, unit="ms", utc=True).to_pydatetime()

                result_set = why.log(df_norm, schema=dataset_schema)
                result_set.set_dataset_timestamp(ms_epoch_datetime)

                views_list: List[SegmentedDatasetProfileView] = result_set.get_writables()
                for segmented_view in views_list:
                    base64_encoded_profile = base64.b64encode(segmented_view.profile_view.serialize()).decode("utf-8")
                    debug_info["segment_key"] = str(segmented_view.segment.key)
                    base64_encoded_segment = base64.b64encode(pickle.dumps(segmented_view.segment)).decode("utf-8")
                    yield pd.DataFrame(
                        {
                            "profile_view": [base64_encoded_profile],
                            "dataset_timestamp": [ms_epoch],
                            "segment_partition": [segmented_view.partition.name],
                            "segment": [base64_encoded_segment],
                            "rows_processed": [len(dataframe)],
                            "debug_info": [format_debug_info(debug_info)],
                        }
                    )

        else:
            for date_group, dataframe in grouped:
                if len(dataframe) == 0:
                    continue

                ms_epoch = date_group.timestamp() * 1000
                ms_epoch_datetime = pd.to_datetime(ms_epoch, unit="ms", utc=True).to_pydatetime()

                # Log the dataframe with whylogs and create a profile

                result_set, debug_info["profile_time"] = timeit(lambda: why.log(df_norm))
                result_set.set_dataset_timestamp(ms_epoch_datetime)

                view: DatasetProfileView = result_set.profile().view()
                base64_encoded_profile = base64.b64encode(view.serialize()).decode("utf-8")
                yield pd.DataFrame(
                    {
                        "profile_view": [base64_encoded_profile],
                        "dataset_timestamp": [ms_epoch],
                        "segment_partition": [None],
                        "segment": [None],
                        "rows_processed": [len(dataframe)],
                        "debug_info": [format_debug_info(debug_info)],
                    }
                )


handler.end_partition._sf_vectorized_input = pd.DataFrame  # type: ignore
