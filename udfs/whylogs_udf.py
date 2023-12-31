import whylogs as why
from whylogs.core.view import DatasetProfileView
from whylogs.core.segmentation_partition import ColumnMapperFunction, SegmentationPartition
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from whylogs.core.schema import DatasetSchema

import pandas as pd
from typing import List, Dict, Union

from .config import get_freq_config, get_segment_columns_config
from .udf_utils import (
    attach_metadata,
    serialize_profile_view,
    serialize_segment,
    timeit,
    format_debug_info,
    drop_metadata_columns,
)


date_col = "DATASET_TIMESTAMP"


class handler:
    def end_partition(self, df: pd.DataFrame):
        """
        Profile data with whylogs.

        The input to this UDF should be an object on the SQL side. The object should have certain additional properties
        that influence the results.

        - DATASET_TIMESTAMP (int): The millisecond epoch time for that row. If this column is present then the udf will
            use it to group data into profiles. You can get this from a timestamp column in your query with
            `date_part(EPOCH_MILLISECONDS, dataset_timestamp)`.
        - SEGMENT_COLUMNS (string): A comma separated list of columns to use to segment data. For example, `state,city`.
        - GROUP_BY_FREQUENCY (string): The pandas grouper frequency to group data into profiles. Defaults to 'D' for
            daily. You should make this match the type of model you have in WhyLabs.

        Args:
            df: A dataframe containing all of the data for profiling.
        """
        debug_info: Dict[str, Union[str, int, float]] = {}

        # TODO This used to get the first series with df[0] and then randomly changed to df['DATA']
        df, debug_info["norm_time"] = timeit(lambda: pd.DataFrame(list(df["DATA"])))

        df[date_col], debug_info["date_conversion_time"] = timeit(lambda: pd.to_datetime(df[date_col], unit="ms"))

        freq = get_freq_config(df)
        segment_columns = get_segment_columns_config(df)

        grouped, debug_info["grouping_time"] = timeit(lambda: df.set_index(date_col).groupby(pd.Grouper(freq=freq)))
        debug_info["group_count"] = len(grouped)

        if segment_columns is not None:
            segmentation_partition = SegmentationPartition(
                name=",".join(segment_columns), mapper=ColumnMapperFunction(col_names=segment_columns)
            )

            if df[date_col].isna().values.any():
                # TODO document this in the readme
                raise ValueError("Segmentation columns cannot contain null timestamps. Filter null timestamps out of the query.")

            multi_column_segments = {segmentation_partition.name: segmentation_partition}
            dataset_schema = DatasetSchema(segments=multi_column_segments)

            for date_group, dataframe in grouped:
                if len(dataframe) == 0:
                    continue

                ms_epoch = date_group.timestamp() * 1000
                ms_epoch_datetime = pd.to_datetime(ms_epoch, unit="ms", utc=True).to_pydatetime()

                # Remove the metadata columns from the dataframe
                dataframe = drop_metadata_columns(dataframe)

                result_set, debug_info["profile_time"] = timeit(lambda: why.log(dataframe, schema=dataset_schema))
                result_set.set_dataset_timestamp(ms_epoch_datetime)

                views_list: List[SegmentedDatasetProfileView] = result_set.get_writables()
                for segmented_view in views_list:
                    attach_metadata(segmented_view._profile_view)
                    base64_encoded_profile = serialize_profile_view(segmented_view)
                    debug_info["segment_key"] = str(segmented_view.segment.key)
                    base64_encoded_segment = serialize_segment(segmented_view.segment)
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

                dataframe = drop_metadata_columns(dataframe)
                # Log the dataframe with whylogs and create a profile

                result_set, debug_info["profile_time"] = timeit(lambda: why.log(dataframe))
                result_set.set_dataset_timestamp(ms_epoch_datetime)

                view: DatasetProfileView = result_set.profile().view()
                attach_metadata(view)
                base64_encoded_profile = serialize_profile_view(view)
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
