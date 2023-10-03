import whylogs as why
import pandas as pd
from typing import List
from whylogs.core.segmentation_partition import ColumnMapperFunction, SegmentationPartition
from whylogs.core.schema import DatasetSchema
from whylogs.core.view.segmented_dataset_profile_view import SegmentedDatasetProfileView
from udfs.util import serialize_profile_view, deserialize_profile_view, serialize_segment, deserialize_segment


def test_profile_serialization():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})

    result_set = why.log(df)
    view = result_set.profile().view()

    serialized_view = serialize_profile_view(view)
    deserialized_view = deserialize_profile_view(serialized_view)

    assert view._columns.keys() == deserialized_view._columns.keys()


def test_segment_serialization():
    df = pd.DataFrame({"a": [1, 2, 3, 3], "b": ["a", "b", "c", "c"]})

    segment_columns = "b"
    segmentation_partition = SegmentationPartition(name=",".join(segment_columns), mapper=ColumnMapperFunction(col_names=segment_columns))
    multi_column_segments = {segmentation_partition.name: segmentation_partition}
    dataset_schema = DatasetSchema(segments=multi_column_segments)

    result_set = why.log(df, schema=dataset_schema)
    views_list: List[SegmentedDatasetProfileView] = result_set.get_writables()

    assert len(views_list) == 3

    for segmented_view in views_list:
        deserialized_view: SegmentedDatasetProfileView = deserialize_profile_view(serialize_profile_view(segmented_view))

        # TODO Shouldn't this be possible? Can you even recover a serialized SegmentedDatasetProfileView?
        # assert segmented_view.partition.name == deserialized_view.partition.name
        assert deserialized_view._columns.keys() == deserialized_view._columns.keys()

        serialized_segment = serialize_segment(segmented_view.segment)
        deserialized_segment = deserialize_segment(serialized_segment)

        assert deserialized_segment.key == segmented_view.segment.key
