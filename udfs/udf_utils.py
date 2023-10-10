from typing import Tuple, Dict, Union, Callable, TypeVar
from whylogs.core.view import DatasetProfileView
from whylogs.core.view.segmented_dataset_profile_view import Segment, SegmentedDatasetProfileView
import time
import base64
import pickle
from .version import get_version


T = TypeVar("T")


def attach_metadata(profile: DatasetProfileView):
    profile._metadata = profile._metadata or {}
    profile._metadata["integration"] = "snowflake"
    profile._metadata["integration_version"] = get_version()


def timeit(fn: Callable[[], T]) -> Tuple[T, float]:
    start = time.perf_counter()
    result = fn()
    end = time.perf_counter()
    return result, end - start


def format_debug_info(perf_times: Dict[str, Union[int, str, float]]) -> str:
    return "\n".join([f"{k}: {round(v, 3) if type(v) == float else v}" for k, v in perf_times.items()])


def serialize_profile_view(profile_view: Union[DatasetProfileView, SegmentedDatasetProfileView]) -> str:
    if isinstance(profile_view, SegmentedDatasetProfileView):
        view = profile_view.profile_view
    else:
        view = profile_view

    return base64.b64encode(view.serialize()).decode("utf-8")


def deserialize_profile_view(encoded_profile_view: str) -> Union[DatasetProfileView, SegmentedDatasetProfileView]:
    decoded_profile = base64.b64decode(encoded_profile_view)
    return DatasetProfileView.deserialize(decoded_profile)


def serialize_segment(segment: Segment) -> str:
    return base64.b64encode(pickle.dumps(segment)).decode("utf-8")


def deserialize_segment(encoded_segment: str) -> Segment:
    decoded_segment = base64.b64decode(encoded_segment)
    return pickle.loads(decoded_segment)
