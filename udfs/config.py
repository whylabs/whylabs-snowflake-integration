from typing import Optional, List
import _snowflake


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
