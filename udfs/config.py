from typing import Optional, List
import pandas as pd


def get_segment_columns_config(df: pd.DataFrame) -> Optional[List[str]]:
    try:
        if "SEGMENT_COLUMNS" in df.columns:
            segment_columns = df["SEGMENT_COLUMNS"][0].split(",")
            return [col.upper() for col in segment_columns]
        else:
            return None
    except Exception:
        return None


def get_freq_config(df: pd.DataFrame) -> str:
    try:
        if "GROUP_BY_FREQUENCY" in df.columns:
            return df["GROUP_BY_FREQUENCY"][0]
        else:
            return "D"
    except Exception:
        return "D"


def get_dataset_id_config(df: pd.DataFrame) -> str:
    try:
        if "WHYLABS_DATASET_ID" in df.columns:
            return df["WHYLABS_DATASET_ID"][0]
        else:
            raise Exception("WHYLABS_DATASET_ID not found in input dataframe")
    except Exception as e:
        raise Exception(f"WHYLABS_DATASET_ID not found in input dataframe with cols: {df.columns}") from e
