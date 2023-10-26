from whylogs.core.dataset_profile import DatasetProfileView
import base64
import sys

filepath = sys.argv[1]

# Read ./profile.base64
profile = open(filepath, "rb").read()
view = DatasetProfileView.deserialize(profile)

print(view.to_pandas())
print(view._metadata)
