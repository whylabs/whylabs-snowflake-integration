import whylogs as why
from whylogs.core.dataset_profile import DatasetProfileView
import base64


# Read ./profile.base64
profile_base64 = open("./profile.base64", "r").read()

decoded_profile = base64.b64decode(profile_base64)


view = DatasetProfileView.deserialize(decoded_profile)

print(view.to_pandas())