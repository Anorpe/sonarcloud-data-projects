import pytest
from dependencies.google_cloud_functions import get_last_secret


class TestDependenciesGoogleCloudFunctions:
    def get_last_secret(self):
        assert str(["a","b","c"]) == "abc"
