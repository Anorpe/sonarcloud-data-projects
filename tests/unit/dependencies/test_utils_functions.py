import pytest
from dependencies.utils_functions import list_to_string


class TestDependenciesUtilsFunctions:
    def test_list_to_string(self):
        assert list_to_string(["a","b","c"]) == "abc"



