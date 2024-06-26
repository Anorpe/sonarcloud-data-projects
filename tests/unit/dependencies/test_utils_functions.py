import pytest
from dependencies.utils_functions import *


class TestDependenciesUtilsFunctions:
    
    def test_list_to_string(self):
        assert list_to_string(["a","b","c"]) == "abc"
        assert list_to_string([1,2,3]) == "123"
        assert list_to_string([1,"b",3]) == "1b3"

    def test_dict_to_string_schema(self):
        assert dict_to_string_schema({"name":"STRING"}) == "name STRING"
        assert dict_to_string_schema({"name":"STRING","last_name":"STRING"}) == "name STRING,last_name STRING"



