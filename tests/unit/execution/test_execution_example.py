import pytest
from execution.execution_example import function_example


class TestExecutionExample:
    def function_example(self):
        assert str(['a','b','c']) == "['a','b','c']"