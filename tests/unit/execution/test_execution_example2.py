import pytest
from execution.execution_example2 import function_example2


class TestExecutionExample:
    def test_function_example2(self):
        assert function_example2(5) == 25