import pytest
from execution.execution_example import function_example


class TestExecutionExample:
    def test_function_example(self):
        assert function_example(5) == 25