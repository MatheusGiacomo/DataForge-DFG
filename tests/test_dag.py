# tests/test_dag.py
"""Testes unitários do DAGResolver."""
import pytest
from dfg.dag import DAGResolver


class TestDAGResolver:
    def test_simple_linear_order(self):
        deps = {"b": ["a"], "a": []}
        order = DAGResolver(deps).get_execution_order()
        assert order.index("a") < order.index("b")

    def test_diamond_dependency(self):
        # a → b → d
        # a → c → d
        deps = {"d": ["b", "c"], "b": ["a"], "c": ["a"], "a": []}
        order = DAGResolver(deps).get_execution_order()
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_no_dependencies(self):
        deps = {"a": [], "b": [], "c": []}
        order = DAGResolver(deps).get_execution_order()
        assert set(order) == {"a", "b", "c"}

    def test_cycle_raises(self):
        deps = {"a": ["b"], "b": ["a"]}
        with pytest.raises(RuntimeError, match="circular"):
            DAGResolver(deps).get_execution_order()

    def test_empty_graph(self):
        assert DAGResolver({}).get_execution_order() == []
