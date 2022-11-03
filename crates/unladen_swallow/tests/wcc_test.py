import pytest

from graph_mate import DiGraph


def test_wcc(g: DiGraph):
    wcc = g.wcc()

    assert wcc.micros > 0

    components = wcc.components()

    assert len(components) == 1 << 8
    for component in components:
        assert component >= 0
        assert component < g.node_count()


def test_config_must_be_kwargs(g: DiGraph):
    with pytest.raises(TypeError):
        g.wcc(42, 1.0, 0.1)
