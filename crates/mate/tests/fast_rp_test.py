import pytest
import numpy as np
from graph_mate import DiGraph


def test_fast_rp(g: DiGraph):
    frp = g.fast_rp()
    assert frp.micros > 0

    embeddings = frp.embeddings()
    assert len(embeddings) == 1 << 8


def test_config_must_be_kwargs(g: DiGraph):
    with pytest.raises(TypeError):
        g.fast_rp(8, np.array([0., 1.], dtype=np.float32), 0., 0)


def test_out_dim(g: DiGraph):
    out_dim = 16
    frp = g.fast_rp(out_dim=out_dim)
    embeddings = frp.embeddings()
    assert embeddings.shape[1] == out_dim


def test_coefficients(g: DiGraph):
    pass


def test_normalization_strength(g: DiGraph):
    pass


def test_random_seed(g: DiGraph):
    pass
