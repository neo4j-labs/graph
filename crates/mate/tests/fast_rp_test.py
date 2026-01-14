import pytest
import numpy as np
from graph_mate import DiGraph


def test_fast_rp(g: DiGraph):
    frp = g.fast_rp()
    assert frp.micros > 0

    embeddings = frp.embeddings()
    assert len(embeddings) == g.node_count()


def test_config_must_be_kwargs(g: DiGraph):
    with pytest.raises(TypeError):
        g.fast_rp(8, np.array([0., 1.], dtype=np.float32), 0., 0)


def test_out_dim(g: DiGraph):
    out_dim = 16
    frp = g.fast_rp(out_dim=out_dim)
    embeddings = frp.embeddings()
    assert embeddings.shape[1] == out_dim


def test_coefficients(g: DiGraph):
    frp1 = g.fast_rp(coefficients=np.array([1., 0., 2.], dtype=np.float32), common_random_seed=0)
    frp2 = g.fast_rp(coefficients=np.array([2., 0., 4.], dtype=np.float32), common_random_seed=0)
    assert np.abs(2 * frp1.embeddings() - frp2.embeddings()).sum() < 1e-8 * g.node_count()


def test_normalization_strength(g: DiGraph):
    frp1_emb = g.fast_rp(common_random_seed=0, coefficients=np.array([1.], dtype=np.float32), normalization_strength=0.).embeddings()
    frp2_emb = g.fast_rp(common_random_seed=0, coefficients=np.array([1.], dtype=np.float32), normalization_strength=-1.).embeddings()
    for u in range(g.node_count()):
        x1 = frp1_emb[u] * (g.out_degree(u)**(-1.) if g.out_degree(u) > 0 else 1)
        x2 = frp2_emb[u]
        assert np.abs(x1 - x2).sum() < 1e-8


def test_common_random_seed(g: DiGraph):
    frp1 = g.fast_rp(common_random_seed=42)
    frp2 = g.fast_rp(common_random_seed=42)
    assert np.abs(frp1.embeddings() - frp2.embeddings()).sum() < 1e-8 * g.node_count()

    frp1 = g.fast_rp()
    frp2 = g.fast_rp()
    assert np.abs(frp1.embeddings() - frp2.embeddings()).sum() > 1e-1 * g.node_count()


def test_node_random_seed(g: DiGraph):
    frp1 = g.fast_rp(common_random_seed=0)
    frp2 = g.fast_rp(common_random_seed=0, node_random_seeds=np.arange(g.node_count(), dtype=np.int64))
    assert np.abs(frp1.embeddings() - frp2.embeddings()).sum() < 1e-8 * g.node_count()


def test_gds_consistent(g: DiGraph):
    frp1 = g.fast_rp(common_random_seed=0)
    frp2 = g.fast_rp(common_random_seed=0, gds_consistent=True)
    assert np.abs(frp1.embeddings() - frp2.embeddings()).sum() > 1e-1 * g.node_count()
