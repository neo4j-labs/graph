from unladen_swallow import Graph

import pytest


def test_page_rank(g: Graph):
    pr = g.page_rank()

    assert len(pr) == 1 << 8
    for score in pr:
        assert score > 0.0


def test_pr_max_iterations(g: Graph):
    pr = g.page_rank(max_iterations=1)
    assert pr.ran_iterations == 1


def test_pr_tolerance(g: Graph):
    pr = g.page_rank(tolerance=1)
    assert pr.ran_iterations == 1


def test_pr_damping_factor(g: Graph):
    pr = g.page_rank(damping_factor=0)
    assert pr.ran_iterations == 1
    for score in pr:
        assert score == 1 / (1 << 8)


def test_config_must_be_kwargs(g: Graph):
    with pytest.raises(TypeError):
        g.page_rank(42, 1.0, 0.1)