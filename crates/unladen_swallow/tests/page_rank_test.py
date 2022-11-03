import pytest

from graph_mate import DiGraph


def test_page_rank(g: DiGraph):
    pr = g.page_rank()

    assert pr.ran_iterations >= 1
    assert pr.error < 1.0
    assert pr.micros > 0

    scores = pr.scores()
    assert len(scores) == 1 << 8
    for score in scores:
        assert score > 0.0


def test_pr_max_iterations(g: DiGraph):
    pr = g.page_rank(max_iterations=1)
    assert pr.ran_iterations == 1


def test_pr_tolerance(g: DiGraph):
    pr = g.page_rank(tolerance=1)
    assert pr.ran_iterations == 1


def test_pr_damping_factor(g: DiGraph):
    pr = g.page_rank(damping_factor=0)
    assert pr.ran_iterations == 1
    for score in pr.scores():
        assert score == 1 / (1 << 8)


def test_config_must_be_kwargs(g: DiGraph):
    with pytest.raises(TypeError):
        g.page_rank(42, 1.0, 0.1)
