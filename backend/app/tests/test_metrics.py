from app.pipeline.metrics import compute_momentum


def test_momentum_growth():
    assert compute_momentum(15, 10) == 0.5


def test_momentum_decline():
    assert compute_momentum(6, 12) == -0.5


def test_momentum_prev_zero_guard():
    assert compute_momentum(3, 0) == 3.0
