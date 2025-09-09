import pytest

import gamecock.rate_limiter as rl


class FakeTime:
    def __init__(self, start=0.0):
        self.now = start
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        # record and advance logical time instead of real sleeping
        self.sleeps.append(seconds)
        # guard against negative or zero
        if seconds > 0:
            self.now += seconds


@pytest.fixture()
def fake_time(monkeypatch):
    fake = FakeTime(start=1000.0)
    # Patch the entire time module used inside rate_limiter
    monkeypatch.setattr(rl, 'time', fake, raising=True)
    return fake


def test_initial_tokens_do_not_block(fake_time):
    limiter = rl.RateLimiter(max_requests=2, time_window=1.0)

    # First two acquires should not sleep
    limiter.acquire()
    limiter.acquire()
    assert sum(fake_time.sleeps) == 0

    # Third acquire should sleep until half the window (0.5s) to refill 1 token
    limiter.acquire()
    assert pytest.approx(sum(fake_time.sleeps), rel=1e-6) == 0.5


def test_replenish_full_window_allows_burst_again(fake_time):
    limiter = rl.RateLimiter(max_requests=3, time_window=1.2)

    # Consume all tokens
    for _ in range(3):
        limiter.acquire()
    assert sum(fake_time.sleeps) == 0

    # Advance time a full window without calling sleep directly
    fake_time.now += 1.2

    # Should be able to acquire 3 more without sleeping
    for _ in range(3):
        limiter.acquire()
    assert sum(fake_time.sleeps) == 0


def test_partial_refill_causes_fractional_sleep(fake_time):
    limiter = rl.RateLimiter(max_requests=4, time_window=2.0)  # rate = 2 tokens/sec

    # Use up all 4 tokens quickly (no sleep expected)
    for _ in range(4):
        limiter.acquire()
    assert sum(fake_time.sleeps) == 0

    # Advance 0.25 seconds -> should regenerate 0.5 tokens
    fake_time.now += 0.25

    # Next acquire needs 0.5 more tokens. Sleep needed should be 0.25 seconds
    limiter.acquire()
    assert pytest.approx(sum(fake_time.sleeps), rel=1e-6) == 0.25


def test_does_not_exceed_max_tokens_when_waiting(fake_time):
    limiter = rl.RateLimiter(max_requests=2, time_window=1.0)

    # Burn both tokens
    limiter.acquire()
    limiter.acquire()
    assert sum(fake_time.sleeps) == 0

    # Advance more than a window and then acquire once; should not have to sleep
    fake_time.now += 5.0
    limiter.acquire()
    assert sum(fake_time.sleeps) == 0


def test_multiple_wait_cycles(fake_time):
    limiter = rl.RateLimiter(max_requests=1, time_window=0.6)
    # 1 token/sec ~ actually 1 per 0.6s

    # Use first token, no sleep
    limiter.acquire()
    assert sum(fake_time.sleeps) == 0

    # Next two acquires should each require sleeping 0.6s to regenerate a token
    limiter.acquire()
    limiter.acquire()
    assert pytest.approx(sum(fake_time.sleeps), rel=1e-6) == 1.2
