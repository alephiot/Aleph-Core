import pytest
import time

from aleph_core.utils.wait_one_step import WaitOneStep


def test_wait_one_step():
    wait_one_step = WaitOneStep(1)
    t = time.time()

    # First step should be skipped
    wait_one_step.wait()
    assert time.time() - t < 0.1

    # Second step should not be skipped
    wait_one_step.wait()
    assert 1 <= time.time() - t < 1.1


def test_wait_one_step_changing_time():
    wait_one_step = WaitOneStep(1)
    t = time.time()

    # First step should be skipped
    wait_one_step.wait()
    assert round(time.time() - t) == 0

    # Second step
    time.sleep(0.5)
    wait_one_step.wait()
    assert round(time.time() - t) == 1

    # Third step
    time.sleep(2)
    wait_one_step.wait()
    assert round(time.time() - t) == 3

    # Fourth step
    wait_one_step.wait()
    assert round(time.time() - t) == 4
