import time


def loop() -> None:
    """
    Run an infinite loop
    """
    while True:
        time.sleep(0.1)


def current_timestamp() -> int:
    """
    Returns the current time in milliseconds
    """
    return int(time.time() * 1000)
